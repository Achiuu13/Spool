# Replication design

Spool replicates data across three nodes so that a published record, once
acknowledged, survives the loss of any single node. The design is a
Raft-style replicated operation log: every state change (publish, offset
commit) is an entry in a single ordered log that a leader replicates to
followers. Broker state — partition logs and committed offsets — is derived
deterministically by applying that log in order.

This document answers the design questions before the code: who accepts
writes, how a leader is chosen, when a write is acknowledged, how a lagging
node catches up, how a stale leader is fenced, and which records survive a
failover.

## Which node accepts writes

Only the current leader. Producers and offset commits sent to a follower are
answered with `307 Temporary Redirect` and a `Location` header pointing at
the leader's advertised address, so clients converge on the leader without
configuration. Reads of replicated state are also served by the leader by
default; followers redirect them the same way, which keeps the read-your-
writes story simple (a client that follows redirects always observes
committed state).

There is exactly one leader per term. Two nodes can *believe* they are
leader momentarily (a partitioned stale leader), but only one of them can
commit — see fencing below.

## How the leader is chosen

Election is term-based voting, as in Raft:

- Every node persists `currentTerm` and `votedFor` (fsynced before any
  reply, so a restart cannot double-vote in the same term).
- Nodes start as followers. If a follower hears nothing from a leader for a
  randomized election timeout (150–300ms scale, configurable), it increments
  the term, votes for itself, and requests votes from its peers.
- A peer grants its vote if it has not voted in that term and the
  candidate's log is at least as up-to-date as its own (compared by last
  entry's term, then length). This restriction guarantees the winner already
  holds every committed entry.
- A candidate that collects a majority (2 of 3) becomes leader and
  immediately replicates a no-op entry in its new term. Committing that
  no-op is what allows the new leader to conclude that all earlier entries
  are committed too.

Randomized timeouts make split votes rare; a split vote just retries with a
new term.

## When a write is acknowledged

A publish is acknowledged to the producer only after the entry is:

1. appended and fsynced on the leader's log, and
2. replicated to and fsynced by a majority of the cluster (leader plus at
   least one follower in a 3-node cluster), and
3. marked committed by the leader and applied to the leader's derived state
   (so the response can carry the assigned partition and offset).

A leader only counts replication of entries *from its own term* toward
commitment; earlier-term entries become committed indirectly when a
current-term entry (the no-op at election, or any later write) commits.
This is Raft's rule for avoiding the committed-entry-overwritten anomaly.

Majority ack means the cluster tolerates one node down: writes still commit
with 2 of 3. With two nodes down the cluster refuses writes rather than
risk acknowledging data that could be lost — availability is sacrificed,
never a committed record.

## How a lagging or restarted node catches up

The leader tracks a `nextIndex` per follower and sends AppendEntries
batches containing everything the follower is missing, each prefixed with
the index and term of the entry immediately before the batch. The follower
accepts only if its log matches that prefix:

- If the prefix matches, it appends the entries (fsync) and advances.
- If not, it rejects; the leader decrements `nextIndex` and retries further
  back until they agree, then streams forward from there.

A node that was down for a while simply rejoins as a follower, fails the
consistency check at the leader's first guess, and the walk-back finds the
divergence point. Catch-up is ordinary AppendEntries traffic — there is no
separate snapshot path at Spool's scale; the operation log is the full
history and replay rebuilds derived state on restart.

If the follower has *conflicting* entries (uncommitted writes from a
deposed leader), it truncates its log from the first conflicting index
before appending the leader's entries. Truncation only ever removes
uncommitted entries: committed entries are on a majority and the log-
completeness vote rule prevents any leader that lacks them from being
elected.

## How a stale leader is fenced

Terms fence everything:

- Every RPC carries the sender's term. A node that sees a higher term
  steps down to follower immediately and adopts it.
- A stale leader (isolated by a partition, deposed by a new election)
  cannot commit: it needs a majority ack, and a majority of nodes have
  moved to the new term and reject its lower-term AppendEntries.
- Producers talking to the stale leader get errors or timeouts, never false
  acks; when the partition heals, the stale leader hears the higher term,
  steps down, truncates its unreplicated tail, and catches up as a
  follower.

Consumer offset commits go through the same replicated log, so offset state
cannot fork either: a commit acknowledged by the old leader was on a
majority and survives; one that wasn't acknowledged may be lost, and the
consumer re-reads — which is exactly the at-least-once contract Spool
already documents.

## Which records survive a failover

Every record whose publish was acknowledged. The chain:

1. Ack implies the entry was fsynced on a majority (2 of 3 nodes).
2. Electing a leader requires a majority of votes.
3. Any two majorities intersect, and voters refuse candidates whose log is
   less up-to-date than their own — so the new leader's log contains every
   acknowledged entry.
4. The new leader never truncates its own log; followers truncate only to
   match the leader.

Records published but *not yet acknowledged* at the moment of failover may
or may not survive; producers must treat a missing ack as "retry", which
can produce duplicates downstream — consistent with at-least-once delivery.

## What is replicated, exactly

One operation log per cluster (`data/raft/oplog`), reusing the stage-1
`internal/log` format (CRC-framed, fsync-per-append, torn-tail recovery).
Each entry's value is `[8B term][op bytes]`; ops are JSON:

- `publish {stream, key, value, partition}` — the leader picks the
  partition at propose time (hash or round-robin), so replay is
  deterministic on every node regardless of interleaving.
- `commit_offset {group, stream, partition, next}` — offset commits, after
  the leader validates fencing (membership stays node-local and
  non-replicated: consumers re-join after failover, which is the same
  rejoin path they already handle for session expiry).

Derived state (partition logs under `data/state/`, offsets store) is
rebuilt by replaying the operation log from the last applied index at
startup. Apply is idempotent because entries are applied exactly once in
log order on each node.

## Modes

- **Standalone** (no `-peers` flag): the replication layer is bypassed
  entirely and Spool behaves exactly as in stages 1–4. All existing
  guarantees and benchmarks stand.
- **Cluster** (`-node-id`, `-peers`, `-advertise`): three nodes, one
  elected leader, majority-ack writes, follower redirects.
