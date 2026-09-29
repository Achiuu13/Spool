# Guarantees

What Spool promises in its current single-node form, and what it does not.

## Durability

**Durable mode (default, `-sync=true`).** A publish is acknowledged only
after the record's frame has been written and `fsync`ed to the partition
log. An acknowledged record survives process crashes (`kill -9`) and clean
restarts, keeping its offset and content. This is verified by
`tests/e2e_test.go` and `benchmarks/crash_test.sh`.

**Buffered mode (`-sync=false`).** A publish is acknowledged after the
write enters the OS page cache, without fsync. This is a separate, weaker
guarantee: a *process* crash does not lose acknowledged records (the page
cache survives), but an OS crash or power loss can lose the unsynced tail.
Use it only where the ~200x throughput gain is worth that risk (see
`benchmarks/results.md`).

All results are process-crash results. They do not prove behavior under
storage failure, filesystem bugs, or power loss.

## Ordering and offsets

- Offsets in a partition start at 0 and increase by exactly 1 per
  acknowledged record. They are never reused or reassigned.
- Reads return records in offset order. Reading does not remove records.
- Concurrent producers never receive duplicate offsets, and readers never
  observe a partially written record (frames are checksummed and appends
  are serialized).
- Ordering is **per partition**; no cross-partition order is promised.
  Records with the same key always route to the same partition (FNV-1a mod
  partition count), so per-key order holds. Keyless records are distributed
  round-robin with no ordering relationship to each other.

## Recovery

- An incomplete or torn trailing write is truncated at startup; all earlier
  acknowledged records remain readable at their original offsets.
- Corruption *before* the end of a log is reported as an explicit error at
  startup. Spool refuses to serve a silently-holed log.

## Consumer progress and delivery semantics

- Committed offsets are stored atomically (write-temp, fsync, rename). If
  Spool stops partway through a commit, the previous committed offset
  survives; a commit is never half-applied.
- Delivery is **at-least-once**: a consumer that crashes after processing
  but before committing will receive that record again. Consumers must be
  idempotent or otherwise tolerate duplicates.
- Within a group, each partition is assigned to at most one active member
  at a time (range assignment). Members that stop heartbeating past the
  session timeout are expired and their partitions reassigned.
- Commits that carry a `member_id` are fenced: a member that expired or
  lost a partition gets `404`/`409` instead of overwriting the new owner's
  progress. Commits without `member_id` bypass fencing and are unfenced by
  design (single-writer scripts).
- Group membership is in-memory: a broker restart clears it and consumers
  rejoin. Committed offsets persist across restarts.

## Limits and validation

| Limit | Value | On violation |
|---|---|---|
| Record size (key + value) | 1 MiB | `413 Request Entity Too Large` |
| Publish request body | 2 MiB | `400 Bad Request` |
| Stream / group name | `[a-zA-Z0-9][a-zA-Z0-9._-]{0,199}` | `400 Bad Request` |
| Read `limit` | 1–1000 (default 100) | `400 Bad Request` |
| Read `offset` | ≥ 0 | `400 Bad Request` |

## Error responses

All errors are JSON: `{"error": "<message>"}`.

| Status | Meaning |
|---|---|
| 400 | Malformed body, invalid name, bad query parameter |
| 404 | Unknown stream or partition |
| 413 | Record exceeds 1 MiB |
| 500 | Internal storage error |
| 307 | Cluster mode: this node is a follower; retry at `Location` (the leader) |
| 503 | Cluster mode: no leader elected, or the proposal could not reach a majority — retry |

Server timeouts: 10 s read, 30 s write, 60 s idle.

## Cluster mode

With `-peers`, the guarantees strengthen and shift (see
[replication.md](replication.md) for the design):

- A publish is acknowledged only after the record is durable (fsynced) on
  a **majority** of the three nodes. Acknowledged records survive the
  permanent loss of any single node, including the leader.
- Committed consumer offsets are replicated the same way and survive
  failover.
- With a majority down, Spool refuses writes (`503`) rather than
  acknowledge records it could lose.
- A publish that got no response (crash mid-request) may or may not have
  committed; producers must retry, which can duplicate records —
  at-least-once, as in standalone mode.
- Group membership lives on the leader only; after failover, consumers
  get `404` on heartbeat and re-join, exactly as they do after session
  expiry.

## Non-goals (this stage)

- Standalone mode is single-node: a lost disk loses data. The majority-ack
  claim above applies to cluster mode only.
- No exactly-once delivery.
- No retention or compaction: logs grow without bound (including the
  cluster-mode operation log).
- No authentication or TLS.
- Cluster membership is static (the `-peers` list); no dynamic
  reconfiguration.
