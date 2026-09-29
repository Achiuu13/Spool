# Architecture

Spool's core path is **API → broker → partition log**, with consumer
progress stored separately.

```
producer ──POST /streams/{s}/records──▶ ┌─────────┐
                                        │   api   │  HTTP handlers, validation
consumer ──GET  .../records?offset=N──▶ └────┬────┘
                                             │
                                        ┌────▼────┐
                                        │ broker  │  stream lookup, name checks
                                        └──┬───┬──┘
                                           │   │
                              ┌────────────▼┐ ┌▼──────────────┐
                              │ log (per    │ │ offsets store │
                              │ partition)  │ │ offsets.json  │
                              └─────────────┘ └───────────────┘
```

## Packages

| Package | Responsibility |
|---|---|
| `internal/log` | On-disk record format, append, reads, startup recovery. No HTTP knowledge; fully testable on its own. |
| `internal/offsets` | Committed next-offset per (group, stream, partition), persisted atomically. |
| `internal/groups` | Consumer-group membership: joins, heartbeats, session expiry, range partition assignment, commit fencing. |
| `internal/broker` | Maps stream names to partition logs, routes records to partitions, validates names, creates streams on first publish. |
| `internal/api` | HTTP routing, JSON encoding, request validation, error mapping. |

## On-disk layout

```
data/
  streams/<stream>/meta.json   partition count, fixed at stream creation
  streams/<stream>/<p>.log     one append-only log per partition
  offsets.json                 committed consumer progress
```

## Record format

Length-prefixed binary frames, all integers big-endian:

```
[4B body length][4B CRC-32C of body][body]
body = [8B offset][8B unix-nano timestamp][4B key len][key][4B value len][value]
```

All lengths are validated against hard bounds (1 MiB key+value) before any
allocation. The checksum covers the entire body, so a torn or damaged record
never decodes successfully.

## Append path

1. Lock the partition's append mutex.
2. Assign `nextOffset` and stamp the record.
3. Encode the full frame and write it at the end of the file.
4. `fsync` (when `-sync=true`, the default) — this is the durability point.
5. Update the in-memory offset → file-position index.
6. Return the offset to the producer.

Offsets are assigned under the same lock that orders writes, so concurrent
producers can never receive duplicate offsets or interleave partial frames.

## Read path

The in-memory index maps offset → file position. Reads use positional I/O
(`ReadAt`) so they never move the append position and can run concurrently
with appends; each frame's length and checksum are re-validated on the way
out. Reads at or past the end return an empty list, never an error.

## Startup recovery

On open, each log is scanned front to back:

1. Validate each frame's length bounds, checksum, and sequential offset.
2. Rebuild the in-memory index as valid frames are found.
3. An incomplete frame at the tail (crash mid-write) is truncated.
4. A complete-but-invalid final frame (torn write) is truncated.
5. Invalid data **followed by more data** is real corruption: `Open` fails
   with a `CorruptionError` naming the byte position and expected offset,
   rather than silently skipping records.

The index is rebuilt by scanning on every startup; persisting it is a later
optimization (recovery of a 10k-record log measures ~67 ms, see
`benchmarks/results.md`).

## Partitions and routing

A stream's partition count is fixed when the stream is created (broker flag
`-partitions`, recorded in `meta.json`). Records with a key are routed by
`FNV-1a(key) % partitions`, so the same key always lands on the same
partition and stays ordered relative to itself. Keyless records are spread
round-robin. Each partition has its own log file, offset sequence, and
index; ordering is guaranteed within a partition, not across partitions.

## Consumer groups

`internal/groups` tracks membership in memory per (group, stream):

- **Join** (`POST /groups/{g}/streams/{s}/members`) returns a server-assigned
  member id, the group generation, and the member's partitions.
- **Assignment** is range-based: member ids are sorted and each gets a
  contiguous span, so every partition has exactly one owner.
- **Heartbeats** (`PUT .../members/{id}/heartbeat`) refresh the session and
  return the current assignment; a changed generation tells the consumer it
  was rebalanced. Members silent past the session timeout (broker flag
  `-session-timeout`) are dropped and their partitions reassigned.
- **Fencing**: a commit carrying a `member_id` is rejected (`409`, or `404`
  after expiry) unless that member currently owns the partition, so a stale
  member cannot overwrite the new owner's progress. Commits without a
  `member_id` skip fencing (simple mode for scripts and demos).

Membership is volatile; committed offsets are not. After a broker restart,
consumers rejoin and resume from persisted offsets.

## Consumer progress

`offsets.json` holds the committed *next offset to read* per
(group, stream, partition). Every commit rewrites the file to a temp path,
fsyncs, and renames it over the old file. Rename is atomic on POSIX
filesystems, so a crash mid-commit leaves the previous committed state
intact — a commit is applied entirely or not at all.

Consumer flow (at-least-once): read record → process record → commit
`offset+1`. A crash between processing and commit redelivers the record, so
consumers must tolerate duplicates.

## Current limits (by design, this stage)

- Partition count is fixed per stream at creation; no repartitioning.
- Single log file per partition; segments come with retention work.
- Group membership is in-memory on one node; it resets on restart (offsets
  do not).
- Single node; replication is a separate milestone with its own design doc.
