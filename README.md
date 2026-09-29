# Spool — Go Event-Stream Broker

Spool lets producers publish records to named streams and lets consumers
read or replay those records by offset. Records are stored on disk in a
checksummed append-only log, so they remain available after consumers
disconnect or the broker restarts — acknowledged records survive `kill -9`.

- **Durable by default**: a publish is acknowledged only after fsync.
- **Replayable**: any consumer can re-read from any offset; reading never
  removes records.
- **Recoverable**: torn trailing writes are truncated at startup; mid-log
  corruption is reported, never silently skipped.
- **At-least-once consumers**: committed progress per group survives
  restarts and crashes.
- **Replicated**: in cluster mode, writes are acknowledged only after a
  majority of three nodes has them durably; acked records survive losing
  the leader.

See [docs/architecture.md](docs/architecture.md) for the design,
[docs/guarantees.md](docs/guarantees.md) for exactly what is and is not
promised, and [docs/replication.md](docs/replication.md) for the failover
design. Measured results live in [benchmarks/results.md](benchmarks/results.md).

## Quick start

Requires Go 1.22+.

```sh
go build -o bin/ ./cmd/...

# 1. Start the broker (durable mode)
./bin/broker -addr :8080 -data data

# 2. Publish records
./bin/producer -stream orders -key order-123 -value order.created
./bin/producer -stream orders -key order-123 -value order.shipped

# 3. Consume as a group; progress is committed after each record
./bin/consumer -stream orders -group activity-feed -once

# 4. Kill the broker (Ctrl-C or kill -9), restart it, and re-run the
#    consumer: it resumes exactly where it committed.
./bin/broker -addr :8080 -data data
./bin/consumer -stream orders -group activity-feed -once

# 5. A late-starting analytics service replays everything from offset 0
./bin/consumer -stream orders -group analytics -from-beginning -once

# 6. Two consumers in one group split the partitions between them;
#    kill one and the survivor takes over its partitions.
./bin/consumer -stream orders -group workers &
./bin/consumer -stream orders -group workers
```

### Three-node cluster

```sh
PEERS="n1=http://127.0.0.1:8081,n2=http://127.0.0.1:8082,n3=http://127.0.0.1:8083"
./bin/broker -addr :8081 -data data-n1 -node-id n1 -peers "$PEERS" &
./bin/broker -addr :8082 -data data-n2 -node-id n2 -peers "$PEERS" &
./bin/broker -addr :8083 -data data-n3 -node-id n3 -peers "$PEERS" &

# Publish to any node: followers redirect (307) to the elected leader, and
# the ack means a majority has the record durably on disk.
curl -sL -X POST localhost:8082/streams/orders/records -d '{"value":"hi"}'

# Kill the leader (see /raft/status); a new leader takes over in well under
# a second, and every acknowledged record is still readable.
curl -s localhost:8082/raft/status
```

## HTTP API

```sh
# Publish
curl -s -X POST localhost:8080/streams/orders/records \
  -d '{"key":"order-123","value":"order.created"}'
# -> {"partition":0,"offset":42}

# Read from an offset
curl -s 'localhost:8080/streams/orders/partitions/0/records?offset=40&limit=100'
# -> {"records":[...],"next_offset":43}

# Stream metadata
curl -s localhost:8080/streams/orders
# -> {"stream":"orders","partitions":3}

# Consumer-group membership
curl -s -X POST localhost:8080/groups/analytics/streams/orders/members
# -> {"member_id":"a1b2...","generation":1,"partitions":[0,1,2]}
curl -s -X PUT localhost:8080/groups/analytics/streams/orders/members/a1b2.../heartbeat
curl -s -X DELETE localhost:8080/groups/analytics/streams/orders/members/a1b2...

# Consumer-group progress (member_id makes the commit fenced)
curl -s localhost:8080/groups/analytics/streams/orders/partitions/0/offset
curl -s -X PUT localhost:8080/groups/analytics/streams/orders/partitions/0/offset \
  -d '{"next_offset":43,"member_id":"a1b2..."}'

# Health
curl -s localhost:8080/health/live
curl -s localhost:8080/health/ready
```

Errors are JSON (`{"error":"..."}`); limits and status codes are documented
in [docs/guarantees.md](docs/guarantees.md).

## Tests and benchmarks

```sh
go test ./... -race -count=1     # unit + e2e process-crash tests
./benchmarks/crash_test.sh       # SIGKILL under load, timed recovery, ack verification
./bin/loadgen -stream bench -producers 8 -records 1250 -size 256 -verify
```

The test suite covers ordered reads, replay, restart persistence,
torn-write truncation, mid-log corruption detection, concurrent-publish
offset uniqueness, at-least-once redelivery, committed-progress survival
across SIGKILL, deterministic key routing, group assignment invariants,
timeout-driven reassignment, stale-member commit fencing, leader election,
majority-ack commits, follower catch-up after downtime, stale-leader
truncation, quorum-loss write rejection, and leader-kill failover with
acked-record verification against real broker processes.

## Status

Stages 1–5 of the build plan are complete: durable log, HTTP broker,
persisted consumer progress, crash tests and benchmarks, multi-partition
streams with key routing, consumer groups with membership and commit
fencing, and three-node replication with majority-ack writes and safe
leader failover. Kubernetes deployment is the remaining milestone.
