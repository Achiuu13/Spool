# Benchmark results

Raw output from documented commands. Three trials per configuration, same
machine, same day. Numbers below are process-crash results where noted; they
do not prove behavior under storage or power failure.

## Environment

| | |
|---|---|
| CPU | Apple M3 Pro, 12 cores |
| Memory | 18 GiB |
| Storage | Internal Apple SSD (Apple Fabric) |
| OS | macOS 14 (Darwin 23.6.0), arm64 |
| Go | go1.27.1 |
| Setup | Local process, client and broker on the same machine over loopback |

## Commands

```sh
go build -o bin/ ./cmd/...

# durable mode (fsync per append before ack)
./bin/broker -addr :8092 -data /tmp/spool-bench -sync=true
./bin/loadgen -addr http://localhost:8092 -stream bench1 -producers 8 -records 1250 -size 256 -verify

# buffered mode (no fsync; weaker guarantee, see docs/guarantees.md)
./bin/broker -addr :8092 -data /tmp/spool-bench2 -sync=false
./bin/loadgen -addr http://localhost:8092 -stream bench1 -producers 8 -records 1250 -size 256 -verify

# process-crash test (SIGKILL + timed recovery)
./benchmarks/crash_test.sh
```

## Durable mode (`-sync=true`, fsync per append — the acknowledged-write policy)

Workload: 8 producers x 1,250 records each (10,000 total), 256-byte values.

| Trial | Throughput | p50 | p95 | p99 | Failed | Missing acked |
|---|---|---|---|---|---|---|
| 1 | 245 rec/s | 32.024 ms | 34.999 ms | 41.163 ms | 0/10000 | 0 |
| 2 | 244 rec/s | 32.058 ms | 35.966 ms | 40.328 ms | 0/10000 | 0 |
| 3 | 248 rec/s | 32.017 ms | 33.887 ms | 36.879 ms | 0/10000 | 0 |

Throughput is fsync-bound: Go's `File.Sync` on macOS issues `F_FULLFSYNC`,
which forces the write through the drive cache (~30 ms on this SSD). Each
append pays one full sync, so latency clusters tightly around it.

## Buffered mode (`-sync=false` — weaker guarantee, OS page cache only)

Same workload.

| Trial | Throughput | p50 | p95 | p99 | Failed | Missing acked |
|---|---|---|---|---|---|---|
| 1 | 48,172 rec/s | 132 µs | 374 µs | 558 µs | 0/10000 | 0 |
| 2 | 48,191 rec/s | 128 µs | 387 µs | 552 µs | 0/10000 | 0 |
| 3 | 51,251 rec/s | 122 µs | 361 µs | 500 µs | 0/10000 | 0 |

## Process-crash recovery (`./benchmarks/crash_test.sh`)

10,000 durable-mode records published and acknowledged, broker SIGKILLed,
restarted on the same data directory:

- Recovery time (kill → ready → reads served): **0.067 s**
- Acknowledged records readable after restart: **10,000 / 10,000**
- `next_offset` before crash: 10000; after crash: 10000

Automated equivalents (concurrent publish + SIGKILL + verify, torn-write
truncation, mid-log corruption detection) run in CI-style form via:

```sh
go test ./... -race -count=1
```
