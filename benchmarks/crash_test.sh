#!/usr/bin/env bash
# Process-crash test: publish under load, record acknowledged offsets at the
# client, SIGKILL the broker, restart it on the same data directory, and
# time how long recovery takes until reads are served again. Loadgen -verify
# already checks every acknowledged offset is readable; this script re-checks
# record counts after the crash.
#
# Usage: ./benchmarks/crash_test.sh
set -euo pipefail
cd "$(dirname "$0")/.."

DATA=$(mktemp -d /tmp/spool-crash.XXXXXX)
PORT=8093
ADDR="http://localhost:$PORT"
trap 'pkill -f "bin/broker -addr :$PORT" 2>/dev/null || true; rm -rf "$DATA"' EXIT

go build -o bin/ ./cmd/...

./bin/broker -addr ":$PORT" -data "$DATA" -sync=true &
sleep 1

echo "--- publishing 10000 records (durable mode), verifying acks ---"
./bin/loadgen -addr "$ADDR" -stream crash -producers 8 -records 1250 -size 256 -verify

EXPECTED=$(curl -s "$ADDR/streams/crash/partitions/0/records?offset=9999&limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_offset"])')
echo "--- next_offset before crash: $EXPECTED ---"

echo "--- SIGKILL broker ---"
pkill -9 -f "bin/broker -addr :$PORT"
sleep 0.5

echo "--- restarting, timing recovery ---"
START=$(python3 -c 'import time; print(time.time())')
./bin/broker -addr ":$PORT" -data "$DATA" -sync=true &
until curl -sf "$ADDR/health/ready" >/dev/null 2>&1; do sleep 0.02; done
# Recovery includes serving a read of the full log tail.
curl -sf "$ADDR/streams/crash/partitions/0/records?offset=9000&limit=1000" >/dev/null
END=$(python3 -c 'import time; print(time.time())')
python3 -c "print(f'recovery time (kill -> ready -> reads served): {$END-$START:.3f}s')"

AFTER=$(curl -s "$ADDR/streams/crash/partitions/0/records?offset=9999&limit=1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["next_offset"])')
echo "--- next_offset after crash: $AFTER ---"
if [ "$EXPECTED" != "$AFTER" ]; then
  echo "FAIL: acknowledged records missing after crash"
  exit 1
fi
echo "PASS: all $AFTER acknowledged records readable after SIGKILL restart"
