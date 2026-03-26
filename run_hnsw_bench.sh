#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

mix compile --force 2>&1 | tail -3
pkill -9 -f beam.smp 2>/dev/null; sleep 1
elixir --erl "+SDcpu 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt &
sleep 6

# Setup: 1000 vectors, 32-dim
$CLI -c "CREATE TABLE hnsw_bench (id int, embedding vector);" 2>/dev/null
python3 -c "
import random; random.seed(42)
vals = []
for i in range(1000):
    vec = ','.join([f'{random.random():.4f}' for _ in range(32)])
    vals.append(f\"({i+1}, '[{vec}]')\")
# Batch insert in groups of 100
for start in range(0, 1000, 100):
    batch = ','.join(vals[start:start+100])
    print(f'INSERT INTO hnsw_bench VALUES {batch};')
" | while read sql; do
    $CLI -c "$sql" 2>/dev/null
done

QVEC=$(python3 -c "import random; random.seed(99); print('[' + ','.join([f'{random.random():.4f}' for _ in range(32)]) + ']')")

echo "============================================"
echo "  HNSW Benchmark: 1000 vectors, 32-dim"
echo "============================================"
echo ""
echo "=== HNSW KNN: 1 client ==="
$CLI --bench 1000 --clients 1 -c "SELECT id FROM hnsw_bench ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg"
echo ""
echo "=== HNSW KNN: 10 clients ==="
$CLI --bench 500 --clients 10 -c "SELECT id FROM hnsw_bench ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg"
echo ""
echo "=== HNSW KNN: 50 clients ==="
$CLI --bench 200 --clients 50 -c "SELECT id FROM hnsw_bench ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg"

kill %1 2>/dev/null; wait 2>/dev/null
