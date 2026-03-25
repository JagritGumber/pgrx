#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

# Force NIF recompile
mix compile --force 2>&1 | tail -3

pkill -9 -f beam.smp 2>/dev/null; sleep 1

# Use +SDcpu 8 from vm.args via elixir flags
elixir --erl "+SDcpu 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix run --no-halt &
SERVER_PID=$!
sleep 6

# Setup 1000-row table
$CLI -c "CREATE TABLE bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
for i in $(seq 1 10); do
  VALS=""; for j in $(seq 1 100); do N=$(((i-1)*100+j)); VALS="${VALS}($N,'n$N',$((N*10)))"; [ $j -lt 100 ] && VALS="${VALS},"; done
  $CLI -c "INSERT INTO bench VALUES ${VALS};" 2>/dev/null
done

# Setup 100-vector table
$CLI -c "CREATE TABLE vecs (id int, embedding vector);" 2>/dev/null
SQL=$(python3 -c "
import random; random.seed(42)
vals = []
for i in range(100):
    vec = ','.join([f'{random.random():.4f}' for _ in range(32)])
    vals.append(f\"({i+1}, '[{vec}]')\")
print('INSERT INTO vecs VALUES ' + ','.join(vals) + ';')
")
$CLI -c "$SQL" 2>/dev/null

QVEC=$(python3 -c "import random; random.seed(99); print('[' + ','.join([f'{random.random():.4f}' for _ in range(32)]) + ']')")

BEAM_PID=$(pgrep -f beam.smp | head -1)

echo "============================================"
echo "  Concurrency Benchmark: After 3 Fixes"
echo "  Per-table RwLock + RwLock parse cache + SDcpu 8"
echo "============================================"
echo ""

echo "=== Point query: 1 client ==="
$CLI --bench 10000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== Point query: 10 clients ==="
$CLI --bench 5000 --clients 10 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== Point query: 50 clients ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" &
B=$!; sleep 3
CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
wait $B 2>/dev/null
echo "  BEAM CPU: ${CPU}%"

echo ""
echo "=== Point query: 100 clients ==="
$CLI --bench 2000 --clients 100 -c "SELECT * FROM bench WHERE id = 42;" &
B=$!; sleep 3
CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
wait $B 2>/dev/null
echo "  BEAM CPU: ${CPU}%"

echo ""
echo "=== Vector KNN: 1 client (32-dim, 100 vecs) ==="
$CLI --bench 500 --clients 1 -c "SELECT id FROM vecs ORDER BY embedding <-> '${QVEC}' LIMIT 5;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== Vector KNN: 10 clients ==="
$CLI --bench 200 --clients 10 -c "SELECT id FROM vecs ORDER BY embedding <-> '${QVEC}' LIMIT 5;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== Vector KNN: 50 clients ==="
$CLI --bench 100 --clients 50 -c "SELECT id FROM vecs ORDER BY embedding <-> '${QVEC}' LIMIT 5;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "--- PostgreSQL 17 comparison ---"
echo ""
echo "=== PG: Point query 50 clients ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "Throughput|Avg"

echo ""
echo "=== PG: pgvector KNN 10 clients (128-dim, 1000 vecs) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 100 --clients 10 -c "SELECT id FROM vec_bench ORDER BY embedding <-> '$(python3 -c "import random; random.seed(99); print('[' + ','.join([f'{random.random():.4f}' for _ in range(128)]) + ']')")' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg"

kill $SERVER_PID 2>/dev/null; wait 2>/dev/null
