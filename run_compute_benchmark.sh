#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres

echo "============================================"
echo "  Compute Benchmark: pgrx vs PostgreSQL 17"
echo "============================================"
echo ""

CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"

# === PostgreSQL setup ===
echo "=== PostgreSQL 17 Setup ==="
psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS bench;" 2>/dev/null
psql -h 127.0.0.1 -p 5432 -U postgres -c "CREATE TABLE bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
psql -h 127.0.0.1 -p 5432 -U postgres -c "INSERT INTO bench SELECT g, 'name_' || g, g * 10 FROM generate_series(1, 1000) g;" 2>/dev/null
echo "  1000 rows inserted"
echo ""

# PostgreSQL benchmarks using our CLI (persistent connections, barrier sync)
echo "=== PostgreSQL: Point Query (SELECT WHERE id = 42) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "clients|queries|wall|QPS|avg|p50|p99"
echo ""

echo "=== PostgreSQL: Aggregate (SELECT COUNT(*)) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres --bench 1000 --clients 50 -c "SELECT COUNT(*) FROM bench;" 2>&1 | grep -E "clients|queries|wall|QPS|avg|p50|p99"
echo ""

echo "=== PostgreSQL: JOIN ==="
psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS orders; CREATE TABLE orders (id int, bench_id int, total int); INSERT INTO orders SELECT g, (g % 1000) + 1, g * 5 FROM generate_series(1, 5000) g;" 2>/dev/null
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres --bench 1000 --clients 50 -c "SELECT bench.name, COUNT(*) FROM bench JOIN orders ON bench.id = orders.bench_id GROUP BY bench.name LIMIT 10;" 2>&1 | grep -E "clients|queries|wall|QPS|avg|p50|p99"
echo ""

# === pgrx setup ===
echo "=== pgrx Setup ==="
cd /home/jagrit/pgrx
pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 5

$CLI -c "CREATE TABLE bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
# Insert 1000 rows in batches of 100
for i in $(seq 1 10); do
  VALS=""
  for j in $(seq 1 100); do
    N=$(( (i-1)*100 + j ))
    VALS="${VALS}(${N}, 'name_${N}', $((N * 10)))"
    if [ $j -lt 100 ]; then VALS="${VALS}, "; fi
  done
  $CLI -c "INSERT INTO bench VALUES ${VALS};" 2>/dev/null
done
echo "  1000 rows inserted"
echo ""

# pgrx benchmarks
echo "=== pgrx: Point Query (SELECT WHERE id = 42) ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1 | grep -E "clients|queries|wall|QPS|avg|p50|p99"
echo ""

echo "=== pgrx: Aggregate (SELECT COUNT(*)) ==="
$CLI --bench 1000 --clients 50 -c "SELECT COUNT(*) FROM bench;" 2>&1 | grep -E "clients|queries|wall|QPS|avg|p50|p99"
echo ""

echo "=== pgrx: JOIN ==="
$CLI -c "CREATE TABLE orders (id int, bench_id int, total int);" 2>/dev/null
for i in $(seq 1 50); do
  VALS=""
  for j in $(seq 1 100); do
    N=$(( (i-1)*100 + j ))
    BID=$(( (N % 1000) + 1 ))
    VALS="${VALS}(${N}, ${BID}, $((N * 5)))"
    if [ $j -lt 100 ]; then VALS="${VALS}, "; fi
  done
  $CLI -c "INSERT INTO orders VALUES ${VALS};" 2>/dev/null
done
$CLI --bench 1000 --clients 50 -c "SELECT bench.name, COUNT(*) FROM bench JOIN orders ON bench.id = orders.bench_id GROUP BY bench.name LIMIT 10;" 2>&1 | grep -E "clients|queries|wall|QPS|avg|p50|p99"
echo ""

# CPU measurement
echo "=== CPU During Sustained Load ==="
BEAM_PID=$(pgrep -f beam.smp | head -1)
$CLI --bench 10000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" &
BENCH_PID=$!
sleep 2
for s in 1 2 3; do
  CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
  echo "  BEAM CPU sample $s: ${CPU}%"
  sleep 1
done
wait $BENCH_PID 2>/dev/null

pkill -9 -f beam.smp 2>/dev/null
echo ""
echo "============================================"
echo "  Benchmark complete"
echo "============================================"
