#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 1

# Force NIF recompile
mix compile --force 2>&1 | tail -3

mix run --no-halt &
SERVER_PID=$!
sleep 6

# Setup
$CLI -c "CREATE TABLE bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
for i in $(seq 1 10); do
  VALS=""
  for j in $(seq 1 100); do
    N=$(( (i-1)*100 + j )); VALS="${VALS}(${N}, 'name_${N}', $((N * 10)))"; [ $j -lt 100 ] && VALS="${VALS}, "
  done
  $CLI -c "INSERT INTO bench VALUES ${VALS};" 2>/dev/null
done

echo "============================================"
echo "  Speed Comparison: pgrx (with parse cache)"
echo "  vs PostgreSQL 17"
echo "============================================"
echo ""

echo "=== pgrx: SELECT 1 (1 client) ==="
$CLI --bench 10000 --clients 1 -c "SELECT 1;" 2>&1

echo ""
echo "=== pgrx: Point query (1 client) ==="
$CLI --bench 10000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

echo ""
echo "=== pgrx: Point query (50 clients) ==="
BEAM_PID=$(pgrep -f beam.smp | head -1)
$CLI --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" &
BENCH=$!
sleep 3
CPU=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
wait $BENCH 2>/dev/null
echo "  BEAM CPU: ${CPU}%"

echo ""
echo "--- PostgreSQL 17 ---"
echo ""
echo "=== PG: Point query (1 client) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 10000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

echo ""
echo "=== PG: Point query (50 clients) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null
