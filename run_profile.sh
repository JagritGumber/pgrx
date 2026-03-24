#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
cd /home/jagrit/pgrx

CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"

pkill -9 -f beam.smp 2>/dev/null; sleep 1
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

BEAM_PID=$(pgrep -f beam.smp | head -1)

echo "=== 1. Single-client SELECT 1 (pure overhead) ==="
$CLI --bench 5000 --clients 1 -c "SELECT 1;" 2>&1

echo ""
echo "=== 2. Single-client point query (WHERE id = 42) ==="
$CLI --bench 5000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

echo ""
echo "=== 3. Single-client full scan (SELECT * FROM bench) ==="
$CLI --bench 100 --clients 1 -c "SELECT * FROM bench;" 2>&1

echo ""
echo "=== 4. 50-client point query ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" &
BENCH=$!
sleep 3
CPU1=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
CPU2=$(ps -p $BEAM_PID -o %cpu= 2>/dev/null | tr -d ' ')
wait $BENCH 2>/dev/null
echo "  BEAM CPU during load: ${CPU1}%, ${CPU2}%"

echo ""
echo "=== 5. PostgreSQL comparison: single-client ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 5000 --clients 1 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

echo ""
echo "=== 6. PostgreSQL: 50-client ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null
