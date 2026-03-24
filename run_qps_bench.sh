#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null
sleep 1

# Start pgrx
mix run --no-halt &
SERVER_PID=$!
sleep 6

# Setup
$CLI -c "CREATE TABLE bench (id int PRIMARY KEY, name text, val int);" 2>/dev/null
for i in $(seq 1 10); do
  VALS=""
  for j in $(seq 1 100); do
    N=$(( (i-1)*100 + j ))
    VALS="${VALS}(${N}, 'name_${N}', $((N * 10)))"
    if [ $j -lt 100 ]; then VALS="${VALS}, "; fi
  done
  $CLI -c "INSERT INTO bench VALUES ${VALS};" 2>/dev/null
done

echo "=== pgrx: Point Query (50 clients x 5000) ==="
$CLI --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

echo ""
echo "=== pgrx: COUNT(*) (50 clients x 1000) ==="
$CLI --bench 1000 --clients 50 -c "SELECT COUNT(*) FROM bench;" 2>&1

echo ""
echo "=== PostgreSQL 17: Point Query (50 clients x 5000) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 5000 --clients 50 -c "SELECT * FROM bench WHERE id = 42;" 2>&1

echo ""
echo "=== PostgreSQL 17: COUNT(*) (50 clients x 1000) ==="
$CLI -h 127.0.0.1 -p 5432 -U postgres -d postgres -W postgres --bench 1000 --clients 50 -c "SELECT COUNT(*) FROM bench;" 2>&1

kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
