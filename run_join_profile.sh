#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

$CLI -c "CREATE TABLE users (id int PRIMARY KEY, name text);" 2>/dev/null
V=""; for i in $(seq 1 1000); do V="${V}($i,'user_$i')"; [ $i -lt 1000 ] && V="${V},"; done
$CLI -c "INSERT INTO users VALUES ${V};" 2>/dev/null
$CLI -c "CREATE TABLE orders (id int, user_id int, total int);" 2>/dev/null
for b in $(seq 1 5); do
  V=""; for i in $(seq 1 1000); do N=$(((b-1)*1000+i)); BID=$(((N%1000)+1)); V="${V}($N,$BID,$((N*5)))"; [ $i -lt 1000 ] && V="${V},"; done
  $CLI -c "INSERT INTO orders VALUES ${V};" 2>/dev/null
done

echo "=== JOIN only (no agg) ==="
$CLI --bench 20 --clients 1 -c "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id LIMIT 10;" 2>&1

echo ""
echo "=== JOIN + GROUP BY (no ORDER BY) ==="
$CLI --bench 20 --clients 1 -c "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name LIMIT 10;" 2>&1

echo ""
echo "=== JOIN + GROUP BY + ORDER BY ==="
$CLI --bench 20 --clients 1 -c "SELECT users.name, COUNT(*) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY COUNT(*) DESC LIMIT 5;" 2>&1

echo ""
echo "=== GROUP BY + ORDER BY (no JOIN, single table) ==="
$CLI --bench 50 --clients 1 -c "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 5;" 2>&1

kill %1 2>/dev/null; wait 2>/dev/null
