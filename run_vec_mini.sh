#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

$CLI -c "CREATE TABLE vt (id int, embedding vector);" 2>/dev/null

# Insert 100 vectors, 32-dim
SQL=$(python3 -c "
import random; random.seed(42)
vals = []
for i in range(100):
    vec = ','.join([f'{random.random():.4f}' for _ in range(32)])
    vals.append(f\"({i+1}, '[{vec}]')\")
print('INSERT INTO vt VALUES ' + ','.join(vals) + ';')
")
$CLI -c "$SQL" 2>/dev/null

QVEC=$(python3 -c "import random; random.seed(99); print('[' + ','.join([f'{random.random():.4f}' for _ in range(32)]) + ']')")

echo "=== pgrx: KNN 32-dim, 100 vectors, 1 client ==="
$CLI --bench 500 --clients 1 -c "SELECT id FROM vt ORDER BY embedding <-> '${QVEC}' LIMIT 5;" 2>&1

echo ""
echo "=== pgrx: KNN 32-dim, 100 vectors, 10 clients ==="
$CLI --bench 200 --clients 10 -c "SELECT id FROM vt ORDER BY embedding <-> '${QVEC}' LIMIT 5;" 2>&1

kill %1 2>/dev/null; wait 2>/dev/null
