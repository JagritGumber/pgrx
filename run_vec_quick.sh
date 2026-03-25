#!/bin/bash
export PATH="$HOME/.local/share/mise/installs/erlang/27.2/bin:$HOME/.local/share/mise/installs/elixir/1.18.2-otp-27/bin:$PATH"
export PGPASSWORD=postgres
CLI="/home/jagrit/pgrx/native/cli/target/release/pgrx"
cd /home/jagrit/pgrx

pkill -9 -f beam.smp 2>/dev/null; sleep 1
mix run --no-halt &
sleep 6

# Setup: 1000 vectors, 128-dim (using simpler generation)
$CLI -c "CREATE TABLE vb (id int PRIMARY KEY, embedding vector);" 2>/dev/null

# Generate insert SQL with Python (much faster than bash)
python3 -c "
import random
random.seed(42)
for batch in range(10):
    vals = []
    for i in range(100):
        n = batch*100 + i + 1
        vec = ','.join([f'{random.random():.4f}' for _ in range(128)])
        vals.append(f\"({n}, '[{vec}]')\")
    print('INSERT INTO vb VALUES ' + ','.join(vals) + ';')
" | while read sql; do
    $CLI -c "$sql" 2>/dev/null
done

echo "=== pgrx: KNN 128-dim, 1000 vectors, 1 client ==="
QVEC=$(python3 -c "import random; random.seed(99); print('[' + ','.join([f'{random.random():.4f}' for _ in range(128)]) + ']')")
$CLI --bench 100 --clients 1 -c "SELECT id FROM vb ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg|P99"

echo ""
echo "=== pgrx: KNN 128-dim, 1000 vectors, 10 clients ==="
$CLI --bench 100 --clients 10 -c "SELECT id FROM vb ORDER BY embedding <-> '${QVEC}' LIMIT 10;" 2>&1 | grep -E "Throughput|Avg|P99"

echo ""
echo "=== pgvector reference (same dataset, no index): 478 QPS (1 client), 3862 QPS (10 clients) ==="

kill %1 2>/dev/null; wait 2>/dev/null
