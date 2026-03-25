# pgRx Architecture Principles

> Every allocation is a cost. Every pointer is pressure. Every copy is waste.
> — Inspired by VictoriaMetrics' zero-allocation philosophy

This document defines the architectural principles that govern ALL implementation decisions in pgRx. Every PR, every new feature, every refactor must align with these principles.

---

## Core Philosophy: Zero Unnecessary Allocation

VictoriaMetrics achieves 10x less memory than competitors not through any single trick but by **systematically eliminating every source of unnecessary heap allocation** across the entire codebase. pgRx adopts this as its foundational engineering discipline.

The rules:

1. **Never allocate what you can reference.** Use `Arc<str>`, slices, and borrowed references over owned `String` and `Vec` clones.
2. **Never allocate per-row what you can allocate per-query.** Use arena allocation (bumpalo) for query temporaries.
3. **Never allocate per-query what you can pool.** Reuse buffers across queries via thread-local pools.
4. **Never keep on the heap what can live off-heap.** Use mmap-backed arenas for buffer pools and page caches.
5. **Never store uncompressed what can be compressed.** Adaptive compression per block — pick the best encoding automatically.

---

## Principle 1: String Interning

**Problem:** Every `Text(String)` in a row is a separate heap allocation. A table with 100K rows and a `status` column with 5 unique values allocates 100K Strings where 5 would suffice.

**Rule:** All repeated text values MUST be interned.

```rust
// BAD: fresh allocation per row
Value::Text("active".to_string())

// GOOD: shared reference, one allocation
Value::Text(Arc<str>)  // interned via global DashMap
```

**Where to apply:**
- Column values with low cardinality (enums, status fields, boolean-as-text)
- Table names, column names, schema names in catalog
- SQL keywords and identifiers in parse cache

**Source:** VictoriaMetrics achieves 3x memory reduction via `sync.Map.LoadOrStore()` for label interning.

---

## Principle 2: Buffer Pooling

**Problem:** Each query allocates fresh `Vec<Row>`, `Vec<Value>`, and temporary buffers. After the query, they're dropped. The next query allocates again. This churns the allocator.

**Rule:** All per-query temporary buffers MUST be pooled and reused.

```rust
// BAD: allocate per query
let mut results: Vec<Row> = Vec::new();

// GOOD: take from pool, return after use
thread_local! {
    static ROW_BUF: RefCell<Vec<Row>> = RefCell::new(Vec::with_capacity(1024));
}
```

**Key technique (from VM):** Reset length to zero while keeping backing array:
```rust
buf.clear();  // length = 0, capacity unchanged, no deallocation
```

**Where to apply:**
- Query result buffers
- Intermediate join results
- Sort temporaries
- Serialization/deserialization buffers
- Wire protocol send/receive buffers

**Source:** VictoriaMetrics uses `sync.Pool` for every decompression, serialization, and query buffer. `bb.B[:0]` is their signature pattern.

---

## Principle 3: Arena Allocation

**Problem:** Per-row heap allocation in tight loops (scan, filter, join, aggregate) fragments memory and destroys cache locality. The allocator becomes a bottleneck.

**Rule:** CPU-bound query execution MUST use arena allocation for all intermediates.

```rust
use bumpalo::Bump;

fn execute_query(sql: &str) -> Result<Vec<Row>> {
    let arena = Bump::new();  // one allocation for the query

    // All intermediate values live in the arena
    let filtered = arena.alloc_slice_fill_iter(rows.iter().filter(predicate));
    let sorted = arena.alloc_slice_copy(filtered);
    sorted.sort_by(comparator);

    // Only the final result escapes the arena
    let result = sorted.to_vec();
    drop(arena);  // everything freed in one shot
    result
}
```

**Where to apply:**
- WHERE clause evaluation intermediates
- JOIN build/probe temporaries
- GROUP BY hash table entries
- ORDER BY sort keys
- Expression evaluation scratch space

**Source:** DuckDB, CockroachDB, and virtually every production query engine uses arena allocation. VictoriaMetrics uses mmap-backed arenas via fastcache.

---

## Principle 4: Vectorized Processing

**Problem:** Row-at-a-time execution through `eval_expr()` causes one function call per row per expression. This prevents SIMD auto-vectorization and wastes instruction cache.

**Rule:** Scan-heavy operations (WHERE, aggregates, vector distance) MUST process data in batches of 1024 rows.

```rust
const BATCH_SIZE: usize = 1024;  // fits L1 cache (32-128KB)

// BAD: row-at-a-time
for row in table.rows() {
    if eval_expr(&row, &where_clause) { results.push(row); }
}

// GOOD: vectorized batch
for batch in table.rows().chunks(BATCH_SIZE) {
    let mut mask = [true; BATCH_SIZE];
    eval_filter_batch(batch, &where_clause, &mut mask);
    for (i, row) in batch.iter().enumerate() {
        if mask[i] { results.push(row); }
    }
}
```

**Why 1024:** Fits in L1 cache. Tight loops over arrays of this size auto-vectorize into SIMD instructions. Larger batches blow the cache. Smaller batches underutilize SIMD width.

**Where to apply:**
- WHERE clause filtering (highest impact)
- Aggregate computation (SUM, COUNT, AVG)
- Vector distance operations (L2, cosine, dot product)
- Hash computation for GROUP BY and JOIN

**Source:** DuckDB's entire speed advantage. 10-100x faster than row-at-a-time for analytical queries.

---

## Principle 5: Off-Heap Storage via mmap

**Problem:** Large data structures on the Rust heap compete with query execution for memory. The OS has no visibility into what's hot and what's cold.

**Rule:** Buffer pool and page cache MUST use mmap, not heap allocation.

```rust
use memmap2::MmapMut;

// BAD: heap-allocated buffer pool
let pages: Vec<Page> = Vec::with_capacity(num_pages);

// GOOD: mmap-backed, OS manages paging
let mmap = MmapMut::map_anon(page_size * num_pages)?;
```

**Benefits:**
- OS page cache handles eviction automatically (no custom LRU needed initially)
- No double-buffering (the exact problem pgRx solves vs PostgreSQL)
- Pages stay warm in kernel cache even if Rust drops references
- Transparent huge pages can be enabled for large pools

**Where to apply:**
- Buffer pool (Phase 2: persistent storage)
- Page cache for B-tree nodes
- WAL buffer
- Large temporary sort files (spill to disk)

**Source:** VictoriaMetrics' fastcache uses `unix.Mmap(MAP_ANON|MAP_PRIVATE)`. Neon's pageserver uses similar patterns. PostgreSQL's double-buffering (shared_buffers + OS cache) is the anti-pattern we're avoiding.

---

## Principle 6: CPU-Core Sharded Writes

**Problem:** Single write lock per table serializes all inserts. Under concurrent agent workloads (the primary use case), this becomes the bottleneck.

**Rule:** Write buffers MUST be sharded by CPU core for high-throughput tables.

```rust
struct ShardedWriteBuffer {
    shards: Vec<Mutex<Vec<Row>>>,  // one per CPU core
}

impl ShardedWriteBuffer {
    fn insert(&self, row: Row) {
        let shard_id = thread_id() % self.shards.len();
        self.shards[shard_id].lock().push(row);
    }

    fn flush(&self) -> Vec<Row> {
        // Background merger combines all shards
        self.shards.iter()
            .flat_map(|s| s.lock().drain(..))
            .collect()
    }
}
```

**Where to apply:**
- INSERT path (high-throughput tables)
- WAL write buffer
- Metric/telemetry collection

**Source:** VictoriaMetrics uses per-CPU-core raw-row shards (~8MB each). Eliminates write contention entirely.

---

## Principle 7: Adaptive Compression

**Problem:** One compression algorithm doesn't fit all data. Compressing random UUIDs wastes CPU. Not compressing repetitive status values wastes storage.

**Rule:** Storage layer MUST select compression per block automatically.

```
Decision tree per block:
  All values identical?         → Constant encoding (1 byte total)
  Sequential integers?          → Delta encoding
  Low cardinality strings?      → Dictionary encoding
  Monotonic counter?            → Delta-of-delta encoding
  Random/high entropy?          → ZSTD or no compression
  Already compact (< threshold) → Skip compression
```

**Where to apply:**
- Per-page compression in B-tree storage
- Per-column-chunk compression for scan optimization
- WAL segment compression
- Backup compression

**Source:** VictoriaMetrics achieves 0.4 bytes per data point vs 4 bytes raw. They test multiple encodings per block and pick the winner.

---

## Principle 8: Bounded Everything

**Problem:** Unbounded caches, unbounded result sets, unbounded connection counts — each is a potential OOM.

**Rule:** Every cache, buffer, and queue MUST have a hard upper bound.

| Resource | Bound | Eviction |
|---|---|---|
| Parse cache | 10,000 entries | LRU |
| Buffer pool pages | Configured at startup | LRU-K(2) |
| Query result rows | Configurable per-query | Error if exceeded |
| Connection count | Configurable | Reject with error |
| Write buffer per shard | 8 MB | Flush to storage |
| WAL buffer | 64 MB | Flush to disk |

**Source:** Every production database bounds these. VictoriaMetrics uses ring buffers with automatic FIFO eviction. SQLite bounds page cache. PostgreSQL bounds shared_buffers. Our current parse cache is unbounded — that's a bug.

---

## Principle 9: Immutable Parts with Background Merge

**Problem:** In-place updates require locking during reads. Readers and writers contend.

**Rule:** Written data MUST be immutable. Updates create new versions. Background processes merge old versions.

```
Write path:
  INSERT → write buffer (mutable, per-core sharded)
    → flush → immutable part (compressed, on disk)
      → background merge → larger immutable part

Read path:
  Scan all relevant parts (newest first)
  Merge results (newer versions shadow older)
  No locks needed on immutable parts — just reference counting
```

**Where to apply:**
- Storage engine (Phase 2)
- Undo-log MVCC (version chains are naturally immutable)
- Index updates (new entries in delta, background merge into main tree)

**Source:** VictoriaMetrics, Neon (image + delta layers), LSM trees (LevelDB, RocksDB, Pebble). The "never overwrite" principle simplifies crash recovery, enables branching, and eliminates read-write contention.

---

## Principle 10: Measure Before Optimizing

**Rule:** Every optimization MUST be benchmarked before and after. No "it should be faster" without numbers.

Maintain benchmarks for:
- Memory per connection (idle and active)
- Memory per 1000 rows inserted
- Query throughput (queries/second) for standard workloads
- INSERT throughput (rows/second)
- P50/P95/P99 query latency
- Vector distance computation throughput

**Source:** VictoriaMetrics publishes benchmarks against every competitor. SQLite has 92,000 KSLOC of tests. DuckDB benchmarks every commit. If you can't measure it, you can't improve it.

---

## Quick Reference: Decision Matrix

| Situation | Do This | Don't Do This |
|---|---|---|
| Need temporary storage during query | Arena allocation (bumpalo) | `Vec::new()` per operation |
| Repeated string values | Intern via `Arc<str>` + DashMap | Clone `String` per row |
| Query result buffers | Thread-local pool, `.clear()` | Fresh allocation per query |
| Large data (pages, cache) | mmap-backed arena | Heap-allocated Vec |
| Concurrent writes | Per-core sharded buffers | Single RwLock |
| Persistent storage | Immutable parts + merge | In-place updates |
| Compression | Adaptive per-block | One algorithm everywhere |
| Caches | Bounded LRU/LRU-K | Unbounded HashMap |
| Processing rows | 1024-row vectorized batches | Row-at-a-time |
| CPU-bound work | Bound to core count | Unbounded concurrency |
