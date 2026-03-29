# pgrx — Project-Level Instructions

## Architecture

pgrx is a PostgreSQL-compatible database engine built on BEAM (Elixir) + Rust.
- **BEAM**: TCP connections, wire protocol, session state, hibernate, fault tolerance
- **Rust NIF**: SQL parsing (pg_query), query execution, storage, vector operations
- **Wire protocol**: PostgreSQL v3 (simple + extended query protocol)

## Codebase Structure

```
apps/wire/lib/wire/
  connection.ex     — PostgreSQL wire protocol handler (simple + extended query)
  listener.ex       — TCP listener
apps/engine/lib/
  engine.ex         — NIF bindings (execute_sql, parse_sql)
native/engine/src/
  executor.rs       — SQL executor (eval_expr, JOINs, subqueries, aggregates, vector ops)
  storage.rs        — In-memory row storage (per-table RwLock, scan_with)
  catalog.rs        — Table/column metadata, DefaultExpr
  types.rs          — Value enum (Int, Float, Text, Vector, etc.), TypeOid
  sequence.rs       — Auto-increment sequences (SERIAL)
  parser.rs         — pg_query wrapper
  lib.rs            — Rustler NIF entry points
```

## Implementation Review Protocol

**CRITICAL: Before shipping any feature, verify the implementation is fundamentally correct — not just that tests pass.**

### When to Run a Full Implementation Review

- After completing a major feature (JOINs, subqueries, vector search)
- After significant performance refactoring
- Before any public demo, benchmark claim, or grant submission
- When a Reddit/HN commenter questions the implementation

### The 3-Layer Review

**Layer 1: Architecture Review**
- Is the BEAM ↔ Rust boundary correct? (NIF scheduling, data ownership, lock lifetimes)
- Is the per-table RwLock hierarchy correct? (outer map → get_table Arc clone → per-table lock)
- Are there any TOCTOU races between scan and mutation?
- Does hibernate correctly preserve all connection state?
- Is the extended query protocol spec-compliant? (Parse/Bind/Describe/Execute/Sync lifecycle)

**Layer 2: Query Engine Correctness Review**
- Does eval_expr handle ALL pg_query AST node types we claim to support?
- Is NULL three-valued logic correct everywhere? (WHERE, JOIN ON, HAVING, IN, EXISTS)
- Are type coercions consistent? (Int vs Float promotion, Text → Vector)
- Does the hash join produce identical results to nested loop for all join types?
- Do subqueries correctly handle correlated vs uncorrelated execution?
- Is the RETURNING clause evaluated on the correct row state? (INSERT: new, UPDATE: after SET, DELETE: before removal)

**Layer 3: Performance & Correctness Under Load**
- Does the query produce correct results at 50 concurrent clients? (not just fast results)
- Are there any race conditions in the per-table RwLock under concurrent INSERT + SELECT?
- Does the parse cache produce correct results for different queries with the same prefix?
- Under memory pressure, do we degrade gracefully or crash?

### Review Methodology

For each layer, the reviewer MUST:
1. **Read the actual code** (not just the tests or commit messages)
2. **Trace through a specific query path end-to-end** (e.g., trace `SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 50 ORDER BY u.name LIMIT 5` through every function call)
3. **Identify the invariants** each function assumes and verify they hold
4. **Check edge cases**: empty tables, NULL values, 0 rows matching, 1M rows, mixed types
5. **Compare with PostgreSQL behavior** for any query that could differ

### What to Check in the 7-Step Code Review (Enhanced)

The standard 7-step review (Memory, Modularity, Security, Best Practices, Speed, Control Flow, Standardized Code) MUST also include:

**8. Correctness Verification**
- Pick 3 non-trivial SQL queries and trace them through the executor step by step
- Verify the result matches what PostgreSQL would return
- Specifically check: NULL handling, type coercion, empty result sets, edge cases

**9. Concurrency Safety**
- Identify every shared mutable state access point
- Verify lock ordering is consistent (no deadlocks)
- Check for TOCTOU between validation and mutation
- Verify eval_where error propagation through closures

## Benchmark Claims Policy

**Never claim a number without measuring it yourself.** When citing comparisons:

- State the measurement methodology (VmRSS vs Pss_Anon+VmPTE)
- State the hardware and configuration
- State whether huge_pages is on/off
- State the PostgreSQL version and relevant settings (shared_buffers, work_mem)
- Use ranges when the number depends on configuration ("90-5000x" not "960x")
- Link to the benchmark script in the repo

**Known correction**: Per-connection memory comparison uses VmRSS method (14.9 MB measured). Andres Freund's Pss_Anon+VmPTE methodology gives 1.3 MB (huge_pages=on) to 7.6 MB (huge_pages=off). Our BEAM processes hibernate to ~400 bytes. The defensible range is 90-5000x.

## SQL Feature Completeness

Currently supported:
- DDL: CREATE TABLE (IF NOT EXISTS), DROP TABLE (IF EXISTS), ALTER TABLE (ADD/DROP/RENAME COLUMN, RENAME TABLE), CREATE TABLE AS SELECT
- DML: INSERT (VALUES + SELECT), SELECT, UPDATE, DELETE, TRUNCATE (all with RETURNING)
- Queries: WHERE, ORDER BY, LIMIT, OFFSET, GROUP BY, HAVING, DISTINCT, BETWEEN
- Set operations: UNION, UNION ALL, INTERSECT, EXCEPT
- JOINs: INNER, LEFT, RIGHT, FULL, CROSS, implicit (hash join for equi, nested loop for theta)
- Subqueries: IN, EXISTS, scalar, ALL, derived tables (correlated supported)
- Expressions: arithmetic (+ - * / %), comparison, boolean logic, IS NULL, LIKE, ILIKE, CASE WHEN, COALESCE, NULLIF
- Aggregates: COUNT, SUM, AVG, MIN, MAX, STRING_AGG, BOOL_AND, BOOL_OR (with DISTINCT support)
- String functions: upper, lower, length, concat, substring, trim, replace, position, left, right
- Math functions: abs, ceil, floor, round, mod, power, sqrt
- Sequence functions: nextval, currval, setval
- Type casting: ::int, ::float8, ::text, ::bool, ::vector, CAST(x AS type)
- Types: int, bigint, float, text, bool, bytea, vector
- Vector: <-> (L2), <=> (cosine), <#> (inner product), ORDER BY distance LIMIT K

NOT yet supported (known gaps):
- UPDATE FROM / DELETE USING (cross-table mutations)
- ORDER BY expression alias
- Transactions (BEGIN/COMMIT/ROLLBACK)
- Persistent storage (WAL, disk)
- Indexes (B-tree) — HNSW done
- Write throughput (single-writer bottleneck)
- Horizontal scaling
- Vectorized/batch execution
