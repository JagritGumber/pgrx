# Vector Search Industry Research: Competitive Landscape & The Parent-Finding Problem

**Date:** 2026-03-23
**Purpose:** Intelligence for pgrx vector search architecture decisions

---

## 1. COMPETITIVE LANDSCAPE

### 1.1 Dedicated Vector Databases

#### Pinecone (Proprietary, Managed)
- **Architecture:** Slab-based storage. Writes go to an in-memory memtable, flush to blob storage as L0 slabs with lightweight indexes (scalar quantization, random projections). Compaction merges slabs and builds heavier graph/partition indexes asynchronously. Reads fan out to all valid slabs in parallel, results merged.
- **Performance:** Sub-100ms latency for most use cases. 2026 Milvus benchmarks showed Milvus achieving 1.5x better QPS than Pinecone, Zilliz Cloud 3x better.
- **Strengths:** Fully managed, auto-scaling, predictable caching (no warmup), eventually-consistent mode for speed.
- **Weaknesses:** Closed-source, expensive at scale, index type is opaque to users, updates require new slab creation (no in-place modification), vendor lock-in.
- **Key insight:** Their slab architecture is write-optimized with amortized index costs. Good idea but not fundamentally faster at the search layer.

#### Qdrant (Rust, Open Source)
- **Architecture:** Rust + SIMD, custom storage engine (Gridstore). Segment-based architecture where segments go through appendable -> indexed lifecycle. WAL for durability, RocksDB for payloads, memory-mapped files for vectors.
- **HNSW:** m=16 default, ef_construct=100, supports asymmetric search. GPU-accelerated HNSW indexing available.
- **Quantization:** Scalar (4x), Product (4-64x), Binary (32x, including 1.5-bit and 2-bit). Quantized vectors can be embedded directly in the graph (Inline Storage).
- **Performance:** Strong concurrent read/write handling via WAL buffering and replica consistency modes. Memory reduction up to 97% with quantization.
- **Weaknesses:** Still fundamentally HNSW-based (same local-minimum problems). At 50M vectors/99% recall, pgvectorscale beats it on throughput (471 vs 41 QPS). Tail latency: Qdrant has 39% lower p95 and 48% lower p99 than pgvectorscale though.
- **Key insight:** Great engineering, but HNSW is the ceiling. Inline storage (quantized vectors in graph) is a clever cache optimization worth studying.

#### Milvus/Zilliz (Go + C++, Open Source)
- **Architecture:** Disaggregated storage/compute. Stream processing (Streaming Node) separated from batch (Query Node, Data Node). MixCoord unifies coordinators. Supports Faiss, HNSW, DiskANN, ScaNN as index backends.
- **Performance:** Sub-50ms latency for 100M vectors at 99% recall. Single-digit ms for million-scale. 1.5x better QPS than Pinecone.
- **Storage Format V2:** Adaptive columnar layout with "narrow column merging + wide column independence" solving point lookup bottlenecks.
- **Weaknesses:** Complex multi-component architecture (operational overhead), Go + C++ codebase is harder to optimize holistically, relies on third-party index libraries rather than owning the algorithm.
- **Key insight:** Their multi-index backend approach (choose HNSW/DiskANN/ScaNN per collection) is pragmatic but means they're only as good as the underlying algorithm.

#### Weaviate (Go, Open Source)
- **Architecture:** Go-based, module system where vectorizers are containerized ML models with Go hooks. Uses HNSW for vector index.
- **Performance:** 10-NN search in <100ms for million-scale. GOMEMLIMIT and GOMAXPROCS for resource control.
- **Weaknesses:** Go's GC pauses can affect tail latency. Module system adds latency for vectorization. Not as performant as Rust-based alternatives for raw search. Limited index type options.
- **Key insight:** Module system is good for developer experience but adds latency. Go is the wrong language for this workload.

#### Chroma (Python -> Rust rewrite in progress)
- **Architecture:** Originally Python with SQLite backend and HNSW via hnswlib. 2025 Rust-core rewrite delivered 4x faster write and query, eliminated GIL bottleneck.
- **Weaknesses:** Single-node architecture, limited configuration options, no native reranking, horizontal scaling requires significant engineering effort. Designed for prototyping, not production scale.
- **Key insight:** Developer experience is excellent but it's not a serious contender for performance. The Rust rewrite validates Rust as the right choice.

#### LanceDB (Rust, Open Source)
- **Architecture:** Built entirely in Rust. Uses Lance columnar format, Apache Arrow for in-memory ops, DataFusion for query execution. Disk-native design using Product Quantization with IVF-PQ indexes.
- **Performance:** 100K pairs of 1000-dim vectors in <20ms. Handles 200M+ vectors, approaching billion-scale.
- **Strengths:** Zero-copy disk-native design, embedded deployment (no server), multi-modal (images, text, video).
- **Weaknesses:** Relatively young, smaller community. IVF-PQ accuracy vs HNSW. Less flexible than graph-based indexes.
- **Key insight:** Disk-native + columnar is the right idea for cost efficiency. Their Lance format is worth studying.

### 1.2 PostgreSQL Extensions

#### pgvector (C, Open Source)
- **Architecture:** C extension for PostgreSQL. Supports IVFFlat and HNSW index types. Integrated with PostgreSQL's storage and transaction system.
- **HNSW:** Standard implementation with parallel builds, scalar/binary quantization, SIMD optimizations (10-150x speedups since 0.6.0).
- **Performance:** Workable for <10M vectors. Beyond 50-100M, hits throughput and latency limits.
- **Critical Limitations:**
  - HNSW index build for 17M vectors at 1536 dims: crashes after ~2 hours
  - Memory consumption grows non-linearly with dimensionality
  - When HNSW index exceeds buffer cache: exponential performance degradation
  - Unpredictable latency spikes as PostgreSQL's relational engine manages a graph-based memory store
- **Key insight:** pgvector proves there's massive demand for vector search in PostgreSQL. Its C implementation is the bottleneck — it can't leverage modern Rust safety guarantees or easily adopt new algorithms.

#### pgvectorscale (Rust/pgrx, Timescale)
- **Architecture:** Rust extension using the PGRX framework. Implements StreamingDiskANN (based on Microsoft's DiskANN/Vamana) instead of HNSW. Disk-optimized index.
- **Innovation:** Statistical Binary Quantization (better than standard BQ), filtered DiskANN for combined vector + label search.
- **Performance:**
  - 28x lower p95 latency vs Pinecone s1 at 99% recall
  - 16x higher throughput vs Pinecone s1
  - 471 QPS at 99% recall on 50M vectors (11.4x better than Qdrant's 41 QPS)
  - 75% cheaper than Pinecone when self-hosted
- **Weaknesses:** Still DiskANN — single medoid entry point, doesn't solve the parent-finding problem optimally. Tail latency worse than Qdrant (Qdrant has 39% lower p95, 48% lower p99).
- **Key insight:** This is the closest competitor to what pgrx could build. They proved Rust + PGRX + novel algorithm can beat both pgvector AND dedicated vector DBs. But DiskANN has its own entry point limitations.

#### Lantern (C++/Usearch, Open Source)
- **Architecture:** Uses Usearch (optimized C++ HNSW). Key innovation: external indexing — build the index outside PostgreSQL using lantern-cli, import as file.
- **Performance:** 90x faster index creation than pgvector via external parallel indexing.
- **Weaknesses:** Still HNSW underneath. External indexing is a build-time optimization, not a search-time one.
- **Key insight:** External indexing is clever for build performance. pgrx could offer this too.

#### pg_embedding (Neon, C)
- **Architecture:** Neon's HNSW extension based on ivf-hnsw (billion-scale NN search system).
- **Performance:** 20x speed improvement over pgvector at 99% recall.
- **Status:** Largely superseded by pgvector's own HNSW support. Neon now recommends pgvector.

### 1.3 Current SOTA Benchmark Numbers

| System | Dataset | Recall | QPS | Latency | Notes |
|--------|---------|--------|-----|---------|-------|
| ScyllaDB | 1B vec, 96d | 70% | 252,000 | p99=1.7ms | 3x r7i.48xlarge (576 vCPUs total) |
| ScyllaDB | 1B vec, 96d | 98% | 16,600 | p99<20ms | Same hardware |
| pgvectorscale | 50M vec, 768d | 99% | 471 | - | Single node |
| Qdrant | 50M vec, 768d | 99% | 41 | - | Single node |
| Couchbase | 1B vec, 128d | 93% | 703 | 369ms | - |
| Couchbase | 100M vec, 768d | 66% | 19,057 | 28ms | Speed-optimized |
| VSAG (Ant Group) | GIST1M | 90% | 2,167 | - | 4.2x faster than HNSWlib |
| delta-EMG | SIFT1M | 99% | 19,000 | - | k=1, 1.2-3.2x faster than baselines |

**Critical gap:** No published billion-scale benchmarks at 1536 dimensions (OpenAI embedding size). Most benchmarks use 96-768 dims. At 1536 dims, memory bandwidth becomes the dominant bottleneck.

---

## 2. THE "PARENT FINDING" PROBLEM

### 2.1 What Is It?

In any graph-based ANN index, search is a two-phase process:
1. **Routing phase (finding the parent):** Navigate from some entry point to the right neighborhood/cluster of the query vector
2. **Refinement phase (everything else is simple):** Once in the right neighborhood, greedily explore nearby nodes to find actual nearest neighbors

Your friend is exactly right: the routing phase is the hard part. The refinement phase is well-solved by greedy beam search.

### 2.2 How Each Algorithm Handles It

#### HNSW: Multi-Layer Navigation
- **Mechanism:** Fixed entry point at top layer. Greedy search to local minimum at each layer. Descend. Beam search (width=efSearch) at layer 0.
- **The problem:**
  - Entry point is fixed (usually the first node inserted) — terrible for skewed distributions
  - Upper layers are randomly sampled (exponential decay probability) — no principled routing
  - If the greedy path takes a wrong turn in upper layers, you're stuck in a local minimum
  - The "Steiner-hardness" paper (VLDB 2024) proved that query difficulty varies vastly and distance-based hardness measures (like LID) fail because they ignore graph structure
- **Cost:** O(log n) layers * O(M * efSearch) distance computations per layer

#### DiskANN/Vamana: Medoid Entry Point
- **Mechanism:** Single flat graph. Entry point is the medoid (point with minimum average distance to all others). Greedy search with backtracking.
- **The problem:**
  - Single entry point — slightly better than HNSW's arbitrary choice, but still one point for all queries
  - Flat graph means longer traversal paths than HNSW's hierarchical shortcuts
  - Edge pruning promotes angular diversity but doesn't guarantee monotonic paths
- **Cost:** O(log n) hops on average, but worst case much worse

#### IVFFlat / ScaNN: Partition-Based Routing
- **Mechanism:** Cluster the dataset into Voronoi cells. At query time, find the closest centroids (nprobe cells) and search within them.
- **The problem:**
  - "Finding the parent" = finding the right cluster. With k=1000 clusters, you search ~10-50 (nprobe).
  - Cluster boundaries are hard: queries near boundaries need multiple clusters (expensive)
  - ScaNN improves this with anisotropic quantization and SOAR (spilling with orthogonality-amplified residuals) for redundant cluster assignment
- **Cost:** O(k) centroid comparisons + O(nprobe * n/k) within-cluster search

#### RoarGraph: Query-Aware Graph Construction
- **Mechanism:** Uses query distribution knowledge during index construction. Projects query vectors onto their nearest base vectors to create connections.
- **The problem:** Requires knowing the query distribution at index time. If the distribution shifts, the index degrades.

### 2.3 Cutting-Edge Solutions

#### CatapultDB (2025) — Dynamic Workload-Aware Shortcuts
- **Core idea:** Real-world queries have spatial/temporal locality. After each search, record the best result's location in an LSH bucket. Future similar queries use these as superior entry points.
- **Performance:** 2.51x higher throughput vs vanilla DiskANN. Reduces nodes visited by 66.3%, distance computations by 63.5%.
- **Limitation:** Needs warmup (cold start problem). Only helps for workloads with locality.
- **Relevance to pgrx:** This is a runtime optimization layer, not a fundamental algorithm change. Easy to add on top of any graph.

#### delta-EMG (2025) — Monotonic Graph Guarantee
- **Core idea:** Enforce a delta-monotonic constraint during construction: any greedy search path must have strictly decreasing distances to the query. No local minima possible.
- **Theoretical guarantee:** Search converges to a (1/delta)-approximate neighbor without backtracking.
- **Performance:** 19,000 QPS at 99% recall on SIFT1M (k=1). 50-80% reduction in distance computations vs baselines.
- **Limitation:** Construction is more expensive. The monotonic guarantee may increase graph degree.
- **Relevance to pgrx:** This is the most promising fundamental advance. It directly solves the parent-finding problem by making it impossible to get stuck.

#### VSAG (Ant Group, VLDB 2025) — System-Level Optimization
- **Three optimizations:**
  1. Deterministic Access Greedy Search with prefetching: reduces L3 cache misses from 94.46% to 39.23%
  2. Automated parameter tuning via GBDT classifier + edge labeling (encode multiple configs in one index)
  3. Scalar quantization (SQ4) with selective re-ranking
- **Performance:** 4.2x speedup over HNSWlib at same accuracy.
- **Relevance to pgrx:** These are engineering optimizations that should be standard in any production implementation. Not algorithmic breakthroughs, but massive real-world impact.

#### MARGO (VLDB 2025) — Monotonic Path-Aware Graph Layout
- **Core idea:** Optimize disk layout by weighing edges based on their importance in monotonic paths.
- **Performance:** 26.6% improvement in search efficiency over SOTA (Starling), 5.5x faster layout optimization.
- **Relevance to pgrx:** Critical for disk-based indexes. Layout matters more than algorithm for IO-bound workloads.

#### Dual-Branch HNSW with LID-Driven Optimization (2025)
- **Core idea:** Two parallel HNSW branches with LID-based insertion, enabling traversal from multiple directions to mitigate local optima.
- **Relevance to pgrx:** Interesting but complex. Multiple entry points from different graph "perspectives" is a sound principle.

#### GPU-Accelerated CAGRA (NVIDIA cuVS)
- **Performance:** 12.3x faster index build, 18x higher search throughput, 4.7x faster online search vs CPU.
- **Relevance to pgrx:** GPU offloading for distance computation is the future for large-scale deployments, but adds hardware dependency.

### 2.4 The Fundamental Insight

The parent-finding problem reduces to: **given a query vector q, find the region of the graph where q's true nearest neighbors live, in sublinear time, with high probability.**

Current approaches:
- HNSW: Random hierarchical shortcuts (probabilistic, no guarantees)
- DiskANN: Single medoid (data-dependent but query-blind)
- IVF/ScaNN: Centroid comparison (O(k) brute force on centroids)
- CatapultDB: Workload-aware shortcuts (dynamic but needs warmup)
- delta-EMG: Monotonic guarantees (construction-time, no routing shortcuts)

**What nobody has done:** Combined fast routing (like IVF centroids organized in a tree) with graph-based refinement (like HNSW/Vamana layer 0), where the routing structure is itself adaptive.

---

## 3. THE OPPORTUNITY FOR PGRX

### 3.1 Architecture Proposal: Hybrid Adaptive Routing + Monotonic Graph

**Core idea: B-tree on cluster centroids -> monotonic graph refinement**

```
Query arrives
    |
    v
[Phase 1: O(log k) Routing via B-tree on Centroids]
    |-- Cluster centroids stored in a B-tree (PostgreSQL native!)
    |-- Find top-m nearest centroids in O(log k) time
    |-- Each centroid points to a graph partition
    |
    v
[Phase 2: Parallel Graph Refinement]
    |-- Enter top-m partitions simultaneously
    |-- Each partition is a delta-monotonic graph (no local minima)
    |-- BEAM processes: one per partition, parallel beam search
    |-- Merge results from all partitions
    |
    v
[Phase 3: Exact Re-ranking]
    |-- Top candidates re-scored with full-precision vectors
    |-- Return final k results
```

### 3.2 Why This Could Break the Industry

**The routing problem is solved differently than anyone else:**

1. **B-tree on centroids (O(log k) instead of O(k)):** IVF/ScaNN do linear scan of all centroids. A B-tree (or ball tree) on centroids gives logarithmic routing. PostgreSQL already has world-class B-tree infrastructure. pgrx can leverage this natively.

2. **Monotonic graph partitions (no local minima):** Within each partition, use delta-EMG-style construction to guarantee greedy convergence. This eliminates the #1 source of search quality loss.

3. **BEAM-powered parallel partition search:** This is pgrx's secret weapon. Spawn one lightweight BEAM process per candidate partition. Each process does independent beam search. No locks, no shared memory, pure message passing. The BEAM scheduler handles load balancing across cores automatically. This turns the multi-probe problem from sequential to truly parallel with zero synchronization overhead.

4. **Adaptive routing via CatapultDB-style learning:** Record successful search paths in the B-tree. Over time, the routing structure learns the workload distribution. Hot regions get more precise centroids.

5. **Rust SIMD for distance computation:** The inner loop (distance computation) runs in Rust NIF with AVX-512/NEON, using SimSIMD-style optimizations. This is where the raw speed comes from.

6. **io_uring for disk-based operation:** For datasets that don't fit in memory, use io_uring for async I/O with prefetching (VSAG-style), overlapping I/O with computation.

### 3.3 Concrete Performance Targets

**Current best (approximate, normalized to 1M vectors, 1536 dims, 99% recall):**
- pgvectorscale: ~500 QPS (extrapolated from 50M benchmark)
- Qdrant: ~200 QPS (extrapolated)
- VSAG: ~2,000 QPS (GIST1M, 960 dims, 90% recall — would be lower at 1536d/99% recall)
- delta-EMG: ~19,000 QPS (SIFT1M, 128 dims, 99% recall — would be much lower at 1536d)

**Scaling factor for 1536 dims vs 128 dims:** Roughly 10-12x slower due to:
- Distance computation: 12x more FLOPs
- Memory bandwidth: 12x more bytes per vector access
- Cache efficiency: fewer vectors per cache line

**Realistic SOTA at 1M vectors, 1536 dims, 99% recall:** ~2,000-5,000 QPS on modern hardware (single node, 32 cores, 128GB RAM).

**pgrx target: 10x = 20,000-50,000 QPS** at the same recall. How:

| Optimization | Expected Speedup | Mechanism |
|---|---|---|
| Monotonic graph (no backtracking) | 2-3x | 50-80% fewer distance computations (delta-EMG paper) |
| B-tree routing (O(log k) vs O(k)) | 1.5-2x | Faster entry point selection, fewer wasted hops |
| BEAM parallel partition search | 2-4x | True parallelism across partitions with zero sync overhead |
| VSAG-style prefetching | 1.5-2x | L3 cache miss reduction from 94% to ~40% |
| Rust SIMD (AVX-512) distance | 1.5-2x | SimSIMD-level optimization in the hot loop |
| SQ4 quantization + selective reranking | 1.3-1.5x | Cheaper distance computations for initial candidates |

**Combined theoretical speedup: 14-96x** (multiplicative, but real-world is always less due to Amdahl's law). **Conservative estimate: 10-20x.**

### 3.4 Where the Bottleneck Actually Is

At 1536 dimensions:

1. **Memory bandwidth is king.** A single 1536-dim float32 vector = 6KB. Reading 1000 candidates = 6MB. L3 cache is typically 32-64MB. The entire search fits in L3 cache for million-scale, but cache misses during graph traversal dominate.

2. **Distance computation is secondary.** With AVX-512, a 1536-dim L2 distance takes ~100ns. At 1000 candidates, that's 100us of pure compute. But memory stalls can 10x this.

3. **Graph traversal pattern matters most.** Random access patterns in HNSW cause L3 cache miss rates of 94.46% (VSAG paper). VSAG's prefetching reduces this to 39.23%. This is where the biggest gains are.

4. **For billion-scale (disk-based):** I/O dominates. DiskANN spends 92.5% of query time on disk I/O. io_uring + prefetching + graph layout optimization (MARGO) are essential.

### 3.5 Hardware Assumptions

- **CPU:** Intel Sapphire Rapids / AMD Genoa with AVX-512 (or NEON on ARM)
- **Memory:** DDR5, 128GB+ for million-scale in-memory
- **Storage:** NVMe Gen4+ for billion-scale disk-based
- **No GPU required** for base performance (GPU is optional accelerator for index build)

### 3.6 What "Breaking" the Industry Means

1. **A PostgreSQL extension that beats Pinecone** — pgvectorscale already did this on throughput. pgrx needs to do it on BOTH throughput AND tail latency.

2. **A PostgreSQL extension that beats Qdrant on tail latency** — This is the harder target. Qdrant's Rust engineering is excellent. pgrx's advantage is the BEAM scheduler for parallel partition search (eliminates thread contention that Rust async runtimes still have).

3. **The first vector index with provable search quality guarantees** — delta-EMG's monotonic guarantee is a theoretical breakthrough. Implementing this in production (in a PostgreSQL extension) would be unprecedented.

4. **10x QPS at 99% recall** — The headline number that makes people switch. At 1M vectors, 1536 dims: from ~2,000 QPS to ~20,000 QPS on the same hardware.

---

## 4. PAPER REFERENCES

### Foundational
- Malkov & Yashunin, "Efficient and Robust ANN Search using HNSW Graphs" (2016/2018) — [arXiv:1603.09320](https://arxiv.org/abs/1603.09320)
- Subramanya et al., "DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node" (NeurIPS 2019)

### Cutting-Edge (2024-2025)
- **delta-EMG:** "A Monotonic Graph Index for Approximate Nearest Neighbor Search" (2025) — [arXiv:2511.16921](https://arxiv.org/abs/2511.16921) — Provable monotonic convergence, 19K QPS at 99% recall
- **VSAG:** "An Optimized Search Framework for Graph-based ANN Search" (VLDB 2025) — [arXiv:2503.17911](https://arxiv.org/abs/2503.17911) — 4.2x over HNSWlib via prefetching + auto-tuning
- **CatapultDB:** "Catapults to the Rescue: Accelerating Vector Search by Exploiting Query Locality" (2025) — [arXiv:2603.02164](https://arxiv.org/abs/2603.02164) — Dynamic workload-aware shortcuts, 2.51x throughput
- **MARGO:** "Select Edges Wisely: Monotonic Path Aware Graph Layout" (VLDB 2025) — Disk layout optimization, 26.6% improvement
- **Steiner-hardness:** "A Query Hardness Measure for Graph-Based ANN Indexes" (VLDB 2024) — [arXiv:2408.13899](https://arxiv.org/abs/2408.13899) — Proves query difficulty is graph-structure-dependent
- **SOAR:** "Improved Indexing for ANN Search" (NeurIPS 2024) — [arXiv:2404.00774](https://arxiv.org/abs/2404.00774) — Orthogonality-amplified redundancy for ScaNN
- **GASS:** "Graph-Based Vector Search: An Experimental Evaluation of the State-of-the-Art" (SIGMOD 2025) — [arXiv:2502.05575](https://arxiv.org/abs/2502.05575) — Comprehensive 12-algorithm comparison, incremental insertion + neighborhood diversification wins
- **BAMG:** "Block-Aware Monotonic Graph Index for Disk-Based ANN" (2025) — [arXiv:2509.03226](https://arxiv.org/abs/2509.03226)
- **Dual-Branch HNSW:** with LID-Driven Optimization (OpenReview 2025) — Multi-directional traversal to escape local optima
- **Dynamic HNSW (2026):** Adaptive M/ef parameters based on local data density — 33% build time reduction, 32% memory reduction

### Industry Systems
- **RoarGraph:** Projected bipartite graph for cross-modal ANN (VLDB 2024) — Query-distribution-aware construction
- **ParaGraph (2025):** Matches/beats RoarGraph for cross-modal search
- **GaussDB-Vector:** Large-scale persistent real-time vector database (VLDB 2025) — Adaptive hybrid index, incremental redistribution

---

## 5. EXECUTIVE SUMMARY

### The Market Gap

Every existing solution has one of these problems:
1. **pgvector:** C implementation, HNSW only, crashes at scale, poor memory management
2. **pgvectorscale:** Good (Rust/PGRX, DiskANN), but single entry point, no parallel partition search
3. **Qdrant/Milvus/Weaviate:** Great standalone DBs, but require separate infrastructure — can't query your relational data and vectors together
4. **Pinecone:** Vendor lock-in, expensive, not open-source

### pgrx's Unique Position

pgrx is the only project that can combine:
- **BEAM concurrency** for parallel graph traversal (no other vector DB has this)
- **Rust SIMD** for distance computation (matching Qdrant's inner loop)
- **PostgreSQL integration** for combined relational + vector queries (matching pgvector's convenience)
- **Novel algorithms** (delta-EMG monotonic graphs, B-tree routing) that go beyond what anyone has shipped

### The Three Things to Build (in order)

1. **Monotonic graph index with B-tree routing** — The algorithmic breakthrough. O(log k) entry point selection + guaranteed convergence. This is the "no local minima" selling point.

2. **BEAM-parallel partition search** — The systems breakthrough. One BEAM process per partition, true parallelism, zero-synchronization merge. This is what makes pgrx fundamentally faster than Rust-only solutions.

3. **VSAG-style system optimizations** — The engineering baseline. Prefetching, SQ4 quantization, io_uring for disk. These are table stakes for production performance.

### The Headline Claim

> "pgrx vector search: the first PostgreSQL extension with provable search quality guarantees and 10x the throughput of any existing solution."

This is achievable. The algorithms exist in papers. The systems infrastructure exists in BEAM + Rust. Nobody has combined them.
