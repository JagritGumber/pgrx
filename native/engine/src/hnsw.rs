//! HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search.
//!
//! Pure-Rust implementation with no external dependencies.
//! Parameters: M=16, M_MAX_0=32, ef_construction=64.
//! Supports L2, cosine, and inner product distance metrics.

use std::collections::{BinaryHeap, HashSet};

/// HNSW graph parameters.
const M: usize = 16;
const M_MAX_0: usize = 32;
const EF_CONSTRUCTION: usize = 64;
// ML = 1/ln(M) — computed at runtime since ln() is not const
fn ml() -> f64 {
    1.0 / (M as f64).ln()
}

/// Distance metric for vector comparisons.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DistanceMetric {
    L2,
    Cosine,
    InnerProduct,
}

/// A node in the HNSW graph.
struct HnswNode {
    row_id: usize,
    vector: Vec<f32>,
    neighbors: Vec<Vec<usize>>, // neighbors[level] = list of neighbor node IDs
    #[allow(dead_code)]
    level: usize,               // max level for this node (used for debugging)
}

/// The HNSW index.
pub struct HnswIndex {
    nodes: Vec<HnswNode>,
    entry_point: Option<usize>,
    max_level: usize,
    metric: DistanceMetric,
    col_idx: usize,
    rng_state: u64, // simple RNG state for level generation
}

impl HnswIndex {
    pub fn new(metric: DistanceMetric, col_idx: usize) -> Self {
        // Seed from a combination of address and a counter to get diversity
        use std::time::SystemTime;
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            nodes: Vec::new(),
            entry_point: None,
            max_level: 0,
            metric,
            col_idx,
            rng_state: seed | 1, // ensure odd for better LCG behavior
        }
    }

    pub fn col_idx(&self) -> usize {
        self.col_idx
    }

    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Insert a vector into the index, associated with the given row_id.
    pub fn insert(&mut self, row_id: usize, vector: Vec<f32>) {
        let level = self.random_level();
        let node_id = self.nodes.len();

        self.nodes.push(HnswNode {
            row_id,
            vector,
            neighbors: (0..=level).map(|_| Vec::new()).collect(),
            level,
        });

        if self.entry_point.is_none() {
            self.entry_point = Some(node_id);
            self.max_level = level;
            return;
        }

        let mut current = self.entry_point.unwrap();

        // Phase 1: Greedy descent from top level to node's level + 1
        for l in (level.saturating_add(1)..=self.max_level).rev() {
            current = self.greedy_closest(current, &self.nodes[node_id].vector, l);
        }

        // Phase 2: Insert at each level from min(node_level, max_level) down to 0
        let top = level.min(self.max_level);
        for l in (0..=top).rev() {
            let neighbors =
                self.search_layer(current, &self.nodes[node_id].vector, EF_CONSTRUCTION, l);
            let m = if l == 0 { M_MAX_0 } else { M };

            // Select M closest neighbors
            let selected: Vec<usize> = neighbors.into_iter().take(m).map(|(_, id)| id).collect();

            // Connect bidirectionally
            self.nodes[node_id].neighbors[l] = selected.clone();
            for &neighbor_id in &selected {
                self.nodes[neighbor_id].neighbors[l].push(node_id);
                if self.nodes[neighbor_id].neighbors[l].len() > m {
                    self.prune_neighbors(neighbor_id, l, m);
                }
            }

            if !selected.is_empty() {
                current = selected[0];
            }
        }

        // Update entry point if new node has higher level
        if level > self.max_level {
            self.entry_point = Some(node_id);
            self.max_level = level;
        }
    }

    /// Search for K nearest neighbors. Returns (distance, row_id) sorted ascending.
    pub fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<(f32, usize)> {
        if self.entry_point.is_none() || self.nodes.is_empty() {
            return Vec::new();
        }

        let entry = self.entry_point.unwrap();
        let mut current = entry;

        // Greedy descent to layer 0
        for l in (1..=self.max_level).rev() {
            current = self.greedy_closest(current, query, l);
        }

        // Search at layer 0 with ef_search beam width
        let candidates = self.search_layer(current, query, ef_search.max(k), 0);

        // Return top K, mapped to row_ids
        candidates
            .into_iter()
            .take(k)
            .map(|(dist, node_id)| (dist, self.nodes[node_id].row_id))
            .collect()
    }

    /// Greedy closest traversal at a given level.
    fn greedy_closest(&self, start: usize, query: &[f32], level: usize) -> usize {
        let mut current = start;
        let mut current_dist = self.distance(query, &self.nodes[current].vector);

        loop {
            let mut changed = false;
            if level < self.nodes[current].neighbors.len() {
                for &neighbor in &self.nodes[current].neighbors[level] {
                    let d = self.distance(query, &self.nodes[neighbor].vector);
                    if d < current_dist {
                        current = neighbor;
                        current_dist = d;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        current
    }

    /// Beam search at a single layer. Returns (distance, node_id) sorted ascending.
    fn search_layer(
        &self,
        start: usize,
        query: &[f32],
        ef: usize,
        level: usize,
    ) -> Vec<(f32, usize)> {
        let mut visited = HashSet::new();
        // Min-heap of candidates (closest first)
        let mut candidates: BinaryHeap<std::cmp::Reverse<(OrdF32, usize)>> = BinaryHeap::new();
        // Max-heap of results (farthest first for easy pruning)
        let mut results: BinaryHeap<(OrdF32, usize)> = BinaryHeap::new();

        let start_dist = self.distance(query, &self.nodes[start].vector);
        visited.insert(start);
        candidates.push(std::cmp::Reverse((OrdF32(start_dist), start)));
        results.push((OrdF32(start_dist), start));

        while let Some(std::cmp::Reverse((OrdF32(c_dist), c_id))) = candidates.pop() {
            let worst_result = results.peek().map(|(OrdF32(d), _)| *d).unwrap_or(f32::MAX);
            if c_dist > worst_result && results.len() >= ef {
                break;
            }

            if level < self.nodes[c_id].neighbors.len() {
                for &neighbor in &self.nodes[c_id].neighbors[level] {
                    if visited.insert(neighbor) {
                        let d = self.distance(query, &self.nodes[neighbor].vector);
                        let worst =
                            results.peek().map(|(OrdF32(d), _)| *d).unwrap_or(f32::MAX);
                        if d < worst || results.len() < ef {
                            candidates.push(std::cmp::Reverse((OrdF32(d), neighbor)));
                            results.push((OrdF32(d), neighbor));
                            if results.len() > ef {
                                results.pop(); // remove farthest
                            }
                        }
                    }
                }
            }
        }

        let mut sorted: Vec<(f32, usize)> = results
            .into_iter()
            .map(|(OrdF32(d), id)| (d, id))
            .collect();
        sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        sorted
    }

    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::L2 => {
                a.iter()
                    .zip(b.iter())
                    .map(|(x, y)| {
                        let d = x - y;
                        d * d
                    })
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceMetric::Cosine => {
                let (dot, na, nb) =
                    a.iter()
                        .zip(b.iter())
                        .fold((0.0f32, 0.0f32, 0.0f32), |(d, na, nb), (x, y)| {
                            (d + x * y, na + x * x, nb + y * y)
                        });
                let denom = na.sqrt() * nb.sqrt();
                if denom == 0.0 {
                    1.0
                } else {
                    1.0 - dot / denom
                }
            }
            DistanceMetric::InnerProduct => {
                let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                -dot // negative so ascending order = highest inner product first
            }
        }
    }

    fn random_level(&mut self) -> usize {
        let r = self.rand_f64();
        if r <= 0.0 {
            return 0;
        }
        (-r.ln() * ml()) as usize
    }

    /// Simple xorshift64 PRNG — deterministic per-index, no external deps.
    fn rand_f64(&mut self) -> f64 {
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;
        (self.rng_state as f64) / (u64::MAX as f64)
    }

    fn prune_neighbors(&mut self, node_id: usize, level: usize, max_neighbors: usize) {
        let node_vec = self.nodes[node_id].vector.clone();
        let mut neighbors_with_dist: Vec<(f32, usize)> = self.nodes[node_id].neighbors[level]
            .iter()
            .map(|&n| (self.distance(&node_vec, &self.nodes[n].vector), n))
            .collect();
        neighbors_with_dist
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        neighbors_with_dist.truncate(max_neighbors);
        self.nodes[node_id].neighbors[level] =
            neighbors_with_dist.into_iter().map(|(_, id)| id).collect();
    }
}

/// Ordered f32 wrapper for BinaryHeap (f32 doesn't implement Ord).
#[derive(Clone, Copy, PartialEq)]
struct OrdF32(f32);

impl Eq for OrdF32 {}

impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hnsw_insert_and_search_basic() {
        let mut idx = HnswIndex::new(DistanceMetric::L2, 0);
        // Insert 5 simple 3D vectors
        idx.insert(0, vec![0.0, 0.0, 0.0]);
        idx.insert(1, vec![1.0, 0.0, 0.0]);
        idx.insert(2, vec![0.0, 1.0, 0.0]);
        idx.insert(3, vec![1.0, 1.0, 0.0]);
        idx.insert(4, vec![10.0, 10.0, 10.0]);

        // Search for nearest to [0.1, 0.1, 0.0] — should be row 0
        let results = idx.search(&[0.1, 0.1, 0.0], 2, 16);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, 0); // row_id 0 is closest
    }

    #[test]
    fn hnsw_empty_search() {
        let idx = HnswIndex::new(DistanceMetric::L2, 0);
        let results = idx.search(&[1.0, 2.0], 5, 16);
        assert!(results.is_empty());
    }

    #[test]
    fn hnsw_single_element() {
        let mut idx = HnswIndex::new(DistanceMetric::L2, 0);
        idx.insert(42, vec![1.0, 2.0, 3.0]);
        let results = idx.search(&[1.0, 2.0, 3.0], 5, 16);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 42);
        assert!(results[0].0 < 1e-6); // distance should be ~0
    }

    #[test]
    fn hnsw_cosine_metric() {
        let mut idx = HnswIndex::new(DistanceMetric::Cosine, 0);
        idx.insert(0, vec![1.0, 0.0]);
        idx.insert(1, vec![0.0, 1.0]);
        idx.insert(2, vec![0.707, 0.707]);

        // Nearest to [1, 0] by cosine: row 0 (distance=0), then row 2
        let results = idx.search(&[1.0, 0.0], 3, 16);
        assert_eq!(results[0].1, 0);
    }

    #[test]
    fn hnsw_inner_product_metric() {
        let mut idx = HnswIndex::new(DistanceMetric::InnerProduct, 0);
        idx.insert(0, vec![1.0, 0.0, 0.0]);
        idx.insert(1, vec![3.0, 2.0, 1.0]);

        // Inner product with [1,0,0]: row 0=1, row 1=3
        // Negative IP: row 0=-1, row 1=-3
        // Ascending: row 1 first (highest IP)
        let results = idx.search(&[1.0, 0.0, 0.0], 2, 16);
        assert_eq!(results[0].1, 1); // highest inner product
    }

    #[test]
    fn hnsw_100_vectors_recall() {
        let mut idx = HnswIndex::new(DistanceMetric::L2, 0);
        let dim = 16;
        let n = 100;

        // Insert deterministic vectors
        let vectors: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                (0..dim)
                    .map(|d| ((i * 7 + d * 13) as f32 * 0.01) % 1.0)
                    .collect()
            })
            .collect();

        for (i, v) in vectors.iter().enumerate() {
            idx.insert(i, v.clone());
        }

        let query: Vec<f32> = vec![0.5; dim];
        let k = 5;

        // Brute-force ground truth
        let mut brute: Vec<(f32, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let d: f32 = v
                    .iter()
                    .zip(query.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();
                (d, i)
            })
            .collect();
        brute.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let truth: HashSet<usize> = brute.iter().take(k).map(|(_, id)| *id).collect();

        // HNSW search with high ef for better recall
        let hnsw_results = idx.search(&query, k, 64);
        let hnsw_ids: HashSet<usize> = hnsw_results.iter().map(|(_, id)| *id).collect();

        let recall = truth.intersection(&hnsw_ids).count() as f32 / k as f32;
        assert!(
            recall >= 0.8,
            "HNSW recall {:.0}% is below 80% threshold",
            recall * 100.0
        );
    }
}
