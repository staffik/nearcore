use std::collections::{HashMap, HashSet};

use lru::LruCache;
use near_chain_configs::default_orphan_state_witness_pool_size;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::{BlockHeight, ShardId};

use crate::metrics;

/// We keep only orphan witnesses that are within this distance of
/// the current chain head. This helps to reduce the size of
/// OrphanStateWitnessPool and protects against spam attacks.
pub const MAX_ORPHAN_WITNESS_DISTANCE_FROM_HEAD: BlockHeight = 5;

/// We keep only orphan witnesses which are smaller than this size.
/// This limits the maximum memory usage of OrphanStateWitnessPool.
pub const MAX_ORPHAN_WITNESS_SIZE: usize = 16_000_000;

/// `OrphanStateWitnessPool` is used to keep orphaned ChunkStateWitnesses until it's possible to process them.
/// To process a ChunkStateWitness we need to have the previous block, but it might happen that a ChunkStateWitness
/// shows up before the block is available. In such cases the witness is put in `OrphanStateWitnessPool` until the
/// required block arrives and the witness can be processed.
pub struct OrphanStateWitnessPool {
    witness_cache: LruCache<(ShardId, BlockHeight), CacheEntry>,
    /// List of orphaned witnesses that wait for this block to appear.
    /// Maps block hash to entries in `witness_cache`.
    /// Must be kept in sync with `witness_cache`.
    waiting_for_block: HashMap<CryptoHash, HashSet<(ShardId, BlockHeight)>>,
}

struct CacheEntry {
    pub witness: ChunkStateWitness,
    pub witness_size: usize,
}

impl OrphanStateWitnessPool {
    /// Create a new `OrphanStateWitnessPool` with a capacity of `cache_capacity` witnesses.
    /// The `Default` trait implementation provides reasonable defaults.
    pub fn new(cache_capacity: usize) -> Self {
        OrphanStateWitnessPool {
            witness_cache: LruCache::new(cache_capacity),
            waiting_for_block: HashMap::new(),
        }
    }

    /// Add an orphaned chunk state witness to the pool. The witness will be put in a cache and it'll
    /// wait there for the block that's required to process it.
    /// It's expected that this `ChunkStateWitness` has gone through basic validation - including signature,
    /// shard_id, size and distance from the tip. The pool would still work without it, but without validation
    /// it'd be possible to fill the whole cache with spam.
    /// `witness_size` is only used for metrics, it's okay to pass 0 if you don't care about the metrics.
    pub fn add_orphan_state_witness(&mut self, witness: ChunkStateWitness, witness_size: usize) {
        if self.witness_cache.cap() == 0 {
            // A cache with 0 capacity doesn't keep anything.
            return;
        }

        // Insert the new ChunkStateWitness into the cache
        let chunk_header = &witness.inner.chunk_header;
        let prev_block_hash = *chunk_header.prev_block_hash();
        let cache_key = (chunk_header.shard_id(), chunk_header.height_created());
        let cache_entry = CacheEntry { witness, witness_size };
        self.on_entry_added(&cache_entry);
        if let Some((_, ejected_entry)) = self.witness_cache.push(cache_key, cache_entry) {
            // If another witness has been ejected from the cache due to capacity limit,
            // then remove the ejected witness from `waiting_for_block` to keep them in sync
            tracing::debug!(
                target: "client",
                witness_height = ejected_entry.witness.inner.chunk_header.height_created(),
                witness_shard = ejected_entry.witness.inner.chunk_header.shard_id(),
                "Ejecting orphaned ChunkStateWitness from the cache due to capacity limit. It will not be processed."
            );
            self.remove_from_waiting_for_block(&ejected_entry.witness);
            self.on_entry_removed(&ejected_entry);
        }

        // Add the new orphaned state witness to `waiting_for_block`
        self.waiting_for_block
            .entry(prev_block_hash)
            .or_insert_with(|| HashSet::new())
            .insert(cache_key);
    }

    fn remove_from_waiting_for_block(&mut self, witness: &ChunkStateWitness) {
        let chunk_header = &witness.inner.chunk_header;
        let waiting_set = self
            .waiting_for_block
            .get_mut(chunk_header.prev_block_hash())
            .expect("Every ejected witness must have a corresponding entry in waiting_for_block.");
        waiting_set.remove(&(chunk_header.shard_id(), chunk_header.height_created()));
        if waiting_set.is_empty() {
            self.waiting_for_block.remove(chunk_header.prev_block_hash());
        }
    }

    /// Find all orphaned witnesses that were waiting for this block and remove them from the pool.
    /// The block has arrived, so they can be now processed, they're no longer orphans.
    pub fn take_state_witnesses_waiting_for_block(
        &mut self,
        prev_block: &CryptoHash,
    ) -> Vec<ChunkStateWitness> {
        let Some(waiting) = self.waiting_for_block.remove(prev_block) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        for (shard_id, height) in waiting {
            // Remove this witness from `witness_cache` to keep them in sync
            let entry = self.witness_cache.pop(&(shard_id, height)).expect(
                "Every entry in waiting_for_block must have a corresponding witness in the cache",
            );
            self.on_entry_removed(&entry);

            result.push(entry.witness);
        }
        result
    }

    fn on_entry_added(&self, entry: &CacheEntry) {
        let shard_id_str = entry.witness.inner.chunk_header.shard_id().to_string();
        metrics::ORPHAN_CHUNK_STATE_WITNESSES_TOTAL_COUNT
            .with_label_values(&[shard_id_str.as_str()])
            .inc();
        metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_SIZE
            .with_label_values(&[shard_id_str.as_str()])
            .inc();
        metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_MEMORY_USED
            .with_label_values(&[shard_id_str.as_str()])
            .add(witness_size_to_i64(entry.witness_size));
    }

    fn on_entry_removed(&self, entry: &CacheEntry) {
        let shard_id_str = entry.witness.inner.chunk_header.shard_id().to_string();
        metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_SIZE
            .with_label_values(&[shard_id_str.as_str()])
            .dec();
        metrics::ORPHAN_CHUNK_STATE_WITNESS_POOL_MEMORY_USED
            .with_label_values(&[shard_id_str.as_str()])
            .sub(witness_size_to_i64(entry.witness_size));
    }
}

fn witness_size_to_i64(witness_size: usize) -> i64 {
    witness_size.try_into().expect(
        "Orphaned ChunkStateWitness size can't be converted to i64. \
    This should be impossible, is it over one exabyte in size?",
    )
}

impl Drop for OrphanStateWitnessPool {
    fn drop(&mut self) {
        // Clear the metrics when an `OrphanStateWitnessPool` is dropped
        for (_, entry) in self.witness_cache.iter() {
            self.on_entry_removed(entry);
        }
    }
}

impl Default for OrphanStateWitnessPool {
    fn default() -> OrphanStateWitnessPool {
        OrphanStateWitnessPool::new(default_orphan_state_witness_pool_size())
    }
}
