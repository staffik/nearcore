use std::collections::{HashMap, HashSet};

use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::{AccountId, BlockHeight, ShardId};

/// Identifies orphan state witnesses produced by a specific chunk producer on a shard.
type ChunkProducerId = (AccountId, ShardId);

/// `OrphanStateWitnessPool` is used to keep orphaned ChunkStateWitnesses until it's possible to process them.
/// To process a ChunkStateWitness we need to have the previous block, but it might happen that a ChunkStateWitness
/// shows up before the block is available. In such case the witness is put in `OrphanStateWitnessPool` until the
/// required block arrives and the witness can be processed.
pub struct OrphanStateWitnessPool {
    /// For each chunk_producer+shard_id, keep a small cache of orphaned witnesses produced by this
    /// chunk producer. Each chunk producer gets a separate small cache to prevent spam attacks.
    per_producer_caches: LruCache<ChunkProducerId, LruCache<BlockHeight, ChunkStateWitness>>,
    /// Capacity of one per-producer cache.
    single_producer_cache_capacity: usize,
    /// List of orphaned witnesses that wait for this block to appear.
    /// Maps block hash to entries in `per_producer_caches`.
    /// Must be kept in sync with `per_producer_caches`.
    waiting_for_block: HashMap<CryptoHash, HashSet<(ChunkProducerId, BlockHeight)>>,
}

impl OrphanStateWitnessPool {
    /// Create a new `OrphanStateWitnessPool`. For each (chunk_producer, shard_id) the pool
    /// keeps a cache of size `single_producer_cache_capacity`, which contains all orphaned state
    /// witnesses produced by this chunk producer on this shard. There will be capacity for at
    /// most `max_producer_caches` such caches.
    /// The `Default` trait implementation provides reasonable defaults.
    pub fn new(max_producer_caches: usize, single_producer_cache_capacity: usize) -> Self {
        OrphanStateWitnessPool {
            per_producer_caches: LruCache::new(max_producer_caches),
            single_producer_cache_capacity,
            waiting_for_block: HashMap::new(),
        }
    }

    /// Add an orphaned chunk state witness to the pool. The witness will be put in a cache and it'll
    /// wait there for the block that's required to process it.
    /// It's expected that this `ChunkStateWitness` has gone through basic validation - including signature,
    /// shard_id and size. The pool would still work without it, but without validation it'd be possible
    /// to fill the whole cache with spam.
    pub fn add_orphan_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        chunk_producer: AccountId,
    ) {
        if self.per_producer_caches.cap() == 0 || self.single_producer_cache_capacity == 0 {
            // A cache with 0 capacity doesn't keep anything.
            return;
        }

        let chunk_header = &witness.inner.chunk_header;
        let height = chunk_header.height_created();
        let prev_block_hash = *chunk_header.prev_block_hash();
        let chunk_producer_id = (chunk_producer, chunk_header.shard_id());

        match self.per_producer_caches.get_mut(&chunk_producer_id) {
            Some(chunk_producer_cache) => {
                // There's already a per_producer_cache for this chunk_producer, add an entry to it.
                let ejected = chunk_producer_cache.push(height, witness);

                // If a witness was ejected from the cache due to capacity limit, remove it from `waiting_for_block`.
                if let Some((_height, ejected_witness)) = ejected {
                    self.remove_from_waiting_for_block(chunk_producer_id.clone(), ejected_witness);
                }
            }
            None => {
                // There's no per_producer_cache for this chunk_producer, create a new per_producer_cache with one entry.
                let mut new_cache = LruCache::new(self.single_producer_cache_capacity);
                new_cache.put(height, witness);
                let ejected = self.per_producer_caches.push(chunk_producer_id.clone(), new_cache);

                // If some per_producer_cache has been ejected due to capacity limit, remove its witnesses from `waiting_for_block`.
                if let Some((ejected_chunk_producer_id, ejected_cache)) = ejected {
                    for (_height, ejected_witness) in ejected_cache {
                        self.remove_from_waiting_for_block(
                            ejected_chunk_producer_id.clone(),
                            ejected_witness,
                        );
                    }
                }
            }
        }

        // Add the new orphaned state witness to `waiting_for_block`
        self.waiting_for_block
            .entry(prev_block_hash)
            .or_insert_with(|| HashSet::new())
            .insert((chunk_producer_id, height));
    }

    fn remove_from_waiting_for_block(
        &mut self,
        chunk_producer_id: ChunkProducerId,
        witness: ChunkStateWitness,
    ) {
        let block_hash = witness.inner.chunk_header.prev_block_hash();
        let height = witness.inner.chunk_header.height_created();
        let waiting_set = self
            .waiting_for_block
            .get_mut(block_hash)
            .expect("Every ejected witness must have a corresponding entry in waiting_for_block.");
        waiting_set.remove(&(chunk_producer_id, height));
        if waiting_set.is_empty() {
            self.waiting_for_block.remove(block_hash);
        }
    }

    /// Find and all orphaned witnesses that were waiting for this block and remove them from the pool.
    /// The block has arrived, so they can be now processed, they're no longer orphans.
    pub fn take_state_witnesses_waiting_for_block(
        &mut self,
        prev_block: &CryptoHash,
    ) -> Vec<ChunkStateWitness> {
        let Some(waiting) = self.waiting_for_block.remove(prev_block) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        for (chunk_producer_id, height) in waiting {
            // Remove this witness from `per_producer_caches`
            let producer_cache = self.per_producer_caches.get_mut(&chunk_producer_id).expect(
                "Every entry in waiting_for_block must have a corresponding witness in the cache.",
            );
            let witness = producer_cache.pop(&height).expect(
                "Every entry in waiting_for_block must have a corresponding witness in the cache",
            );
            if producer_cache.is_empty() {
                self.per_producer_caches.pop(&chunk_producer_id);
            }

            result.push(witness);
        }
        result
    }
}

impl Default for OrphanStateWitnessPool {
    fn default() -> OrphanStateWitnessPool {
        OrphanStateWitnessPool::new(16, 3)
    }
}
