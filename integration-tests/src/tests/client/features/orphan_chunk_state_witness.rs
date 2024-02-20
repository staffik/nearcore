use std::collections::HashSet;

use near_chain::{Block, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client::{Client, ProcessingDoneTracker, ProcessingDoneWaiter};
use near_client::{HandleOrphanWitnessOutcome, MAX_ORPHAN_WITNESS_SIZE};
use near_crypto::Signature;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_o11y::testonly::init_integration_logger;
use near_primitives::block::Tip;
use near_primitives::merkle::{Direction, MerklePathItem};
use near_primitives::network::PeerId;
use near_primitives::sharding::{
    ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderInner, ShardChunkHeaderInnerV2,
    ShardProof,
};
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives_core::checked_feature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, ShardId};
use near_primitives_core::version::PROTOCOL_VERSION;
use nearcore::config::GenesisExt;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

struct OrphanWitnessTest {
    env: TestEnv,
    block1: Block,
    block2: Block,
    witness: ChunkStateWitness,
    excluded_validator: AccountId,
    excluded_validator_idx: usize,
    chunk_producer: AccountId,
}

/// Returns the block producer for the height of head + height_offset.
fn get_block_producer(env: &TestEnv, head: &Tip, height_offset: u64) -> AccountId {
    let client = &env.clients[0];
    let epoch_manager = &client.epoch_manager;
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let height = head.height + height_offset;
    let block_producer = epoch_manager.get_block_producer(&epoch_id, height).unwrap();
    block_producer
}

/// Returns the chunk producer for the height and shard of head + height_offset.
fn get_chunk_producer(
    env: &TestEnv,
    head: &Tip,
    height_offset: u64,
    shard_id: ShardId,
) -> AccountId {
    let client = &env.clients[0];
    let epoch_manager = &client.epoch_manager;
    let parent_hash = &head.last_block_hash;
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
    let height = head.height + height_offset;
    let chunk_producer = epoch_manager.get_chunk_producer(&epoch_id, height, shard_id).unwrap();
    chunk_producer
}

fn setup_orphan_witness_test() -> OrphanWitnessTest {
    let accounts: Vec<AccountId> = (0..5).map(|i| format!("test{i}").parse().unwrap()).collect();
    let genesis = Genesis::test(accounts.clone(), accounts.len().try_into().unwrap());
    let mut env = TestEnv::builder(&genesis.config)
        .clients(accounts.clone())
        .validators(accounts.clone())
        .nightshade_runtimes(&genesis)
        .build();

    // Run the blockchain for a few blocks
    for height in 1..6 {
        let tip = env.clients[0].chain.head().unwrap();
        let block_producer = get_block_producer(&env, &tip, 1);
        tracing::info!(target: "test", "Producing block at height: {height} by {block_producer}");
        let block = env.client(&block_producer).produce_block(tip.height + 1).unwrap().unwrap();
        tracing::info!(target: "test", "Block produced at height {} has chunk {:?}", height, block.chunks()[0].chunk_hash());
        for i in 0..env.clients.len() {
            let blocks_processed =
                env.clients[i].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
            assert_eq!(blocks_processed, vec![*block.hash()]);
        }

        env.process_partial_encoded_chunks();
        for client_idx in 0..env.clients.len() {
            env.process_shards_manager_responses_and_finish_processing_blocks(client_idx);
        }
        env.propagate_chunk_state_witnesses_and_endorsements(false);

        let heads = env
            .clients
            .iter()
            .map(|client| client.chain.head().unwrap().last_block_hash)
            .collect::<HashSet<_>>();
        assert_eq!(heads.len(), 1, "All clients should have the same head");
    }

    // Produce two more blocks (`block1` and `block2`), but don't send them to the `excluded_validator`.
    // The `excluded_validator` will receive a chunk witness for the chunk in `block2`, but it won't
    // have `block1`, so it will be an orphaned chunk state witness.
    let tip = env.clients[0].chain.head().unwrap();

    let block1_producer = get_block_producer(&env, &tip, 1);
    let block2_producer = get_block_producer(&env, &tip, 2);
    let block2_chunk_producer = get_chunk_producer(&env, &tip, 2, 0);

    // The excluded validator shouldn't produce any blocks or chunks in the next two blocks.
    // There's 4 validators and at most 3 aren't a good candidate, so there's always at least
    // one that fits all the criteria, the `unwrap()` won't fail.
    let excluded_validator = accounts
        .into_iter()
        .filter(|acc| {
            acc != &block1_producer && acc != &block2_producer && acc != &block2_chunk_producer
        })
        .next()
        .unwrap();
    let excluded_validator_idx = env.get_client_index(&excluded_validator);
    let clients_without_excluded =
        (0..env.clients.len()).filter(|idx| *idx != excluded_validator_idx);

    tracing::info!(target:"test", "Producing block1 at height {}", tip.height + 1);
    let block1 = env.client(&block1_producer).produce_block(tip.height + 1).unwrap().unwrap();
    for client_idx in clients_without_excluded.clone() {
        let blocks_processed = env.clients[client_idx]
            .process_block_test(block1.clone().into(), Provenance::NONE)
            .unwrap();
        assert_eq!(blocks_processed, vec![*block1.hash()]);
    }
    env.process_partial_encoded_chunks();
    for client_idx in 0..env.clients.len() {
        env.process_shards_manager_responses(client_idx);
    }

    let mut witness_opt = None;
    let network_adapter =
        env.network_adapters[env.get_client_index(&block2_chunk_producer)].clone();
    network_adapter.handle_filtered(|request| match request {
        PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ChunkStateWitness(
            account_ids,
            state_witness,
        )) => {
            let mut witness_processing_done_waiters: Vec<ProcessingDoneWaiter> = Vec::new();
            for account_id in account_ids.iter().filter(|acc| **acc != excluded_validator) {
                let processing_done_tracker = ProcessingDoneTracker::new();
                witness_processing_done_waiters.push(processing_done_tracker.make_waiter());
                env.client(account_id)
                    .process_chunk_state_witness(
                        state_witness.clone(),
                        PeerId::random(),
                        Some(processing_done_tracker),
                    )
                    .unwrap();
            }
            witness_opt = Some(state_witness);
            None
        }
        _ => Some(request),
    });
    let witness = witness_opt.unwrap();
    env.propagate_chunk_state_witnesses_and_endorsements(false);

    tracing::info!(target:"test", "Producing block2 at height {}", tip.height + 2);
    let block2 = env.client(&block2_producer).produce_block(tip.height + 2).unwrap().unwrap();
    assert_eq!(witness.inner.chunk_header.chunk_hash(), block2.chunks()[0].chunk_hash());

    for client_idx in clients_without_excluded {
        let blocks_processed = env.clients[client_idx]
            .process_block_test(block2.clone().into(), Provenance::NONE)
            .unwrap();
        assert_eq!(blocks_processed, vec![*block2.hash()]);
    }

    env.process_partial_encoded_chunks();
    for client_idx in 0..env.clients.len() {
        env.process_shards_manager_responses_and_finish_processing_blocks(client_idx);
    }

    OrphanWitnessTest {
        env,
        block1,
        block2,
        witness,
        excluded_validator,
        excluded_validator_idx,
        chunk_producer: block2_chunk_producer,
    }
}

#[test]
fn test_orphan_witness_valid() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTest {
        mut env,
        block1,
        block2,
        witness,
        excluded_validator,
        excluded_validator_idx,
        ..
    } = setup_orphan_witness_test();

    env.client(&excluded_validator)
        .process_chunk_state_witness(witness, PeerId::random(), None)
        .unwrap();

    let block_processed = env
        .client(&excluded_validator)
        .process_block_test(block1.clone().into(), Provenance::NONE)
        .unwrap();
    assert_eq!(block_processed, vec![*block1.hash()]);

    // After processing `block1`, `exclued_validator` should process the orphaned witness for the chunk belonging to `block2`
    // and it should send out an endorsement for this chunk. This happens asynchronously, so we have to wait for it.
    env.wait_for_chunk_endorsement(excluded_validator_idx, &block2.chunks()[0].chunk_hash())
        .unwrap();
}

#[test]
fn test_orphan_witness_bad_signature() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTest { mut env, mut witness, excluded_validator, .. } =
        setup_orphan_witness_test();

    // Modify the witness to contain an invalid signature
    witness.signature = Signature::default();

    let error = env
        .client(&excluded_validator)
        .process_chunk_state_witness(witness, PeerId::random(), None)
        .unwrap_err();
    let error_message = format!("{error}").to_lowercase();
    tracing::info!(target:"test", "Error message: {}", error_message);
    assert!(error_message.contains("invalid signature"));
}

#[test]
fn test_orphan_witness_signature_from_wrong_peer() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTest { mut env, mut witness, excluded_validator, .. } =
        setup_orphan_witness_test();

    // Sign the witness using another validator's key.
    // Only witnesses from the chunk producer that produced this witness should be accepted.
    resign_witness(&mut witness, env.client(&excluded_validator));

    let error = env
        .client(&excluded_validator)
        .process_chunk_state_witness(witness, PeerId::random(), None)
        .unwrap_err();
    let error_message = format!("{error}").to_lowercase();
    tracing::info!(target:"test", "Error message: {}", error_message);
    assert!(error_message.contains("invalid signature"));
}

#[test]
fn test_orphan_witness_invalid_shard_id() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTest { mut env, mut witness, excluded_validator, chunk_producer, .. } =
        setup_orphan_witness_test();

    // Set invalid shard_id in the witness header
    modify_witness_header_inner(&mut witness, |header| header.shard_id = 10000000);
    resign_witness(&mut witness, env.client(&chunk_producer));

    // The witness should be rejected
    let error = env
        .client(&excluded_validator)
        .process_chunk_state_witness(witness, PeerId::random(), None)
        .unwrap_err();
    let error_message = format!("{error}").to_lowercase();
    tracing::info!(target:"test", "Error message: {}", error_message);
    assert!(error_message.contains("shard"));
}

#[test]
fn test_orphan_witness_too_large() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTest { mut env, mut witness, excluded_validator, chunk_producer, .. } =
        setup_orphan_witness_test();

    // Modify the witness to be larger than the allowed limit
    let dummy_merkle_path_item =
        MerklePathItem { hash: CryptoHash::default(), direction: Direction::Left };
    let items_count = MAX_ORPHAN_WITNESS_SIZE / std::mem::size_of::<MerklePathItem>() + 1;
    let big_path = vec![dummy_merkle_path_item; items_count];
    let big_receipt_proof =
        ReceiptProof(Vec::new(), ShardProof { from_shard_id: 0, to_shard_id: 0, proof: big_path });
    witness.inner.source_receipt_proofs.insert(ChunkHash::default(), big_receipt_proof);
    resign_witness(&mut witness, env.client(&chunk_producer));

    // The witness should not be saved too the pool, as it's too big
    let outcome = env.client(&excluded_validator).handle_orphan_state_witness(witness).unwrap();
    assert!(matches!(outcome, HandleOrphanWitnessOutcome::TooBig(_)))
}

#[test]
fn test_orphan_witness_far_from_tip() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTest { mut env, mut witness, chunk_producer, excluded_validator, .. } =
        setup_orphan_witness_test();

    modify_witness_header_inner(&mut witness, |header| header.height_created = 10000000);
    resign_witness(&mut witness, env.client(&chunk_producer));

    let outcome = env.client(&excluded_validator).handle_orphan_state_witness(witness).unwrap();
    assert!(matches!(outcome, HandleOrphanWitnessOutcome::TooFarFromHead(_)));
}

#[test]
fn test_orphan_witness_not_fully_validated() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let OrphanWitnessTest { mut env, mut witness, chunk_producer, excluded_validator, .. } =
        setup_orphan_witness_test();

    witness.inner.source_receipt_proofs.insert(
        ChunkHash::default(),
        ReceiptProof(
            vec![],
            ShardProof { from_shard_id: 100230230, to_shard_id: 383939, proof: vec![] },
        ),
    );
    resign_witness(&mut witness, env.client(&chunk_producer));

    env.client(&excluded_validator)
        .process_chunk_state_witness(witness, PeerId::random(), None)
        .unwrap();
}

fn modify_witness_header_inner(
    witness: &mut ChunkStateWitness,
    f: impl FnOnce(&mut ShardChunkHeaderInnerV2),
) {
    match &mut witness.inner.chunk_header {
        ShardChunkHeader::V3(header) => match &mut header.inner {
            ShardChunkHeaderInner::V2(header_inner) => f(header_inner),
            _ => panic!(),
        },
        _ => panic!(),
    };
}

fn resign_witness(witness: &mut ChunkStateWitness, signer: &Client) {
    witness.signature =
        signer.validator_signer.as_ref().unwrap().sign_chunk_state_witness(&witness.inner).0;
}
