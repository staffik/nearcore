use near_chain::types::RuntimeAdapter;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnvBuilder;
use near_epoch_manager::EpochManagerHandle;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_store::genesis::initialize_genesis_state;
use near_store::Store;
use std::path::PathBuf;
use std::sync::Arc;

use crate::NightshadeRuntime;

pub trait TestEnvNightshadeSetupExt {
    fn nightshade_runtimes(self, genesis: &Genesis) -> Self;
    fn nightshade_runtimes_with_runtime_config_store(
        self,
        genesis: &Genesis,
        runtime_configs: Vec<RuntimeConfigStore>,
    ) -> Self;
}

impl TestEnvNightshadeSetupExt for TestEnvBuilder {
    fn nightshade_runtimes(self, genesis: &Genesis) -> Self {
        let runtime_configs = vec![RuntimeConfigStore::test(); self.num_clients()];
        self.nightshade_runtimes_with_runtime_config_store(genesis, runtime_configs)
    }

    fn nightshade_runtimes_with_runtime_config_store(
        self,
        genesis: &Genesis,
        runtime_configs: Vec<RuntimeConfigStore>,
    ) -> Self {
        let state_snapshot_type = self.state_snapshot_type();
        let nightshade_runtime_creator = |home_dir: PathBuf,
                                          store: Store,
                                          epoch_manager: Arc<EpochManagerHandle>,
                                          runtime_config: RuntimeConfigStore|
         -> Arc<dyn RuntimeAdapter> {
            // TODO: It's not ideal to initialize genesis state with the nightshade runtime here for tests
            // Tests that don't use nightshade runtime have genesis initialized in kv_runtime.
            // We should instead try to do this while configuring store.
            let home_dir = home_dir.as_path();
            initialize_genesis_state(store.clone(), genesis, Some(home_dir));
            NightshadeRuntime::test_with_runtime_config_store(
                home_dir,
                store,
                &genesis.config,
                epoch_manager,
                runtime_config,
                state_snapshot_type.clone(),
            )
        };
        self.internal_initialize_nightshade_runtimes(runtime_configs, nightshade_runtime_creator)
    }
}
