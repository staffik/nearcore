use near_primitives_core::hash::{hash as sha256, CryptoHash};
use std::sync::Arc;

pub struct ContractCode {
    code: Arc<[u8]>,
    hash: CryptoHash,
}

impl ContractCode {
    pub fn new(code: Vec<u8>, hash: Option<CryptoHash>) -> ContractCode {
        let hash = hash.unwrap_or_else(|| sha256(&code));
        debug_assert_eq!(hash, sha256(&code));

        ContractCode { code: code.into(), hash }
    }

    pub fn code(&self) -> &[u8] {
        self.code.as_ref()
    }

    pub fn into_code(self) -> Vec<u8> {
        self.code.to_vec()
    }

    pub fn hash(&self) -> &CryptoHash {
        &self.hash
    }
}
