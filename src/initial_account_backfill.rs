use std::sync::Arc;

pub const INITIAL_BACKFILL_QUEUE_CAPACITY: usize = 1024;
pub const INITIAL_BACKFILL_MAX_RPC_KEYS_PER_REQUEST: usize = 100;
pub const INITIAL_BACKFILL_MAX_ATTEMPTS: usize = 10;
pub const INITIAL_BACKFILL_INITIAL_BACKOFF_MS: u64 = 250;
pub const INITIAL_BACKFILL_MAX_BACKOFF_MS: u64 = 5_000;

#[derive(Clone, Default)]
pub struct InitialAccountBackfillHandle {
    _inner: Arc<()>,
}

impl InitialAccountBackfillHandle {
    pub fn new_noop() -> Self {
        Self::default()
    }

    pub fn enqueue(&self, _pubkeys: Vec<[u8; 32]>) -> EnqueueResult {
        EnqueueResult {
            accepted: true,
            queue_full: false,
        }
    }
}

pub struct EnqueueResult {
    pub accepted: bool,
    pub queue_full: bool,
}
