use {
    crate::{AccountSubscriptions, Filter, Publisher},
    dashmap::DashMap,
    log::*,
    std::sync::Arc,
    tokio::{
        runtime::Runtime,
        sync::mpsc::{self, error::TrySendError},
    },
};

pub const INITIAL_BACKFILL_QUEUE_CAPACITY: usize = 1024;
pub const INITIAL_BACKFILL_MAX_RPC_KEYS_PER_REQUEST: usize = 100;
pub const INITIAL_BACKFILL_MAX_ATTEMPTS: usize = 10;
pub const INITIAL_BACKFILL_INITIAL_BACKOFF_MS: u64 = 250;
pub const INITIAL_BACKFILL_MAX_BACKOFF_MS: u64 = 5_000;

pub struct InitialAccountBackfill {
    handle: InitialAccountBackfillHandle,
    runtime: Option<Runtime>,
}

impl InitialAccountBackfill {
    pub fn new(
        publisher: Arc<Publisher>,
        filters: Arc<Vec<Filter>>,
        subscriptions: AccountSubscriptions,
        local_rpc_url: String,
    ) -> std::io::Result<Self> {
        let runtime = Runtime::new()?;
        let (tx, mut rx) = mpsc::channel(INITIAL_BACKFILL_QUEUE_CAPACITY);
        let inner = Arc::new(InitialAccountBackfillInner {
            tx,
            in_flight: DashMap::new(),
            _publisher: Some(publisher),
            _filters: filters,
            _subscriptions: subscriptions,
            _local_rpc_url: local_rpc_url,
        });
        let handle = InitialAccountBackfillHandle {
            inner: inner.clone(),
        };

        runtime.spawn(async move {
            while let Some(request) = rx.recv().await {
                debug!(
                    "Drained initial account backfill request for {} pubkeys",
                    request.pubkeys.len()
                );
                for pubkey in request.pubkeys {
                    inner.in_flight.remove(&pubkey);
                }
            }
        });

        Ok(Self {
            handle,
            runtime: Some(runtime),
        })
    }

    pub fn handle(&self) -> InitialAccountBackfillHandle {
        self.handle.clone()
    }
}

impl Default for InitialAccountBackfill {
    fn default() -> Self {
        Self::new_disabled()
    }
}

impl InitialAccountBackfill {
    fn new_disabled() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        let inner = Arc::new(InitialAccountBackfillInner {
            tx,
            in_flight: DashMap::new(),
            _publisher: None,
            _filters: Arc::new(Vec::new()),
            _subscriptions: AccountSubscriptions::new(),
            _local_rpc_url: String::new(),
        });
        Self {
            handle: InitialAccountBackfillHandle { inner },
            runtime: None,
        }
    }
}

impl Drop for InitialAccountBackfill {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(std::time::Duration::from_secs(10));
        }
    }
}

#[derive(Clone)]
pub struct InitialAccountBackfillHandle {
    inner: Arc<InitialAccountBackfillInner>,
}

impl InitialAccountBackfillHandle {
    pub fn enqueue(&self, pubkeys: Vec<[u8; 32]>) -> EnqueueResult {
        if pubkeys.is_empty() {
            return EnqueueResult {
                accepted: true,
                queue_full: false,
            };
        }

        for pubkey in &pubkeys {
            self.inner
                .in_flight
                .insert(*pubkey, InFlightBackfill::default());
        }

        match self.inner.tx.try_send(BackfillRequest { pubkeys }) {
            Ok(()) => EnqueueResult {
                accepted: true,
                queue_full: false,
            },
            Err(TrySendError::Full(request)) => {
                self.cleanup_in_flight(&request.pubkeys);
                EnqueueResult {
                    accepted: false,
                    queue_full: true,
                }
            }
            Err(TrySendError::Closed(request)) => {
                self.cleanup_in_flight(&request.pubkeys);
                EnqueueResult {
                    accepted: false,
                    queue_full: false,
                }
            }
        }
    }

    pub fn mark_live_update_seen(&self, pubkey: &[u8; 32]) {
        if let Some(mut entry) = self.inner.in_flight.get_mut(pubkey) {
            entry._live_seen = true;
        }
    }

    fn cleanup_in_flight(&self, pubkeys: &[[u8; 32]]) {
        for pubkey in pubkeys {
            self.inner.in_flight.remove(pubkey);
        }
    }
}

pub struct EnqueueResult {
    pub accepted: bool,
    pub queue_full: bool,
}

#[derive(Default)]
struct InFlightBackfill {
    _live_seen: bool,
}

struct BackfillRequest {
    pubkeys: Vec<[u8; 32]>,
}

struct InitialAccountBackfillInner {
    tx: mpsc::Sender<BackfillRequest>,
    in_flight: DashMap<[u8; 32], InFlightBackfill>,
    _publisher: Option<Arc<Publisher>>,
    _filters: Arc<Vec<Filter>>,
    _subscriptions: AccountSubscriptions,
    _local_rpc_url: String,
}
