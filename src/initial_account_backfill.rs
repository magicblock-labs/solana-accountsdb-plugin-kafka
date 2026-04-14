use {
    crate::{
        AccountSubscriptions, Filter, Publisher, UpdateAccountEvent,
        account_update_publisher::publish_account_update,
    },
    dashmap::DashMap,
    log::*,
    solana_account::Account,
    solana_commitment_config::CommitmentConfig,
    solana_pubkey::{Pubkey, pubkey},
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    std::{io, sync::Arc},
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
    ) -> io::Result<Self> {
        let runtime = Runtime::new()?;
        let (tx, mut rx) = mpsc::channel(INITIAL_BACKFILL_QUEUE_CAPACITY);
        let inner = Arc::new(InitialAccountBackfillInner {
            tx,
            in_flight: DashMap::new(),
            publisher: Some(publisher),
            filters,
            subscriptions,
            client: RpcClient::new_with_commitment(local_rpc_url, CommitmentConfig::confirmed()),
        });
        let handle = InitialAccountBackfillHandle {
            inner: inner.clone(),
        };

        runtime.spawn(async move {
            while let Some(request) = rx.recv().await {
                match inner
                    .fetch_account_events_for_request(&request.pubkeys)
                    .await
                {
                    Ok(events) => {
                        debug!(
                            "Fetched {} initial account backfill snapshots for {} pubkeys",
                            events.len(),
                            request.pubkeys.len()
                        );

                        for event in events {
                            if let Err(error) = inner.complete_backfill_event(event) {
                                error!(
                                    "Failed to publish initial account backfill snapshot: {error:?}"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        error!(
                            "Initial account backfill RPC fetch failed for {} pubkeys: {error}",
                            request.pubkeys.len()
                        );
                    }
                }

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

    fn new_disabled() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        let inner = Arc::new(InitialAccountBackfillInner {
            tx,
            in_flight: DashMap::new(),
            publisher: None,
            filters: Arc::new(Vec::new()),
            subscriptions: AccountSubscriptions::new(),
            client: RpcClient::new_with_commitment(String::new(), CommitmentConfig::confirmed()),
        });
        Self {
            handle: InitialAccountBackfillHandle { inner },
            runtime: None,
        }
    }
}

impl Default for InitialAccountBackfill {
    fn default() -> Self {
        Self::new_disabled()
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
            entry.live_seen = true;
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
    live_seen: bool,
}

struct BackfillRequest {
    pubkeys: Vec<[u8; 32]>,
}

struct InitialAccountBackfillInner {
    tx: mpsc::Sender<BackfillRequest>,
    in_flight: DashMap<[u8; 32], InFlightBackfill>,
    publisher: Option<Arc<Publisher>>,
    filters: Arc<Vec<Filter>>,
    subscriptions: AccountSubscriptions,
    client: RpcClient,
}

impl InitialAccountBackfillInner {
    fn complete_backfill_event(
        &self,
        event: UpdateAccountEvent,
    ) -> agave_geyser_plugin_interface::geyser_plugin_interface::Result<()> {
        let pubkey = match <[u8; 32]>::try_from(event.pubkey.as_slice()) {
            Ok(pubkey) => pubkey,
            Err(_) => return Ok(()),
        };

        let Some((_, in_flight)) = self.in_flight.remove(&pubkey) else {
            return Ok(());
        };

        if in_flight.live_seen {
            info!(
                "Suppressing initial account backfill snapshot for {} because a live update arrived first",
                Pubkey::new_from_array(pubkey)
            );
            return Ok(());
        }

        if !self.subscriptions.contains_sync(&pubkey) {
            debug!(
                "Skipping initial account backfill snapshot for unsubscribed pubkey {}",
                Pubkey::new_from_array(pubkey)
            );
            return Ok(());
        }

        let Some(publisher) = self.publisher.as_deref() else {
            return Ok(());
        };

        publish_account_update(
            publisher,
            self.filters.as_slice(),
            &self.subscriptions,
            event,
        )
    }

    async fn fetch_account_events_for_request(
        &self,
        pubkeys: &[[u8; 32]],
    ) -> io::Result<Vec<UpdateAccountEvent>> {
        let mut events = Vec::with_capacity(pubkeys.len());
        for chunk in pubkeys.chunks(INITIAL_BACKFILL_MAX_RPC_KEYS_PER_REQUEST) {
            events.extend(self.fetch_account_events_for_chunk(chunk).await?);
        }
        Ok(events)
    }

    async fn fetch_account_events_for_chunk(
        &self,
        pubkeys: &[[u8; 32]],
    ) -> io::Result<Vec<UpdateAccountEvent>> {
        let keys = pubkeys
            .iter()
            .map(|pubkey| Pubkey::new_from_array(*pubkey))
            .collect::<Vec<_>>();
        let response = self
            .client
            .get_multiple_accounts_with_commitment(&keys, CommitmentConfig::confirmed())
            .await
            .map_err(io::Error::other)?;

        if response.value.len() != pubkeys.len() {
            return Err(io::Error::other(format!(
                "rpc returned {} accounts for {} requested pubkeys",
                response.value.len(),
                pubkeys.len()
            )));
        }

        Ok(pubkeys
            .iter()
            .zip(response.value.into_iter())
            .map(|(pubkey, maybe_account)| match maybe_account {
                Some(account) => map_existing_account(account, response.context.slot, *pubkey),
                None => map_missing_account(response.context.slot, *pubkey),
            })
            .collect())
    }
}

const SYSTEM_PROGRAM_ID: Pubkey = pubkey!("11111111111111111111111111111111");

fn map_existing_account(account: Account, slot: u64, pubkey: [u8; 32]) -> UpdateAccountEvent {
    UpdateAccountEvent {
        slot,
        pubkey: pubkey.to_vec(),
        lamports: account.lamports,
        owner: account.owner.to_bytes().to_vec(),
        executable: account.executable,
        rent_epoch: account.rent_epoch,
        data: account.data,
        write_version: 0,
        txn_signature: None,
        data_version: 0,
        is_startup: false,
        account_age: 0,
    }
}

fn map_missing_account(slot: u64, pubkey: [u8; 32]) -> UpdateAccountEvent {
    UpdateAccountEvent {
        slot,
        pubkey: pubkey.to_vec(),
        lamports: 0,
        owner: SYSTEM_PROGRAM_ID.to_bytes().to_vec(),
        executable: false,
        rent_epoch: 0,
        data: Vec::new(),
        write_version: 0,
        txn_signature: None,
        data_version: 0,
        is_startup: false,
        account_age: 0,
    }
}
