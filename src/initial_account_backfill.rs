use {
    crate::{
        AccountSubscriptions, Filter, Publisher, UpdateAccountEvent,
        account_update_publisher::publish_account_update,
        server::prom::{
            INITIAL_BACKFILL_IN_FLIGHT, INITIAL_BACKFILL_PUBKEYS_ENQUEUED_TOTAL,
            INITIAL_BACKFILL_REQUESTS_ENQUEUED_TOTAL, INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL,
            INITIAL_BACKFILL_RPC_FAILURES_TOTAL, INITIAL_BACKFILL_SNAPSHOTS_TOTAL,
        },
    },
    dashmap::DashMap,
    log::*,
    solana_account::Account,
    solana_commitment_config::CommitmentConfig,
    solana_pubkey::{Pubkey, pubkey},
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    std::{io, sync::Arc, time::Duration},
    tokio::{
        runtime::Runtime,
        sync::mpsc::{self, error::TrySendError},
        time::sleep,
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
                        info!(
                            "Initial account backfill RPC request succeeded for {} pubkeys",
                            request.pubkeys.len()
                        );
                        debug!(
                            "Fetched {} initial account backfill snapshots for {} pubkeys",
                            events.len(),
                            request.pubkeys.len()
                        );

                        for event in events {
                            if let Err(error) = inner.complete_backfill_event(event) {
                                INITIAL_BACKFILL_SNAPSHOTS_TOTAL
                                    .with_label_values(&["publish_failed"])
                                    .inc();
                                error!(
                                    "Failed to publish initial account backfill snapshot: {error:?}"
                                );
                            }
                        }
                    }
                    Err(error) => {
                        INITIAL_BACKFILL_RPC_FAILURES_TOTAL
                            .with_label_values(&["request_failed"])
                            .inc();
                        error!(
                            "Initial account backfill RPC fetch failed for {} pubkeys: {error}",
                            request.pubkeys.len()
                        );
                    }
                }

                for pubkey in request.pubkeys {
                    inner.in_flight.remove(&pubkey);
                }
                INITIAL_BACKFILL_IN_FLIGHT.set(inner.in_flight.len() as i64);
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
        let request_pubkey_count = pubkeys.len();

        for pubkey in &pubkeys {
            self.inner
                .in_flight
                .insert(*pubkey, InFlightBackfill::default());
        }

        match self.inner.tx.try_send(BackfillRequest { pubkeys }) {
            Ok(()) => {
                INITIAL_BACKFILL_REQUESTS_ENQUEUED_TOTAL
                    .with_label_values(&["accepted"])
                    .inc();
                INITIAL_BACKFILL_PUBKEYS_ENQUEUED_TOTAL
                    .with_label_values(&["accepted"])
                    .inc_by(request_pubkey_count as u64);
                INITIAL_BACKFILL_IN_FLIGHT.set(self.inner.in_flight.len() as i64);
                info!(
                    "Enqueued initial account backfill request for {} pubkeys, in_flight={}",
                    request_pubkey_count,
                    self.inner.in_flight.len()
                );
                EnqueueResult {
                    accepted: true,
                    queue_full: false,
                }
            }
            Err(TrySendError::Full(request)) => {
                self.cleanup_in_flight(&request.pubkeys);
                INITIAL_BACKFILL_REQUESTS_ENQUEUED_TOTAL
                    .with_label_values(&["queue_full"])
                    .inc();
                INITIAL_BACKFILL_PUBKEYS_ENQUEUED_TOTAL
                    .with_label_values(&["queue_full"])
                    .inc_by(request.pubkeys.len() as u64);
                EnqueueResult {
                    accepted: false,
                    queue_full: true,
                }
            }
            Err(TrySendError::Closed(request)) => {
                self.cleanup_in_flight(&request.pubkeys);
                INITIAL_BACKFILL_REQUESTS_ENQUEUED_TOTAL
                    .with_label_values(&["closed"])
                    .inc();
                INITIAL_BACKFILL_PUBKEYS_ENQUEUED_TOTAL
                    .with_label_values(&["closed"])
                    .inc_by(request.pubkeys.len() as u64);
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
        INITIAL_BACKFILL_IN_FLIGHT.set(self.inner.in_flight.len() as i64);
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
            Err(_) => {
                warn!(
                    "Unexpected pubkey length {} in backfill event, skipping",
                    event.pubkey.len()
                );
                return Ok(());
            }
        };

        let Some((_, in_flight)) = self.in_flight.remove(&pubkey) else {
            return Ok(());
        };
        INITIAL_BACKFILL_IN_FLIGHT.set(self.in_flight.len() as i64);

        if in_flight.live_seen {
            INITIAL_BACKFILL_SNAPSHOTS_TOTAL
                .with_label_values(&["suppressed_live_seen"])
                .inc();
            info!(
                "Suppressing initial account backfill snapshot for {} because a live update arrived first",
                Pubkey::new_from_array(pubkey)
            );
            return Ok(());
        }

        if !self.subscriptions.contains_sync(&pubkey) {
            INITIAL_BACKFILL_SNAPSHOTS_TOTAL
                .with_label_values(&["skipped_unsubscribed"])
                .inc();
            debug!(
                "Skipping initial account backfill snapshot for unsubscribed pubkey {}",
                Pubkey::new_from_array(pubkey)
            );
            return Ok(());
        }

        let Some(publisher) = self.publisher.as_deref() else {
            INITIAL_BACKFILL_SNAPSHOTS_TOTAL
                .with_label_values(&["skipped_no_publisher"])
                .inc();
            return Ok(());
        };

        let result = publish_account_update(
            publisher,
            self.filters.as_slice(),
            &self.subscriptions,
            event,
        );
        INITIAL_BACKFILL_SNAPSHOTS_TOTAL
            .with_label_values(&[if result.is_ok() {
                "published"
            } else {
                "publish_failed"
            }])
            .inc();
        if result.is_ok() {
            info!(
                "Published initial account backfill snapshot for {}",
                Pubkey::new_from_array(pubkey)
            );
        }
        result
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

        let mut backoff_ms = INITIAL_BACKFILL_INITIAL_BACKOFF_MS;
        let mut last_error = None;

        for attempt in 1..=INITIAL_BACKFILL_MAX_ATTEMPTS {
            INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                .with_label_values(&["started"])
                .inc();
            info!(
                "Starting initial account backfill RPC request for {} pubkeys, attempt={}/{}",
                pubkeys.len(),
                attempt,
                INITIAL_BACKFILL_MAX_ATTEMPTS
            );

            match self
                .client
                .get_multiple_accounts_with_commitment(&keys, CommitmentConfig::confirmed())
                .await
            {
                Ok(response) => {
                    info!(
                        "Initial account backfill RPC request succeeded for {} pubkeys at slot {}",
                        pubkeys.len(),
                        response.context.slot
                    );

                    if response.value.len() != pubkeys.len() {
                        INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                            .with_label_values(&["failed"])
                            .inc();
                        INITIAL_BACKFILL_RPC_FAILURES_TOTAL
                            .with_label_values(&["length_mismatch"])
                            .inc();
                        return Err(io::Error::other(format!(
                            "rpc returned {} accounts for {} requested pubkeys",
                            response.value.len(),
                            pubkeys.len()
                        )));
                    }

                    INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                        .with_label_values(&["succeeded"])
                        .inc();
                    return Ok(pubkeys
                        .iter()
                        .zip(response.value.into_iter())
                        .map(|(pubkey, maybe_account)| match maybe_account {
                            Some(account) => {
                                map_existing_account(account, response.context.slot, *pubkey)
                            }
                            None => map_missing_account(response.context.slot, *pubkey),
                        })
                        .collect());
                }
                Err(error) => {
                    INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                        .with_label_values(&["failed"])
                        .inc();
                    warn!(
                        "Initial account backfill RPC request failed for {} pubkeys, \
                         attempt={}/{}: {error}",
                        pubkeys.len(),
                        attempt,
                        INITIAL_BACKFILL_MAX_ATTEMPTS
                    );
                    last_error = Some(error);

                    if attempt < INITIAL_BACKFILL_MAX_ATTEMPTS {
                        sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(INITIAL_BACKFILL_MAX_BACKOFF_MS);
                    }
                }
            }
        }

        Err(io::Error::other(last_error.unwrap()))
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

#[cfg(test)]
mod tests {
    use {super::*, tokio::sync::mpsc};

    fn pk(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn test_inner(
        channel_capacity: usize,
    ) -> (
        Arc<InitialAccountBackfillInner>,
        mpsc::Receiver<BackfillRequest>,
    ) {
        let (tx, rx) = mpsc::channel(channel_capacity);
        (
            Arc::new(InitialAccountBackfillInner {
                tx,
                in_flight: DashMap::new(),
                publisher: None,
                filters: Arc::new(Vec::new()),
                subscriptions: AccountSubscriptions::new(),
                client: RpcClient::new_with_commitment(
                    String::new(),
                    CommitmentConfig::confirmed(),
                ),
            }),
            rx,
        )
    }

    #[test]
    fn existing_account_maps_to_expected_update_event() {
        let event = map_existing_account(
            Account {
                lamports: 42,
                data: vec![1, 2, 3],
                owner: Pubkey::new_from_array(pk(9)),
                executable: true,
                rent_epoch: 7,
            },
            99,
            pk(1),
        );

        assert_eq!(event.slot, 99);
        assert_eq!(event.pubkey, pk(1).to_vec());
        assert_eq!(event.lamports, 42);
        assert_eq!(event.owner, pk(9).to_vec());
        assert!(event.executable);
        assert_eq!(event.rent_epoch, 7);
        assert_eq!(event.data, vec![1, 2, 3]);
        assert_eq!(event.write_version, 0);
        assert_eq!(event.txn_signature, None);
        assert_eq!(event.data_version, 0);
        assert!(!event.is_startup);
        assert_eq!(event.account_age, 0);
    }

    #[test]
    fn missing_account_maps_to_sentinel_event() {
        let event = map_missing_account(55, pk(2));

        assert_eq!(event.slot, 55);
        assert_eq!(event.pubkey, pk(2).to_vec());
        assert_eq!(event.lamports, 0);
        assert_eq!(event.owner, SYSTEM_PROGRAM_ID.to_bytes().to_vec());
        assert!(!event.executable);
        assert_eq!(event.rent_epoch, 0);
        assert!(event.data.is_empty());
        assert_eq!(event.write_version, 0);
        assert_eq!(event.txn_signature, None);
        assert_eq!(event.data_version, 0);
        assert!(!event.is_startup);
        assert_eq!(event.account_age, 0);
    }

    #[test]
    fn enqueue_marks_pubkeys_in_flight() {
        let (inner, _rx) = test_inner(4);
        let handle = InitialAccountBackfillHandle { inner };

        let result = handle.enqueue(vec![pk(1), pk(2)]);

        assert!(result.accepted);
        assert!(!result.queue_full);
        assert!(handle.inner.in_flight.contains_key(&pk(1)));
        assert!(handle.inner.in_flight.contains_key(&pk(2)));
    }

    #[test]
    fn mark_live_update_seen_flips_in_flight_state() {
        let (inner, _rx) = test_inner(4);
        let handle = InitialAccountBackfillHandle { inner };
        let pubkey = pk(3);
        let _ = handle.enqueue(vec![pubkey]);

        handle.mark_live_update_seen(&pubkey);

        assert!(
            handle
                .inner
                .in_flight
                .get(&pubkey)
                .expect("missing in-flight entry")
                .live_seen
        );
    }

    #[test]
    fn complete_backfill_event_suppresses_when_live_update_won_race() {
        let (inner, _rx) = test_inner(4);
        let handle = InitialAccountBackfillHandle {
            inner: inner.clone(),
        };
        let pubkey = pk(4);
        let _ = handle.enqueue(vec![pubkey]);
        handle.mark_live_update_seen(&pubkey);

        let result = inner.complete_backfill_event(map_missing_account(1, pubkey));

        assert!(result.is_ok());
        assert!(!inner.in_flight.contains_key(&pubkey));
    }

    #[test]
    fn enqueue_failure_cleans_up_temporary_in_flight_markers() {
        let (inner, mut rx) = test_inner(1);
        inner
            .tx
            .try_send(BackfillRequest {
                pubkeys: vec![pk(8)],
            })
            .expect("failed to prefill queue");
        let handle = InitialAccountBackfillHandle { inner };

        let result = handle.enqueue(vec![pk(9), pk(10)]);

        assert!(!result.accepted);
        assert!(result.queue_full);
        assert!(!handle.inner.in_flight.contains_key(&pk(9)));
        assert!(!handle.inner.in_flight.contains_key(&pk(10)));

        drop(rx.try_recv());
    }
}
