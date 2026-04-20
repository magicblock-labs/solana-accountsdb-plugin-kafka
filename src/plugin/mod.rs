// Copyright 2022 Blockdaemon Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod dispatch;
mod mapping;

use {
    crate::{
        config::Config,
        confirmation_buffer::{ConfirmedAccounts, InternalSlotStatus},
        initial_account_backfill::InitialAccountBackfill,
        ksql::{INIT_TRACKING_RESTORE_CHUNK_SIZE, KsqlPubkeyRestoreClient},
        metrics::StatsThreadedProducerContext,
        publisher::Publisher,
        server::subscriptions::AccountSubscriptions,
        server::{
            HttpService,
            accounts::{AddAccountsError, AddAccountsOutcome, add_accounts},
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError as PluginError, ReplicaAccountInfoVersions,
        Result as PluginResult, SlotStatus as PluginSlotStatus,
    },
    log::{error, info, warn},
    rdkafka::util::get_rdkafka_version,
    rdkafka::{ClientConfig, config::FromClientConfigAndContext},
    std::{
        fmt::{Debug, Formatter},
        sync::{Arc, Mutex, MutexGuard},
        time::Duration,
    },
};

pub struct KafkaPlugin {
    publisher: Option<Arc<Publisher>>,
    update_account_topic: Option<Arc<String>>,
    http_service: Option<HttpService>,
    account_subscriptions: AccountSubscriptions,
    initial_account_backfill: InitialAccountBackfill,
    confirmed_accounts: Mutex<ConfirmedAccounts>,
}

impl Default for KafkaPlugin {
    fn default() -> Self {
        Self {
            publisher: None,
            update_account_topic: None,
            http_service: None,
            account_subscriptions: AccountSubscriptions::default(),
            initial_account_backfill: InitialAccountBackfill::default(),
            confirmed_accounts: Mutex::new(ConfirmedAccounts::default()),
        }
    }
}

impl Debug for KafkaPlugin {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for KafkaPlugin {
    fn name(&self) -> &'static str {
        "KafkaPlugin"
    }

    fn on_load(&mut self, config_file: &str, _: bool) -> PluginResult<()> {
        if self.publisher.is_some() {
            return Err(PluginError::Custom("plugin already loaded".into()));
        }

        agave_logger::setup_with_default("info");
        info!(
            "Loading plugin {:?} from config_file {:?}",
            self.name(),
            config_file
        );
        let config = Config::read_from(config_file)?;

        let (version_n, version_s) = get_rdkafka_version();
        info!("rd_kafka_version: {:#08x}, {}", version_n, version_s);

        let mut producer_config = ClientConfig::new();
        for (key, value) in &config.kafka {
            producer_config.set(key, value);
        }
        let producer = rdkafka::producer::ThreadedProducer::from_config_and_context(
            &producer_config,
            StatsThreadedProducerContext,
        )
        .map_err(|error| {
            error!("Failed to create kafka producer: {error:?}");
            PluginError::Custom(Box::new(error))
        })?;
        let publisher = Arc::new(Publisher::new(
            producer,
            Duration::from_millis(config.shutdown_timeout_ms),
        ));
        let update_account_topic = Arc::new(config.update_account_topic.clone());
        let initial_account_backfill = InitialAccountBackfill::new(
            publisher.clone(),
            update_account_topic.clone(),
            self.account_subscriptions.clone(),
            config.local_rpc_url.clone(),
        )
        .map_err(|error| PluginError::Custom(Box::new(error)))?;
        let http_service = HttpService::new(
            config.admin,
            self.account_subscriptions.clone(),
            initial_account_backfill.handle(),
            config.metrics,
        )
        .map_err(|error| PluginError::Custom(Box::new(error)))?;
        Self::restore_tracking_from_ksql(
            &config,
            &self.account_subscriptions,
            &initial_account_backfill,
        )?;

        self.publisher = Some(publisher);
        self.update_account_topic = Some(update_account_topic);
        self.http_service = Some(http_service);
        self.initial_account_backfill = initial_account_backfill;
        *self.lock_confirmed_accounts()? = ConfirmedAccounts::new();

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(http_service) = self.http_service.take() {
            http_service.shutdown();
        }
        self.publisher = None;
        self.update_account_topic = None;
        self.initial_account_backfill = InitialAccountBackfill::default();
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        let info = mapping::unwrap_update_account(account);
        let event = mapping::build_update_account_event(info, slot, is_startup);
        self.lock_confirmed_accounts()?.record_account(event);
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &PluginSlotStatus,
    ) -> PluginResult<()> {
        let transition = self.lock_confirmed_accounts()?.record_slot_status(
            slot,
            parent,
            InternalSlotStatus::from(status.clone()),
        );

        dispatch::publish_confirmed_account_updates(
            self.unwrap_publisher(),
            self.update_account_topic(),
            &self.account_subscriptions,
            self.initial_account_backfill.handle_ref(),
            transition.confirmed_updates,
        )
    }

    fn account_data_notifications_enabled(&self) -> bool {
        self.update_account_topic.is_some()
    }

    fn transaction_notifications_enabled(&self) -> bool {
        false
    }
}

impl KafkaPlugin {
    pub fn new() -> Self {
        Default::default()
    }

    fn lock_confirmed_accounts(&self) -> PluginResult<MutexGuard<'_, ConfirmedAccounts>> {
        self.confirmed_accounts.lock().map_err(|error| {
            PluginError::Custom(Box::new(std::io::Error::other(format!(
                "confirmed_accounts mutex poisoned: {error}"
            ))))
        })
    }

    fn unwrap_publisher(&self) -> &Publisher {
        self.publisher.as_deref().expect("publisher is unavailable")
    }

    fn update_account_topic(&self) -> &str {
        self.update_account_topic
            .as_deref()
            .expect("update_account_topic is unavailable")
    }

    fn restore_tracking_from_ksql(
        config: &Config,
        account_subscriptions: &AccountSubscriptions,
        initial_account_backfill: &InitialAccountBackfill,
    ) -> PluginResult<()> {
        let Some(url) = config.init_tracking_from_ksql_url.as_deref() else {
            return Ok(());
        };

        let table = &config.init_tracking_from_ksql_table;
        info!("Startup ksql restore enabled, url={}, table={}", url, table);

        let client = KsqlPubkeyRestoreClient::new(url, table)
            .map_err(|error| PluginError::Custom(Box::new(error)))?;
        let pubkeys = client
            .fetch_pubkeys()
            .map_err(|error| PluginError::Custom(Box::new(error)))?;
        let fetched_count = pubkeys.len();
        info!(
            "Fetched {} pubkeys from ksql startup restore",
            fetched_count
        );

        let summary = restore_pubkeys_in_chunks(
            account_subscriptions,
            initial_account_backfill.handle_ref(),
            pubkeys,
        )
        .map_err(add_accounts_error_to_plugin_error)?;

        info!(
            "Completed startup ksql restore, fetched_count={}, deduplicated_count={}, chunk_count={}, accepted_count={}, newly_added_count={}, retried_backfill_count={}, duplicate_count={}",
            fetched_count,
            summary.deduplicated_count,
            summary.chunk_count,
            summary.accepted_count,
            summary.newly_added_count,
            summary.retried_backfill_count,
            summary.duplicate_count
        );

        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RestoreTrackingSummary {
    deduplicated_count: usize,
    chunk_count: usize,
    accepted_count: usize,
    newly_added_count: usize,
    retried_backfill_count: usize,
    duplicate_count: usize,
}

fn restore_pubkeys_in_chunks(
    account_subscriptions: &AccountSubscriptions,
    initial_account_backfill: &crate::initial_account_backfill::InitialAccountBackfillHandle,
    mut pubkeys: Vec<[u8; 32]>,
) -> Result<RestoreTrackingSummary, AddAccountsError> {
    pubkeys.sort();
    pubkeys.dedup();

    let mut summary = RestoreTrackingSummary {
        deduplicated_count: pubkeys.len(),
        ..RestoreTrackingSummary::default()
    };

    for chunk in pubkeys.chunks(INIT_TRACKING_RESTORE_CHUNK_SIZE) {
        let outcome = add_accounts(
            account_subscriptions,
            initial_account_backfill,
            chunk.to_vec(),
        )?;
        log_restore_chunk_outcome(&outcome);
        summary.chunk_count += 1;
        summary.accepted_count += outcome.accepted_count;
        summary.newly_added_count += outcome.newly_added_count;
        summary.retried_backfill_count += outcome.retried_backfill_count;
        summary.duplicate_count += outcome.duplicate_count;
    }

    Ok(summary)
}

fn log_restore_chunk_outcome(outcome: &AddAccountsOutcome) {
    info!(
        "Processed startup restore chunk, accepted_count={}, newly_added_count={}, retried_backfill_count={}, duplicate_count={}, active_count={}, needs_backfill_count={}",
        outcome.accepted_count,
        outcome.newly_added_count,
        outcome.retried_backfill_count,
        outcome.duplicate_count,
        outcome.active_count,
        outcome.needs_backfill_count
    );
}

fn add_accounts_error_to_plugin_error(error: AddAccountsError) -> PluginError {
    match error {
        AddAccountsError::QueueFull(outcome) => {
            warn!(
                "Startup ksql restore failed because initial backfill queue is full, accepted_count={}, newly_added_count={}, retried_backfill_count={}, duplicate_count={}, needs_backfill_count={}",
                outcome.accepted_count,
                outcome.newly_added_count,
                outcome.retried_backfill_count,
                outcome.duplicate_count,
                outcome.needs_backfill_count
            );
            PluginError::Custom(Box::new(std::io::Error::other(format!(
                "startup ksql restore failed: initial backfill queue full, accepted_count={}, newly_added_count={}, retried_backfill_count={}, duplicate_count={}, needs_backfill_count={}",
                outcome.accepted_count,
                outcome.newly_added_count,
                outcome.retried_backfill_count,
                outcome.duplicate_count,
                outcome.needs_backfill_count
            ))))
        }
        AddAccountsError::BackfillUnavailable(outcome) => {
            error!(
                "Startup ksql restore failed because initial backfill is unavailable, accepted_count={}, newly_added_count={}, retried_backfill_count={}, duplicate_count={}, needs_backfill_count={}",
                outcome.accepted_count,
                outcome.newly_added_count,
                outcome.retried_backfill_count,
                outcome.duplicate_count,
                outcome.needs_backfill_count
            );
            PluginError::Custom(Box::new(std::io::Error::other(format!(
                "startup ksql restore failed: initial backfill unavailable, accepted_count={}, newly_added_count={}, retried_backfill_count={}, duplicate_count={}, needs_backfill_count={}",
                outcome.accepted_count,
                outcome.newly_added_count,
                outcome.retried_backfill_count,
                outcome.duplicate_count,
                outcome.needs_backfill_count
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{KafkaPlugin, restore_pubkeys_in_chunks},
        crate::{
            config::Config, initial_account_backfill::InitialAccountBackfillHandle,
            ksql::INIT_TRACKING_RESTORE_CHUNK_SIZE, server::subscriptions::AccountSubscriptions,
        },
    };

    fn pk(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn restore_tracking_from_ksql_is_noop_when_disabled() {
        let config = Config::default();
        let subs = AccountSubscriptions::new();
        let initial_account_backfill =
            crate::initial_account_backfill::InitialAccountBackfill::default();

        let result =
            KafkaPlugin::restore_tracking_from_ksql(&config, &subs, &initial_account_backfill);

        assert!(result.is_ok());
        assert!(!subs.contains_sync(&pk(1)));
    }

    #[test]
    fn restore_pubkeys_in_chunks_deduplicates_before_subscribing() {
        let subs = AccountSubscriptions::new();
        let test_handle = InitialAccountBackfillHandle::new_test(8);

        let summary =
            restore_pubkeys_in_chunks(&subs, &test_handle, vec![pk(1), pk(1), pk(2), pk(2), pk(3)])
                .unwrap();

        assert_eq!(summary.deduplicated_count, 3);
        assert_eq!(summary.accepted_count, 3);
        assert_eq!(summary.newly_added_count, 3);
        assert_eq!(summary.duplicate_count, 0);
        assert!(subs.contains_sync(&pk(1)));
        assert!(subs.contains_sync(&pk(2)));
        assert!(subs.contains_sync(&pk(3)));
    }

    #[test]
    fn restore_pubkeys_in_chunks_handles_chunk_boundaries() {
        let subs = AccountSubscriptions::new();
        let test_handle = InitialAccountBackfillHandle::new_test(8);
        let pubkeys = (0..=INIT_TRACKING_RESTORE_CHUNK_SIZE)
            .map(|index| {
                let mut pubkey = [0_u8; 32];
                pubkey[0] = (index & 0xff) as u8;
                pubkey[1] = ((index >> 8) & 0xff) as u8;
                pubkey
            })
            .collect::<Vec<_>>();

        let summary = restore_pubkeys_in_chunks(&subs, &test_handle, pubkeys).unwrap();

        assert_eq!(
            summary.deduplicated_count,
            INIT_TRACKING_RESTORE_CHUNK_SIZE + 1
        );
        assert_eq!(summary.chunk_count, 2);
        assert_eq!(summary.accepted_count, INIT_TRACKING_RESTORE_CHUNK_SIZE + 1);
    }

    #[test]
    fn restore_pubkeys_in_chunks_aborts_when_queue_is_full() {
        let subs = AccountSubscriptions::new();
        let test_handle = InitialAccountBackfillHandle::new_test(1);
        test_handle.prefill_queue_for_test(vec![pk(9)]);

        let error = restore_pubkeys_in_chunks(&subs, &test_handle, vec![pk(7), pk(8)]);

        assert!(error.is_err());
        assert_eq!(subs.needs_backfill_count(), 2);
    }
}
