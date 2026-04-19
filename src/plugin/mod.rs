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
        HttpService, InternalSlotStatus, Publisher,
        server::subscriptions::AccountSubscriptions,
        {Config, ConfirmedAccounts, InitialAccountBackfill},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError as PluginError, ReplicaAccountInfoVersions,
        Result as PluginResult, SlotStatus as PluginSlotStatus,
    },
    log::{error, info},
    rdkafka::util::get_rdkafka_version,
    std::{
        fmt::{Debug, Formatter},
        sync::{Arc, Mutex, MutexGuard},
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

        let producer = config.producer().map_err(|error| {
            error!("Failed to create kafka producer: {error:?}");
            PluginError::Custom(Box::new(error))
        })?;
        let publisher = Arc::new(Publisher::new(producer, &config));
        let update_account_topic = Arc::new(config.update_account_topic.clone());
        let initial_account_backfill = InitialAccountBackfill::new(
            publisher.clone(),
            update_account_topic.clone(),
            self.account_subscriptions.clone(),
            config.local_rpc_url.clone(),
        )
        .map_err(|error| PluginError::Custom(Box::new(error)))?;
        let http_service = config
            .create_http_service(
                self.account_subscriptions.clone(),
                initial_account_backfill.handle(),
            )
            .map_err(|error| PluginError::Custom(Box::new(error)))?;

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
            &self.initial_account_backfill.handle(),
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
}
