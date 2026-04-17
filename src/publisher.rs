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

use {
    crate::{
        Config, MessageWrapper, UpdateAccountEvent,
        message_wrapper::EventMessage::{self, Account},
        server::prom::{StatsThreadedProducerContext, UPLOAD_ACCOUNTS_TOTAL},
    },
    log::{debug, error},
    prost::Message,
    rdkafka::{
        error::KafkaError,
        producer::{BaseRecord, Producer, ThreadedProducer},
    },
    solana_pubkey::Pubkey,
    std::time::Duration,
};

pub struct Publisher {
    producer: ThreadedProducer<StatsThreadedProducerContext>,
    shutdown_timeout: Duration,
}

impl Publisher {
    pub fn new(producer: ThreadedProducer<StatsThreadedProducerContext>, config: &Config) -> Self {
        Self {
            producer,
            shutdown_timeout: Duration::from_millis(config.shutdown_timeout_ms),
        }
    }

    pub fn update_account(&self, ev: UpdateAccountEvent, topic: &str) -> Result<(), KafkaError> {
        let log_pubkey = Pubkey::try_from(ev.pubkey.as_slice()).ok();
        let key = ev.pubkey.clone();
        let buf = Self::encode_with_wrapper(Account(Box::new(ev)));
        let record = BaseRecord::<Vec<u8>, _>::to(topic).key(&key).payload(&buf);
        let result = self.producer.send(record).map(|_| ()).map_err(|(e, _)| e);
        match &result {
            Ok(()) => match log_pubkey {
                Some(pubkey) => debug!(
                    "Published account update for pubkey: {} with key {:?}",
                    pubkey, key
                ),
                None => debug!(
                    "Published account update with invalid pubkey bytes and key {:?}",
                    key
                ),
            },
            Err(e) => match log_pubkey {
                Some(pubkey) => error!(
                    "Failed to publish account update for pubkey: {}: {}",
                    pubkey, e
                ),
                None => error!("Failed to publish account update: {}", e),
            },
        }
        UPLOAD_ACCOUNTS_TOTAL
            .with_label_values(&[if result.is_ok() { "success" } else { "failed" }])
            .inc();
        result
    }

    fn encode_with_wrapper(message: EventMessage) -> Vec<u8> {
        MessageWrapper {
            event_message: Some(message),
        }
        .encode_to_vec()
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let _ = self.producer.flush(self.shutdown_timeout);
    }
}
