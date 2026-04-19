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
        MessageWrapper, UpdateAccountEvent,
        message_wrapper::EventMessage::{self, Account},
        metrics::{StatsThreadedProducerContext, UPLOAD_ACCOUNTS_TOTAL},
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
    pub fn new(
        producer: ThreadedProducer<StatsThreadedProducerContext>,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            producer,
            shutdown_timeout,
        }
    }

    pub fn update_account(&self, ev: UpdateAccountEvent, topic: &str) -> Result<(), KafkaError> {
        let log_pubkey = Pubkey::try_from(ev.pubkey.as_slice()).ok();
        let (key, buf) = Self::encode_account_update(ev);
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

    fn encode_account_update(ev: UpdateAccountEvent) -> (Vec<u8>, Vec<u8>) {
        let key = ev.pubkey.clone();
        let buf = Self::encode_with_wrapper(Account(Box::new(ev)));
        (key, buf)
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

#[cfg(test)]
mod tests {
    use super::Publisher;
    use crate::{MessageWrapper, UpdateAccountEvent, message_wrapper::EventMessage};
    use prost::Message;

    fn sample_event() -> UpdateAccountEvent {
        UpdateAccountEvent {
            slot: 42,
            pubkey: vec![7; 32],
            lamports: 99,
            owner: vec![8; 32],
            executable: true,
            rent_epoch: 11,
            data: vec![1, 2, 3],
            write_version: 123,
            txn_signature: Some(vec![9; 64]),
            data_version: 5,
            is_startup: false,
            account_age: 77,
        }
    }

    #[test]
    fn account_encoding_uses_raw_pubkey_as_key() {
        let event = sample_event();
        let expected_key = event.pubkey.clone();

        let (key, _payload) = Publisher::encode_account_update(event);

        assert_eq!(key, expected_key);
    }

    #[test]
    fn account_encoding_wraps_payload_in_message_wrapper() {
        let event = sample_event();
        let expected = event.clone();

        let (_key, payload) = Publisher::encode_account_update(event);
        let wrapper = MessageWrapper::decode(payload.as_slice()).unwrap();

        match wrapper.event_message {
            Some(EventMessage::Account(account)) => assert_eq!(*account, expected),
            other => panic!("unexpected wrapper payload: {other:?}"),
        }
    }
}
