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

use crate::ConfigFilter;

pub struct Filter {
    pub publish_all_accounts: bool,
    pub update_account_topic: String,
    pub slot_status_topic: String,
    pub transaction_topic: String,
    pub wrap_messages: bool,
}

impl Filter {
    pub fn new(config: &ConfigFilter) -> Self {
        Self {
            publish_all_accounts: config.publish_all_accounts,
            update_account_topic: config.update_account_topic.clone(),
            slot_status_topic: config.slot_status_topic.clone(),
            transaction_topic: config.transaction_topic.clone(),
            wrap_messages: config.wrap_messages,
        }
    }
}
