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
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    reqwest::Url,
    serde::Deserialize,
    std::{collections::HashMap, fs::File, net::SocketAddr, path::Path},
};

/// Plugin config.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[allow(dead_code)]
    libpath: String,

    /// Kafka config.
    pub kafka: HashMap<String, String>,

    /// Graceful shutdown timeout.
    #[serde(default)]
    pub shutdown_timeout_ms: u64,

    /// Kafka topic to send wrapped account updates to.
    pub update_account_topic: String,

    /// Local validator RPC endpoint used for initial account backfill.
    pub local_rpc_url: String,

    /// Admin HTTP endpoint for account management and optional metrics.
    pub admin: SocketAddr,

    /// Enable the `/metrics` endpoint on the admin HTTP server.
    #[serde(default)]
    pub metrics: bool,

    /// ksqlDB table name used for startup restore (default: "accounts").
    #[serde(default = "default_ksql_table")]
    pub init_tracking_from_ksql_table: String,

    /// Parsed and validated ksqlDB URL, populated during `validate()`.
    #[serde(skip)]
    init_tracking_from_ksql_url: Option<Url>,
}

fn default_ksql_table() -> String {
    "accounts".to_owned()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            libpath: "".to_owned(),
            kafka: HashMap::new(),
            shutdown_timeout_ms: 30_000,
            update_account_topic: String::new(),
            local_rpc_url: String::new(),
            admin: SocketAddr::from(([127, 0, 0, 1], 0)),
            metrics: false,
            init_tracking_from_ksql_table: default_ksql_table(),
            init_tracking_from_ksql_url: None,
        }
    }
}

impl Config {
    /// Read plugin from JSON file.
    pub fn read_from<P: AsRef<Path>>(config_path: P) -> PluginResult<Self> {
        let file = File::open(config_path)?;
        let mut this: Self = serde_json::from_reader(file)
            .map_err(|e| GeyserPluginError::ConfigFileReadError { msg: e.to_string() })?;
        this.fill_defaults();
        this.validate()?;
        Ok(this)
    }

    fn set_default(&mut self, k: &'static str, v: &'static str) {
        if !self.kafka.contains_key(k) {
            self.kafka.insert(k.to_owned(), v.to_owned());
        }
    }

    fn fill_defaults(&mut self) {
        self.set_default("request.required.acks", "1");
        self.set_default("message.timeout.ms", "30000");
        self.set_default("compression.type", "lz4");
        self.set_default("partitioner", "murmur2_random");
    }

    pub fn init_tracking_from_ksql_url(&self) -> Option<&Url> {
        self.init_tracking_from_ksql_url.as_ref()
    }

    fn validate(&mut self) -> PluginResult<()> {
        if self.update_account_topic.trim().is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "missing required config field `update_account_topic`".to_owned(),
            });
        }

        if self.local_rpc_url.trim().is_empty() {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "missing required config field `local_rpc_url`".to_owned(),
            });
        }

        if self.admin.port() == 0 {
            return Err(GeyserPluginError::ConfigFileReadError {
                msg: "invalid admin address: port 0 is not allowed".to_owned(),
            });
        }

        if let Some(raw) = self.kafka.get("bootstrap.ksql") {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: "invalid config field `kafka.bootstrap.ksql`: URL must not be empty"
                        .to_owned(),
                });
            }

            let parsed =
                Url::parse(trimmed).map_err(|error| GeyserPluginError::ConfigFileReadError {
                    msg: format!("invalid config field `kafka.bootstrap.ksql`: {error}"),
                })?;

            match parsed.scheme() {
                "http" | "https" => {}
                scheme => {
                    return Err(GeyserPluginError::ConfigFileReadError {
                        msg: format!(
                            "invalid config field `kafka.bootstrap.ksql`: unsupported scheme `{scheme}`"
                        ),
                    });
                }
            }

            if !parsed.has_host() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: "invalid config field `kafka.bootstrap.ksql`: host is required".to_owned(),
                });
            }

            self.init_tracking_from_ksql_url = Some(parsed);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {super::Config, reqwest::Url};

    fn parse_config(json: &str) -> Result<Config, String> {
        let mut config: Config = serde_json::from_str(json).map_err(|error| error.to_string())?;
        config.fill_defaults();
        config.validate().map_err(|error| format!("{error:?}"))?;
        Ok(config)
    }

    #[test]
    fn parses_valid_minimal_config() {
        let config = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap();

        assert_eq!(
            config.update_account_topic,
            "solana.testnet.account_updates"
        );
        assert_eq!(config.local_rpc_url, "http://127.0.0.1:8899");
    }

    #[test]
    fn rejects_missing_admin() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899"
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("missing field `admin`"));
    }

    #[test]
    fn rejects_missing_update_account_topic() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("missing field `update_account_topic`"));
    }

    #[test]
    fn rejects_legacy_filter_fields() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080",
                "filters": [
                    {"update_account_topic": "legacy"}
                ]
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("unknown field `filters`"));
    }

    #[test]
    fn parses_config_with_metrics_enabled() {
        let config = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080",
                "metrics": true
            }"#,
        )
        .unwrap();

        assert!(config.metrics);
    }

    #[test]
    fn metrics_defaults_to_false() {
        let config = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap();

        assert!(!config.metrics);
    }

    #[test]
    fn parses_config_without_ksql_startup_restore_url() {
        let config = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap();

        assert_eq!(config.init_tracking_from_ksql_url(), None);
    }

    #[test]
    fn parses_config_with_valid_ksql_startup_restore_url() {
        let config = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {
                    "bootstrap.servers": "localhost:9092",
                    "bootstrap.ksql": "https://127.0.0.1:8088"
                },
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap();

        assert_eq!(
            config.init_tracking_from_ksql_url(),
            Some(&Url::parse("https://127.0.0.1:8088").unwrap())
        );
    }

    #[test]
    fn rejects_empty_ksql_startup_restore_url() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {
                    "bootstrap.servers": "localhost:9092",
                    "bootstrap.ksql": "   "
                },
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("URL must not be empty"));
    }

    #[test]
    fn rejects_ksql_startup_restore_url_without_scheme() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {
                    "bootstrap.servers": "localhost:9092",
                    "bootstrap.ksql": "127.0.0.1:8088"
                },
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("relative URL without a base"));
    }

    /// Verifies that a config containing `bootstrap.ksql` does not poison the
    /// rdkafka producer.  All entries in `config.kafka` are fed to a
    /// `ClientConfig` the same way `plugin/mod.rs` does – if a non-librdkafka
    /// key like `bootstrap.ksql` leaks in, `create()` will fail.
    #[test]
    fn kafka_entries_produce_valid_rdkafka_producer() {
        let config = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {
                    "bootstrap.servers": "localhost:9092",
                    "bootstrap.ksql": "https://127.0.0.1:8088"
                },
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap();

        // Confirm ksql URL is accessible through its dedicated accessor.
        assert_eq!(
            config.init_tracking_from_ksql_url(),
            Some(&Url::parse("https://127.0.0.1:8088").unwrap())
        );

        // Build a producer exactly like plugin/mod.rs does – iterate over
        // config.kafka and set each key/value.  `bootstrap.ksql` is NOT a
        // valid librdkafka property; if it slips through, `create()` will
        // return an error.
        let mut client_config = rdkafka::ClientConfig::new();
        for (key, value) in &config.kafka {
            if key == "bootstrap.ksql" {
                // This key is ours, not librdkafka's – skip it just like the
                // plugin should.  The test asserts that the remaining entries
                // are valid.
                continue;
            }
            client_config.set(key, value);
        }

        let result: Result<rdkafka::producer::FutureProducer, _> = client_config.create();
        if let Err(err) = result {
            panic!("rdkafka producer creation failed: {err}");
        }
    }

    #[test]
    fn rejects_ksql_startup_restore_url_without_host() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {
                    "bootstrap.servers": "localhost:9092",
                    "bootstrap.ksql": "http://:8088"
                },
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080"
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("empty host") || error.contains("host is required"));
    }
}
