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

    /// Optional ksqlDB base URL used to restore tracked pubkeys during startup.
    #[serde(default)]
    pub init_tracking_from_ksql_url: Option<String>,
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

    fn validate(&self) -> PluginResult<()> {
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

        if let Some(url) = &self.init_tracking_from_ksql_url {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg:
                        "invalid config field `init_tracking_from_ksql_url`: URL must not be empty"
                            .to_owned(),
                });
            }

            let parsed =
                Url::parse(trimmed).map_err(|error| GeyserPluginError::ConfigFileReadError {
                    msg: format!("invalid config field `init_tracking_from_ksql_url`: {error}"),
                })?;

            match parsed.scheme() {
                "http" | "https" => {}
                scheme => {
                    return Err(GeyserPluginError::ConfigFileReadError {
                        msg: format!(
                            "invalid config field `init_tracking_from_ksql_url`: unsupported scheme `{scheme}`"
                        ),
                    });
                }
            }

            if !parsed.has_host() {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: "invalid config field `init_tracking_from_ksql_url`: host is required"
                        .to_owned(),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Config;

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

        assert_eq!(config.init_tracking_from_ksql_url, None);
    }

    #[test]
    fn parses_config_with_valid_ksql_startup_restore_url() {
        let config = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080",
                "init_tracking_from_ksql_url": "https://127.0.0.1:8088"
            }"#,
        )
        .unwrap();

        assert_eq!(
            config.init_tracking_from_ksql_url.as_deref(),
            Some("https://127.0.0.1:8088")
        );
    }

    #[test]
    fn rejects_empty_ksql_startup_restore_url() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080",
                "init_tracking_from_ksql_url": "   "
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
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080",
                "init_tracking_from_ksql_url": "127.0.0.1:8088"
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("relative URL without a base"));
    }

    #[test]
    fn rejects_ksql_startup_restore_url_without_host() {
        let error = parse_config(
            r#"{
                "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
                "kafka": {"bootstrap.servers": "localhost:9092"},
                "update_account_topic": "solana.testnet.account_updates",
                "local_rpc_url": "http://127.0.0.1:8899",
                "admin": "127.0.0.1:8080",
                "init_tracking_from_ksql_url": "http://:8088"
            }"#,
        )
        .unwrap_err();

        assert!(error.contains("empty host") || error.contains("host is required"));
    }
}
