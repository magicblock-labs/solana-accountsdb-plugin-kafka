# Solana AccountsDB Plugin for Kafka

Kafka publisher for Solana's [Geyser plugin framework](https://docs.solana.com/developing/plugins/geyser-plugins).

## Current Product Direction

This repository is being stripped down to a narrower product surface.

The intended supported model is:

- publish account updates only
- use the `POST /filters/accounts` whitelist as the only inclusion mechanism
- keep slot tracking only as an internal detail needed to release confirmed account updates
- use the wrapped account message as the only supported Kafka payload shape

Legacy features are deprecated and will be removed:

- transaction publishing
- slot-status publishing as a Kafka output
- block publishing
- config-driven program filters and ignore lists
- startup-wide `publish_all_accounts`
- unwrapped payload encoding

## Quick Start

Use a single account-update topic and manage the whitelist through the HTTP API.

```json
{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "kafka": {
    "bootstrap.servers": "localhost:9092"
  },
  "shutdown_timeout_ms": 30000,
  "local_rpc_url": "http://127.0.0.1:8899",
  "prometheus": "127.0.0.1:8080",
  "filters": [
    {
      "update_account_topic": "solana.testnet.account_updates"
    }
  ]
}
```

After startup, add accounts to the whitelist with `POST /filters/accounts`. Only subscribed accounts are intended to be published.

## Installation

### Binary releases

Find binary releases [on GitHub](https://github.com/Blockdaemon/solana-accountsdb-plugin-kafka/releases).

### Building from source

#### Prerequisites

- Rust 1.93.1 from `rust-toolchain.toml`
- `protoc` 3.15 or later

#### Build

```shell
cargo build --release
```

- Linux: `./target/release/libsolana_accountsdb_plugin_kafka.so`
- macOS: `./target/release/libsolana_accountsdb_plugin_kafka.dylib`

The Solana validator and this plugin must be built against matching Solana and Rust toolchains.

## Configuration

Config is provided as JSON.

### Intended minimal shape

The target config shape for the stripped plugin is:

```json
{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "kafka": {
    "bootstrap.servers": "localhost:9092"
  },
  "shutdown_timeout_ms": 30000,
  "local_rpc_url": "http://127.0.0.1:8899",
  "prometheus": "127.0.0.1:8080",
  "filters": [
    {
      "update_account_topic": "solana.testnet.account_updates"
    }
  ]
}
```

Fields that matter going forward:

- `libpath`: path to the plugin shared library
- `kafka`: `librdkafka` producer configuration
- `shutdown_timeout_ms`: producer flush timeout on shutdown
- `local_rpc_url`: local validator RPC endpoint used for initial account backfill
- `prometheus`: optional listen address for metrics and whitelist management
- `filters[0].update_account_topic`: Kafka topic for wrapped account updates

### Whitelist management

Account inclusion is intended to come only from the HTTP-managed whitelist:

- `POST /filters/accounts` adds accounts
- the plugin uses that subscription set for live account-update publication
- initial backfill uses the same subscription set

Slot notifications may still be consumed internally so confirmed account updates can be released, but slot-status events are not part of the intended external product surface.

## Message Format

The only intended wire format is the wrapped account message.

- Kafka key: raw account pubkey bytes
- Kafka value: `MessageWrapper::Account(UpdateAccountEvent)`

Legacy unwrapped payloads and mixed message-type topics are deprecated and will be removed.

## Migration

Move existing configs toward this shape now:

1. Keep only `filters[0].update_account_topic`.
2. Stop relying on `program_filters`, `program_ignores`, `account_filters`, or `publish_all_accounts`.
3. Stop configuring transaction, slot-status, or block topics.
4. Treat wrapped account messages as the only supported downstream schema.

During the transition, some legacy keys may still deserialize, but they should be considered deprecated and scheduled for removal.

## Buffering

The Kafka producer is non-blocking so validator progress is not stalled by broker latency. If the producer queue fills, additional events may be dropped.

Useful `librdkafka` settings include:

- `queue.buffering.max.messages`
- `queue.buffering.max.kbytes`
- `statistics.interval.ms`

## Status

This README describes the intended stripped-down product contract, not a promise that every legacy code path has already been deleted in the current commit.
