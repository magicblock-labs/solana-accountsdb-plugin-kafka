# Solana AccountsDB Plugin for Kafka

Kafka publisher for Solana's [Geyser plugin framework](https://docs.solana.com/developing/plugins/geyser-plugins).

## What This Plugin Does

This plugin publishes confirmed Solana account updates to Kafka.

- account updates are the only published event type
- accounts are included only after they are added through `POST /filters/accounts`
- slot notifications are consumed internally so buffered updates can be released once confirmed
- Kafka payloads always use `MessageWrapper::Account(UpdateAccountEvent)`

## Quick Start

```json
{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "kafka": {
    "bootstrap.servers": "localhost:9092"
  },
  "shutdown_timeout_ms": 30000,
  "update_account_topic": "solana.testnet.account_updates",
  "local_rpc_url": "http://127.0.0.1:8899",
  "prometheus": "127.0.0.1:8080"
}
```

After the plugin is running, add accounts to the whitelist:

```shell
curl -X POST http://127.0.0.1:8080/filters/accounts \
  -H 'content-type: application/json' \
  -d '{"pubkeys":["YourAccountPubkey111111111111111111111111111111"]}'
```

## Installation

### Binary releases

Find binary releases [on GitHub](https://github.com/Blockdaemon/solana-accountsdb-plugin-kafka/releases).

### Building from source

Prerequisites:

- Rust 1.93.1 from `rust-toolchain.toml`
- `protoc` 3.15 or later

Build:

```shell
cargo build --release
```

- Linux: `./target/release/libsolana_accountsdb_plugin_kafka.so`
- macOS: `./target/release/libsolana_accountsdb_plugin_kafka.dylib`

The Solana validator and this plugin must be built against matching Solana and Rust toolchains.

## Configuration

Config is provided as JSON.

Supported fields:

- `libpath`: path to the plugin shared library
- `kafka`: `librdkafka` producer configuration
- `shutdown_timeout_ms`: producer flush timeout on shutdown
- `update_account_topic`: Kafka topic for wrapped account updates
- `local_rpc_url`: local validator RPC endpoint used for initial account backfill
- `prometheus`: optional listen address for metrics and whitelist management

Minimal config:

```json
{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "kafka": {
    "bootstrap.servers": "localhost:9092"
  },
  "update_account_topic": "solana.testnet.account_updates",
  "local_rpc_url": "http://127.0.0.1:8899"
}
```

`update_account_topic` and `local_rpc_url` are required. Legacy filter arrays and legacy transaction, slot-status, block, and wrapping options are rejected during config parsing.

## Whitelist Management

Account inclusion is managed through the HTTP API:

- `POST /filters/accounts` adds accounts to the active whitelist
- newly added accounts are queued for initial backfill from the local RPC endpoint
- live updates and backfill snapshots are both gated by the same whitelist

If initial backfill enqueue fails because the queue is full, the keys are kept for retry on a later request.

## Message Format

The plugin publishes one wire format only:

- Kafka key: raw account pubkey bytes
- Kafka value: protobuf `MessageWrapper` with the `account` variant populated

`UpdateAccountEvent` contains the account fields, slot, write version, optional transaction signature, and the additional account metadata already present in the schema.

## Buffering

The Kafka producer is non-blocking so validator progress is not stalled by broker latency. If the producer queue fills, additional events may be dropped.

Useful `librdkafka` settings include:

- `queue.buffering.max.messages`
- `queue.buffering.max.kbytes`
- `statistics.interval.ms`

## Migration

Older configs must be reduced to the account-only shape above.

Remove:

- `filters`
- `block_events_topic`
- `slot_status_topic`
- `transaction_topic`
- `program_filters`
- `program_ignores`
- `account_filters`
- `publish_all_accounts`
- `include_vote_transactions`
- `include_failed_transactions`
- `wrap_messages`
