# Solana AccountsDB Plugin for Kafka

Kafka publisher for Solana's [Geyser plugin framework](https://docs.solana.com/developing/plugins/geyser-plugins).

## What This Plugin Does

This plugin publishes confirmed Solana account updates to Kafka.

- account updates are the only published event type
- accounts are included only after they are added through `POST /filters/accounts`
- slot notifications are consumed internally so buffered updates can be released once confirmed
- Kafka payloads always use `MessageWrapper::Account(UpdateAccountEvent)`

## Architecture

- `plugin/` is the Geyser adapter. It reads config, wires runtime services, maps validator callbacks into internal events, and releases buffered updates when slots become confirmed.
- `confirmation_buffer.rs` owns the slot graph and confirmed-update buffering so account callbacks stay separate from confirmation policy.
- `initial_account_backfill/` owns subscription bootstrap. It enqueues requested pubkeys, fetches current account state from local RPC, and suppresses stale snapshots when a live update wins the race.
- `server/` owns the admin HTTP API. `server/mod.rs` boots the listener, `server/accounts.rs` handles `POST /filters/accounts`, and `server/prom.rs` exposes `GET /metrics`.
- `account_update_publisher.rs` owns publication policy, while `publisher.rs` only encodes and hands approved account updates to Kafka.

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
  "admin": "127.0.0.1:8080",
  "init_tracking_from_ksql_url": "http://127.0.0.1:8088"
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
- `admin`: required listen address for the admin HTTP API (account management and metrics)
- `metrics`: optional boolean (default `false`); set to `true` to enable the `/metrics` endpoint
- `init_tracking_from_ksql_url`: optional ksqlDB base URL; when set, startup restores tracked pubkeys from `accounts` and fails fast if restore cannot complete

Minimal config:

```json
{
  "libpath": "target/release/libsolana_accountsdb_plugin_kafka.so",
  "kafka": {
    "bootstrap.servers": "localhost:9092"
  },
  "update_account_topic": "solana.testnet.account_updates",
  "local_rpc_url": "http://127.0.0.1:8899",
  "admin": "127.0.0.1:8080"
}
```

`update_account_topic`, `local_rpc_url`, and `admin` are required. The `admin` bind address serves `POST /filters/accounts` and, when `metrics` is `true`, also `GET /metrics`. If `init_tracking_from_ksql_url` is set, it must be a valid absolute `http` or `https` base URL and startup will fail if the restore query cannot complete. Legacy filter arrays and legacy transaction, slot-status, block, and wrapping options are rejected during config parsing.

## Whitelist Management

Account inclusion is managed through the HTTP API:

- `POST /filters/accounts` adds accounts to the active whitelist
- newly added accounts are queued for initial backfill from the local RPC endpoint
- live updates and backfill snapshots are both gated by the same whitelist
- optional startup restore can replay previously tracked pubkeys from ksqlDB through the same whitelist and backfill path

Startup replay updates are not used as a second bootstrap publication path; subscribed bootstrap delivery comes from the initial RPC backfill flow.

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
