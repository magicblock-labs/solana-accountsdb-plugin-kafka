use {
    crate::{
        initial_account_backfill::{
            INITIAL_BACKFILL_INITIAL_BACKOFF_MS, INITIAL_BACKFILL_MAX_ATTEMPTS,
            INITIAL_BACKFILL_MAX_BACKOFF_MS, INITIAL_BACKFILL_MAX_RPC_KEYS_PER_REQUEST,
        },
        metrics::{INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL, INITIAL_BACKFILL_RPC_FAILURES_TOTAL},
        wire::UpdateAccountEvent,
    },
    log::*,
    solana_account::Account,
    solana_commitment_config::CommitmentConfig,
    solana_pubkey::{Pubkey, pubkey},
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    std::io,
    std::time::Duration,
    tokio::time::sleep,
};

pub(crate) async fn fetch_account_events_for_request(
    client: &RpcClient,
    pubkeys: &[[u8; 32]],
) -> io::Result<Vec<UpdateAccountEvent>> {
    let mut events = Vec::with_capacity(pubkeys.len());
    for chunk in pubkeys.chunks(INITIAL_BACKFILL_MAX_RPC_KEYS_PER_REQUEST) {
        events.extend(fetch_account_events_for_chunk(client, chunk).await?);
    }
    Ok(events)
}

async fn fetch_account_events_for_chunk(
    client: &RpcClient,
    pubkeys: &[[u8; 32]],
) -> io::Result<Vec<UpdateAccountEvent>> {
    let keys = pubkeys
        .iter()
        .map(|pubkey| Pubkey::new_from_array(*pubkey))
        .collect::<Vec<_>>();

    let mut backoff_ms = INITIAL_BACKFILL_INITIAL_BACKOFF_MS;
    let mut last_error = None;

    for attempt in 1..=INITIAL_BACKFILL_MAX_ATTEMPTS {
        INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
            .with_label_values(&["started"])
            .inc();
        info!(
            "Starting initial account backfill RPC request for {} pubkeys, attempt={}/{}",
            pubkeys.len(),
            attempt,
            INITIAL_BACKFILL_MAX_ATTEMPTS
        );

        match client
            .get_multiple_accounts_with_commitment(&keys, CommitmentConfig::confirmed())
            .await
        {
            Ok(response) => {
                info!(
                    "Initial account backfill RPC request succeeded for {} pubkeys at slot {}",
                    pubkeys.len(),
                    response.context.slot
                );

                if response.value.len() != pubkeys.len() {
                    INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                        .with_label_values(&["failed"])
                        .inc();
                    INITIAL_BACKFILL_RPC_FAILURES_TOTAL
                        .with_label_values(&["length_mismatch"])
                        .inc();
                    return Err(io::Error::other(format!(
                        "rpc returned {} accounts for {} requested pubkeys",
                        response.value.len(),
                        pubkeys.len()
                    )));
                }

                INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                    .with_label_values(&["succeeded"])
                    .inc();
                return Ok(pubkeys
                    .iter()
                    .zip(response.value.into_iter())
                    .map(|(pubkey, maybe_account)| match maybe_account {
                        Some(account) => {
                            map_existing_account(account, response.context.slot, *pubkey)
                        }
                        None => map_missing_account(response.context.slot, *pubkey),
                    })
                    .collect());
            }
            Err(error) => {
                INITIAL_BACKFILL_RPC_ATTEMPTS_TOTAL
                    .with_label_values(&["failed"])
                    .inc();
                warn!(
                    "Initial account backfill RPC request failed for {} pubkeys, \
                     attempt={}/{}: {error}",
                    pubkeys.len(),
                    attempt,
                    INITIAL_BACKFILL_MAX_ATTEMPTS
                );
                last_error = Some(error);

                if attempt < INITIAL_BACKFILL_MAX_ATTEMPTS {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(INITIAL_BACKFILL_MAX_BACKOFF_MS);
                }
            }
        }
    }

    Err(io::Error::other(last_error.unwrap()))
}

pub(crate) const SYSTEM_PROGRAM_ID: Pubkey = pubkey!("11111111111111111111111111111111");

pub(crate) fn map_existing_account(
    account: Account,
    slot: u64,
    pubkey: [u8; 32],
) -> UpdateAccountEvent {
    UpdateAccountEvent {
        slot,
        pubkey: pubkey.to_vec(),
        lamports: account.lamports,
        owner: account.owner.to_bytes().to_vec(),
        executable: account.executable,
        rent_epoch: account.rent_epoch,
        data: account.data,
        write_version: 0,
        txn_signature: None,
        data_version: 0,
        is_startup: false,
        account_age: 0,
    }
}

pub(crate) fn map_missing_account(slot: u64, pubkey: [u8; 32]) -> UpdateAccountEvent {
    UpdateAccountEvent {
        slot,
        pubkey: pubkey.to_vec(),
        lamports: 0,
        owner: SYSTEM_PROGRAM_ID.to_bytes().to_vec(),
        executable: false,
        rent_epoch: 0,
        data: Vec::new(),
        write_version: 0,
        txn_signature: None,
        data_version: 0,
        is_startup: false,
        account_age: 0,
    }
}
