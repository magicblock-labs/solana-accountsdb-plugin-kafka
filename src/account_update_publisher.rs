use {
    crate::{
        initial_account_backfill::InitialAccountBackfillHandle,
        publisher::Publisher,
        server::subscriptions::AccountSubscriptions,
        wire::UpdateAccountEvent,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError as PluginError, Result as PluginResult,
    },
    log::{debug, error, log_enabled},
    solana_pubkey::Pubkey,
};

pub enum AccountUpdatePublishOutcome {
    Published,
    SkippedStartupReplay,
    SkippedUnsubscribed,
    SkippedLiveUpdateWon,
}

pub fn publish_confirmed_account_update(
    publisher: &Publisher,
    topic: &str,
    subs: &AccountSubscriptions,
    backfill_handle: &InitialAccountBackfillHandle,
    event: UpdateAccountEvent,
) -> PluginResult<AccountUpdatePublishOutcome> {
    let decision = should_publish_confirmed_account(subs, &event);
    match decision {
        AccountUpdatePublishOutcome::Published => {
            let pubkey = <[u8; 32]>::try_from(event.pubkey.as_slice()).ok();
            publish_raw_account_update(publisher, topic, event)?;
            if let Some(pubkey) = pubkey {
                backfill_handle.mark_live_update_seen(&pubkey);
            }
            Ok(AccountUpdatePublishOutcome::Published)
        }
        AccountUpdatePublishOutcome::SkippedUnsubscribed => {
            log_ignore_account_update(&event.pubkey);
            Ok(AccountUpdatePublishOutcome::SkippedUnsubscribed)
        }
        outcome => Ok(outcome),
    }
}

pub fn publish_backfill_account_update(
    publisher: &Publisher,
    topic: &str,
    subs: &AccountSubscriptions,
    pubkey: [u8; 32],
    live_update_seen: bool,
    event: UpdateAccountEvent,
) -> PluginResult<AccountUpdatePublishOutcome> {
    let decision = should_publish_backfill_account(subs, pubkey, live_update_seen);
    match decision {
        AccountUpdatePublishOutcome::Published => {
            publish_raw_account_update(publisher, topic, event)?;
            Ok(AccountUpdatePublishOutcome::Published)
        }
        outcome => Ok(outcome),
    }
}

fn publish_raw_account_update(
    publisher: &Publisher,
    topic: &str,
    event: UpdateAccountEvent,
) -> PluginResult<()> {
    if let Ok(key) = <[u8; 32]>::try_from(event.pubkey.as_slice()) {
        debug!(
            "Matched account update {} in slot {}",
            Pubkey::new_from_array(key),
            event.slot
        );
    }

    let slot = event.slot;
    publisher.update_account(event, topic).map_err(|error| {
        let plugin_error = PluginError::AccountsUpdateError {
            msg: error.to_string(),
        };
        error!(
            "failed to publish account update for slot {}: {plugin_error:?}",
            slot
        );
        plugin_error
    })
}

fn should_publish_confirmed_account(
    subs: &AccountSubscriptions,
    event: &UpdateAccountEvent,
) -> AccountUpdatePublishOutcome {
    if event.is_startup {
        return AccountUpdatePublishOutcome::SkippedStartupReplay;
    }

    if !should_publish_subscribed_account(subs, &event.pubkey) {
        return AccountUpdatePublishOutcome::SkippedUnsubscribed;
    }

    AccountUpdatePublishOutcome::Published
}

fn should_publish_backfill_account(
    subs: &AccountSubscriptions,
    pubkey: [u8; 32],
    live_update_seen: bool,
) -> AccountUpdatePublishOutcome {
    if live_update_seen {
        return AccountUpdatePublishOutcome::SkippedLiveUpdateWon;
    }

    if !subs.contains_sync(&pubkey) {
        return AccountUpdatePublishOutcome::SkippedUnsubscribed;
    }

    AccountUpdatePublishOutcome::Published
}

fn should_publish_subscribed_account(subs: &AccountSubscriptions, pubkey: &[u8]) -> bool {
    match <&[u8; 32]>::try_from(pubkey) {
        Ok(key) => subs.contains_sync(key),
        Err(_) => false,
    }
}

fn log_ignore_account_update(pubkey: &[u8]) {
    if log_enabled!(::log::Level::Debug) {
        match <&[u8; 32]>::try_from(pubkey) {
            Ok(key) => debug!(
                "Ignoring update for account key: {:?}",
                Pubkey::new_from_array(*key)
            ),
            Err(_err) => debug!("Ignoring update for account key bytes: {:?}", pubkey),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccountUpdatePublishOutcome, should_publish_backfill_account,
        should_publish_confirmed_account,
    };
    use crate::{server::subscriptions::AccountSubscriptions, wire::UpdateAccountEvent};

    fn sample_event(is_startup: bool) -> UpdateAccountEvent {
        UpdateAccountEvent {
            slot: 42,
            pubkey: vec![1; 32],
            lamports: 5,
            owner: vec![2; 32],
            executable: false,
            rent_epoch: 0,
            data: vec![],
            write_version: 7,
            txn_signature: None,
            data_version: 7,
            is_startup,
            account_age: 0,
        }
    }

    #[test]
    fn confirmed_startup_replay_updates_are_suppressed() {
        assert!(matches!(
            should_publish_confirmed_account(&AccountSubscriptions::new(), &sample_event(true)),
            AccountUpdatePublishOutcome::SkippedStartupReplay
        ));
    }

    #[test]
    fn backfill_snapshots_are_suppressed_after_live_updates() {
        assert!(matches!(
            should_publish_backfill_account(&AccountSubscriptions::new(), [7; 32], true),
            AccountUpdatePublishOutcome::SkippedLiveUpdateWon
        ));
    }
}
