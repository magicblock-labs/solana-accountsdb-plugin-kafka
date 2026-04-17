use {
    crate::{AccountSubscriptions, Publisher, UpdateAccountEvent},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError as PluginError, Result as PluginResult,
    },
    log::{debug, error, log_enabled},
    solana_pubkey::Pubkey,
};

pub fn publish_account_update(
    publisher: &Publisher,
    topic: &str,
    subs: &AccountSubscriptions,
    event: UpdateAccountEvent,
) -> PluginResult<()> {
    if !should_publish_account(subs, &event) {
        log_ignore_account_update(&event.pubkey);
        return Ok(());
    }

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

fn should_publish_account(subs: &AccountSubscriptions, event: &UpdateAccountEvent) -> bool {
    match <&[u8; 32]>::try_from(event.pubkey.as_slice()) {
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
