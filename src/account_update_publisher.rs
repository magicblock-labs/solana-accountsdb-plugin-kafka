use {
    crate::{AccountSubscriptions, Filter, Publisher, UpdateAccountEvent},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError as PluginError, Result as PluginResult,
    },
    log::{debug, error, log_enabled},
    solana_pubkey::Pubkey,
};

pub fn publish_account_update(
    publisher: &Publisher,
    filters: &[Filter],
    subs: &AccountSubscriptions,
    event: UpdateAccountEvent,
) -> PluginResult<()> {
    let publish = should_publish_account(subs, &event);
    let mut matched_any_filter = false;
    let mut first_error = None;

    for filter in filters {
        if filter.update_account_topic.is_empty() || !publish {
            continue;
        }

        matched_any_filter = true;
        if let Ok(key) = <[u8; 32]>::try_from(event.pubkey.as_slice()) {
            debug!(
                "Matched account update {} in slot {}",
                Pubkey::new_from_array(key),
                event.slot
            );
        }

        if let Err(error) = publisher.update_account(
            event.clone(),
            filter.wrap_messages,
            &filter.update_account_topic,
        ) {
            let plugin_error = PluginError::AccountsUpdateError {
                msg: error.to_string(),
            };
            error!(
                "failed to publish account update for slot {}: {plugin_error:?}",
                event.slot
            );
            if first_error.is_none() {
                first_error = Some(plugin_error);
            }
        }
    }

    if !matched_any_filter {
        log_ignore_account_update(&event.pubkey);
    }

    first_error.map_or(Ok(()), Err)
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
