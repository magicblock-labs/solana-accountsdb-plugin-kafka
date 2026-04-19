use {
    crate::{
        InitialAccountBackfillHandle, Publisher, UpdateAccountEvent,
        account_update_publisher::publish_account_update,
        server::subscriptions::AccountSubscriptions,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::Result as PluginResult,
};

pub(super) fn publish_confirmed_account_updates(
    publisher: &Publisher,
    topic: &str,
    account_subscriptions: &AccountSubscriptions,
    backfill_handle: &InitialAccountBackfillHandle,
    updates: Vec<UpdateAccountEvent>,
) -> PluginResult<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let mut first_error = None;

    for event in updates {
        if !should_publish_confirmed_update(&event) {
            continue;
        }

        if let Ok(pubkey) = <[u8; 32]>::try_from(event.pubkey.as_slice()) {
            backfill_handle.mark_live_update_seen(&pubkey);
        }
        if let Err(error) = publish_account_update(publisher, topic, account_subscriptions, event)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }

    first_error.map_or(Ok(()), Err)
}

pub(super) fn should_publish_confirmed_update(event: &UpdateAccountEvent) -> bool {
    !event.is_startup
}
