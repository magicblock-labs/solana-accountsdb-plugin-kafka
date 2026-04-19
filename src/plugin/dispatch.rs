use {
    crate::{
        InitialAccountBackfillHandle, Publisher, UpdateAccountEvent,
        account_update_publisher::publish_confirmed_account_update,
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
        if let Err(error) = publish_confirmed_account_update(
            publisher,
            topic,
            account_subscriptions,
            backfill_handle,
            event,
        ) && first_error.is_none()
        {
            first_error = Some(error);
        }
    }

    first_error.map_or(Ok(()), Err)
}
