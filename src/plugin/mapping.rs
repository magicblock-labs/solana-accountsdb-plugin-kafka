use {
    crate::wire::UpdateAccountEvent,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaAccountInfoVersions,
    },
};

pub(super) fn unwrap_update_account(
    account: ReplicaAccountInfoVersions<'_>,
) -> &ReplicaAccountInfoV3<'_> {
    match account {
        ReplicaAccountInfoVersions::V0_0_1(_info) => {
            panic!(
                "ReplicaAccountInfoVersions::V0_0_1 unsupported, please upgrade your Solana node."
            );
        }
        ReplicaAccountInfoVersions::V0_0_2(_info) => {
            panic!(
                "ReplicaAccountInfoVersions::V0_0_2 unsupported, please upgrade your Solana node."
            );
        }
        ReplicaAccountInfoVersions::V0_0_3(info) => info,
    }
}

pub(super) fn build_update_account_event(
    info: &ReplicaAccountInfoV3<'_>,
    slot: u64,
    is_startup: bool,
) -> UpdateAccountEvent {
    UpdateAccountEvent {
        slot,
        pubkey: info.pubkey.to_vec(),
        lamports: info.lamports,
        owner: info.owner.to_vec(),
        executable: info.executable,
        rent_epoch: info.rent_epoch,
        data: info.data.to_vec(),
        write_version: info.write_version,
        txn_signature: info.txn.map(|v| v.signature().as_ref().to_owned()),
        data_version: info.write_version as u32,
        is_startup,
        account_age: slot.saturating_sub(info.rent_epoch),
    }
}
