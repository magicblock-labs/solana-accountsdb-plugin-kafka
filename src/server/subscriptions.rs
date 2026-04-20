use {
    dashmap::DashSet,
    std::{collections::HashSet, sync::Arc},
};

/// Shared dynamic account filter state backed by Arc<DashSet<[u8; 32]>>.
/// Clone-cheap (Arc); uses DashSet's fine-grained sharded locking for concurrent access.
/// Operations may acquire shard-level read/write locks and can block if concurrent
/// operations hold the shard's lock. Not strictly lock-free, but provides better
/// concurrency than a single global lock via per-shard locking.
#[derive(Clone)]
pub struct AccountSubscriptions {
    inner: Arc<DashSet<[u8; 32]>>,
    /// Keys that are subscribed but whose initial backfill enqueue failed.
    /// Subsequent requests will attempt to re-enqueue these.
    needs_backfill: Arc<DashSet<[u8; 32]>>,
}

impl Default for AccountSubscriptions {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountSubscriptions {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashSet::new()),
            needs_backfill: Arc::new(DashSet::new()),
        }
    }

    /// Synchronous check for use in the geyser callback (non-async context).
    /// Uses DashSet::contains to check membership. This method acquires a read lock
    /// on the shard and may block if a concurrent write operation holds the shard's lock.
    /// Suitable for synchronous use but not strictly lock-free.
    pub fn contains_sync(&self, pubkey: &[u8; 32]) -> bool {
        self.inner.contains(pubkey)
    }

    /// Add pubkeys and report how many were newly inserted versus duplicates.
    pub fn add<I: IntoIterator<Item = [u8; 32]>>(&self, pubkeys: I) -> AddAccountsResult {
        let mut newly_added = Vec::new();
        let mut request_seen = HashSet::new();
        let mut duplicate_count = 0;

        for pk in pubkeys {
            if !request_seen.insert(pk) {
                duplicate_count += 1;
                continue;
            }

            if self.inner.insert(pk) {
                newly_added.push(pk);
            } else {
                duplicate_count += 1;
            }
        }

        AddAccountsResult {
            active_count: self.inner.len(),
            newly_added,
            duplicate_count,
        }
    }

    /// Mark keys as needing backfill (enqueue failed).
    pub fn mark_needs_backfill(&self, pubkeys: &[[u8; 32]]) {
        for pk in pubkeys {
            self.needs_backfill.insert(*pk);
        }
    }

    #[cfg(test)]
    /// Clear keys from the needs_backfill set.
    pub fn clear_needs_backfill(&self, pubkeys: &[[u8; 32]]) {
        for pk in pubkeys {
            self.needs_backfill.remove(pk);
        }
    }

    /// Drain all keys currently pending backfill.
    /// Uses `retain` so each entry is visited-and-removed under the same shard
    /// write lock, preventing concurrent callers from seeing the same keys.
    pub fn drain_needs_backfill(&self) -> Vec<[u8; 32]> {
        let mut keys = Vec::new();
        self.needs_backfill.retain(|k| {
            keys.push(*k);
            false
        });
        keys
    }

    #[cfg(test)]
    pub fn needs_backfill_count(&self) -> usize {
        self.needs_backfill.len()
    }
}

pub struct AddAccountsResult {
    pub active_count: usize,
    pub newly_added: Vec<[u8; 32]>,
    pub duplicate_count: usize,
}

#[cfg(test)]
mod tests {
    use super::AccountSubscriptions;

    fn pk(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn empty_add_keeps_counts_zero() {
        let subs = AccountSubscriptions::new();

        let result = subs.add(Vec::<[u8; 32]>::new());

        assert_eq!(result.active_count, 0);
        assert!(result.newly_added.is_empty());
        assert_eq!(result.duplicate_count, 0);
    }

    #[test]
    fn all_new_pubkeys_are_reported_as_newly_added() {
        let subs = AccountSubscriptions::new();

        let result = subs.add(vec![pk(1), pk(2), pk(3)]);

        assert_eq!(result.active_count, 3);
        assert_eq!(result.newly_added, vec![pk(1), pk(2), pk(3)]);
        assert_eq!(result.duplicate_count, 0);
    }

    #[test]
    fn duplicate_pubkeys_within_request_are_counted_once() {
        let subs = AccountSubscriptions::new();

        let result = subs.add(vec![pk(1), pk(1), pk(2), pk(2), pk(2)]);

        assert_eq!(result.active_count, 2);
        assert_eq!(result.newly_added, vec![pk(1), pk(2)]);
        assert_eq!(result.duplicate_count, 3);
    }

    #[test]
    fn readding_existing_pubkeys_counts_as_duplicates() {
        let subs = AccountSubscriptions::new();
        let _ = subs.add(vec![pk(1), pk(2)]);

        let result = subs.add(vec![pk(2), pk(3), pk(3)]);

        assert_eq!(result.active_count, 3);
        assert_eq!(result.newly_added, vec![pk(3)]);
        assert_eq!(result.duplicate_count, 2);
    }

    #[test]
    fn mark_needs_backfill_is_idempotent_per_key() {
        let subs = AccountSubscriptions::new();

        subs.mark_needs_backfill(&[pk(1)]);
        subs.mark_needs_backfill(&[pk(1)]);

        assert_eq!(subs.needs_backfill_count(), 1);
        let drained = subs.drain_needs_backfill();
        assert_eq!(drained, vec![pk(1)]);
    }

    #[test]
    fn mark_then_drain_returns_all_and_empties() {
        let subs = AccountSubscriptions::new();

        subs.mark_needs_backfill(&[pk(1), pk(2), pk(3)]);

        let mut drained = subs.drain_needs_backfill();
        drained.sort();
        assert_eq!(drained, vec![pk(1), pk(2), pk(3)]);
        assert_eq!(subs.needs_backfill_count(), 0);
    }

    #[test]
    fn drain_on_empty_returns_empty_and_zero_count() {
        let subs = AccountSubscriptions::new();

        let drained = subs.drain_needs_backfill();

        assert!(drained.is_empty());
        assert_eq!(subs.needs_backfill_count(), 0);
    }

    #[test]
    fn clear_needs_backfill_removes_only_specified_keys() {
        let subs = AccountSubscriptions::new();
        subs.mark_needs_backfill(&[pk(1), pk(2), pk(3), pk(4)]);

        subs.clear_needs_backfill(&[pk(2), pk(4)]);

        assert_eq!(subs.needs_backfill_count(), 2);
        let mut drained = subs.drain_needs_backfill();
        drained.sort();
        assert_eq!(drained, vec![pk(1), pk(3)]);
    }
}
