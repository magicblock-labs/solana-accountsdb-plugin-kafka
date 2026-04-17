use {
    crate::InitialAccountBackfillHandle,
    bytes::Bytes,
    dashmap::DashSet,
    http::StatusCode,
    http_body_util::{Full, Limited},
    hyper::{Request, Response, body::Incoming},
    log::*,
    solana_pubkey::Pubkey,
    std::{collections::HashSet, str::FromStr, sync::Arc},
};

/// Maximum request body size: 1 MiB
const MAX_BODY_SIZE: usize = 1024 * 1024;

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

    /// Clear keys from the needs_backfill set (enqueue succeeded).
    pub fn clear_needs_backfill(&self, pubkeys: &[[u8; 32]]) {
        for pk in pubkeys {
            self.needs_backfill.remove(pk);
        }
    }

    /// Drain all keys currently pending backfill.
    pub fn drain_needs_backfill(&self) -> Vec<[u8; 32]> {
        let keys: Vec<_> = self.needs_backfill.iter().map(|r| *r.key()).collect();
        for k in &keys {
            self.needs_backfill.remove(k);
        }
        keys
    }

    pub fn needs_backfill_count(&self) -> usize {
        self.needs_backfill.len()
    }
}

pub struct AddAccountsResult {
    pub active_count: usize,
    pub newly_added: Vec<[u8; 32]>,
    pub duplicate_count: usize,
}

// ----- REST handler -----

/// Request body for `POST /filters/accounts`.
#[derive(serde::Deserialize)]
struct AddAccountsRequest {
    pubkeys: Vec<String>,
}

/// Response body.
#[derive(serde::Serialize)]
struct AccountsResponse {
    active_count: usize,
    accepted_count: usize,
    newly_added_count: usize,
    duplicate_count: usize,
}

#[derive(serde::Serialize)]
struct ErrorResponse {
    error: String,
}

/// Handle `POST /filters/accounts`.
pub async fn handle_post_accounts(
    req: Request<Incoming>,
    subs: AccountSubscriptions,
    initial_account_backfill: InitialAccountBackfillHandle,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;
    let body_bytes = match Limited::new(req.into_body(), MAX_BODY_SIZE).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return if e
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                error_response(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    &format!("body exceeds max size of {MAX_BODY_SIZE} bytes"),
                )
            } else {
                error_response(StatusCode::BAD_REQUEST, &format!("body read error: {e}"))
            };
        }
    };

    let parsed: AddAccountsRequest = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };

    let mut keys = Vec::with_capacity(parsed.pubkeys.len());
    for pk_str in &parsed.pubkeys {
        match Pubkey::from_str(pk_str) {
            Ok(pk) => keys.push(pk.to_bytes()),
            Err(_) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    &format!("invalid pubkey: {pk_str}"),
                );
            }
        }
    }

    let accepted_count = keys.len();
    let result = subs.add(keys);
    info!(
        "Processed {} pubkeys, accepted_count={}, newly_added_count={}, duplicate_count={}, active_count={}",
        parsed.pubkeys.len(),
        accepted_count,
        result.newly_added.len(),
        result.duplicate_count,
        result.active_count
    );

    // Collect keys that need backfill: newly added ones plus any previously
    // failed ones still in needs_backfill.
    let mut to_enqueue = result.newly_added.clone();
    let pending_backfill = subs.drain_needs_backfill();
    to_enqueue.extend_from_slice(&pending_backfill);

    if to_enqueue.is_empty() {
        return json_response(
            StatusCode::OK,
            &AccountsResponse {
                active_count: result.active_count,
                accepted_count,
                newly_added_count: 0,
                duplicate_count: result.duplicate_count,
            },
        );
    }

    let enqueue_count = to_enqueue.len();
    let enqueue_result = initial_account_backfill.enqueue(to_enqueue.clone());
    if enqueue_result.queue_full {
        // Enqueue failed — mark all keys as needing backfill so the next
        // request (retry) will attempt to enqueue them again.
        subs.mark_needs_backfill(&to_enqueue);
        warn!(
            "Initial account backfill queue full, marked {} keys as needs_backfill, accepted_count={}, newly_added_count={}, duplicate_count={}, active_count={}",
            enqueue_count,
            accepted_count,
            result.newly_added.len(),
            result.duplicate_count,
            result.active_count
        );
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &AccountsResponse {
                active_count: result.active_count,
                accepted_count,
                newly_added_count: enqueue_count,
                duplicate_count: result.duplicate_count,
            },
        );
    }

    if !enqueue_result.accepted {
        // Channel closed — mark keys so they can be retried if the channel recovers.
        subs.mark_needs_backfill(&to_enqueue);
        error!(
            "Initial account backfill enqueue failed unexpectedly, marked {} keys as needs_backfill, accepted_count={}, newly_added_count={}, duplicate_count={}, active_count={}",
            enqueue_count,
            accepted_count,
            result.newly_added.len(),
            result.duplicate_count,
            result.active_count
        );
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to enqueue initial account backfill",
        );
    }

    info!(
        "Enqueued initial account backfill for {} pubkeys (newly_added={}, retried_backfill={}), active_count={}",
        enqueue_count,
        result.newly_added.len(),
        pending_backfill.len(),
        result.active_count
    );

    json_response(
        StatusCode::OK,
        &AccountsResponse {
            active_count: result.active_count,
            accepted_count,
            newly_added_count: enqueue_count,
            duplicate_count: result.duplicate_count,
        },
    )
}

fn json_response<T: serde::Serialize>(status: StatusCode, body: &T) -> Response<Full<Bytes>> {
    let json = match serde_json::to_vec(body) {
        Ok(j) => j,
        Err(e) => {
            error!("failed to serialize JSON response: {e}");
            return error_500();
        }
    };

    match Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(json)))
    {
        Ok(resp) => resp,
        Err(e) => {
            error!("failed to build response: {e}");
            error_500()
        }
    }
}

fn error_500() -> Response<Full<Bytes>> {
    match Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            r#"{"error":"internal server error"}"#,
        ))) {
        Ok(resp) => resp,
        Err(_) => {
            // Fallback: minimal response without headers
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::new()))
                .unwrap_or_else(|_| {
                    // Final fallback: bare 500 response
                    let (mut parts, _) = Response::new(Full::new(Bytes::new())).into_parts();
                    parts.status = StatusCode::INTERNAL_SERVER_ERROR;
                    Response::from_parts(parts, Full::new(Bytes::new()))
                })
        }
    }
}

fn error_response(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    json_response(
        status,
        &ErrorResponse {
            error: msg.to_owned(),
        },
    )
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
}
