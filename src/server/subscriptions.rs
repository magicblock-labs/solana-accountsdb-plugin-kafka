use {
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

    json_response(
        StatusCode::OK,
        &AccountsResponse {
            active_count: result.active_count,
            accepted_count,
            newly_added_count: result.newly_added.len(),
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
