use {
    bytes::Bytes,
    http::StatusCode,
    http_body_util::Full,
    hyper::{Request, Response, body::Incoming},
    log::*,
    solana_pubkey::Pubkey,
    std::{collections::HashSet, str::FromStr, sync::Arc},
    tokio::sync::RwLock,
};

/// Shared dynamic account filter state.
/// Clone-cheap (Arc); the inner RwLock is read-biased for the hot path.
#[derive(Clone)]
pub struct AccountSubscriptions {
    inner: Arc<RwLock<HashSet<[u8; 32]>>>,
}

impl Default for AccountSubscriptions {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountSubscriptions {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Synchronous check for use in the geyser callback (non-async context).
    /// Uses `try_read` to avoid blocking the validator replay thread.
    /// If the lock is currently held by a writer, returns false (miss one update
    /// rather than block the validator).
    pub fn contains_sync(&self, pubkey: &[u8; 32]) -> bool {
        match self.inner.try_read() {
            Ok(guard) => guard.contains(pubkey),
            Err(_) => false,
        }
    }

    /// Add pubkeys, return new total count.
    pub async fn add(&self, pubkeys: Vec<[u8; 32]>) -> usize {
        let mut set = self.inner.write().await;
        for pk in pubkeys {
            set.insert(pk);
        }
        set.len()
    }
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
    let body_bytes = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &format!("body read error: {e}")),
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

    let active_count = subs.add(keys).await;
    info!(
        "Added {} pubkeys, active_count={active_count}",
        parsed.pubkeys.len()
    );

    json_response(StatusCode::OK, &AccountsResponse { active_count })
}

fn json_response<T: serde::Serialize>(status: StatusCode, body: &T) -> Response<Full<Bytes>> {
    let json = serde_json::to_vec(body).unwrap();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(json)))
        .unwrap()
}

fn error_response(status: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    json_response(
        status,
        &ErrorResponse {
            error: msg.to_owned(),
        },
    )
}
