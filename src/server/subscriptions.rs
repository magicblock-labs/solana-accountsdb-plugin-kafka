use {
    bytes::Bytes,
    dashmap::DashSet,
    http::StatusCode,
    http_body_util::Full,
    hyper::{Request, Response, body::Incoming},
    log::*,
    solana_pubkey::Pubkey,
    std::{str::FromStr, sync::Arc},
};

/// Shared dynamic account filter state.
/// Clone-cheap (Arc); uses DashSet for lock-free concurrent access.
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
    /// Lock-free check against DashSet, always returns accurate result without blocking.
    pub fn contains_sync(&self, pubkey: &[u8; 32]) -> bool {
        self.inner.contains(pubkey)
    }

    /// Add pubkeys, return new total count.
    pub async fn add<I: IntoIterator<Item = [u8; 32]>>(&self, pubkeys: I) -> usize {
        for pk in pubkeys {
            self.inner.insert(pk);
        }
        self.inner.len()
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
