use {
    super::subscriptions::AccountSubscriptions,
    crate::initial_account_backfill::InitialAccountBackfillHandle,
    bytes::Bytes,
    http::StatusCode,
    http_body_util::{Full, Limited},
    hyper::{Request, Response, body::Incoming},
    log::*,
    solana_pubkey::Pubkey,
    std::str::FromStr,
};

/// Maximum request body size: 1 MiB
const MAX_BODY_SIZE: usize = 1024 * 1024;

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
    retried_backfill_count: usize,
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
                retried_backfill_count: 0,
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
                newly_added_count: result.newly_added.len(),
                retried_backfill_count: pending_backfill.len(),
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
            newly_added_count: result.newly_added.len(),
            retried_backfill_count: pending_backfill.len(),
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
