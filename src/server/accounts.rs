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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AddAccountsOutcome {
    pub active_count: usize,
    pub accepted_count: usize,
    pub newly_added_count: usize,
    pub retried_backfill_count: usize,
    pub duplicate_count: usize,
    pub needs_backfill_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AddAccountsError {
    QueueFull(AddAccountsOutcome),
    BackfillUnavailable(AddAccountsOutcome),
}

pub(crate) fn add_accounts(
    subs: &AccountSubscriptions,
    initial_account_backfill: &InitialAccountBackfillHandle,
    pubkeys: Vec<[u8; 32]>,
) -> Result<AddAccountsOutcome, AddAccountsError> {
    let accepted_count = pubkeys.len();
    let result = subs.add(pubkeys);
    info!(
        "Processed pubkeys, accepted_count={}, newly_added_count={}, duplicate_count={}, active_count={}",
        accepted_count,
        result.newly_added.len(),
        result.duplicate_count,
        result.active_count
    );

    let mut to_enqueue = result.newly_added.clone();
    let pending_backfill = subs.drain_needs_backfill();
    to_enqueue.extend_from_slice(&pending_backfill);

    let base_outcome = AddAccountsOutcome {
        active_count: result.active_count,
        accepted_count,
        newly_added_count: result.newly_added.len(),
        retried_backfill_count: pending_backfill.len(),
        duplicate_count: result.duplicate_count,
        needs_backfill_count: 0,
    };

    if to_enqueue.is_empty() {
        return Ok(base_outcome);
    }

    let enqueue_count = to_enqueue.len();
    let enqueue_result = initial_account_backfill.enqueue(to_enqueue.clone());
    if enqueue_result.queue_full {
        subs.mark_needs_backfill(&to_enqueue);
        let outcome = AddAccountsOutcome {
            needs_backfill_count: enqueue_count,
            ..base_outcome
        };
        warn!(
            "Initial account backfill queue full, marked {} keys as needs_backfill, accepted_count={}, newly_added_count={}, duplicate_count={}, active_count={}",
            enqueue_count,
            outcome.accepted_count,
            outcome.newly_added_count,
            outcome.duplicate_count,
            outcome.active_count
        );
        return Err(AddAccountsError::QueueFull(outcome));
    }

    if !enqueue_result.accepted {
        subs.mark_needs_backfill(&to_enqueue);
        let outcome = AddAccountsOutcome {
            needs_backfill_count: enqueue_count,
            ..base_outcome
        };
        error!(
            "Initial account backfill enqueue failed unexpectedly, marked {} keys as needs_backfill, accepted_count={}, newly_added_count={}, duplicate_count={}, active_count={}",
            enqueue_count,
            outcome.accepted_count,
            outcome.newly_added_count,
            outcome.duplicate_count,
            outcome.active_count
        );
        return Err(AddAccountsError::BackfillUnavailable(outcome));
    }

    info!(
        "Enqueued initial account backfill for {} pubkeys (newly_added={}, retried_backfill={}), active_count={}",
        enqueue_count,
        base_outcome.newly_added_count,
        base_outcome.retried_backfill_count,
        base_outcome.active_count
    );

    Ok(base_outcome)
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

    match add_accounts(&subs, &initial_account_backfill, keys) {
        Ok(outcome) => json_response(StatusCode::OK, &AccountsResponse::from(outcome)),
        Err(AddAccountsError::QueueFull(outcome)) => json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &AccountsResponse::from(outcome),
        ),
        Err(AddAccountsError::BackfillUnavailable(outcome)) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &AccountsResponse::from(outcome),
        ),
    }
}

impl From<AddAccountsOutcome> for AccountsResponse {
    fn from(outcome: AddAccountsOutcome) -> Self {
        Self {
            active_count: outcome.active_count,
            accepted_count: outcome.accepted_count,
            newly_added_count: outcome.newly_added_count,
            retried_backfill_count: outcome.retried_backfill_count,
            duplicate_count: outcome.duplicate_count,
        }
    }
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
    use {
        super::{AddAccountsError, add_accounts},
        crate::initial_account_backfill::InitialAccountBackfillHandle,
        crate::server::subscriptions::AccountSubscriptions,
    };

    fn pk(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    #[test]
    fn add_accounts_enqueues_all_new_keys() {
        let subs = AccountSubscriptions::new();
        let test_handle = InitialAccountBackfillHandle::new_test(4);

        let outcome = add_accounts(&subs, &test_handle, vec![pk(1), pk(2)]).unwrap();

        assert_eq!(outcome.accepted_count, 2);
        assert_eq!(outcome.newly_added_count, 2);
        assert_eq!(outcome.duplicate_count, 0);
        assert_eq!(outcome.retried_backfill_count, 0);
        assert_eq!(subs.needs_backfill_count(), 0);
    }

    #[test]
    fn add_accounts_counts_duplicates_without_readding() {
        let subs = AccountSubscriptions::new();
        let test_handle = InitialAccountBackfillHandle::new_test(4);

        let _ = add_accounts(&subs, &test_handle, vec![pk(3)]).unwrap();
        let outcome = add_accounts(&subs, &test_handle, vec![pk(3), pk(4), pk(4)]).unwrap();

        assert_eq!(outcome.accepted_count, 3);
        assert_eq!(outcome.newly_added_count, 1);
        assert_eq!(outcome.duplicate_count, 2);
    }

    #[test]
    fn add_accounts_retries_previously_pending_backfills() {
        let subs = AccountSubscriptions::new();
        let test_handle = InitialAccountBackfillHandle::new_test(4);
        subs.mark_needs_backfill(&[pk(8), pk(9)]);

        let outcome = add_accounts(&subs, &test_handle, vec![pk(10)]).unwrap();

        assert_eq!(outcome.newly_added_count, 1);
        assert_eq!(outcome.retried_backfill_count, 2);
        assert_eq!(subs.needs_backfill_count(), 0);
    }

    #[test]
    fn add_accounts_re_marks_needs_backfill_when_queue_is_full() {
        let subs = AccountSubscriptions::new();
        let test_handle = InitialAccountBackfillHandle::new_test(1);
        test_handle.prefill_queue_for_test(vec![pk(50)]);

        let error = add_accounts(&subs, &test_handle, vec![pk(51), pk(52)]).unwrap_err();

        match error {
            AddAccountsError::QueueFull(outcome) => {
                assert_eq!(outcome.accepted_count, 2);
                assert_eq!(outcome.newly_added_count, 2);
                assert_eq!(outcome.needs_backfill_count, 2);
                assert_eq!(subs.needs_backfill_count(), 2);
            }
            other => panic!("expected queue full error, got {other:?}"),
        }
    }
}
