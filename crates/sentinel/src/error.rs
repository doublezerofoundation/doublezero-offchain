use backon::{ExponentialBuilder, Retryable};
use solana_client::{
    client_error::{ClientError, ClientErrorKind, reqwest::StatusCode},
    nonblocking::pubsub_client::PubsubClientError,
    rpc_custom_error::{
        JSON_RPC_SCAN_ERROR, JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP,
        JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
        JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
        JSON_RPC_SERVER_ERROR_EPOCH_REWARDS_PERIOD_ACTIVE,
        JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
        JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE,
        JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED, JSON_RPC_SERVER_ERROR_NO_SNAPSHOT,
        JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY, JSON_RPC_SERVER_ERROR_SLOT_NOT_EPOCH_BOUNDARY,
        JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
        JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE,
        JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE,
        JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH,
        JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE,
        JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION,
    },
    rpc_request::{RpcError, RpcResponseErrorData},
};
use solana_sdk::signature::{ParseSignatureError, Signature};
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use thiserror::Error;
use tracing::warn;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),
    #[error("bincode deserialization error: {0}")]
    BincodeDeser(#[from] bincode::Error),
    #[error("borsh deserialization error: {0}")]
    BorshIo(#[from] borsh::io::Error),
    #[error("instruction not found in transaction: {0}")]
    InstructionNotFound(Signature),
    #[error("invalid instruction data: {0}")]
    InstructionInvalid(Signature),
    #[error("no account keys for transaction ix: {0}")]
    MissingAccountKeys(Signature),
    #[error("no program id at expected instruction index: {0}")]
    MissingProgramId(Signature),
    #[error("no transaction id signature")]
    MissingTxnSignature,
    #[error("pubsub client error: {0}")]
    PubsubClient(Box<PubsubClientError>),
    #[error("request channel error: {0}")]
    ReqChannel(#[from] tokio::sync::mpsc::error::SendError<Signature>),
    #[error("rpc client error: {0}")]
    RpcClient(Box<ClientError>),
    #[error("invalid transaction signature: {0}")]
    SignatureInvalid(#[from] ParseSignatureError),
    #[error("access request signature did not verify")]
    SignatureVerify,
    #[error("invalid transaction encoding: {0}")]
    TransactionEncoding(Signature),
    #[error("solana offchain message error: {0}")]
    OffchainSanitize(#[from] solana_sanitize::SanitizeError),
}

impl From<ClientError> for Error {
    fn from(err: ClientError) -> Self {
        Error::RpcClient(Box::new(err))
    }
}

impl From<PubsubClientError> for Error {
    fn from(err: PubsubClientError) -> Self {
        Error::PubsubClient(Box::new(err))
    }
}

pub async fn rpc_with_retry<F, Fut, T>(operation: F, label: &'static str) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut op = operation;
    let attempts = AtomicUsize::new(0);
    let backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_secs(1))
        .with_max_delay(Duration::from_secs(30))
        .with_max_times(8)
        .with_jitter();

    (move || op())
        .retry(backoff)
        .when(|err: &Error| should_retry(err))
        .notify(|err: &Error, delay: Duration| {
            let attempt = attempts.fetch_add(1, Ordering::Relaxed) + 1;
            warn!(attempt, retry_in = ?delay, error = ?err, operation = label, "transient RPC failure");
        })
        .await
}

fn should_retry(err: &Error) -> bool {
    match err {
        Error::RpcClient(client_err) => retryable_client_error(client_err.as_ref()),
        _ => false,
    }
}

fn retryable_client_error(err: &ClientError) -> bool {
    match err.kind() {
        ClientErrorKind::Io(_) => true,
        ClientErrorKind::Reqwest(reqwest_err) => {
            if reqwest_err.is_timeout() || reqwest_err.is_connect() {
                return true;
            }
            if let Some(status) = reqwest_err.status() {
                return status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS;
            }
            false
        }
        ClientErrorKind::RpcError(rpc_err) => retryable_rpc_error(rpc_err),
        ClientErrorKind::Middleware(_) => true,
        ClientErrorKind::Custom(_) => false,
        ClientErrorKind::SerdeJson(_) => false,
        ClientErrorKind::SigningError(_) => false,
        ClientErrorKind::TransactionError(_) => false,
    }
}

fn retryable_rpc_error(rpc_error: &RpcError) -> bool {
    match rpc_error {
        RpcError::RpcResponseError { code, data, .. } => match data {
            RpcResponseErrorData::SendTransactionPreflightFailure(_) => false,
            RpcResponseErrorData::NodeUnhealthy { .. } => true,
            RpcResponseErrorData::Empty => match *code {
                JSON_RPC_SCAN_ERROR
                | JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE
                | JSON_RPC_SERVER_ERROR_BLOCK_CLEANED_UP
                | JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET
                | JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED
                | JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE
                | JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED
                | JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY
                | JSON_RPC_SERVER_ERROR_NO_SNAPSHOT
                | JSON_RPC_SERVER_ERROR_SLOT_SKIPPED => true,
                JSON_RPC_SERVER_ERROR_EPOCH_REWARDS_PERIOD_ACTIVE
                | JSON_RPC_SERVER_ERROR_KEY_EXCLUDED_FROM_SECONDARY_INDEX
                | JSON_RPC_SERVER_ERROR_SLOT_NOT_EPOCH_BOUNDARY
                | JSON_RPC_SERVER_ERROR_TRANSACTION_HISTORY_NOT_AVAILABLE
                | JSON_RPC_SERVER_ERROR_TRANSACTION_PRECOMPILE_VERIFICATION_FAILURE
                | JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_LEN_MISMATCH
                | JSON_RPC_SERVER_ERROR_TRANSACTION_SIGNATURE_VERIFICATION_FAILURE
                | JSON_RPC_SERVER_ERROR_UNSUPPORTED_TRANSACTION_VERSION => false,
                _ => false,
            },
        },
        RpcError::RpcRequestError(_) => true,
        RpcError::ParseError(_) | RpcError::ForUser(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_client::{
        rpc_custom_error::JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE,
        rpc_response::RpcSimulateTransactionResult,
    };
    use solana_sdk::transaction::TransactionError;

    #[test]
    fn retries_io_errors() {
        let err = Error::from(ClientError::from(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out",
        )));
        assert!(should_retry(&err));
    }

    #[test]
    fn retries_node_unhealthy_errors() {
        let rpc_err = RpcError::RpcResponseError {
            code: JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY,
            message: "node unhealthy".into(),
            data: RpcResponseErrorData::NodeUnhealthy {
                num_slots_behind: Some(10),
            },
        };
        let err = Error::from(ClientError::from(rpc_err));
        assert!(should_retry(&err));
    }

    #[test]
    fn does_not_retry_preflight_failures() {
        let rpc_err = RpcError::RpcResponseError {
            code: JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE,
            message: "preflight failure".into(),
            data: RpcResponseErrorData::SendTransactionPreflightFailure(
                RpcSimulateTransactionResult {
                    err: Some(TransactionError::AccountNotFound),
                    logs: None,
                    accounts: None,
                    units_consumed: None,
                    loaded_accounts_data_size: None,
                    return_data: None,
                    inner_instructions: None,
                    replacement_blockhash: None,
                },
            ),
        };
        let err = Error::from(ClientError::from(rpc_err));
        assert!(!should_retry(&err));
    }

    #[test]
    fn does_not_retry_transaction_errors() {
        let err = Error::from(ClientError::from(TransactionError::AccountNotFound));
        assert!(!should_retry(&err));
    }
}
