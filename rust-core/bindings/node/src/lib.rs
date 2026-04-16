use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use engine_orchestrator::augmentation::AugmentationInput;
use engine_orchestrator::retrieval::RetrievalRequest;
use engine_orchestrator::search::FactId;
use engine_orchestrator::storage::{
    CandidateFactRow, EmbeddingRow, FetchEmbeddingsRequest, FetchFactsByIdsRequest,
    HostStorageError, StorageBridge, WriteAck, WriteBatch,
};
use engine_orchestrator::{EngineOrchestrator, OrchestratorError};
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Error, JsFunction, Result, Status};
use napi_derive::napi;

struct NodeStorageBridge {
    fetch_embeddings_cb: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    fetch_facts_by_ids_cb: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    write_batch_cb: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
}

impl NodeStorageBridge {
    const CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);

    fn call_json(
        callback: &ThreadsafeFunction<String, ErrorStrategy::Fatal>,
        payload_json: String,
    ) -> std::result::Result<String, HostStorageError> {
        let (tx, rx) = mpsc::sync_channel::<String>(1);
        let status = callback.call_with_return_value(
            payload_json,
            ThreadsafeFunctionCallMode::Blocking,
            move |value: String| {
                let _ = tx.send(value);
                Ok(())
            },
        );
        if status != Status::Ok {
            return Err(HostStorageError::new(
                "node_callback_failed",
                format!("callback status: {status:?}"),
            ));
        }
        rx.recv_timeout(Self::CALLBACK_TIMEOUT)
            .map_err(|e| match e {
                mpsc::RecvTimeoutError::Timeout => HostStorageError::new(
                    "node_callback_timeout",
                    format!(
                        "callback did not return within {}s",
                        Self::CALLBACK_TIMEOUT.as_secs()
                    ),
                ),
                mpsc::RecvTimeoutError::Disconnected => {
                    HostStorageError::new("node_callback_channel_closed", "callback channel closed")
                }
            })
    }
}

impl StorageBridge for NodeStorageBridge {
    fn fetch_embeddings(
        &self,
        entity_id: &str,
        limit: usize,
    ) -> std::result::Result<Vec<EmbeddingRow>, HostStorageError> {
        let request = FetchEmbeddingsRequest {
            entity_id: entity_id.to_string(),
            limit,
        };
        let payload = serde_json::to_string(&request)
            .map_err(|e| HostStorageError::new("serialization_error", e.to_string()))?;
        let result = Self::call_json(&self.fetch_embeddings_cb, payload)?;
        serde_json::from_str::<Vec<EmbeddingRow>>(&result)
            .map_err(|e| HostStorageError::new("deserialization_error", e.to_string()))
    }

    fn fetch_facts_by_ids(
        &self,
        ids: &[FactId],
    ) -> std::result::Result<Vec<CandidateFactRow>, HostStorageError> {
        let request = FetchFactsByIdsRequest { ids: ids.to_vec() };
        let payload = serde_json::to_string(&request)
            .map_err(|e| HostStorageError::new("serialization_error", e.to_string()))?;
        let result = Self::call_json(&self.fetch_facts_by_ids_cb, payload)?;
        serde_json::from_str::<Vec<CandidateFactRow>>(&result)
            .map_err(|e| HostStorageError::new("deserialization_error", e.to_string()))
    }

    fn write_batch(&self, batch: &WriteBatch) -> std::result::Result<WriteAck, HostStorageError> {
        let payload = serde_json::to_string(batch)
            .map_err(|e| HostStorageError::new("serialization_error", e.to_string()))?;
        let result = Self::call_json(&self.write_batch_cb, payload)?;
        serde_json::from_str::<WriteAck>(&result)
            .map_err(|e| HostStorageError::new("deserialization_error", e.to_string()))
    }
}

#[napi]
pub struct MemoriEngine {
    orchestrator: Arc<EngineOrchestrator>,
}

#[napi]
impl MemoriEngine {
    #[napi(constructor)]
    pub fn new(
        model_name: Option<String>,
        fetch_embeddings_cb: JsFunction,
        fetch_facts_by_ids_cb: JsFunction,
        write_batch_cb: JsFunction,
    ) -> Result<Self> {
        let fetch_embeddings_tsfn = fetch_embeddings_cb
            .create_threadsafe_function::<String, String, _, ErrorStrategy::Fatal>(0, |ctx| Ok(vec![ctx.value]))
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;

        let fetch_facts_tsfn = fetch_facts_by_ids_cb
            .create_threadsafe_function::<String, String, _, ErrorStrategy::Fatal>(0, |ctx| Ok(vec![ctx.value]))
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;

        let write_batch_tsfn = write_batch_cb
            .create_threadsafe_function::<String, String, _, ErrorStrategy::Fatal>(0, |ctx| Ok(vec![ctx.value]))
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?;

        let bridge = NodeStorageBridge {
            fetch_embeddings_cb: fetch_embeddings_tsfn,
            fetch_facts_by_ids_cb: fetch_facts_tsfn,
            write_batch_cb: write_batch_tsfn,
        };

        let orchestrator = Arc::new(
            EngineOrchestrator::new_with_storage(model_name.as_deref(), Some(Arc::new(bridge)))
                .map_err(orchestrator_error_to_napi_error)?
        );
        Ok(Self { orchestrator })
    }

    #[napi]
    pub async fn retrieve(&self, request_json: String) -> Result<String> {
        let request: RetrievalRequest = serde_json::from_str(&request_json)
            .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
        let orch = self.orchestrator.clone();
        
        napi::tokio::task::spawn_blocking(move || {
            let ranked = orch.retrieve(request).map_err(orchestrator_error_to_napi_error)?;
            serde_json::to_string(&ranked).map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
        })
        .await
        .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?
    }

    #[napi]
    pub async fn recall(&self, request_json: String) -> Result<String> {
        let request: RetrievalRequest = serde_json::from_str(&request_json)
            .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
        let orch = self.orchestrator.clone();
        
        napi::tokio::task::spawn_blocking(move || {
            orch.recall(request).map_err(orchestrator_error_to_napi_error)
        })
        .await
        .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?
    }

    #[napi]
    pub async fn wait_for_augmentation(&self, timeout_ms: Option<u32>) -> Result<bool> {
        let timeout = timeout_ms.map(|ms| Duration::from_millis(ms as u64));
        let orch = self.orchestrator.clone();
        
        napi::tokio::task::spawn_blocking(move || {
            orch.wait_for_augmentation(timeout).map_err(orchestrator_error_to_napi_error)
        })
        .await
        .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?
    }

    #[napi]
    pub fn submit_augmentation(&self, input_json: String) -> Result<String> {
        let input: AugmentationInput = serde_json::from_str(&input_json)
            .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
        self.orchestrator.submit_augmentation(input)
            .map(|accepted| accepted.job_id.to_string())
            .map_err(orchestrator_error_to_napi_error)
    }

    /// Embed a batch of texts using the engine's loaded fastembed model.
    ///
    /// Input:  JSON-encoded `string[]`
    /// Output: JSON-encoded `number[][]` — one float32 vector per input text.
    ///
    /// This is synchronous and safe to call from the Rust engine's writeBatch callback
    /// thread because it runs entirely on the caller thread without touching the event loop.
    #[napi]
    pub fn embed_texts(&self, texts_json: String) -> Result<String> {
        let texts: Vec<String> = serde_json::from_str(&texts_json)
            .map_err(|e| Error::new(Status::InvalidArg, e.to_string()))?;
        if texts.is_empty() {
            return Ok("[]".to_string());
        }
        let (flat, shape) = self.orchestrator.embed(texts);
        let num_texts = shape[0];
        let dim = shape[1];
        if num_texts == 0 || dim == 0 {
            return Ok("[]".to_string());
        }
        let embeddings: Vec<Vec<f32>> = (0..num_texts)
            .map(|i| flat[i * dim..(i + 1) * dim].to_vec())
            .collect();
        serde_json::to_string(&embeddings)
            .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))
    }

    #[napi]
    pub fn execute(&self, command: String) -> Result<String> {
        self.orchestrator.execute(&command).map_err(orchestrator_error_to_napi_error)
    }

    #[napi]
    pub fn hello_world(&self) -> String {
        self.orchestrator.hello_world()
    }
}

fn orchestrator_error_to_napi_error(error: OrchestratorError) -> Error {
    let status = match error.status_code() {
        1 | 2 => Status::InvalidArg,
        3 => Status::QueueFull,
        _ => Status::GenericFailure,
    };
    Error::new(status, error.to_string())
}