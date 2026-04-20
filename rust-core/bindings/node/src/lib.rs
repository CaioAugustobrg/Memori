#![deny(clippy::all)]

use engine_orchestrator::EngineOrchestrator;
use engine_orchestrator::search::FactId;
use engine_orchestrator::storage::{
    CandidateFactRow, EmbeddingRow, HostStorageError, StorageBridge, WriteAck, WriteBatch,
};
use napi::bindgen_prelude::Float32Array;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi_derive::napi;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::panic::catch_unwind;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

type PendingMap = Arc<Mutex<HashMap<u32, oneshot::Sender<String>>>>;

// ---------------------------------------------------------------------------
// 1. N-API STRUCT DEFINITIONS (PHASE 1 OPTIMIZATION)
// ---------------------------------------------------------------------------

#[napi(object)]
#[derive(Serialize, Deserialize)]
pub struct NapiRetrievalRequest {
    pub entity_id: String,
    pub query_text: String,
    pub dense_limit: u32,
    pub limit: u32,
}

#[napi(object)]
#[derive(Serialize, Deserialize)]
pub struct NapiRecallSummary {
    pub content: String,
    pub date_created: String,
    // Safely handle missing IDs from the engine
    pub entity_fact_id: Option<i64>,
    pub fact_id: Option<i64>,
}

#[napi(object)]
#[derive(Serialize, Deserialize)]
pub struct NapiRecallObject {
    pub id: i64,
    pub content: String,
    pub rank_score: Option<f64>,
    pub similarity: Option<f64>,
    pub date_created: Option<String>,
    pub summaries: Option<Vec<NapiRecallSummary>>,
}

#[napi(object)]
#[derive(Serialize, Deserialize)]
pub struct NapiMessage {
    pub role: String,
    pub content: String,
}

#[napi(object)]
#[derive(Serialize, Deserialize)]
pub struct NapiAugmentationInput {
    pub entity_id: String,

    // Strip keys entirely if they are undefined/None to prevent "null" panics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_messages: Option<Vec<NapiMessage>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_provider: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_model: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_provider_sdk_version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub framework: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_provider: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_dialect: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_cockroachdb: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdk_version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_mock_response: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fact_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

// ---------------------------------------------------------------------------
// 2. THE THREADSAFE JS BRIDGE (MANUAL CALLBACK RESOLUTION)
// ---------------------------------------------------------------------------
struct NodeStorageBridge {
    fetch_embeddings_tsfn: ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>,
    fetch_facts_by_ids_tsfn: ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>,
    write_batch_tsfn: ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>,
    pending_requests: PendingMap,
    next_id: AtomicU32,
}

impl NodeStorageBridge {
    async fn call_js_async(
        &self,
        tsfn: &ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>,
        payload: String,
    ) -> std::result::Result<String, HostStorageError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().unwrap().insert(id, tx);

        tsfn.call((id, payload), ThreadsafeFunctionCallMode::NonBlocking);

        rx.await
            .map_err(|_| HostStorageError::new("NAPI_ERR", "JS callback channel dropped"))
    }
}

impl StorageBridge for NodeStorageBridge {
    fn fetch_embeddings(
        &self,
        entity_id: &str,
        limit: usize,
    ) -> std::result::Result<Vec<EmbeddingRow>, HostStorageError> {
        let payload = serde_json::json!({ "entity_id": entity_id, "limit": limit }).to_string();

        let js_result: String = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.call_js_async(&self.fetch_embeddings_tsfn, payload)
                    .await
            })
        })?;

        serde_json::from_str(&js_result)
            .map_err(|e| HostStorageError::new("JSON_ERR", e.to_string()))
    }

    fn fetch_facts_by_ids(
        &self,
        ids: &[FactId],
    ) -> std::result::Result<Vec<CandidateFactRow>, HostStorageError> {
        let payload = serde_json::json!({ "ids": ids }).to_string();

        let js_result: String = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.call_js_async(&self.fetch_facts_by_ids_tsfn, payload)
                    .await
            })
        })?;

        serde_json::from_str(&js_result)
            .map_err(|e| HostStorageError::new("JSON_ERR", e.to_string()))
    }

    fn write_batch(&self, batch: &WriteBatch) -> std::result::Result<WriteAck, HostStorageError> {
        let payload = serde_json::to_string(batch)
            .map_err(|e| HostStorageError::new("JSON_ERR", e.to_string()))?;

        let js_result: String = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.call_js_async(&self.write_batch_tsfn, payload).await })
        })?;

        serde_json::from_str(&js_result)
            .map_err(|e| HostStorageError::new("JSON_ERR", e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// 3. THE ENGINE EXPORT
// ---------------------------------------------------------------------------

#[napi]
pub struct MemoriEngine {
    inner: Arc<EngineOrchestrator>,
    pending_requests: PendingMap,
}

#[napi]
impl MemoriEngine {
    #[napi(constructor)]
    pub fn new(
        model_name: Option<String>,
        #[napi(ts_arg_type = "(id: number, reqJson: string) => void")]
        fetch_embeddings_cb: JsFunction,
        #[napi(ts_arg_type = "(id: number, reqJson: string) => void")]
        fetch_facts_by_ids_cb: JsFunction,
        #[napi(ts_arg_type = "(id: number, reqJson: string) => void")] write_batch_cb: JsFunction,
    ) -> Result<Self> {
        let build_tsfn = |js_func: JsFunction| -> Result<ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>> {
            js_func.create_threadsafe_function(0, |ctx: ThreadSafeCallContext<(u32, String)>| {
                let env = ctx.env;
                let arg1 = env.create_uint32(ctx.value.0)?;
                let arg2 = env.create_string(&ctx.value.1)?;
                Ok(vec![arg1.into_unknown(), arg2.into_unknown()])
            })
        };

        let pending_requests = Arc::new(Mutex::new(HashMap::new()));

        let bridge = Arc::new(NodeStorageBridge {
            fetch_embeddings_tsfn: build_tsfn(fetch_embeddings_cb)?,
            fetch_facts_by_ids_tsfn: build_tsfn(fetch_facts_by_ids_cb)?,
            write_batch_tsfn: build_tsfn(write_batch_cb)?,
            pending_requests: pending_requests.clone(),
            next_id: AtomicU32::new(1),
        });

        let inner = EngineOrchestrator::new_with_storage(model_name.as_deref(), Some(bridge))
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Self {
            inner: Arc::new(inner),
            pending_requests,
        })
    }

    #[napi]
    pub fn resolve_callback(&self, id: u32, result: String) {
        if let Some(tx) = self.pending_requests.lock().unwrap().remove(&id) {
            let _ = tx.send(result);
        }
    }

    #[napi]
    pub fn embed_texts(&self, texts: Vec<String>) -> Result<Vec<Float32Array>> {
        let result = catch_unwind(std::panic::AssertUnwindSafe(|| {
            let (flat_vectors, shape) = self.inner.embed(texts);

            let mut out = Vec::with_capacity(shape[0]);
            let dim = shape[1];
            for chunk in flat_vectors.chunks(dim) {
                out.push(Float32Array::new(chunk.to_vec()));
            }
            Ok(out)
        }));

        match result {
            Ok(Ok(arr)) => Ok(arr),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::from_reason(
                "Rust panicked during embed_texts!".to_string(),
            )),
        }
    }

    #[napi]
    pub async fn retrieve(&self, request: NapiRetrievalRequest) -> Result<Vec<NapiRecallObject>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            // Bridge via internal Serde values to avoid guessing opaque engine_orchestrator types
            let req = serde_json::from_value(serde_json::to_value(&request).unwrap())
                .map_err(|e| Error::from_reason(format!("Invalid retrieval request: {}", e)))?;

            let results = inner
                .retrieve(req)
                .map_err(|e| Error::from_reason(e.to_string()))?;

            let napi_results: Vec<NapiRecallObject> =
                serde_json::from_value(serde_json::to_value(&results).unwrap())
                    .map_err(|e| Error::from_reason(e.to_string()))?;

            Ok(napi_results)
        })
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub async fn recall(&self, request: NapiRetrievalRequest) -> Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let req = serde_json::from_value(serde_json::to_value(&request).unwrap())
                .map_err(|e| Error::from_reason(format!("Invalid recall request: {}", e)))?;

            inner
                .recall(req)
                .map_err(|e| Error::from_reason(e.to_string()))
        })
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub fn submit_augmentation(&self, input: NapiAugmentationInput) -> Result<String> {
        let result = catch_unwind(std::panic::AssertUnwindSafe(|| {
            let core_input = serde_json::from_value(serde_json::to_value(&input).unwrap())
                .map_err(|e| Error::from_reason(format!("Invalid augmentation input: {}", e)))?;

            let accepted = self
                .inner
                .submit_augmentation(core_input)
                .map_err(|e| Error::from_reason(e.to_string()))?;

            Ok(accepted.job_id.to_string())
        }));

        match result {
            Ok(Ok(id)) => Ok(id),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::from_reason(
                "Rust panicked during augmentation submit!".to_string(),
            )),
        }
    }

    #[napi]
    pub async fn wait_for_augmentation(&self, timeout_ms: Option<u32>) -> Result<bool> {
        let timeout = timeout_ms.map(|ms| std::time::Duration::from_millis(ms as u64));
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || inner.wait_for_augmentation(timeout))
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?
            .map_err(|e| Error::from_reason(e.to_string()))
    }
}
