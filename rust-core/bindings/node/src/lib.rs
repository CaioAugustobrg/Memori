#![deny(clippy::all)]

use engine_orchestrator::EngineOrchestrator;
use engine_orchestrator::search::FactId;
use engine_orchestrator::storage::{
    CandidateFactRow, EmbeddingRow, HostStorageError, StorageBridge, WriteAck, WriteBatch,
};
use napi::Either;
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

// ---------------------------------------------------------------------------
// 1. PHASE 1: CORE API N-API STRUCTS
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
// 2. PHASE 2/3: ZERO-COPY STORAGE BRIDGE N-API STRUCTS
// ---------------------------------------------------------------------------

#[napi(object)]
pub struct NapiEmbeddingRow {
    pub id: Either<i64, String>,
    pub content_embedding: Float32Array,
}

#[napi(object)]
#[derive(Serialize)]
pub struct NapiCandidateSummaryRow {
    pub content: String,
    pub date_created: String,
}

#[napi(object)]
pub struct NapiCandidateFactRow {
    pub id: Either<i64, String>,
    pub content: String,
    pub date_created: String,
    pub summaries: Option<Vec<NapiCandidateSummaryRow>>,
}

#[napi(object)]
pub struct NapiWriteAck {
    pub written_ops: u32,
}

type PendingEmbeddingsMap = Arc<Mutex<HashMap<u32, oneshot::Sender<Vec<EmbeddingRow>>>>>;
type PendingFactsMap = Arc<Mutex<HashMap<u32, oneshot::Sender<Vec<CandidateFactRow>>>>>;
type PendingWritesMap = Arc<Mutex<HashMap<u32, oneshot::Sender<WriteAck>>>>;

// ---------------------------------------------------------------------------
// 3. THE THREADSAFE JS BRIDGE (Zero-Copy Callbacks)
// ---------------------------------------------------------------------------
struct NodeStorageBridge {
    fetch_embeddings_tsfn: ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>,
    fetch_facts_by_ids_tsfn: ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>,
    write_batch_tsfn: ThreadsafeFunction<(u32, String), ErrorStrategy::Fatal>,
    pending_embeddings: PendingEmbeddingsMap,
    pending_facts: PendingFactsMap,
    pending_writes: PendingWritesMap,
    next_id: AtomicU32,
}

impl StorageBridge for NodeStorageBridge {
    fn fetch_embeddings(
        &self,
        entity_id: &str,
        limit: usize,
    ) -> std::result::Result<Vec<EmbeddingRow>, HostStorageError> {
        let payload = serde_json::json!({ "entity_id": entity_id, "limit": limit }).to_string();
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending_embeddings.lock().unwrap().insert(id, tx);

        self.fetch_embeddings_tsfn
            .call((id, payload), ThreadsafeFunctionCallMode::NonBlocking);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                rx.await
                    .map_err(|_| HostStorageError::new("NAPI_ERR", "Channel dropped"))
            })
        })
    }

    fn fetch_facts_by_ids(
        &self,
        ids: &[FactId],
    ) -> std::result::Result<Vec<CandidateFactRow>, HostStorageError> {
        let payload = serde_json::json!({ "ids": ids }).to_string();
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending_facts.lock().unwrap().insert(id, tx);

        self.fetch_facts_by_ids_tsfn
            .call((id, payload), ThreadsafeFunctionCallMode::NonBlocking);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                rx.await
                    .map_err(|_| HostStorageError::new("NAPI_ERR", "Channel dropped"))
            })
        })
    }

    fn write_batch(&self, batch: &WriteBatch) -> std::result::Result<WriteAck, HostStorageError> {
        let payload = serde_json::to_string(batch)
            .map_err(|e| HostStorageError::new("JSON_ERR", e.to_string()))?;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending_writes.lock().unwrap().insert(id, tx);

        self.write_batch_tsfn
            .call((id, payload), ThreadsafeFunctionCallMode::NonBlocking);

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                rx.await
                    .map_err(|_| HostStorageError::new("NAPI_ERR", "Channel dropped"))
            })
        })
    }
}

// ---------------------------------------------------------------------------
// 4. THE ENGINE EXPORT
// ---------------------------------------------------------------------------

#[napi]
pub struct MemoriEngine {
    inner: Arc<EngineOrchestrator>,
    pending_embeddings: PendingEmbeddingsMap,
    pending_facts: PendingFactsMap,
    pending_writes: PendingWritesMap,
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

        let pending_embeddings = Arc::new(Mutex::new(HashMap::new()));
        let pending_facts = Arc::new(Mutex::new(HashMap::new()));
        let pending_writes = Arc::new(Mutex::new(HashMap::new()));

        let bridge = Arc::new(NodeStorageBridge {
            fetch_embeddings_tsfn: build_tsfn(fetch_embeddings_cb)?,
            fetch_facts_by_ids_tsfn: build_tsfn(fetch_facts_by_ids_cb)?,
            write_batch_tsfn: build_tsfn(write_batch_cb)?,
            pending_embeddings: pending_embeddings.clone(),
            pending_facts: pending_facts.clone(),
            pending_writes: pending_writes.clone(),
            next_id: AtomicU32::new(1),
        });

        let inner = EngineOrchestrator::new_with_storage(model_name.as_deref(), Some(bridge))
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Self {
            inner: Arc::new(inner),
            pending_embeddings,
            pending_facts,
            pending_writes,
        })
    }

    #[napi]
    pub fn resolve_embeddings_callback(&self, id: u32, result: Vec<NapiEmbeddingRow>) {
        // Collect directly into a Vec, no Result needed
        let rows: Vec<EmbeddingRow> = result
            .into_iter()
            .map(|r| {
                let id_val = match r.id {
                    Either::A(num) => serde_json::json!(num),
                    Either::B(s) => serde_json::json!(s),
                };

                // This is the Magic Zero-Copy! The f32s are pulled directly from V8 Memory
                let floats = r.content_embedding.to_vec();

                let mut obj = serde_json::Map::new();
                obj.insert("id".to_string(), id_val);
                obj.insert("content_embedding".to_string(), serde_json::json!(floats));

                // Unwrap directly since we strictly control the shape of this object
                serde_json::from_value(serde_json::Value::Object(obj)).unwrap()
            })
            .collect();

        if let Some(tx) = self.pending_embeddings.lock().unwrap().remove(&id) {
            let _ = tx.send(rows);
        }
    }

    #[napi]
    pub fn resolve_facts_callback(&self, id: u32, result: Vec<NapiCandidateFactRow>) {
        // Collect directly into a Vec, no Result needed
        let rows: Vec<CandidateFactRow> = result
            .into_iter()
            .map(|r| {
                let id_val = match r.id {
                    Either::A(num) => serde_json::json!(num),
                    Either::B(s) => serde_json::json!(s),
                };

                let mut obj = serde_json::Map::new();
                obj.insert("id".to_string(), id_val);
                obj.insert("content".to_string(), serde_json::json!(r.content));
                obj.insert(
                    "date_created".to_string(),
                    serde_json::json!(r.date_created),
                );
                if let Some(sums) = r.summaries {
                    obj.insert("summaries".to_string(), serde_json::to_value(sums).unwrap());
                }

                // Unwrap directly since we strictly control the shape of this object
                serde_json::from_value(serde_json::Value::Object(obj)).unwrap()
            })
            .collect();

        if let Some(tx) = self.pending_facts.lock().unwrap().remove(&id) {
            let _ = tx.send(rows);
        }
    }

    #[napi]
    pub fn resolve_write_callback(&self, id: u32, result: NapiWriteAck) {
        if let Some(tx) = self.pending_writes.lock().unwrap().remove(&id) {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "written_ops".to_string(),
                serde_json::json!(result.written_ops),
            );
            let ack: WriteAck = serde_json::from_value(serde_json::Value::Object(obj)).unwrap();
            let _ = tx.send(ack);
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
