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
use std::collections::HashMap;
use std::panic::catch_unwind;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

type PendingMap = Arc<Mutex<HashMap<u32, oneshot::Sender<String>>>>;

// ---------------------------------------------------------------------------
// 1. THE THREADSAFE JS BRIDGE (MANUAL CALLBACK RESOLUTION)
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

        // FIX: Removed the Ok() wrapper, pass the tuple directly!
        tsfn.call((id, payload), ThreadsafeFunctionCallMode::NonBlocking);

        // Wait for TypeScript to call `resolve_callback` which sends the data through `tx`
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
// 2. THE ENGINE EXPORT
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

    // TypeScript calls this method when its Promise finally resolves!
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
    pub async fn retrieve(&self, request_json: String) -> Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let req = serde_json::from_str(&request_json)
                .map_err(|e| Error::from_reason(format!("Invalid retrieval request: {}", e)))?;
            let results = inner
                .retrieve(req)
                .map_err(|e| Error::from_reason(e.to_string()))?;
            serde_json::to_string(&results).map_err(|e| Error::from_reason(e.to_string()))
        })
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub async fn recall(&self, request_json: String) -> Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let req = serde_json::from_str(&request_json)
                .map_err(|e| Error::from_reason(format!("Invalid recall request: {}", e)))?;
            inner
                .recall(req)
                .map_err(|e| Error::from_reason(e.to_string()))
        })
        .await
        .map_err(|e| Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub fn submit_augmentation(&self, input_json: String) -> Result<String> {
        let result = catch_unwind(std::panic::AssertUnwindSafe(|| {
            let input = serde_json::from_str(&input_json)
                .map_err(|e| Error::from_reason(format!("Invalid augmentation input: {}", e)))?;
            let accepted = self
                .inner
                .submit_augmentation(input)
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
