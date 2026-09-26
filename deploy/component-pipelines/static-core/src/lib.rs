// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Compiled, typed aggregate orchestration for the AGW static variant.
//!
//! This crate contains stage order and transport only. All preparation, worker
//! selection, inference, detokenization, and response policy remain in Dynamo.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dynamo_component_facades::proto::{
    ChatWorkerRequest, JsonItem, PostprocessOutput, PreparedItem, PreprocessItem, WorkerOutput,
    chat_worker_bridge_client::ChatWorkerBridgeClient, preprocessor_client::PreprocessorClient,
    selector_client::SelectorClient,
};
use serde::Deserialize;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::{Duration, Instant},
};
use tonic::{Streaming, transport::Channel};

const GRPC_STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const GRPC_CONNECTION_WINDOW_BYTES: u32 = 16 * 1024 * 1024;
const MAX_GRPC_CHANNELS_PER_ENDPOINT: usize = 256;
const DEFAULT_PREPROCESS_BATCH_MAX: usize = 32;
const DEFAULT_PREPROCESS_BATCH_LINGER_US: u64 = 200;
const DEFAULT_PREPROCESS_QUEUE_CAPACITY: usize = 8192;

struct PreprocessBatchItem {
    request: PreprocessItem,
    reply: oneshot::Sender<Result<PreparedItem, tonic::Status>>,
}

#[derive(Clone)]
struct PreprocessBatcher {
    sender: mpsc::Sender<PreprocessBatchItem>,
}

impl PreprocessBatcher {
    fn new(
        preprocessors: Arc<Vec<PreprocessorClient<Channel>>>,
        next_preprocessor: Arc<AtomicUsize>,
        max_batch: usize,
        linger_us: u64,
    ) -> Self {
        let queue_capacity = std::env::var("DYN_PREPROCESS_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_PREPROCESS_QUEUE_CAPACITY)
            .clamp(max_batch, 1_048_576);
        let (sender, mut receiver) = mpsc::channel::<PreprocessBatchItem>(queue_capacity);
        tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                let mut batch = Vec::with_capacity(max_batch);
                batch.push(first);
                if linger_us == 0 {
                    tokio::task::yield_now().await;
                    while batch.len() < max_batch {
                        match receiver.try_recv() {
                            Ok(item) => batch.push(item),
                            Err(_) => break,
                        }
                    }
                } else {
                    let deadline = Instant::now() + Duration::from_micros(linger_us);
                    while batch.len() < max_batch {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            break;
                        }
                        match tokio::time::timeout(remaining, receiver.recv()).await {
                            Ok(Some(item)) => batch.push(item),
                            _ => break,
                        }
                    }
                }

                let index = next_preprocessor.fetch_add(1, Ordering::Relaxed) % preprocessors.len();
                let mut client = preprocessors[index].clone();
                tokio::spawn(async move {
                    let (requests, replies): (Vec<_>, Vec<_>) = batch
                        .into_iter()
                        .map(|item| (item.request, item.reply))
                        .unzip();
                    let expected_ids = requests
                        .iter()
                        .map(|item| item.item_id.clone())
                        .collect::<Vec<_>>();
                    match client
                        .prepare_batch(dynamo_component_facades::proto::PreprocessBatchRequest {
                            items: requests,
                        })
                        .await
                    {
                        Ok(response) => {
                            let items = response.into_inner().items;
                            let valid = items.len() == replies.len()
                                && items
                                    .iter()
                                    .zip(&expected_ids)
                                    .all(|(item, expected)| item.item_id == *expected);
                            if valid {
                                for (reply, item) in replies.into_iter().zip(items) {
                                    let _ = reply.send(Ok(item));
                                }
                            } else {
                                for reply in replies {
                                    let _ = reply.send(Err(tonic::Status::internal(
                                        "preprocessor batch response did not preserve request order",
                                    )));
                                }
                            }
                        }
                        Err(status) => {
                            for reply in replies {
                                let _ = reply.send(Err(tonic::Status::new(
                                    status.code(),
                                    status.message().to_string(),
                                )));
                            }
                        }
                    }
                });
            }
        });
        Self { sender }
    }

    async fn prepare(&self, request: PreprocessItem) -> Result<PreparedItem, tonic::Status> {
        let (reply, receiver) = oneshot::channel();
        self.sender
            .send(PreprocessBatchItem { request, reply })
            .await
            .map_err(|_| tonic::Status::unavailable("preprocessor batcher stopped"))?;
        receiver
            .await
            .map_err(|_| tonic::Status::unavailable("preprocessor batcher dropped request"))?
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("preprocessor transport: {0}")]
    PreprocessorTransport(tonic::Status),
    #[error("preprocessor rejected request ({kind}): {message}")]
    PreprocessorItem { kind: String, message: String },
    #[error("selector transport: {0}")]
    SelectorTransport(tonic::Status),
    #[error("selector rejected request ({kind}): {message}")]
    SelectorItem { kind: String, message: String },
    #[error("invalid selector response: {0}")]
    SelectorResponse(serde_json::Error),
    #[error("component endpoint: {0}")]
    Endpoint(String),
    #[error("worker transport: {0}")]
    WorkerTransport(tonic::Status),
    #[error("prefill transport: {0}")]
    PrefillTransport(tonic::Status),
    #[error("prefill rejected request ({kind}): {message}")]
    PrefillItem { kind: String, message: String },
    #[error("invalid prefill chunk: {0}")]
    PrefillChunk(serde_json::Error),
    #[error("invalid prefill response: {0}")]
    PrefillResponse(&'static str),
}

#[derive(Deserialize)]
struct SelectedEndpoint {
    endpoint: String,
}

#[derive(Clone)]
pub struct StaticAggregatePipeline {
    preprocessors: Arc<Vec<PreprocessorClient<Channel>>>,
    next_preprocessor: Arc<AtomicUsize>,
    preprocess_batcher: Option<PreprocessBatcher>,
    selectors: Arc<Vec<SelectorClient<Channel>>>,
    next_selector: Arc<AtomicUsize>,
    prefill_channels: Option<Arc<Vec<Channel>>>,
    next_prefill: Arc<AtomicUsize>,
    worker_channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl StaticAggregatePipeline {
    pub fn new(preprocessor: Channel, selector: Channel) -> Self {
        Self {
            preprocessors: Arc::new(vec![PreprocessorClient::new(preprocessor)]),
            next_preprocessor: Arc::new(AtomicUsize::new(0)),
            preprocess_batcher: None,
            selectors: Arc::new(vec![SelectorClient::new(selector)]),
            next_selector: Arc::new(AtomicUsize::new(0)),
            prefill_channels: None,
            next_prefill: Arc::new(AtomicUsize::new(0)),
            worker_channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn connect(
        preprocessor_endpoint: String,
        selector_endpoint: String,
    ) -> Result<Self, PipelineError> {
        // The existing AGW static host uses this two-argument constructor. An
        // optional endpoint activates the compiled P/D path without moving any
        // Dynamo engine or response policy into the host.
        let prefill_endpoint = std::env::var("DYN_PREFILL_ENDPOINT")
            .ok()
            .filter(|endpoint| !endpoint.is_empty());
        Self::connect_with_prefill(preprocessor_endpoint, selector_endpoint, prefill_endpoint).await
    }

    pub async fn connect_with_prefill(
        preprocessor_endpoint: String,
        selector_endpoint: String,
        prefill_endpoint: Option<String>,
    ) -> Result<Self, PipelineError> {
        // HTTP/2 multiplexing deliberately keeps calls on one TCP connection. That is ideal
        // for one server, but a Kubernetes Service load-balances connections rather than
        // streams, so one channel pins the entire gateway to one preprocessor replica. Open a
        // small pool when requested so an independently scalable tokenizer/preprocessor fleet
        // is actually used. The default remains one for non-Kubernetes deployments.
        let pool_size = std::env::var("DYN_GRPC_CHANNELS_PER_ENDPOINT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1usize)
            .clamp(1, MAX_GRPC_CHANNELS_PER_ENDPOINT);
        let mut preprocessors = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let channel = configured_endpoint(preprocessor_endpoint.clone())?
                .connect()
                .await
                .map_err(|error| PipelineError::Endpoint(error.to_string()))?;
            preprocessors.push(PreprocessorClient::new(channel));
        }
        // Selector traffic also goes through a Kubernetes Service. A single
        // tonic Channel would pin every request to one selector replica.
        let mut selectors = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let channel = configured_endpoint(selector_endpoint.clone())?
                .connect()
                .await
                .map_err(|error| PipelineError::Endpoint(error.to_string()))?;
            selectors.push(SelectorClient::new(channel));
        }
        let preprocessors = Arc::new(preprocessors);
        let next_preprocessor = Arc::new(AtomicUsize::new(0));
        let max_batch = std::env::var("DYN_PREPROCESS_BATCH_MAX")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_PREPROCESS_BATCH_MAX)
            .clamp(1, 1024);
        let linger_us = std::env::var("DYN_PREPROCESS_BATCH_LINGER_US")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_PREPROCESS_BATCH_LINGER_US);
        let preprocess_batcher = Some(PreprocessBatcher::new(
            preprocessors.clone(),
            next_preprocessor.clone(),
            max_batch,
            linger_us,
        ));
        // A cloned tonic Channel shares its HTTP/2 connection. Open independent
        // connections so the Kubernetes Service can distribute prefill streams
        // across replicas, just as it does for the preprocessor pool.
        let prefill_channels = if let Some(endpoint) = prefill_endpoint {
            let mut channels = Vec::with_capacity(pool_size);
            for _ in 0..pool_size {
                channels.push(
                    configured_endpoint(endpoint.clone())?
                        .connect()
                        .await
                        .map_err(|error| PipelineError::Endpoint(error.to_string()))?,
                );
            }
            Some(Arc::new(channels))
        } else {
            None
        };
        Ok(Self {
            preprocessors,
            next_preprocessor,
            preprocess_batcher,
            selectors: Arc::new(selectors),
            next_selector: Arc::new(AtomicUsize::new(0)),
            prefill_channels,
            next_prefill: Arc::new(AtomicUsize::new(0)),
            worker_channels: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn worker_channel(&self, endpoint: &str) -> Result<Channel, PipelineError> {
        if let Some(channel) = self.worker_channels.lock().await.get(endpoint).cloned() {
            return Ok(channel);
        }
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?
            .tcp_nodelay(true)
            .initial_stream_window_size(Some(GRPC_STREAM_WINDOW_BYTES))
            .initial_connection_window_size(Some(GRPC_CONNECTION_WINDOW_BYTES))
            .connect()
            .await
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?;
        self.worker_channels
            .lock()
            .await
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// Execute the compiled prepare -> [prefill] -> select -> decode/postprocess state machine.
    pub async fn execute(
        &self,
        request_id: String,
        openai_request_json: Vec<u8>,
        deadline_unix_ms: i64,
    ) -> Result<Streaming<PostprocessOutput>, PipelineError> {
        let request = PreprocessItem {
            item_id: request_id.clone(),
            openai_request_json,
            preserve_omitted_max_tokens: false,
            lora_name: String::new(),
            deadline_unix_ms,
            packed_tokens_only: true,
        };
        let prepared = if let Some(batcher) = &self.preprocess_batcher {
            batcher
                .prepare(request)
                .await
                .map_err(PipelineError::PreprocessorTransport)?
        } else {
            let index =
                self.next_preprocessor.fetch_add(1, Ordering::Relaxed) % self.preprocessors.len();
            self.preprocessors[index]
                .clone()
                .prepare(request)
                .await
                .map_err(PipelineError::PreprocessorTransport)?
                .into_inner()
        };
        if let Some(error) = prepared.error.as_ref() {
            return Err(PipelineError::PreprocessorItem {
                kind: error.kind.clone(),
                message: error.message.clone(),
            });
        }

        let prefill_result_json = if let Some(channels) = &self.prefill_channels {
            let index = self.next_prefill.fetch_add(1, Ordering::Relaxed) % channels.len();
            let channel = channels[index].clone();
            let mut stream = ChatWorkerBridgeClient::new(channel)
                .generate_raw(ChatWorkerRequest {
                    request_id: request_id.clone(),
                    backend_request_json: prepared.backend_request_json.clone(),
                    normalized_openai_request_json: Vec::new(),
                    prompt_injected_reasoning: false,
                    uses_tool_call_structural_tag: false,
                    prompt_tokens: prepared.prompt_tokens,
                    image_tokens: prepared.image_tokens,
                    image_count: prepared.image_count,
                    video_count: prepared.video_count,
                    audio_count: prepared.audio_count,
                    deadline_unix_ms,
                    token_ids: Vec::new(),
                    token_ids_le: prepared.token_ids_le.clone(),
                    prefill_result_json: Vec::new(),
                })
                .await
                .map_err(PipelineError::PrefillTransport)?
                .into_inner();
            let mut handoff = None;
            let mut finished = false;
            while let Some(output) = stream
                .message()
                .await
                .map_err(PipelineError::PrefillTransport)?
            {
                if output.request_id != request_id {
                    return Err(PipelineError::PrefillResponse("request ID mismatch"));
                }
                if let Some(error) = output.error.as_ref() {
                    return Err(PipelineError::PrefillItem {
                        kind: error.kind.clone(),
                        message: error.message.clone(),
                    });
                }
                if let Some(value) = prefill_handoff(&output)? {
                    handoff = Some(value);
                }
                if output.finished {
                    finished = true;
                    break;
                }
            }
            if !finished {
                return Err(PipelineError::PrefillResponse("missing terminal frame"));
            }
            handoff.ok_or(PipelineError::PrefillResponse(
                "missing disaggregated handoff",
            ))?
        } else {
            Vec::new()
        };

        let selector_index =
            self.next_selector.fetch_add(1, Ordering::Relaxed) % self.selectors.len();
        let selected = self.selectors[selector_index]
            .clone()
            .select(JsonItem {
                item_id: request_id.clone(),
                payload_json: prepared.selector_request_json.clone(),
                deadline_unix_ms,
                token_ids: Vec::new(),
                token_ids_le: prepared.token_ids_le.clone(),
            })
            .await
            .map_err(PipelineError::SelectorTransport)?
            .into_inner();
        if let Some(error) = selected.error.as_ref() {
            return Err(PipelineError::SelectorItem {
                kind: error.kind.clone(),
                message: error.message.clone(),
            });
        }
        let selected: SelectedEndpoint = serde_json::from_slice(&selected.payload_json)
            .map_err(PipelineError::SelectorResponse)?;
        let channel = self.worker_channel(&selected.endpoint).await?;
        ChatWorkerBridgeClient::new(channel)
            .generate(ChatWorkerRequest {
                request_id,
                backend_request_json: prepared.backend_request_json,
                normalized_openai_request_json: prepared.normalized_openai_request_json,
                prompt_injected_reasoning: prepared.prompt_injected_reasoning,
                uses_tool_call_structural_tag: prepared.uses_tool_call_structural_tag,
                prompt_tokens: prepared.prompt_tokens,
                image_tokens: prepared.image_tokens,
                image_count: prepared.image_count,
                video_count: prepared.video_count,
                audio_count: prepared.audio_count,
                deadline_unix_ms,
                token_ids: Vec::new(),
                token_ids_le: prepared.token_ids_le,
                prefill_result_json,
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(PipelineError::WorkerTransport)
    }
}

fn prefill_handoff(output: &WorkerOutput) -> Result<Option<Vec<u8>>, PipelineError> {
    if output.annotated_backend_chunk_json.is_empty() {
        return Ok(None);
    }
    let annotated: serde_json::Value = serde_json::from_slice(&output.annotated_backend_chunk_json)
        .map_err(PipelineError::PrefillChunk)?;
    annotated
        .pointer("/data/disaggregated_params")
        .filter(|value| value.is_object())
        .map(|value| serde_json::to_vec(value).map_err(PipelineError::PrefillChunk))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_only_opaque_disaggregated_handoff() {
        let output = WorkerOutput {
            request_id: "test".into(),
            annotated_backend_chunk_json:
                br#"{"data":{"disaggregated_params":{"benchmark_handoff":true},"token_ids":[1]}}"#
                    .to_vec(),
            finished: false,
            error: None,
        };
        let handoff = prefill_handoff(&output).unwrap().unwrap();
        assert_eq!(handoff, br#"{"benchmark_handoff":true}"#);
    }

    #[test]
    fn unrelated_prefill_output_is_not_a_handoff() {
        let output = WorkerOutput {
            request_id: "test".into(),
            annotated_backend_chunk_json: br#"{"data":{"token_ids":[1]}}"#.to_vec(),
            finished: false,
            error: None,
        };
        assert!(prefill_handoff(&output).unwrap().is_none());
    }
}

fn configured_endpoint(endpoint: String) -> Result<tonic::transport::Endpoint, PipelineError> {
    Channel::from_shared(endpoint)
        .map_err(|error| PipelineError::Endpoint(error.to_string()))
        .map(|endpoint| {
            endpoint
                .tcp_nodelay(true)
                .initial_stream_window_size(Some(GRPC_STREAM_WINDOW_BYTES))
                .initial_connection_window_size(Some(GRPC_CONNECTION_WINDOW_BYTES))
        })
}
