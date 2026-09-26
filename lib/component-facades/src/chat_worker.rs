// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{pin::Pin, sync::Arc};

use dynamo_llm::{
    preprocessor::{MultimodalCounts, OpenAIPreprocessor, PreprocessedRequest},
    protocols::openai::chat_completions::NvCreateChatCompletionRequest,
};
use dynamo_runtime::pipeline::Context;
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::{
    deadline_expired, item_error,
    proto::{
        ChatWorkerRequest, PostprocessOutput, WorkerOutput,
        chat_worker_bridge_server::ChatWorkerBridge,
    },
    token_ids_from_wire,
    worker::CanonicalBackendEngine,
};

/// Composes an existing Dynamo backend pipeline with Dynamo's canonical response
/// processing at the worker-side boundary. It adds no inference or parsing policy.
#[derive(Clone)]
pub struct ChatWorkerFacade {
    engine: CanonicalBackendEngine,
    processor: Arc<OpenAIPreprocessor>,
    max_request_bytes: usize,
    max_chunk_bytes: usize,
}

impl ChatWorkerFacade {
    pub fn new(
        engine: CanonicalBackendEngine,
        processor: Arc<OpenAIPreprocessor>,
        max_request_bytes: usize,
        max_chunk_bytes: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(max_request_bytes > 0, "max_request_bytes must be positive");
        anyhow::ensure!(max_chunk_bytes > 0, "max_chunk_bytes must be positive");
        Ok(Self {
            engine,
            processor,
            max_request_bytes,
            max_chunk_bytes,
        })
    }

    fn backend_request(
        &self,
        request: &mut ChatWorkerRequest,
    ) -> Result<PreprocessedRequest, Status> {
        if request.request_id.is_empty() {
            return Err(Status::invalid_argument("request_id is empty"));
        }
        if deadline_expired(request.deadline_unix_ms) {
            return Err(Status::deadline_exceeded("request deadline has expired"));
        }
        let total_bytes = request
            .backend_request_json
            .len()
            .saturating_add(request.normalized_openai_request_json.len())
            .saturating_add(request.token_ids.len().saturating_mul(size_of::<u32>()))
            .saturating_add(request.token_ids_le.len())
            .saturating_add(request.prefill_result_json.len());
        if total_bytes > self.max_request_bytes {
            return Err(Status::resource_exhausted("request byte limit exceeded"));
        }
        let mut backend_value: serde_json::Value =
            serde_json::from_slice(&request.backend_request_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let backend_object = backend_value
            .as_object_mut()
            .ok_or_else(|| Status::invalid_argument("backend request must be a JSON object"))?;
        let token_ids = token_ids_from_wire(
            std::mem::take(&mut request.token_ids),
            &request.token_ids_le,
        )
        .map_err(Status::invalid_argument)?;
        backend_object.insert(
            "token_ids".to_string(),
            serde_json::to_value(token_ids)
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
        );
        if !request.prefill_result_json.is_empty() {
            if backend_object.contains_key("prefill_result") {
                return Err(Status::invalid_argument(
                    "prefill_result is already present in backend request",
                ));
            }
            let handoff: serde_json::Value =
                serde_json::from_slice(&request.prefill_result_json)
                    .map_err(|error| Status::invalid_argument(error.to_string()))?;
            if !handoff.is_object() {
                return Err(Status::invalid_argument(
                    "prefill_result_json must contain a JSON object",
                ));
            }
            backend_object.insert(
                "prefill_result".to_string(),
                serde_json::json!({"disaggregated_params": handoff}),
            );
        }
        serde_json::from_value(backend_value)
            .map_err(|error| Status::invalid_argument(error.to_string()))
    }
}

#[tonic::async_trait]
impl ChatWorkerBridge for ChatWorkerFacade {
    type GenerateStream = Pin<Box<dyn Stream<Item = Result<PostprocessOutput, Status>> + Send>>;

    async fn generate(
        &self,
        request: Request<ChatWorkerRequest>,
    ) -> Result<Response<Self::GenerateStream>, Status> {
        let mut request = request.into_inner();
        let backend_request = self.backend_request(&mut request)?;
        let chat_request: NvCreateChatCompletionRequest =
            serde_json::from_slice(&request.normalized_openai_request_json)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let prompt_tokens = u32::try_from(request.prompt_tokens)
            .map_err(|_| Status::invalid_argument("prompt_tokens exceeds u32"))?;
        let request_id = request.request_id;
        let backend_stream = self
            .engine
            .generate(Context::with_id_and_metadata(
                backend_request,
                request_id.clone(),
                Default::default(),
            ))
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let stream = self
            .processor
            .postprocess_backend_chat_stream(
                backend_stream,
                &chat_request,
                request_id.clone(),
                prompt_tokens,
                request.prompt_injected_reasoning,
                request.uses_tool_call_structural_tag,
                MultimodalCounts {
                    image: request.image_count as usize,
                    video: request.video_count as usize,
                    audio: request.audio_count as usize,
                },
                request.image_tokens.map(|value| value as usize),
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let max_chunk_bytes = self.max_chunk_bytes;
        let output = stream.filter_map(move |chunk| {
            let request_id = request_id.clone();
            async move {
                let data = match chunk.into_data() {
                    Ok(Some(data)) => data,
                    Ok(None) => return None,
                    Err(error) => {
                        return Some(Ok(PostprocessOutput {
                            request_id,
                            openai_chunk_json: Vec::new(),
                            finished: true,
                            error: Some(item_error("backend", error.to_string(), false)),
                        }));
                    }
                };
                let payload = match serde_json::to_vec(&data) {
                    Ok(payload) => payload,
                    Err(error) => return Some(Err(Status::internal(error.to_string()))),
                };
                if payload.len() > max_chunk_bytes {
                    return Some(Err(Status::resource_exhausted(
                        "response chunk byte limit exceeded",
                    )));
                }
                Some(Ok(PostprocessOutput {
                    request_id,
                    openai_chunk_json: payload,
                    finished: false,
                    error: None,
                }))
            }
        });
        Ok(Response::new(Box::pin(output)))
    }

    type GenerateRawStream = Pin<Box<dyn Stream<Item = Result<WorkerOutput, Status>> + Send>>;

    async fn generate_raw(
        &self,
        request: Request<ChatWorkerRequest>,
    ) -> Result<Response<Self::GenerateRawStream>, Status> {
        let mut request = request.into_inner();
        let started_at = crate::rpc_sample_start(&request.request_id);
        let shape = started_at.map(|start| {
            (
                start,
                request.backend_request_json.len(),
                request.token_ids_le.len(),
                request.token_ids.len(),
                request.prompt_tokens,
            )
        });
        let backend_request = self.backend_request(&mut request)?;
        let request_id = request.request_id;
        let backend_stream = self
            .engine
            .generate(Context::with_id_and_metadata(
                backend_request,
                request_id.clone(),
                Default::default(),
            ))
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        if let Some((
            start,
            backend_bytes,
            packed_token_bytes,
            repeated_token_count,
            prompt_tokens,
        )) = shape
        {
            tracing::debug!(
                target: "dynamo_component_rpc_sample",
                component = "worker",
                operation = "generate_raw",
                phase = "handler",
                request_id = %request_id,
                elapsed_us = crate::rpc_sample_elapsed_us(start),
                backend_bytes,
                packed_token_bytes,
                repeated_token_count,
                prompt_tokens,
                "component RPC sample",
            );
        }
        let max_chunk_bytes = self.max_chunk_bytes;
        let chunk_id = request_id.clone();
        let chunks = backend_stream.map(move |chunk| {
            let payload =
                serde_json::to_vec(&chunk).map_err(|error| Status::internal(error.to_string()))?;
            if payload.len() > max_chunk_bytes {
                return Err(Status::resource_exhausted(
                    "response chunk byte limit exceeded",
                ));
            }
            Ok(WorkerOutput {
                request_id: chunk_id.clone(),
                annotated_backend_chunk_json: payload,
                finished: false,
                error: None,
            })
        });
        let finished = futures::stream::once(async move {
            if let Some((
                start,
                backend_bytes,
                packed_token_bytes,
                repeated_token_count,
                prompt_tokens,
            )) = shape
            {
                tracing::debug!(
                    target: "dynamo_component_rpc_sample",
                    component = "worker",
                    operation = "generate_raw",
                    phase = "terminal",
                    request_id = %request_id,
                    elapsed_us = crate::rpc_sample_elapsed_us(start),
                    backend_bytes,
                    packed_token_bytes,
                    repeated_token_count,
                    prompt_tokens,
                    "component RPC sample",
                );
            }
            Ok(WorkerOutput {
                request_id,
                annotated_backend_chunk_json: Vec::new(),
                finished: true,
                error: None,
            })
        });
        Ok(Response::new(Box::pin(chunks.chain(finished))))
    }
}
