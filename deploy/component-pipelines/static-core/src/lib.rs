// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Compiled, typed aggregate orchestration for the AGW static variant.
//!
//! This crate contains stage order and transport only. All preparation, worker
//! selection, inference, detokenization, and response policy remain in Dynamo.

use std::{collections::HashMap, sync::Arc};

use dynamo_component_facades::proto::{
    ChatWorkerRequest, JsonItem, PostprocessOutput, PreprocessItem,
    chat_worker_bridge_client::ChatWorkerBridgeClient, preprocessor_client::PreprocessorClient,
    selector_client::SelectorClient,
};
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::{Streaming, transport::Channel};

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
}

#[derive(Deserialize)]
struct SelectedEndpoint {
    endpoint: String,
}

#[derive(Clone)]
pub struct StaticAggregatePipeline {
    preprocessor: PreprocessorClient<Channel>,
    selector: SelectorClient<Channel>,
    worker_channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl StaticAggregatePipeline {
    pub fn new(preprocessor: Channel, selector: Channel) -> Self {
        Self {
            preprocessor: PreprocessorClient::new(preprocessor),
            selector: SelectorClient::new(selector),
            worker_channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn connect(
        preprocessor_endpoint: String,
        selector_endpoint: String,
    ) -> Result<Self, PipelineError> {
        let preprocessor = Channel::from_shared(preprocessor_endpoint)
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?
            .connect()
            .await
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?;
        let selector = Channel::from_shared(selector_endpoint)
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?
            .connect()
            .await
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?;
        Ok(Self::new(preprocessor, selector))
    }

    async fn worker_channel(&self, endpoint: &str) -> Result<Channel, PipelineError> {
        if let Some(channel) = self.worker_channels.lock().await.get(endpoint).cloned() {
            return Ok(channel);
        }
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?
            .connect()
            .await
            .map_err(|error| PipelineError::Endpoint(error.to_string()))?;
        self.worker_channels
            .lock()
            .await
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// Execute the compiled prepare -> select -> worker/postprocess state machine.
    pub async fn execute(
        &self,
        request_id: String,
        openai_request_json: Vec<u8>,
        deadline_unix_ms: i64,
    ) -> Result<Streaming<PostprocessOutput>, PipelineError> {
        let prepared = self
            .preprocessor
            .clone()
            .prepare(PreprocessItem {
                item_id: request_id.clone(),
                openai_request_json,
                preserve_omitted_max_tokens: false,
                lora_name: String::new(),
                deadline_unix_ms,
            })
            .await
            .map_err(PipelineError::PreprocessorTransport)?
            .into_inner();
        if let Some(error) = prepared.error.as_ref() {
            return Err(PipelineError::PreprocessorItem {
                kind: error.kind.clone(),
                message: error.message.clone(),
            });
        }

        let selected = self
            .selector
            .clone()
            .select(JsonItem {
                item_id: request_id.clone(),
                payload_json: prepared.selector_request_json.clone(),
                deadline_unix_ms,
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
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(PipelineError::WorkerTransport)
    }
}
