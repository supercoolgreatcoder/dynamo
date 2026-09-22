// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc};

use dynamo_llm::{
    preprocessor::{OpenAIPreprocessor, PreprocessRequestOptions},
    protocols::openai::{GuidedToolConstraint, chat_completions::NvCreateChatCompletionRequest},
};
use futures::{StreamExt, stream};
use tonic::{Request, Response, Status};

use crate::{
    deadline_expired, item_error,
    proto::{
        PreparedItem, PreprocessBatchRequest, PreprocessBatchResponse,
        preprocessor_server::Preprocessor,
    },
};

#[derive(Clone)]
pub struct PreprocessorFacade {
    processor: Arc<OpenAIPreprocessor>,
    max_batch_items: usize,
    max_batch_bytes: usize,
    max_concurrency: usize,
}

impl PreprocessorFacade {
    pub fn new(
        processor: Arc<OpenAIPreprocessor>,
        max_batch_items: usize,
        max_batch_bytes: usize,
        max_concurrency: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(max_batch_items > 0, "max_batch_items must be positive");
        anyhow::ensure!(max_batch_bytes > 0, "max_batch_bytes must be positive");
        anyhow::ensure!(max_concurrency > 0, "max_concurrency must be positive");
        Ok(Self {
            processor,
            max_batch_items,
            max_batch_bytes,
            max_concurrency,
        })
    }

    async fn prepare_one(
        processor: Arc<OpenAIPreprocessor>,
        item: crate::proto::PreprocessItem,
    ) -> PreparedItem {
        let item_id = item.item_id;
        let error = |kind: &str, message: String, retryable| PreparedItem {
            item_id: item_id.clone(),
            error: Some(item_error(kind, message, retryable)),
            ..Default::default()
        };
        if deadline_expired(item.deadline_unix_ms) {
            return error(
                "deadline_exceeded",
                "item deadline has expired".into(),
                true,
            );
        }
        let mut request: NvCreateChatCompletionRequest =
            match serde_json::from_slice(&item.openai_request_json) {
                Ok(request) => request,
                Err(err) => return error("invalid_argument", err.to_string(), false),
            };
        let original_stream_flag = request.inner.stream.unwrap_or(false);
        processor.normalize_chat_request(&mut request, original_stream_flag);
        let prepared = match processor
            .prepare_chat_request(
                &request,
                None,
                PreprocessRequestOptions {
                    preserve_omitted_max_tokens: item.preserve_omitted_max_tokens,
                },
                (!item.lora_name.is_empty()).then_some(item.lora_name),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(err) => return error("invalid_argument", err.to_string(), false),
        };
        let normalized_openai_request_json = match serde_json::to_vec(&request) {
            Ok(value) => value,
            Err(err) => return error("internal", err.to_string(), false),
        };
        let backend_request_json = match serde_json::to_vec(&prepared.backend_request) {
            Ok(value) => value,
            Err(err) => return error("internal", err.to_string(), false),
        };
        let guided_tool_constraint_json = match serde_json::to_vec(&prepared.guided_tool_constraint)
        {
            Ok(value) => value,
            Err(err) => return error("internal", err.to_string(), false),
        };
        PreparedItem {
            item_id,
            normalized_openai_request_json,
            backend_request_json,
            annotations: prepared.annotations,
            prompt_injected_reasoning: prepared.prompt_injected_reasoning,
            guided_tool_constraint_json,
            image_tokens: prepared.image_tokens.map(|count| count as u64),
            error: None,
            uses_tool_call_structural_tag: matches!(
                prepared.guided_tool_constraint,
                GuidedToolConstraint::StructuralTag
            ),
        }
    }
}

#[tonic::async_trait]
impl Preprocessor for PreprocessorFacade {
    async fn prepare_batch(
        &self,
        request: Request<PreprocessBatchRequest>,
    ) -> Result<Response<PreprocessBatchResponse>, Status> {
        let batch = request.into_inner();
        if batch.items.len() > self.max_batch_items {
            return Err(Status::resource_exhausted("batch item limit exceeded"));
        }
        let bytes = batch
            .items
            .iter()
            .map(|item| item.openai_request_json.len())
            .sum::<usize>();
        if bytes > self.max_batch_bytes {
            return Err(Status::resource_exhausted("batch byte limit exceeded"));
        }
        let mut ids = HashSet::with_capacity(batch.items.len());
        if batch
            .items
            .iter()
            .any(|item| item.item_id.is_empty() || !ids.insert(item.item_id.clone()))
        {
            return Err(Status::invalid_argument(
                "item_id values must be non-empty and unique within a batch",
            ));
        }
        let processor = self.processor.clone();
        let mut items = stream::iter(batch.items.into_iter().enumerate())
            .map(move |(index, item)| {
                let processor = processor.clone();
                async move { (index, Self::prepare_one(processor, item).await) }
            })
            .buffer_unordered(self.max_concurrency)
            .collect::<Vec<_>>()
            .await;
        items.sort_unstable_by_key(|(index, _)| *index);
        Ok(Response::new(PreprocessBatchResponse {
            items: items.into_iter().map(|(_, item)| item).collect(),
        }))
    }
}
