// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dynamo_llm::{
    preprocessor::{MultimodalCounts, OpenAIPreprocessor, PreprocessRequestOptions},
    protocols::openai::{GuidedToolConstraint, chat_completions::NvCreateChatCompletionRequest},
};
use futures::{StreamExt, stream};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

use crate::{
    deadline_expired, encode_token_ids_le, item_error,
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
    concurrency: Arc<Semaphore>,
    batch_stats: Option<Arc<BatchStats>>,
}

#[derive(Default)]
struct BatchStats {
    batches: AtomicU64,
    items: AtomicU64,
    bytes: AtomicU64,
    handler_us: AtomicU64,
    permit_wait_us: AtomicU64,
    prepare_us: AtomicU64,
}

fn elapsed_us(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}

impl PreprocessorFacade {
    fn request_payloads(
        item_id: &str,
        backend_request: &dynamo_llm::protocols::common::preprocessor::PreprocessedRequest,
    ) -> Result<(Vec<u8>, Vec<u8>), serde_json::Error> {
        // `token_ids` is Arc-backed, so this clone does not copy the prompt.
        // Clear it before serialization instead of building a large JSON array
        // only to remove that array from both transport envelopes.
        let mut backend_without_tokens = backend_request.clone();
        backend_without_tokens.token_ids = Arc::new(Vec::new());
        let mut backend_value = serde_json::to_value(backend_without_tokens)?;
        let backend_object = backend_value.as_object_mut().ok_or_else(|| {
            serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "PreprocessedRequest did not serialize as an object",
            ))
        })?;
        backend_object.remove("token_ids");

        let mut selector_value = backend_value.clone();
        let object = selector_value.as_object_mut().ok_or_else(|| {
            serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "PreprocessedRequest did not serialize as an object",
            ))
        })?;

        if let Some(model) = object.remove("model") {
            object.insert("model_name".to_string(), model);
        }
        object.insert(
            "selection_id".to_string(),
            serde_json::Value::String(item_id.to_string()),
        );

        // SelectionService consumes routing hints at the request root, while
        // PreprocessedRequest keeps them grouped for backend transport.
        if let Some(serde_json::Value::Object(routing)) = object.remove("routing") {
            for (key, value) in routing {
                object.entry(key).or_insert(value);
            }
        }
        Ok((
            serde_json::to_vec(&backend_value)?,
            serde_json::to_vec(&selector_value)?,
        ))
    }

    pub fn new(
        processor: Arc<OpenAIPreprocessor>,
        max_batch_items: usize,
        max_batch_bytes: usize,
        max_concurrency: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(max_batch_items > 0, "max_batch_items must be positive");
        anyhow::ensure!(max_batch_bytes > 0, "max_batch_bytes must be positive");
        anyhow::ensure!(max_concurrency > 0, "max_concurrency must be positive");
        let stats_interval = std::env::var("DYN_PREPROCESSOR_STATS_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|interval| (1..=3600).contains(interval));
        let batch_stats = stats_interval.map(|interval| {
            let stats = Arc::new(BatchStats::default());
            let reporter = Arc::downgrade(&stats);
            tokio::spawn(async move {
                let mut timer = tokio::time::interval(Duration::from_secs(interval));
                timer.tick().await;
                loop {
                    timer.tick().await;
                    let Some(stats) = reporter.upgrade() else {
                        break;
                    };
                    tracing::debug!(
                        target: "dynamo_preprocessor_batch_stats",
                        batches = stats.batches.load(Ordering::Relaxed),
                        items = stats.items.load(Ordering::Relaxed),
                        bytes = stats.bytes.load(Ordering::Relaxed),
                        handler_us = stats.handler_us.load(Ordering::Relaxed),
                        permit_wait_us = stats.permit_wait_us.load(Ordering::Relaxed),
                        prepare_us = stats.prepare_us.load(Ordering::Relaxed),
                        "preprocessor batch summary",
                    );
                }
            });
            stats
        });
        Ok(Self {
            processor,
            max_batch_items,
            max_batch_bytes,
            max_concurrency,
            concurrency: Arc::new(Semaphore::new(max_concurrency)),
            batch_stats,
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
        let (backend_request_json, selector_request_json) =
            match Self::request_payloads(&item_id, &prepared.backend_request) {
                Ok(value) => value,
                Err(err) => return error("internal", err.to_string(), false),
            };
        let guided_tool_constraint_json = match serde_json::to_vec(&prepared.guided_tool_constraint)
        {
            Ok(value) => value,
            Err(err) => return error("internal", err.to_string(), false),
        };
        let token_ids = prepared.backend_request.token_ids.as_ref().clone();
        let prompt_tokens = token_ids.len() as u64;
        let (token_ids, token_ids_le) = if item.packed_tokens_only {
            (Vec::new(), encode_token_ids_le(&token_ids))
        } else {
            (token_ids, Vec::new())
        };
        let mm_counts = MultimodalCounts::from_preprocessed(&prepared.backend_request);
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
            prompt_tokens,
            image_count: mm_counts.image as u64,
            video_count: mm_counts.video as u64,
            audio_count: mm_counts.audio as u64,
            selector_request_json,
            token_ids,
            token_ids_le,
        }
    }
}

#[tonic::async_trait]
impl Preprocessor for PreprocessorFacade {
    async fn prepare(
        &self,
        request: Request<crate::proto::PreprocessItem>,
    ) -> Result<Response<PreparedItem>, Status> {
        let item = request.into_inner();
        if item.item_id.is_empty() {
            return Err(Status::invalid_argument("item_id is empty"));
        }
        if item.openai_request_json.len() > self.max_batch_bytes {
            return Err(Status::resource_exhausted("request byte limit exceeded"));
        }
        let _permit = self
            .concurrency
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("preprocessor is shutting down"))?;
        Ok(Response::new(
            Self::prepare_one(self.processor.clone(), item).await,
        ))
    }

    async fn prepare_batch(
        &self,
        request: Request<PreprocessBatchRequest>,
    ) -> Result<Response<PreprocessBatchResponse>, Status> {
        let started_at = self.batch_stats.as_ref().map(|_| Instant::now());
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
        let concurrency = self.concurrency.clone();
        let stats = self.batch_stats.clone();
        let count = batch.items.len() as u64;
        let mut items = stream::iter(batch.items.into_iter().enumerate())
            .map(move |(index, item)| {
                let processor = processor.clone();
                let concurrency = concurrency.clone();
                let stats = stats.clone();
                async move {
                    let permit_started_at = stats.as_ref().map(|_| Instant::now());
                    let prepared = match concurrency.acquire_owned().await {
                        Ok(_permit) => {
                            let prepare_started_at = permit_started_at.map(|start| {
                                if let Some(stats) = &stats {
                                    stats
                                        .permit_wait_us
                                        .fetch_add(elapsed_us(start), Ordering::Relaxed);
                                }
                                Instant::now()
                            });
                            let prepared = Self::prepare_one(processor, item).await;
                            if let (Some(stats), Some(start)) = (&stats, prepare_started_at) {
                                stats
                                    .prepare_us
                                    .fetch_add(elapsed_us(start), Ordering::Relaxed);
                            }
                            prepared
                        }
                        Err(_) => PreparedItem {
                            item_id: item.item_id,
                            error: Some(item_error(
                                "unavailable",
                                "preprocessor is shutting down",
                                true,
                            )),
                            ..Default::default()
                        },
                    };
                    (index, prepared)
                }
            })
            .buffer_unordered(self.max_concurrency)
            .collect::<Vec<_>>()
            .await;
        items.sort_unstable_by_key(|(index, _)| *index);
        if let (Some(stats), Some(start)) = (&self.batch_stats, started_at) {
            stats.batches.fetch_add(1, Ordering::Relaxed);
            stats.items.fetch_add(count, Ordering::Relaxed);
            stats.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            stats
                .handler_us
                .fetch_add(elapsed_us(start), Ordering::Relaxed);
        }
        Ok(Response::new(PreprocessBatchResponse {
            items: items.into_iter().map(|(_, item)| item).collect(),
        }))
    }
}
