// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, pin::Pin, sync::Arc};

use dynamo_llm::{
    preprocessor::OpenAIPreprocessor,
    protocols::openai::chat_completions::{
        NvCreateChatCompletionRequest, NvCreateChatCompletionStreamResponse,
    },
};
use dynamo_runtime::protocols::annotated::Annotated;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::{
    item_error,
    proto::{
        PostprocessInput, PostprocessOutput, postprocess_input, postprocessor_server::Postprocessor,
    },
};

#[derive(Clone)]
pub struct PostprocessorFacade {
    processor: Arc<OpenAIPreprocessor>,
    max_active_requests: usize,
    session_queue_capacity: usize,
    output_queue_capacity: usize,
    max_chunk_bytes: usize,
}

impl PostprocessorFacade {
    pub fn new(
        processor: Arc<OpenAIPreprocessor>,
        max_active_requests: usize,
        session_queue_capacity: usize,
        output_queue_capacity: usize,
        max_chunk_bytes: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            max_active_requests > 0,
            "max_active_requests must be positive"
        );
        anyhow::ensure!(
            session_queue_capacity > 0,
            "session_queue_capacity must be positive"
        );
        anyhow::ensure!(
            output_queue_capacity > 0,
            "output_queue_capacity must be positive"
        );
        anyhow::ensure!(max_chunk_bytes > 0, "max_chunk_bytes must be positive");
        Ok(Self {
            processor,
            max_active_requests,
            session_queue_capacity,
            output_queue_capacity,
            max_chunk_bytes,
        })
    }
}

#[tonic::async_trait]
impl Postprocessor for PostprocessorFacade {
    type ProcessStream = Pin<Box<dyn Stream<Item = Result<PostprocessOutput, Status>> + Send>>;

    async fn process(
        &self,
        request: Request<Streaming<PostprocessInput>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        let mut inbound = request.into_inner();
        let processor = self.processor.clone();
        let max_active_requests = self.max_active_requests;
        let session_queue_capacity = self.session_queue_capacity;
        let max_chunk_bytes = self.max_chunk_bytes;
        let (output_tx, output_rx) = mpsc::channel(self.output_queue_capacity);

        tokio::spawn(async move {
            let mut sessions: HashMap<
                String,
                mpsc::Sender<Annotated<NvCreateChatCompletionStreamResponse>>,
            > = HashMap::new();
            while let Some(frame) = inbound.next().await {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = output_tx.send(Err(error)).await;
                        break;
                    }
                };
                match frame.frame {
                    Some(postprocess_input::Frame::Open(open)) => {
                        if open.request_id.is_empty() {
                            send_error(&output_tx, "", "invalid_argument", "request_id is empty")
                                .await;
                            continue;
                        }
                        if sessions.contains_key(&open.request_id) {
                            send_error(
                                &output_tx,
                                &open.request_id,
                                "conflict",
                                "request is already open",
                            )
                            .await;
                            continue;
                        }
                        if sessions.len() >= max_active_requests {
                            send_error(
                                &output_tx,
                                &open.request_id,
                                "resource_exhausted",
                                "active request limit exceeded",
                            )
                            .await;
                            continue;
                        }
                        let chat_request: NvCreateChatCompletionRequest =
                            match serde_json::from_slice(&open.normalized_openai_request_json) {
                                Ok(request) => request,
                                Err(error) => {
                                    send_error(
                                        &output_tx,
                                        &open.request_id,
                                        "invalid_argument",
                                        &error.to_string(),
                                    )
                                    .await;
                                    continue;
                                }
                            };
                        let (session_tx, session_rx) = mpsc::channel(session_queue_capacity);
                        let stream = match processor.postprocess_chat_stream(
                            ReceiverStream::new(session_rx),
                            &chat_request,
                            open.prompt_injected_reasoning,
                            open.uses_tool_call_structural_tag,
                        ) {
                            Ok(stream) => stream,
                            Err(error) => {
                                send_error(
                                    &output_tx,
                                    &open.request_id,
                                    "invalid_argument",
                                    &error.to_string(),
                                )
                                .await;
                                continue;
                            }
                        };
                        sessions.insert(open.request_id.clone(), session_tx);
                        let request_id = open.request_id;
                        let output_tx = output_tx.clone();
                        tokio::spawn(async move {
                            futures::pin_mut!(stream);
                            while let Some(chunk) = stream.next().await {
                                let payload = match serde_json::to_vec(&chunk) {
                                    Ok(payload) => payload,
                                    Err(error) => {
                                        send_error(
                                            &output_tx,
                                            &request_id,
                                            "internal",
                                            &error.to_string(),
                                        )
                                        .await;
                                        return;
                                    }
                                };
                                if output_tx
                                    .send(Ok(PostprocessOutput {
                                        request_id: request_id.clone(),
                                        openai_chunk_json: payload,
                                        finished: false,
                                        error: None,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            let _ = output_tx
                                .send(Ok(PostprocessOutput {
                                    request_id,
                                    openai_chunk_json: Vec::new(),
                                    finished: true,
                                    error: None,
                                }))
                                .await;
                        });
                    }
                    Some(postprocess_input::Frame::Chunk(chunk)) => {
                        if chunk.openai_chunk_json.len() > max_chunk_bytes {
                            send_error(
                                &output_tx,
                                &chunk.request_id,
                                "resource_exhausted",
                                "chunk byte limit exceeded",
                            )
                            .await;
                            sessions.remove(&chunk.request_id);
                            continue;
                        }
                        let Some(sender) = sessions.get(&chunk.request_id).cloned() else {
                            send_error(
                                &output_tx,
                                &chunk.request_id,
                                "not_found",
                                "request is not open",
                            )
                            .await;
                            continue;
                        };
                        let response: Annotated<NvCreateChatCompletionStreamResponse> =
                            match serde_json::from_slice(&chunk.openai_chunk_json) {
                                Ok(response) => response,
                                Err(error) => {
                                    send_error(
                                        &output_tx,
                                        &chunk.request_id,
                                        "invalid_argument",
                                        &error.to_string(),
                                    )
                                    .await;
                                    sessions.remove(&chunk.request_id);
                                    continue;
                                }
                            };
                        if sender.send(response).await.is_err() {
                            sessions.remove(&chunk.request_id);
                            continue;
                        }
                        if chunk.finished {
                            sessions.remove(&chunk.request_id);
                        }
                    }
                    Some(postprocess_input::Frame::Cancel(cancel)) => {
                        sessions.remove(&cancel.request_id);
                    }
                    None => {
                        send_error(&output_tx, "", "invalid_argument", "missing frame").await;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(output_rx))))
    }
}

async fn send_error(
    output: &mpsc::Sender<Result<PostprocessOutput, Status>>,
    request_id: &str,
    kind: &str,
    message: &str,
) {
    let _ = output
        .send(Ok(PostprocessOutput {
            request_id: request_id.to_string(),
            openai_chunk_json: Vec::new(),
            finished: true,
            error: Some(item_error(kind, message, false)),
        }))
        .await;
}
