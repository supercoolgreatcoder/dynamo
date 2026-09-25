// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, pin::Pin, sync::Arc};

use dynamo_llm::preprocessor::{BackendOutput, PreprocessedRequest};
use dynamo_runtime::{
    pipeline::{Context, ManyOut, ServiceEngine, SingleIn},
    protocols::annotated::Annotated,
};
use futures::{Stream, StreamExt};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::{
    deadline_expired, item_error,
    proto::{WorkerInput, WorkerOutput, worker_bridge_server::WorkerBridge, worker_input},
    token_ids_from_wire,
};

pub type CanonicalBackendEngine =
    ServiceEngine<SingleIn<PreprocessedRequest>, ManyOut<Annotated<BackendOutput>>>;

/// A transport-only adapter around a Dynamo backend pipeline.
///
/// The supplied engine owns routing, inference, detokenization, and engine-specific
/// P/D behavior. This facade only validates envelopes, propagates request identity
/// and cancellation, and serializes canonical Dynamo output.
#[derive(Clone)]
pub struct WorkerFacade {
    engine: CanonicalBackendEngine,
    max_active_requests: usize,
    output_queue_capacity: usize,
    max_request_bytes: usize,
    max_chunk_bytes: usize,
}

impl WorkerFacade {
    pub fn new(
        engine: CanonicalBackendEngine,
        max_active_requests: usize,
        output_queue_capacity: usize,
        max_request_bytes: usize,
        max_chunk_bytes: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            max_active_requests > 0,
            "max_active_requests must be positive"
        );
        anyhow::ensure!(
            output_queue_capacity > 0,
            "output_queue_capacity must be positive"
        );
        anyhow::ensure!(max_request_bytes > 0, "max_request_bytes must be positive");
        anyhow::ensure!(max_chunk_bytes > 0, "max_chunk_bytes must be positive");
        Ok(Self {
            engine,
            max_active_requests,
            output_queue_capacity,
            max_request_bytes,
            max_chunk_bytes,
        })
    }
}

#[tonic::async_trait]
impl WorkerBridge for WorkerFacade {
    type ProcessStream = Pin<Box<dyn Stream<Item = Result<WorkerOutput, Status>> + Send>>;

    async fn process(
        &self,
        request: Request<Streaming<WorkerInput>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        let mut inbound = request.into_inner();
        let engine = self.engine.clone();
        let max_active_requests = self.max_active_requests;
        let max_request_bytes = self.max_request_bytes;
        let max_chunk_bytes = self.max_chunk_bytes;
        let sessions = Arc::new(Mutex::new(
            HashMap::<String, Option<oneshot::Sender<()>>>::new(),
        ));
        let (output_tx, output_rx) = mpsc::channel(self.output_queue_capacity);

        tokio::spawn(async move {
            while let Some(frame) = inbound.next().await {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = output_tx.send(Err(error)).await;
                        break;
                    }
                };
                match frame.frame {
                    Some(worker_input::Frame::Open(open)) => {
                        if open.request_id.is_empty() {
                            send_error(&output_tx, "", "invalid_argument", "request_id is empty")
                                .await;
                            continue;
                        }
                        if deadline_expired(open.deadline_unix_ms) {
                            send_error(
                                &output_tx,
                                &open.request_id,
                                "deadline_exceeded",
                                "request deadline has expired",
                            )
                            .await;
                            continue;
                        }
                        if open
                            .backend_request_json
                            .len()
                            .saturating_add(open.token_ids.len().saturating_mul(size_of::<u32>()))
                            .saturating_add(open.token_ids_le.len())
                            > max_request_bytes
                        {
                            send_error(
                                &output_tx,
                                &open.request_id,
                                "resource_exhausted",
                                "request byte limit exceeded",
                            )
                            .await;
                            continue;
                        }
                        let token_ids =
                            match token_ids_from_wire(open.token_ids, &open.token_ids_le) {
                                Ok(token_ids) => token_ids,
                                Err(error) => {
                                    send_error(
                                        &output_tx,
                                        &open.request_id,
                                        "invalid_argument",
                                        &error,
                                    )
                                    .await;
                                    continue;
                                }
                            };
                        let mut backend_value: serde_json::Value =
                            match serde_json::from_slice(&open.backend_request_json) {
                                Ok(value) => value,
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
                        if !token_ids.is_empty() {
                            let Some(object) = backend_value.as_object_mut() else {
                                send_error(
                                    &output_tx,
                                    &open.request_id,
                                    "invalid_argument",
                                    "backend request must be a JSON object",
                                )
                                .await;
                                continue;
                            };
                            object.insert("token_ids".into(), serde_json::json!(token_ids));
                        }
                        let backend_request: PreprocessedRequest =
                            match serde_json::from_value(backend_value) {
                                Ok(value) => value,
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
                        let (cancel_tx, mut cancel_rx) = oneshot::channel();
                        {
                            let mut active = sessions.lock().await;
                            if active.contains_key(&open.request_id) {
                                drop(active);
                                send_error(
                                    &output_tx,
                                    &open.request_id,
                                    "conflict",
                                    "request is already open",
                                )
                                .await;
                                continue;
                            }
                            if active.len() >= max_active_requests {
                                drop(active);
                                send_error(
                                    &output_tx,
                                    &open.request_id,
                                    "resource_exhausted",
                                    "active request limit exceeded",
                                )
                                .await;
                                continue;
                            }
                            active.insert(open.request_id.clone(), Some(cancel_tx));
                        }

                        let request_id = open.request_id;
                        let task_request_id = request_id.clone();
                        let engine = engine.clone();
                        let output_tx = output_tx.clone();
                        let sessions = sessions.clone();
                        tokio::spawn(async move {
                            let context = Context::with_id_and_metadata(
                                backend_request,
                                task_request_id.clone(),
                                Default::default(),
                            );
                            let mut stream = match engine.generate(context).await {
                                Ok(stream) => stream,
                                Err(error) => {
                                    send_error(
                                        &output_tx,
                                        &task_request_id,
                                        "backend",
                                        &error.to_string(),
                                    )
                                    .await;
                                    sessions.lock().await.remove(&task_request_id);
                                    return;
                                }
                            };
                            loop {
                                tokio::select! {
                                    _ = &mut cancel_rx => break,
                                    chunk = stream.next() => match chunk {
                                        Some(chunk) => {
                                            let payload = match serde_json::to_vec(&chunk) {
                                                Ok(payload) if payload.len() <= max_chunk_bytes => payload,
                                                Ok(_) => {
                                                    send_error(
                                                        &output_tx,
                                                        &task_request_id,
                                                        "resource_exhausted",
                                                        "backend chunk byte limit exceeded",
                                                    ).await;
                                                    break;
                                                }
                                                Err(error) => {
                                                    send_error(
                                                        &output_tx,
                                                        &task_request_id,
                                                        "internal",
                                                        &error.to_string(),
                                                    ).await;
                                                    break;
                                                }
                                            };
                                            if output_tx.send(Ok(WorkerOutput {
                                                request_id: task_request_id.clone(),
                                                annotated_backend_chunk_json: payload,
                                                finished: false,
                                                error: None,
                                            })).await.is_err() {
                                                break;
                                            }
                                        }
                                        None => {
                                            let _ = output_tx.send(Ok(WorkerOutput {
                                                request_id: task_request_id.clone(),
                                                annotated_backend_chunk_json: Vec::new(),
                                                finished: true,
                                                error: None,
                                            })).await;
                                            break;
                                        }
                                    }
                                }
                            }
                            sessions.lock().await.remove(&task_request_id);
                        });
                    }
                    Some(worker_input::Frame::Cancel(cancel)) => {
                        let cancel_tx = sessions
                            .lock()
                            .await
                            .get_mut(&cancel.request_id)
                            .and_then(Option::take);
                        if let Some(cancel_tx) = cancel_tx {
                            let _ = cancel_tx.send(());
                        }
                    }
                    None => {
                        send_error(&output_tx, "", "invalid_argument", "missing frame").await;
                    }
                }
            }
            for (_, cancel) in sessions.lock().await.drain() {
                if let Some(cancel) = cancel {
                    let _ = cancel.send(());
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(output_rx))))
    }
}

async fn send_error(
    output: &mpsc::Sender<Result<WorkerOutput, Status>>,
    request_id: &str,
    kind: &str,
    message: &str,
) {
    let _ = output
        .send(Ok(WorkerOutput {
            request_id: request_id.to_string(),
            annotated_backend_chunk_json: Vec::new(),
            finished: true,
            error: Some(item_error(kind, message, kind == "backend")),
        }))
        .await;
}
