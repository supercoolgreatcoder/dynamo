// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, future::Future, sync::Arc};

use dynamo_kv_router::services::selection::{
    ReservationRequest, SelectAndReserveRequest, SelectRequest, SelectionError, SelectionService,
    WorkerPatchRequest, WorkerRequest,
};
use futures::{StreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
use tonic::{Request, Response, Status};

use crate::{
    deadline_expired, item_error,
    proto::{
        JsonBatchRequest, JsonBatchResponse, JsonResult, ReadyRequest,
        ReservationEventBatchRequest, WorkerMutationBatchRequest, selector_server::Selector,
    },
    token_ids_from_wire,
};

#[derive(Debug)]
struct FacadeFailure {
    kind: String,
    message: String,
    retryable: bool,
}

impl From<SelectionError> for FacadeFailure {
    fn from(error: SelectionError) -> Self {
        let retryable = matches!(error.kind(), "not_ready" | "scheduler");
        Self {
            kind: error.kind().to_string(),
            message: error.to_string(),
            retryable,
        }
    }
}

fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, FacadeFailure> {
    serde_json::from_slice(payload).map_err(|error| FacadeFailure {
        kind: "invalid_argument".to_string(),
        message: error.to_string(),
        retryable: false,
    })
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, FacadeFailure> {
    serde_json::to_vec(value).map_err(|error| FacadeFailure {
        kind: "internal".to_string(),
        message: error.to_string(),
        retryable: false,
    })
}

#[derive(Clone)]
pub struct SelectorFacade {
    service: Arc<SelectionService>,
    max_batch_items: usize,
    max_batch_bytes: usize,
    max_concurrency: usize,
}

impl SelectorFacade {
    pub fn new(
        service: Arc<SelectionService>,
        max_batch_items: usize,
        max_batch_bytes: usize,
        max_concurrency: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(max_batch_items > 0, "max_batch_items must be positive");
        anyhow::ensure!(max_batch_bytes > 0, "max_batch_bytes must be positive");
        anyhow::ensure!(max_concurrency > 0, "max_concurrency must be positive");
        Ok(Self {
            service,
            max_batch_items,
            max_batch_bytes,
            max_concurrency,
        })
    }

    fn validate_json_batch(&self, batch: &JsonBatchRequest) -> Result<(), Status> {
        if batch.items.len() > self.max_batch_items {
            return Err(Status::resource_exhausted("batch item limit exceeded"));
        }
        if batch
            .items
            .iter()
            .map(|item| {
                item.payload_json
                    .len()
                    .saturating_add(item.token_ids.len().saturating_mul(size_of::<u32>()))
                    .saturating_add(item.token_ids_le.len())
            })
            .sum::<usize>()
            > self.max_batch_bytes
        {
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
        Ok(())
    }

    async fn process_json_batch<F, Fut>(
        &self,
        batch: JsonBatchRequest,
        operation: F,
    ) -> Result<JsonBatchResponse, Status>
    where
        F: Fn(Arc<SelectionService>, Vec<u8>, Vec<u32>) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<u8>, FacadeFailure>> + Send,
    {
        self.validate_json_batch(&batch)?;
        let service = self.service.clone();
        let mut items = stream::iter(batch.items.into_iter().enumerate())
            .map(move |(index, item)| {
                let service = service.clone();
                let operation = operation.clone();
                async move {
                    let result = if deadline_expired(item.deadline_unix_ms) {
                        Err(FacadeFailure {
                            kind: "deadline_exceeded".to_string(),
                            message: "item deadline has expired".to_string(),
                            retryable: true,
                        })
                    } else {
                        match token_ids_from_wire(item.token_ids, &item.token_ids_le) {
                            Ok(token_ids) => operation(service, item.payload_json, token_ids).await,
                            Err(message) => Err(FacadeFailure {
                                kind: "invalid_argument".to_string(),
                                message: message.to_string(),
                                retryable: false,
                            }),
                        }
                    };
                    let result = match result {
                        Ok(payload_json) => JsonResult {
                            item_id: item.item_id,
                            payload_json,
                            error: None,
                        },
                        Err(error) => JsonResult {
                            item_id: item.item_id,
                            payload_json: Vec::new(),
                            error: Some(item_error(&error.kind, error.message, error.retryable)),
                        },
                    };
                    (index, result)
                }
            })
            .buffer_unordered(self.max_concurrency)
            .collect::<Vec<_>>()
            .await;
        items.sort_unstable_by_key(|(index, _)| *index);
        Ok(JsonBatchResponse {
            items: items.into_iter().map(|(_, item)| item).collect(),
        })
    }

    fn validate_control_batch(
        &self,
        len: usize,
        ids: impl Iterator<Item = String>,
    ) -> Result<(), Status> {
        if len > self.max_batch_items {
            return Err(Status::resource_exhausted("batch item limit exceeded"));
        }
        let mut seen = HashSet::with_capacity(len);
        if ids.into_iter().any(|id| id.is_empty() || !seen.insert(id)) {
            return Err(Status::invalid_argument(
                "item_id values must be non-empty and unique within a batch",
            ));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Selector for SelectorFacade {
    async fn select(
        &self,
        request: Request<crate::proto::JsonItem>,
    ) -> Result<Response<JsonResult>, Status> {
        let mut response = self
            .process_json_batch(
                JsonBatchRequest {
                    items: vec![request.into_inner()],
                },
                |service, payload, token_ids| async move {
                    let mut request: SelectRequest = decode(&payload)?;
                    request.prompt.token_ids = Some(token_ids);
                    encode(&service.select(request).await.map_err(FacadeFailure::from)?)
                },
            )
            .await?;
        Ok(Response::new(response.items.remove(0)))
    }

    async fn select_and_reserve(
        &self,
        request: Request<crate::proto::JsonItem>,
    ) -> Result<Response<JsonResult>, Status> {
        let mut response = self
            .process_json_batch(
                JsonBatchRequest {
                    items: vec![request.into_inner()],
                },
                |service, payload, token_ids| async move {
                    let mut request: SelectAndReserveRequest = decode(&payload)?;
                    request.prompt.token_ids = Some(token_ids);
                    encode(
                        &service
                            .select_and_reserve(request)
                            .await
                            .map_err(FacadeFailure::from)?,
                    )
                },
            )
            .await?;
        Ok(Response::new(response.items.remove(0)))
    }

    async fn create_reservation(
        &self,
        request: Request<crate::proto::JsonItem>,
    ) -> Result<Response<JsonResult>, Status> {
        let mut response = self
            .process_json_batch(
                JsonBatchRequest {
                    items: vec![request.into_inner()],
                },
                |service, payload, token_ids| async move {
                    let mut request: ReservationRequest = decode(&payload)?;
                    request.prompt.token_ids = Some(token_ids);
                    encode(
                        &service
                            .create_reservation(request)
                            .await
                            .map_err(FacadeFailure::from)?,
                    )
                },
            )
            .await?;
        Ok(Response::new(response.items.remove(0)))
    }

    async fn select_batch(
        &self,
        request: Request<JsonBatchRequest>,
    ) -> Result<Response<JsonBatchResponse>, Status> {
        let response = self
            .process_json_batch(
                request.into_inner(),
                |service, payload, token_ids| async move {
                    let mut request: SelectRequest = decode(&payload)?;
                    request.prompt.token_ids = Some(token_ids);
                    encode(&service.select(request).await.map_err(FacadeFailure::from)?)
                },
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn select_and_reserve_batch(
        &self,
        request: Request<JsonBatchRequest>,
    ) -> Result<Response<JsonBatchResponse>, Status> {
        let response = self
            .process_json_batch(
                request.into_inner(),
                |service, payload, token_ids| async move {
                    let mut request: SelectAndReserveRequest = decode(&payload)?;
                    request.prompt.token_ids = Some(token_ids);
                    encode(
                        &service
                            .select_and_reserve(request)
                            .await
                            .map_err(FacadeFailure::from)?,
                    )
                },
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn create_reservation_batch(
        &self,
        request: Request<JsonBatchRequest>,
    ) -> Result<Response<JsonBatchResponse>, Status> {
        let response = self
            .process_json_batch(
                request.into_inner(),
                |service, payload, token_ids| async move {
                    let mut request: ReservationRequest = decode(&payload)?;
                    request.prompt.token_ids = Some(token_ids);
                    encode(
                        &service
                            .create_reservation(request)
                            .await
                            .map_err(FacadeFailure::from)?,
                    )
                },
            )
            .await?;
        Ok(Response::new(response))
    }

    async fn mutate_workers_batch(
        &self,
        request: Request<WorkerMutationBatchRequest>,
    ) -> Result<Response<JsonBatchResponse>, Status> {
        let batch = request.into_inner();
        self.validate_control_batch(
            batch.items.len(),
            batch.items.iter().map(|item| item.item_id.clone()),
        )?;
        if batch
            .items
            .iter()
            .map(|item| item.payload_json.len())
            .sum::<usize>()
            > self.max_batch_bytes
        {
            return Err(Status::resource_exhausted("batch byte limit exceeded"));
        }
        let mut results = Vec::with_capacity(batch.items.len());
        for item in batch.items {
            let result = if deadline_expired(item.deadline_unix_ms) {
                Err(FacadeFailure {
                    kind: "deadline_exceeded".into(),
                    message: "item deadline has expired".into(),
                    retryable: true,
                })
            } else {
                match item.operation {
                    1 => match decode::<WorkerRequest>(&item.payload_json) {
                        Ok(request) => service_result(self.service.upsert_worker(request).await),
                        Err(error) => Err(error),
                    },
                    2 => match decode::<WorkerPatchRequest>(&item.payload_json) {
                        Ok(request) => {
                            service_result(self.service.patch_worker(item.worker_id, request).await)
                        }
                        Err(error) => Err(error),
                    },
                    3 => service_result(self.service.delete_worker(item.worker_id).await),
                    _ => Err(FacadeFailure {
                        kind: "invalid_argument".into(),
                        message: "unknown worker mutation operation".into(),
                        retryable: false,
                    }),
                }
            };
            results.push(result_to_json(item.item_id, result));
        }
        Ok(Response::new(JsonBatchResponse { items: results }))
    }

    async fn apply_reservation_events_batch(
        &self,
        request: Request<ReservationEventBatchRequest>,
    ) -> Result<Response<JsonBatchResponse>, Status> {
        let batch = request.into_inner();
        self.validate_control_batch(
            batch.items.len(),
            batch.items.iter().map(|item| item.item_id.clone()),
        )?;
        let mut results = Vec::with_capacity(batch.items.len());
        for item in batch.items {
            let result = if deadline_expired(item.deadline_unix_ms) {
                Err(FacadeFailure {
                    kind: "deadline_exceeded".into(),
                    message: "item deadline has expired".into(),
                    retryable: true,
                })
            } else {
                match item.operation {
                    1 => self
                        .service
                        .prefill_complete(&item.reservation_id)
                        .await
                        .map_err(FacadeFailure::from),
                    2 => self
                        .service
                        .add_output_block(&item.reservation_id, item.decay_fraction)
                        .map_err(FacadeFailure::from),
                    3 => self
                        .service
                        .free_reservation(&item.reservation_id)
                        .await
                        .map_err(FacadeFailure::from),
                    _ => Err(FacadeFailure {
                        kind: "invalid_argument".into(),
                        message: "unknown reservation event operation".into(),
                        retryable: false,
                    }),
                }
            };
            results.push(result_to_json(
                item.item_id,
                result.and_then(|()| encode(&serde_json::json!({}))),
            ));
        }
        Ok(Response::new(JsonBatchResponse { items: results }))
    }

    async fn ready(&self, _request: Request<ReadyRequest>) -> Result<Response<JsonResult>, Status> {
        let result = encode(&self.service.ready()).map_err(|error| {
            Status::internal(format!(
                "failed to serialize ready response: {}",
                error.message
            ))
        })?;
        Ok(Response::new(JsonResult {
            item_id: "ready".into(),
            payload_json: result,
            error: None,
        }))
    }
}

fn service_result<T: Serialize>(
    result: Result<T, SelectionError>,
) -> Result<Vec<u8>, FacadeFailure> {
    encode(&result.map_err(FacadeFailure::from)?)
}

fn result_to_json(item_id: String, result: Result<Vec<u8>, FacadeFailure>) -> JsonResult {
    match result {
        Ok(payload_json) => JsonResult {
            item_id,
            payload_json,
            error: None,
        },
        Err(error) => JsonResult {
            item_id,
            payload_json: Vec::new(),
            error: Some(item_error(&error.kind, error.message, error.retryable)),
        },
    }
}
