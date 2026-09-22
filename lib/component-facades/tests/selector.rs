// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use dynamo_component_facades::{
    proto::{
        JsonBatchRequest, JsonItem, WorkerMutation, WorkerMutationBatchRequest,
        selector_server::Selector,
    },
    selector::SelectorFacade,
};
use dynamo_kv_router::{
    WorkerType, config::KvRouterConfig, plugins::RouterPluginRegistry,
    services::selection::SelectionServiceBuilder,
};
use serde_json::json;
use tonic::Request;

async fn facade() -> SelectorFacade {
    let config = KvRouterConfig {
        use_kv_events: false,
        ..KvRouterConfig::default()
    };
    let service = SelectionServiceBuilder::new(
        config,
        WorkerType::Aggregated,
        RouterPluginRegistry::default(),
    )
    .indexer_threads(1)
    .build()
    .await
    .unwrap();
    SelectorFacade::new(Arc::new(service), 8, 1024 * 1024, 4).unwrap()
}

#[tokio::test]
async fn worker_lifecycle_and_selection_use_canonical_service() {
    let facade = facade().await;
    let mutation = facade
        .mutate_workers_batch(Request::new(WorkerMutationBatchRequest {
            items: vec![WorkerMutation {
                item_id: "register".into(),
                operation: 1,
                worker_id: 1,
                payload_json: serde_json::to_vec(&json!({
                    "worker_id": 1,
                    "model_name": "model",
                    "endpoint": "http://worker-1:8000",
                    "block_size": 4,
                    "max_num_batched_tokens": 4096
                }))
                .unwrap(),
                deadline_unix_ms: 0,
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(mutation.items[0].error.is_none());

    let selected = facade
        .select_batch(Request::new(JsonBatchRequest {
            items: vec![JsonItem {
                item_id: "select".into(),
                payload_json: serde_json::to_vec(&json!({
                    "model_name": "model",
                    "token_ids": [1, 2, 3, 4],
                    "selection_id": "request-1"
                }))
                .unwrap(),
                deadline_unix_ms: 0,
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(selected.items[0].error.is_none());
    let response: serde_json::Value =
        serde_json::from_slice(&selected.items[0].payload_json).unwrap();
    assert_eq!(response["worker_id"], 1);
    assert_eq!(response["endpoint"], "http://worker-1:8000");
}

#[tokio::test]
async fn selector_isolates_invalid_item_and_preserves_order() {
    let facade = facade().await;
    let response = facade
        .select_batch(Request::new(JsonBatchRequest {
            items: vec![
                JsonItem {
                    item_id: "malformed".into(),
                    payload_json: b"{".to_vec(),
                    deadline_unix_ms: 0,
                },
                JsonItem {
                    item_id: "valid-but-not-ready".into(),
                    payload_json: serde_json::to_vec(&json!({
                        "model_name": "model",
                        "token_ids": [1, 2, 3, 4]
                    }))
                    .unwrap(),
                    deadline_unix_ms: 0,
                },
            ],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.items[0].item_id, "malformed");
    assert_eq!(
        response.items[0].error.as_ref().unwrap().kind,
        "invalid_argument"
    );
    assert_eq!(response.items[1].item_id, "valid-but-not-ready");
    assert!(response.items[1].error.is_some());
}
