// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use dynamo_component_facades::{
    preprocess::PreprocessorFacade,
    proto::{PreprocessBatchRequest, PreprocessItem, preprocessor_server::Preprocessor},
};
use dynamo_llm::{
    model_card::ModelDeploymentCard,
    preprocessor::{OpenAIPreprocessor, PreprocessRequestOptions},
    protocols::openai::chat_completions::NvCreateChatCompletionRequest,
};
use serde_json::json;
use tonic::Request;

fn processor() -> Arc<OpenAIPreprocessor> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../llm/tests/data/sample-models/mock-llama-3.1-8b-instruct");
    let model_card = ModelDeploymentCard::load_from_disk(path, None).unwrap();
    OpenAIPreprocessor::new(model_card).unwrap()
}

fn request_json() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "model": "test-model",
        "messages": [{"role": "user", "content": "What is the capital of Tuvalu?"}],
        "stream": false,
        "temperature": 0.0,
        "max_completion_tokens": 32
    }))
    .unwrap()
}

#[tokio::test]
async fn facade_matches_direct_canonical_preparation() {
    let processor = processor();
    let mut request: NvCreateChatCompletionRequest =
        serde_json::from_slice(&request_json()).unwrap();
    processor.normalize_chat_request(&mut request, false);
    let direct = processor
        .prepare_chat_request(&request, None, PreprocessRequestOptions::default(), None)
        .await
        .unwrap();

    let facade = PreprocessorFacade::new(processor, 8, 1024 * 1024, 2).unwrap();
    let response = facade
        .prepare_batch(Request::new(PreprocessBatchRequest {
            items: vec![PreprocessItem {
                item_id: "a".into(),
                openai_request_json: request_json(),
                ..Default::default()
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    let actual = &response.items[0];
    assert!(actual.error.is_none());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&actual.normalized_openai_request_json)
            .unwrap(),
        serde_json::to_value(&request).unwrap()
    );
    let backend_request: serde_json::Value =
        serde_json::from_slice(&actual.backend_request_json).unwrap();
    assert!(backend_request.get("token_ids").is_none());
    assert_eq!(
        actual.token_ids,
        direct.backend_request.token_ids.as_ref().clone()
    );
    let selector_request: serde_json::Value =
        serde_json::from_slice(&actual.selector_request_json).unwrap();
    assert_eq!(selector_request["model_name"], "test-model");
    assert_eq!(selector_request["selection_id"], "a");
    assert!(selector_request.get("token_ids").is_none());
    assert!(selector_request.get("model").is_none());
    assert_eq!(actual.annotations, direct.annotations);
    assert_eq!(
        actual.prompt_injected_reasoning,
        direct.prompt_injected_reasoning
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&actual.guided_tool_constraint_json).unwrap(),
        serde_json::to_value(&direct.guided_tool_constraint).unwrap()
    );
}

#[tokio::test]
async fn invalid_item_does_not_fail_valid_batch_peer() {
    let facade = PreprocessorFacade::new(processor(), 8, 1024 * 1024, 2).unwrap();
    let response = facade
        .prepare_batch(Request::new(PreprocessBatchRequest {
            items: vec![
                PreprocessItem {
                    item_id: "bad".into(),
                    openai_request_json: b"{".to_vec(),
                    ..Default::default()
                },
                PreprocessItem {
                    item_id: "good".into(),
                    openai_request_json: request_json(),
                    ..Default::default()
                },
            ],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.items[0].item_id, "bad");
    assert_eq!(
        response.items[0].error.as_ref().unwrap().kind,
        "invalid_argument"
    );
    assert_eq!(response.items[1].item_id, "good");
    assert!(response.items[1].error.is_none());
}

#[tokio::test]
async fn duplicate_ids_reject_the_whole_batch() {
    let facade = PreprocessorFacade::new(processor(), 8, 1024 * 1024, 2).unwrap();
    let item = PreprocessItem {
        item_id: "same".into(),
        openai_request_json: request_json(),
        ..Default::default()
    };
    let error = facade
        .prepare_batch(Request::new(PreprocessBatchRequest {
            items: vec![item.clone(), item],
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}
