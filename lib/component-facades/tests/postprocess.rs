// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use dynamo_component_facades::{
    postprocess::PostprocessorFacade,
    proto::{
        PostprocessChunk, PostprocessInput, PostprocessOpen, postprocess_input,
        postprocessor_client::PostprocessorClient, postprocessor_server::PostprocessorServer,
    },
};
use dynamo_llm::{
    model_card::ModelDeploymentCard,
    preprocessor::OpenAIPreprocessor,
    protocols::openai::chat_completions::{
        NvCreateChatCompletionRequest, NvCreateChatCompletionStreamResponse,
    },
};
use dynamo_runtime::protocols::annotated::Annotated;
use futures::StreamExt;
use serde_json::json;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, transport::Server};

fn processor() -> Arc<OpenAIPreprocessor> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../llm/tests/data/sample-models/mock-llama-3.1-8b-instruct");
    let model_card = ModelDeploymentCard::load_from_disk(path, None).unwrap();
    OpenAIPreprocessor::new(model_card).unwrap()
}

fn request_json() -> Vec<u8> {
    let request: NvCreateChatCompletionRequest = serde_json::from_value(json!({
        "model": "test-model",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": true
    }))
    .unwrap();
    serde_json::to_vec(&request).unwrap()
}

fn annotated_chunk_json() -> Vec<u8> {
    let chunk: NvCreateChatCompletionStreamResponse = serde_json::from_value(json!({
        "id": "chatcmpl-request-1",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "test-model",
        "choices": [{
            "index": 0,
            "delta": {"content": "hello"},
            "finish_reason": "stop"
        }]
    }))
    .unwrap();
    serde_json::to_vec(&Annotated::from_data(chunk)).unwrap()
}

#[tokio::test]
async fn grpc_postprocessor_stream_uses_canonical_parser_and_closes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = PostprocessorFacade::new(processor(), 8, 8, 8, 1024 * 1024).unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(PostprocessorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = PostprocessorClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let input = tokio_stream::iter(vec![
        PostprocessInput {
            frame: Some(postprocess_input::Frame::Open(PostprocessOpen {
                request_id: "request-1".into(),
                normalized_openai_request_json: request_json(),
                prompt_injected_reasoning: false,
                uses_tool_call_structural_tag: false,
            })),
        },
        PostprocessInput {
            frame: Some(postprocess_input::Frame::Chunk(PostprocessChunk {
                request_id: "request-1".into(),
                openai_chunk_json: annotated_chunk_json(),
                finished: true,
            })),
        },
    ]);
    let mut output = client
        .process(Request::new(input))
        .await
        .unwrap()
        .into_inner();
    let first = output.next().await.unwrap().unwrap();
    assert_eq!(first.request_id, "request-1");
    assert!(first.error.is_none());
    let parsed: Annotated<NvCreateChatCompletionStreamResponse> =
        serde_json::from_slice(&first.openai_chunk_json).unwrap();
    let choice = &parsed.data.unwrap().inner.choices[0];
    assert_eq!(
        serde_json::to_value(&choice.delta.content).unwrap(),
        json!("hello")
    );
    assert!(choice.delta.role.is_some());
    let terminal = output.next().await.unwrap().unwrap();
    assert!(terminal.finished);
    server.abort();
}
