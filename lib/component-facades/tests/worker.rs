// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use dynamo_component_facades::{
    chat_worker::ChatWorkerFacade,
    proto::{
        ChatWorkerRequest, WorkerInput, WorkerOpen,
        chat_worker_bridge_client::ChatWorkerBridgeClient,
        chat_worker_bridge_server::ChatWorkerBridgeServer,
        worker_bridge_client::WorkerBridgeClient, worker_bridge_server::WorkerBridgeServer,
        worker_input,
    },
    worker::{CanonicalBackendEngine, WorkerFacade},
};
use dynamo_llm::{
    model_card::ModelDeploymentCard,
    preprocessor::{BackendOutput, OpenAIPreprocessor, PreprocessedRequest},
    protocols::common::{
        OutputOptions, SamplingOptions, StopConditions, llm_backend::FinishReason,
    },
};
use dynamo_runtime::{
    pipeline::{
        AsyncEngine, AsyncEngineContextProvider, Context, Error, ManyOut, ResponseStream, SingleIn,
        async_trait,
    },
    protocols::annotated::Annotated,
};
use futures::{StreamExt, stream};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, transport::Server};

struct FakeCanonicalBackend;

#[async_trait]
impl AsyncEngine<SingleIn<PreprocessedRequest>, ManyOut<Annotated<BackendOutput>>, Error>
    for FakeCanonicalBackend
{
    async fn generate(
        &self,
        request: Context<PreprocessedRequest>,
    ) -> Result<ManyOut<Annotated<BackendOutput>>, Error> {
        let context = request.context();
        let output = BackendOutput {
            token_ids: vec![42],
            tokens: vec![Some("hello".into())],
            text: Some("hello".into()),
            cum_log_probs: None,
            log_probs: None,
            top_logprobs: None,
            finish_reason: Some(FinishReason::Stop),
            stop_reason: None,
            index: Some(0),
            completion_usage: None,
            disaggregated_params: None,
            worker_trace_link: None,
            engine_data: None,
            encoder_result: None,
            routing_data: None,
            jailed_text: None,
        };
        Ok(ResponseStream::new(
            Box::pin(stream::iter([Annotated::from_data(output)])),
            context,
        ))
    }
}

fn backend_request() -> PreprocessedRequest {
    PreprocessedRequest::builder()
        .model("test-model".to_string())
        .token_ids(vec![1, 2, 3])
        .stop_conditions(StopConditions::default())
        .sampling_options(SamplingOptions::default())
        .output_options(OutputOptions::default())
        .build()
        .unwrap()
}

fn processor() -> Arc<OpenAIPreprocessor> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../llm/tests/data/sample-models/mock-llama-3.1-8b-instruct");
    let model_card = ModelDeploymentCard::load_from_disk(path, None).unwrap();
    OpenAIPreprocessor::new(model_card).unwrap()
}

#[tokio::test]
async fn grpc_worker_bridge_relays_a_canonical_dynamo_engine() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let engine: CanonicalBackendEngine = Arc::new(FakeCanonicalBackend);
    let service = WorkerFacade::new(engine, 8, 8, 1024 * 1024, 1024 * 1024).unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerBridgeServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = WorkerBridgeClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(2);
    input_tx
        .send(WorkerInput {
            frame: Some(worker_input::Frame::Open(WorkerOpen {
                request_id: "request-1".into(),
                backend_request_json: serde_json::to_vec(&backend_request()).unwrap(),
                deadline_unix_ms: 0,
            })),
        })
        .await
        .unwrap();
    let mut output = client
        .process(Request::new(ReceiverStream::new(input_rx)))
        .await
        .unwrap()
        .into_inner();
    let chunk = output.next().await.unwrap().unwrap();
    let relayed: Annotated<BackendOutput> =
        serde_json::from_slice(&chunk.annotated_backend_chunk_json).unwrap();
    assert_eq!(relayed.data.unwrap().text.as_deref(), Some("hello"));
    let terminal = output.next().await.unwrap().unwrap();
    assert!(terminal.finished);
    drop(input_tx);
    server.abort();
}

#[tokio::test]
async fn chat_worker_emits_the_canonical_openai_chunk_without_an_annotation_envelope() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let engine: CanonicalBackendEngine = Arc::new(FakeCanonicalBackend);
    let service = ChatWorkerFacade::new(engine, processor(), 1024 * 1024, 1024 * 1024).unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChatWorkerBridgeServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let normalized = serde_json::json!({
        "model": "test-model",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": true
    });
    let mut client = ChatWorkerBridgeClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut output = client
        .generate(ChatWorkerRequest {
            request_id: "request-2".into(),
            backend_request_json: {
                let mut value = serde_json::to_value(backend_request()).unwrap();
                value.as_object_mut().unwrap().remove("token_ids");
                serde_json::to_vec(&value).unwrap()
            },
            normalized_openai_request_json: serde_json::to_vec(&normalized).unwrap(),
            prompt_tokens: 3,
            token_ids: vec![1, 2, 3],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let chunk = output.next().await.unwrap().unwrap();
    let value: serde_json::Value = serde_json::from_slice(&chunk.openai_chunk_json).unwrap();
    assert!(value.get("data").is_none());
    assert_eq!(value["choices"][0]["delta"]["content"], "hello");
    server.abort();
}
