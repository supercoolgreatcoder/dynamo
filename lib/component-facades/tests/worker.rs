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

struct FakeCanonicalBackend {
    handoff: bool,
    expected_prefill: Option<serde_json::Value>,
}

#[async_trait]
impl AsyncEngine<SingleIn<PreprocessedRequest>, ManyOut<Annotated<BackendOutput>>, Error>
    for FakeCanonicalBackend
{
    async fn generate(
        &self,
        request: Context<PreprocessedRequest>,
    ) -> Result<ManyOut<Annotated<BackendOutput>>, Error> {
        assert_eq!(request.token_ids.as_ref().as_slice(), &[1, 2, 3]);
        assert_eq!(
            request
                .prefill_result
                .as_ref()
                .map(|result| &result.disaggregated_params),
            self.expected_prefill.as_ref()
        );
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
            disaggregated_params: self
                .handoff
                .then(|| serde_json::json!({"opaque_kv": "kept"})),
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
    let engine: CanonicalBackendEngine = Arc::new(FakeCanonicalBackend {
        handoff: false,
        expected_prefill: None,
    });
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
                backend_request_json: {
                    let mut value = serde_json::to_value(backend_request()).unwrap();
                    value.as_object_mut().unwrap().remove("token_ids");
                    serde_json::to_vec(&value).unwrap()
                },
                token_ids_le: [1_u32, 2, 3]
                    .into_iter()
                    .flat_map(u32::to_le_bytes)
                    .collect(),
                deadline_unix_ms: 0,
                ..Default::default()
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
    let engine: CanonicalBackendEngine = Arc::new(FakeCanonicalBackend {
        handoff: false,
        expected_prefill: None,
    });
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

#[tokio::test]
async fn chat_worker_raw_stream_retains_opaque_prefill_handoff() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let engine: CanonicalBackendEngine = Arc::new(FakeCanonicalBackend {
        handoff: true,
        expected_prefill: None,
    });
    let service = ChatWorkerFacade::new(engine, processor(), 1024 * 1024, 1024 * 1024).unwrap();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ChatWorkerBridgeServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let mut client = ChatWorkerBridgeClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut output = client
        .generate_raw(ChatWorkerRequest {
            request_id: "prefill-1".into(),
            backend_request_json: {
                let mut value = serde_json::to_value(backend_request()).unwrap();
                value.as_object_mut().unwrap().remove("token_ids");
                serde_json::to_vec(&value).unwrap()
            },
            token_ids_le: [1_u32, 2, 3]
                .into_iter()
                .flat_map(u32::to_le_bytes)
                .collect(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let chunk = output.next().await.unwrap().unwrap();
    let relayed: Annotated<BackendOutput> =
        serde_json::from_slice(&chunk.annotated_backend_chunk_json).unwrap();
    assert_eq!(
        relayed.data.unwrap().disaggregated_params,
        Some(serde_json::json!({"opaque_kv": "kept"}))
    );
    assert!(output.next().await.unwrap().unwrap().finished);
    server.abort();
}

#[tokio::test]
async fn decode_facade_injects_an_opaque_prefill_result_into_dynamo_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handoff = serde_json::json!({"engine_owned": [11, 12]});
    let engine: CanonicalBackendEngine = Arc::new(FakeCanonicalBackend {
        handoff: false,
        expected_prefill: Some(handoff.clone()),
    });
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
    let mut backend = serde_json::to_value(backend_request()).unwrap();
    backend.as_object_mut().unwrap().remove("token_ids");
    let mut client = ChatWorkerBridgeClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    let mut output = client
        .generate(ChatWorkerRequest {
            request_id: "decode-1".into(),
            backend_request_json: serde_json::to_vec(&backend).unwrap(),
            normalized_openai_request_json: serde_json::to_vec(&normalized).unwrap(),
            prefill_result_json: serde_json::to_vec(&handoff).unwrap(),
            token_ids: vec![1, 2, 3],
            prompt_tokens: 3,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(output.next().await.unwrap().unwrap().error.is_none());
    server.abort();
}
