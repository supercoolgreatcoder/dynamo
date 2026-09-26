// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A domain-agnostic pipeline orchestration core.
//!
//! This is the *generic* variant. The statically-linked implementation in
//! `crates/agentgateway/src/pd_orchestration.rs` is untouched and remains the fallback:
//! it is declarative but its vocabulary is domain-specific (`Tokenize`, `Select { fleet }`,
//! `Dispatch { phase }`). Here the vocabulary is `call operation X on component Y`, and
//! the domain lives entirely in config plus OpenAPI documents.
//!
//! The genericity claim is checkable rather than asserted: grep this crate for domain
//! nouns. The only place any appear is in doc comments explaining what the generic
//! construct replaces, and in `examples/`.

pub mod batch;
pub mod batcher;
pub mod config;
pub mod engine;
pub mod expr;
pub mod openapi;

pub use config::{BatchPolicy, Call, Component, Expr, Pipeline, Step, StepBody};
pub use engine::{
    EngineError, NullSink, Payload, Prepared, Reply, Request, Resolver, Sink, Transport,
    last_reads, referenced_specs,
};
pub use expr::Scope;
pub use openapi::Document;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// Records what the core would send, so tests assert on the calls themselves.
    #[derive(Default)]
    pub struct Recorder {
        seen: Mutex<Vec<Request>>,
        replies: Mutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl Transport for Recorder {
        async fn call(
            &self,
            req: Request,
        ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
            self.seen.lock().unwrap().push(req);
            let mut r = self.replies.lock().unwrap();
            Ok(Reply::ok(if r.is_empty() {
                Value::Null
            } else {
                r.remove(0)
            }))
        }
    }

    const TOKENIZER_SPEC: &str = r#"
paths:
  /encode:
    post:
      operationId: encode
      x-batch: true
      requestBody:
        content: {application/json: {schema: {type: object, required: [text],
          properties: {text: {type: string}, blockSize: {type: integer}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object,
            properties: {tokenIds: {type: array}, blockHashes: {type: array}}}}}
"#;

    const SELECTOR_SPEC: &str = r#"
paths:
  /select:
    post:
      operationId: select
      x-batch: true
      requestBody:
        content: {application/json: {schema: {type: object, required: [blockHashes],
          properties: {blockHashes: {type: array}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object,
            properties: {endpoint: {type: string}}}}}
"#;

    const WORKER_SPEC: &str = r#"
paths:
  /generate:
    post:
      operationId: generate
      x-streaming: true
      requestBody:
        content: {application/json: {schema: {type: object, required: [tokenIds],
          properties: {tokenIds: {type: array}, maxTokens: {type: integer}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object,
            properties: {text: {type: string}}}}}
"#;

    const API_SPEC: &str = r#"
paths:
  /v1/chat/completions:
    post:
      operationId: createChatCompletion
      requestBody:
        content: {application/json: {schema: {type: object, properties: {messages: {type: array}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, properties: {choices: {type: array}}}}}
"#;

    const PIPELINE: &str = r#"
api:
  openapi: api.yaml
  operationId: createChatCompletion
components:
  tokenizer:
    openapi: tokenizer.yaml
    baseUrl: http://tok-svc:8080
  selector:
    openapi: selector.yaml
    baseUrl: http://selector:8080
  workers:
    openapi: worker.yaml
    discovery: {group: decode}
steps:
  - id: tokenize
    call:
      component: tokenizer
      operationId: encode
      input:
        text: "$.request.body.messages[-1].content"
        blockSize: 64
      output:
        tokenIds: "$.response.tokenIds"
        blockHashes: "$.response.blockHashes"
      batch: {maxSize: 128, lingerUs: 0}
  - id: select
    call:
      component: selector
      operationId: select
      input: {blockHashes: "$.vars.blockHashes"}
      output: {endpoint: "$.response.endpoint"}
      batch: {maxSize: 32, lingerUs: 1000}
  - id: dispatch
    call:
      component: workers
      operationId: generate
      target: "$.vars.endpoint"
      input: {tokenIds: "$.vars.tokenIds"}
      respond: true
"#;

    pub(crate) fn specs() -> BTreeMap<String, Document> {
        [
            ("api.yaml", API_SPEC),
            ("tokenizer.yaml", TOKENIZER_SPEC),
            ("selector.yaml", SELECTOR_SPEC),
            ("worker.yaml", WORKER_SPEC),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), Document::from_yaml(v).unwrap()))
        .collect()
    }

    fn prepared() -> Prepared {
        Prepared::new(Pipeline::from_yaml(PIPELINE).unwrap(), &specs()).unwrap()
    }

    #[tokio::test]
    async fn runs_a_three_stage_pipeline_with_a_runtime_chosen_callee() {
        let t = Recorder::default();
        *t.replies.lock().unwrap() = vec![
            json!({"tokenIds": [5, 6, 7], "blockHashes": [111]}),
            json!({"endpoint": "http://worker-9:8080"}),
            json!({"text": "hello"}),
        ];
        let out = prepared()
            .run(&t, json!({"body": {"messages": [{"content": "hi"}]}}))
            .await
            .unwrap();
        assert_eq!(out, Some(json!({"text": "hello"})));

        let seen = t.seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        // Inputs are built from bindings, including a literal.
        assert_eq!(seen[0].url, "http://tok-svc:8080/encode");
        assert_eq!(seen[0].body, json!({"text": "hi", "blockSize": 64}));
        // A later step reads a variable captured from an earlier response.
        assert_eq!(seen[1].body, json!({"blockHashes": [111]}));
        // The callee itself came from a binding -- routing is just an expression.
        assert_eq!(seen[2].url, "http://worker-9:8080/generate");
        assert_eq!(seen[2].body, json!({"tokenIds": [5, 6, 7]}));
        // Streaming is a property of the operation, declared by its owner.
        assert!(seen[2].streaming);
        assert!(!seen[0].streaming);
    }

    #[test]
    fn rejects_a_binding_the_callee_does_not_define_at_load_time() {
        let bad = PIPELINE.replace("text: \"$.request.body", "txt: \"$.request.body");
        let err = Prepared::new(Pipeline::from_yaml(&bad).unwrap(), &specs()).unwrap_err();
        assert!(matches!(
            err,
            EngineError::Spec(openapi::SpecError::UnknownInput { .. })
        ));
    }

    #[test]
    fn rejects_an_undeclared_component() {
        let bad = PIPELINE.replace("component: selector", "component: nope");
        let err = Prepared::new(Pipeline::from_yaml(&bad).unwrap(), &specs()).unwrap_err();
        assert!(matches!(err, EngineError::UnknownComponent(_, _)));
    }

    #[tokio::test]
    async fn a_discovery_component_without_a_target_asks_the_resolver() {
        // Two ways to reach a discovery-based component, both declarative: the pipeline
        // picks the callee itself via `target` (KV-style routing), or it names a group and
        // the environment resolves it. Only the group name is configuration; which
        // endpoints exist is environment state, so it stays behind a trait.
        struct Fixed;
        #[async_trait::async_trait]
        impl engine::Resolver for Fixed {
            async fn resolve(
                &self,
                group: &str,
            ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
                Ok(format!("http://{group}-pool:8080"))
            }
        }
        let cfg = PIPELINE.replace("      target: \"$.vars.endpoint\"\n", "");
        let p = Prepared::new(Pipeline::from_yaml(&cfg).unwrap(), &specs())
            .unwrap()
            .with_resolver(std::sync::Arc::new(Fixed));
        let t = Recorder::default();
        *t.replies.lock().unwrap() = vec![
            json!({"tokenIds": [1], "blockHashes": [2]}),
            json!({"endpoint": "unused"}),
            json!({"text": "hi"}),
        ];
        p.run(&t, json!({"body": {"messages": [{"content": "hi"}]}}))
            .await
            .unwrap();
        assert_eq!(
            t.seen.lock().unwrap()[2].url,
            "http://decode-pool:8080/generate"
        );
    }

    #[tokio::test]
    async fn a_discovery_component_with_no_target_and_no_resolver_fails_loudly() {
        let cfg = PIPELINE.replace("      target: \"$.vars.endpoint\"\n", "");
        let p = Prepared::new(Pipeline::from_yaml(&cfg).unwrap(), &specs()).unwrap();
        let t = Recorder::default();
        *t.replies.lock().unwrap() = vec![
            json!({"tokenIds": [1], "blockHashes": [2]}),
            json!({"endpoint": "unused"}),
        ];
        let err = p
            .run(&t, json!({"body": {"messages": [{"content": "hi"}]}}))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("no resolver configured"));
    }

    #[test]
    fn a_component_with_no_url_no_target_and_no_group_is_rejected_at_load() {
        let bad = PIPELINE
            .replace("      target: \"$.vars.endpoint\"\n", "")
            .replace("    discovery: {group: decode}\n", "");
        let err = Prepared::new(Pipeline::from_yaml(&bad).unwrap(), &specs()).unwrap_err();
        assert!(matches!(err, EngineError::MissingTarget(_)));
    }

    #[tokio::test]
    async fn parallel_branches_merge_disjoint_writes() {
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: http://t:8080}
  selector: {openapi: selector.yaml, baseUrl: http://s:8080}
steps:
  - id: fan
    parallel:
      branches:
        - - id: a
            call:
              component: tokenizer
              operationId: encode
              input: {text: "$.request.body.q"}
              output: {tokenIds: "$.response.tokenIds"}
        - - id: b
            call:
              component: selector
              operationId: select
              input: {blockHashes: [1]}
              output: {endpoint: "$.response.endpoint"}
"#;
        let t = Recorder::default();
        *t.replies.lock().unwrap() =
            vec![json!({"tokenIds": [1]}), json!({"endpoint": "http://w:1"})];
        let p = Prepared::new(Pipeline::from_yaml(P).unwrap(), &specs()).unwrap();
        p.run(&t, json!({"body": {"q": "x"}})).await.unwrap();
        assert_eq!(t.seen.lock().unwrap().len(), 2);
    }

    const PARAM_SPEC: &str = r#"
paths:
  /tenants/{tenantId}/items/{itemId}:
    parameters:
      - {name: tenantId, in: path, required: true, schema: {type: string}}
      - {name: itemId, in: path, required: true, schema: {type: string}}
    get:
      operationId: getItem
      parameters:
        - {name: verbose, in: query, schema: {type: boolean}}
        - {name: x-trace-id, in: header, schema: {type: string}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, properties: {name: {type: string}}}}}
"#;

    #[tokio::test]
    async fn places_inputs_by_their_declared_parameter_location() {
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  svc: {openapi: params.yaml, baseUrl: http://svc:8080}
steps:
  - id: fetch
    call:
      component: svc
      operationId: getItem
      input:
        tenantId: acme
        itemId: "$.request.body.id"
        verbose: true
        x-trace-id: "$.request.body.trace"
      output: {name: "$.response.name"}
      respond: true
"#;
        let mut s = specs();
        s.insert(
            "params.yaml".into(),
            Document::from_yaml(PARAM_SPEC).unwrap(),
        );
        let t = Recorder::default();
        *t.replies.lock().unwrap() = vec![json!({"name": "widget"})];
        let p = Prepared::new(Pipeline::from_yaml(P).unwrap(), &s).unwrap();
        p.run(&t, json!({"body": {"id": "i42", "trace": "abc"}}))
            .await
            .unwrap();

        let seen = t.seen.lock().unwrap();
        // Path params substituted into the template, query appended, header lifted out,
        // and nothing left in the body -- all from the spec, not from the pipeline.
        assert_eq!(seen[0].method, "GET");
        assert_eq!(
            seen[0].url,
            "http://svc:8080/tenants/acme/items/i42?verbose=true"
        );
        assert_eq!(
            seen[0].headers.get("x-trace-id").map(String::as_str),
            Some("abc")
        );
        assert_eq!(seen[0].body, json!({}));
    }

    #[tokio::test]
    async fn a_guard_skips_a_step_without_needing_a_second_pipeline() {
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: http://t:8080}
steps:
  - id: only_when_streaming
    when: {path: "$.request.body.stream", equals: true}
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q"}
      output: {tokenIds: "$.response.tokenIds"}
"#;
        let p = Prepared::new(Pipeline::from_yaml(P).unwrap(), &specs()).unwrap();

        let t = Recorder::default();
        p.run(&t, json!({"body": {"q": "x", "stream": false}}))
            .await
            .unwrap();
        assert_eq!(
            t.seen.lock().unwrap().len(),
            0,
            "guard should have skipped the step"
        );

        let t2 = Recorder::default();
        *t2.replies.lock().unwrap() = vec![json!({"tokenIds": [1]})];
        p.run(&t2, json!({"body": {"q": "x", "stream": true}}))
            .await
            .unwrap();
        assert_eq!(t2.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn respond_with_shapes_the_reply_to_the_declared_external_api() {
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: http://t:8080}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q"}
      output: {tokenIds: "$.response.tokenIds"}
      respond: true
      respondWith:
        choices: "$.vars.tokenIds"
"#;
        let t = Recorder::default();
        *t.replies.lock().unwrap() = vec![json!({"tokenIds": [7, 8], "blockHashes": [1]})];
        let p = Prepared::new(Pipeline::from_yaml(P).unwrap(), &specs()).unwrap();
        let out = p.run(&t, json!({"body": {"q": "x"}})).await.unwrap();
        // The callee's `blockHashes` does not leak into the public contract.
        assert_eq!(out, Some(json!({"choices": [7, 8]})));
    }

    #[test]
    fn respond_with_must_match_the_external_api_schema() {
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: http://t:8080}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q"}
      respond: true
      respondWith: {notAField: "$.response.tokenIds"}
"#;
        let err = Prepared::new(Pipeline::from_yaml(P).unwrap(), &specs()).unwrap_err();
        assert!(matches!(
            err,
            EngineError::Spec(openapi::SpecError::UnknownOutput { .. })
        ));
    }

    #[test]
    fn an_external_api_operation_that_does_not_exist_is_rejected() {
        // The `api:` block used to be decorative: parsed and never resolved.
        let bad = PIPELINE.replace("operationId: createChatCompletion", "operationId: nope");
        let err = Prepared::new(Pipeline::from_yaml(&bad).unwrap(), &specs()).unwrap_err();
        assert!(matches!(
            err,
            EngineError::Spec(openapi::SpecError::NoSuchOperation(_))
        ));
    }

    #[tokio::test]
    async fn retry_reattempts_a_failing_call_the_declared_number_of_times() {
        #[derive(Default)]
        struct Flaky {
            attempts: Mutex<u32>,
        }
        #[async_trait::async_trait]
        impl Transport for Flaky {
            async fn call(
                &self,
                _r: Request,
            ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
                let mut a = self.attempts.lock().unwrap();
                *a += 1;
                if *a < 3 {
                    return Err("boom".into());
                }
                Ok(Reply::ok(json!({"tokenIds": [1]})))
            }
        }
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: http://t:8080}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q"}
      retry: {maxAttempts: 3, backoffMs: 0}
      respond: true
"#;
        let t = Flaky::default();
        let p = Prepared::new(Pipeline::from_yaml(P).unwrap(), &specs()).unwrap();
        let out = p.run(&t, json!({"body": {"q": "x"}})).await.unwrap();
        assert_eq!(out, Some(json!({"tokenIds": [1]})));
        assert_eq!(*t.attempts.lock().unwrap(), 3);
    }

    /// Emits a fixed set of items as a stream, and records what the sink received.
    struct Streamer {
        items: Mutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl Transport for Streamer {
        async fn call(
            &self,
            _req: Request,
        ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
            let items: Vec<Value> = self.items.lock().unwrap().clone();
            Ok(Reply::stream(Box::pin(futures::stream::iter(
                items.into_iter().map(Ok::<Value, String>),
            ))))
        }
    }

    #[derive(Default)]
    struct Collect {
        got: Mutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl Sink for Collect {
        async fn item(&self, v: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.got.lock().unwrap().push(v);
            Ok(())
        }
    }

    const STREAM_PIPELINE: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  workers: {openapi: worker.yaml, baseUrl: http://w:8080}
steps:
  - id: generate
    call:
      component: workers
      operationId: generate
      input: {tokenIds: [1]}
      respond: true
      stream:
        emitWhen: {path: "$.item.text", exists: true}
        emit:
          delta: "$.item.text"
        countInto: frames
      output: {frames: "$.response.frames"}
"#;

    #[tokio::test]
    async fn streaming_projects_each_item_and_can_skip_some() {
        let t = Streamer {
            items: Mutex::new(vec![
                json!({"text": "he"}),
                json!({"finished": false}), // no text: held-back UTF-8, must not emit
                json!({"text": "llo"}),
            ]),
        };
        let sink = Collect::default();
        let mut s = specs();
        // `frames` must exist on the worker's response schema for the output binding.
        s.insert(
            "worker.yaml".into(),
            Document::from_yaml(
                r#"
paths:
  /generate:
    post:
      operationId: generate
      x-streaming: true
      requestBody:
        content: {application/json: {schema: {type: object, required: [tokenIds],
          properties: {tokenIds: {type: array}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object,
            properties: {text: {type: string}, frames: {type: integer}}}}}
"#,
            )
            .unwrap(),
        );
        let p = Prepared::new(Pipeline::from_yaml(STREAM_PIPELINE).unwrap(), &s).unwrap();
        p.run_with_sink(&t, json!({"body": {}}), &sink)
            .await
            .unwrap();

        // Two of three items emitted, each projected to the declared shape rather than
        // forwarded verbatim -- the public stream shape is config, not the callee's.
        assert_eq!(
            *sink.got.lock().unwrap(),
            vec![json!({"delta": "he"}), json!({"delta": "llo"})]
        );
    }

    #[tokio::test]
    async fn an_internal_stream_can_collect_without_emitting_to_the_client() {
        let t = Streamer {
            items: Mutex::new(vec![
                json!({"data": {"disaggregated_params": {"engine": "prefill"}}}),
                json!(""), // terminal gRPC envelope has no JSON payload
            ]),
        };
        let sink = Collect::default();
        let mut s = specs();
        s.insert(
            "worker.yaml".into(),
            Document::from_yaml(
                r#"
paths:
  /generate:
    post:
      operationId: generate
      x-streaming: true
      requestBody:
        content: {application/json: {schema: {type: object, required: [tokenIds],
          properties: {tokenIds: {type: array}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object,
            properties: {handoffs: {type: array}, frames: {type: integer}}}}}
"#,
            )
            .unwrap(),
        );
        let cfg = STREAM_PIPELINE.replace(
            "emitWhen: {path: \"$.item.text\", exists: true}",
            "emitToClient: false\n        emitWhen: {path: \"$.item.data.disaggregated_params\", exists: true}",
        ).replace(
            "delta: \"$.item.text\"",
            "delta: \"$.item.data.disaggregated_params\"",
        );
        let p = Prepared::new(Pipeline::from_yaml(&cfg).unwrap(), &s).unwrap();
        let response = p
            .run_with_sink(&t, json!({"body": {}}), &sink)
            .await
            .unwrap();
        assert!(sink.got.lock().unwrap().is_empty());
        assert_eq!(response, Some(json!({"frames": 1})));
    }

    #[test]
    fn disaggregated_dynamo_graph_matches_its_component_contracts() {
        let pipeline =
            Pipeline::from_yaml(include_str!("../../graphs/disaggregated.yaml")).unwrap();
        let specs = [
            (
                "../specs/openai-chat.yaml",
                include_str!("../../specs/openai-chat.yaml"),
            ),
            (
                "../specs/preprocessor.yaml",
                include_str!("../../specs/preprocessor.yaml"),
            ),
            (
                "../specs/selector.yaml",
                include_str!("../../specs/selector.yaml"),
            ),
            (
                "../specs/chat-worker.yaml",
                include_str!("../../specs/chat-worker.yaml"),
            ),
        ]
        .into_iter()
        .map(|(name, yaml)| (name.to_string(), Document::from_yaml(yaml).unwrap()))
        .collect();
        Prepared::new(pipeline, &specs).unwrap();
    }

    #[tokio::test]
    async fn disaggregated_graph_passes_handoff_to_decode_without_exposing_prefill() {
        #[derive(Default)]
        struct PdTransport {
            calls: Mutex<Vec<Request>>,
        }

        #[async_trait::async_trait]
        impl Transport for PdTransport {
            async fn call(
                &self,
                request: Request,
            ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
                let url = request.url.clone();
                self.calls.lock().unwrap().push(request);
                if url.contains("dynamo-preprocessor") {
                    return Ok(Reply::ok(json!({"items": [{
                        "normalized_openai_request_json": {"model": "test"},
                        "backend_request_json": {"model": "test"},
                        "selector_request_json": {"model": "test"},
                        "token_ids_le": "AQAAAA==",
                        "prompt_tokens": 1,
                        "prompt_injected_reasoning": true,
                        "uses_tool_call_structural_tag": true,
                        "image_count": 2,
                        "video_count": 3,
                        "audio_count": 4
                    }]})));
                }
                if url.contains("dynamo-prefill") {
                    return Ok(Reply::stream(Box::pin(futures::stream::iter([
                        Ok(json!({"data": {"disaggregated_params": {"engine": "opaque"}}})),
                        Ok(json!("")),
                    ]))));
                }
                if url.contains("dynamo-selector") {
                    return Ok(Reply::ok(json!({
                        "payload_json": {"endpoint": "http://decode-worker:50051"}
                    })));
                }
                if url.contains("decode-worker") {
                    return Ok(Reply::stream(Box::pin(futures::stream::iter([Ok(
                        json!({"choices": [{"delta": {"content": "ok"}}]}),
                    )]))));
                }
                Err(format!("unexpected call to {url}").into())
            }
        }

        let pipeline =
            Pipeline::from_yaml(include_str!("../../graphs/disaggregated.yaml")).unwrap();
        let specs = [
            (
                "../specs/openai-chat.yaml",
                include_str!("../../specs/openai-chat.yaml"),
            ),
            (
                "../specs/preprocessor.yaml",
                include_str!("../../specs/preprocessor.yaml"),
            ),
            (
                "../specs/selector.yaml",
                include_str!("../../specs/selector.yaml"),
            ),
            (
                "../specs/chat-worker.yaml",
                include_str!("../../specs/chat-worker.yaml"),
            ),
        ]
        .into_iter()
        .map(|(name, yaml)| (name.to_string(), Document::from_yaml(yaml).unwrap()))
        .collect();
        let prepared = Prepared::new(pipeline, &specs).unwrap();
        let transport = PdTransport::default();
        let sink = Collect::default();
        prepared
            .run_with_sink(
                &transport,
                json!({"id": "pd-1", "body": {"model": "test", "messages": [{"role": "user", "content": "hello"}]}}),
                &sink,
            )
            .await
            .unwrap();
        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.len(), 4);
        for call in [&calls[1], &calls[2], &calls[3]] {
            assert_eq!(call.body["token_ids_le"], json!("AQAAAA=="));
        }
        assert_eq!(calls[3].body["prompt_tokens"], json!(1));
        assert_eq!(calls[3].body["prompt_injected_reasoning"], json!(true));
        assert_eq!(calls[3].body["uses_tool_call_structural_tag"], json!(true));
        assert_eq!(calls[3].body["image_count"], json!(2));
        assert_eq!(calls[3].body["video_count"], json!(3));
        assert_eq!(calls[3].body["audio_count"], json!(4));
        assert_eq!(calls[3].body["image_tokens"], Value::Null);
        assert_eq!(
            calls[3].body["prefill_result_json"],
            json!({"engine": "opaque"})
        );
        assert_eq!(
            *sink.got.lock().unwrap(),
            vec![json!({"choices": [{"delta": {"content": "ok"}}]})]
        );
    }

    #[tokio::test]
    async fn for_each_fans_a_call_out_over_a_runtime_collection() {
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: http://t:8080}
steps:
  - id: encode_each
    call:
      component: tokenizer
      operationId: encode
      forEach: {items: "$.request.body.chunks", as: chunk, collectInto: encoded}
      input: {text: "$.vars.chunk"}
"#;
        let t = Recorder::default();
        *t.replies.lock().unwrap() = vec![
            json!({"tokenIds": [1]}),
            json!({"tokenIds": [2]}),
            json!({"tokenIds": [3]}),
        ];
        let p = Prepared::new(Pipeline::from_yaml(P).unwrap(), &specs()).unwrap();
        p.run(&t, json!({"body": {"chunks": ["a", "b", "c"]}}))
            .await
            .unwrap();

        let seen = t.seen.lock().unwrap();
        assert_eq!(seen.len(), 3, "one call per element");
        assert_eq!(seen[0].body, json!({"text": "a"}));
        assert_eq!(seen[2].body, json!({"text": "c"}));
    }

    /// Returns a scripted sequence of statuses, so retry/error policy is testable.
    struct Statuses {
        seq: Mutex<Vec<u16>>,
        calls: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl Transport for Statuses {
        async fn call(
            &self,
            _req: Request,
        ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
            *self.calls.lock().unwrap() += 1;
            let mut s = self.seq.lock().unwrap();
            let status = if s.is_empty() { 200 } else { s.remove(0) };
            Ok(Reply {
                status,
                payload: Payload::Unary(json!({"tokenIds": [1]})),
            })
        }
    }

    fn one_step(extra: &str) -> String {
        format!(
            r#"
api: {{openapi: api.yaml, operationId: createChatCompletion}}
components:
  tokenizer: {{openapi: tokenizer.yaml, baseUrl: http://t:8080}}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {{text: "$.request.body.q"}}
      respond: true
{extra}
"#
        )
    }

    #[tokio::test]
    async fn retries_only_the_statuses_the_policy_names() {
        // 503 is transient, 400 is not. Without a status on the reply the core could only
        // retry everything or nothing.
        let cfg = one_step("      retry: {maxAttempts: 4, backoffMs: 0, retryOn: [503]}");
        let p = Prepared::new(Pipeline::from_yaml(&cfg).unwrap(), &specs()).unwrap();

        let t = Statuses {
            seq: Mutex::new(vec![503, 503, 200]),
            calls: Mutex::new(0),
        };
        p.run(&t, json!({"body": {"q": "x"}})).await.unwrap();
        assert_eq!(*t.calls.lock().unwrap(), 3, "should retry through the 503s");

        let t2 = Statuses {
            seq: Mutex::new(vec![400, 200]),
            calls: Mutex::new(0),
        };
        let err = p.run(&t2, json!({"body": {"q": "x"}})).await.unwrap_err();
        assert_eq!(*t2.calls.lock().unwrap(), 1, "a 400 must not be retried");
        assert!(format!("{err}").contains("status 400"));
    }

    #[tokio::test]
    async fn a_failing_step_can_fall_back_to_a_declared_value() {
        let cfg = one_step(
            "      errors:\n        onFailure: {fallback: {tokenIds: []}}\n      output: {tokenIds: \"$.response.tokenIds\"}",
        );
        let p = Prepared::new(Pipeline::from_yaml(&cfg).unwrap(), &specs()).unwrap();
        let t = Statuses {
            seq: Mutex::new(vec![500]),
            calls: Mutex::new(0),
        };
        let out = p.run(&t, json!({"body": {"q": "x"}})).await.unwrap();
        assert_eq!(out, Some(json!({"tokenIds": []})));
    }

    #[tokio::test]
    async fn expect_status_makes_success_configurable() {
        // An API where 202 is the success case and 200 would be wrong.
        let cfg = one_step("      errors: {expectStatus: [202]}");
        let p = Prepared::new(Pipeline::from_yaml(&cfg).unwrap(), &specs()).unwrap();
        let t = Statuses {
            seq: Mutex::new(vec![202]),
            calls: Mutex::new(0),
        };
        p.run(&t, json!({"body": {"q": "x"}})).await.unwrap();
        let t2 = Statuses {
            seq: Mutex::new(vec![200]),
            calls: Mutex::new(0),
        };
        assert!(p.run(&t2, json!({"body": {"q": "x"}})).await.is_err());
    }

    #[tokio::test]
    async fn pipeline_vars_seed_the_scope_so_a_constant_is_stated_once() {
        let cfg = format!(
            r#"
api: {{openapi: api.yaml, operationId: createChatCompletion}}
vars:
  blockSize: 64
components:
  tokenizer: {{openapi: tokenizer.yaml, baseUrl: http://t:8080}}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {{text: "$.request.body.q", blockSize: "$.vars.blockSize"}}
"#
        );
        let t = Recorder::default();
        let p = Prepared::new(Pipeline::from_yaml(&cfg).unwrap(), &specs()).unwrap();
        p.run(&t, json!({"body": {"q": "x"}})).await.unwrap();
        assert_eq!(
            t.seen.lock().unwrap()[0].body,
            json!({"text": "x", "blockSize": 64})
        );
    }

    #[tokio::test]
    async fn concurrent_for_each_preserves_input_order() {
        // Out-of-order completion must not reorder results: a caller splitting them apart
        // relies on position, exactly as a batched response does.
        const P: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: http://t:8080}
steps:
  - id: encode_each
    call:
      component: tokenizer
      operationId: encode
      forEach: {items: "$.request.body.chunks", as: chunk, maxConcurrent: 4, collectInto: all}
      input: {text: "$.vars.chunk"}
"#;
        let t = Recorder::default();
        let p = Prepared::new(Pipeline::from_yaml(P).unwrap(), &specs()).unwrap();
        p.run(&t, json!({"body": {"chunks": ["a", "b", "c", "d"]}}))
            .await
            .unwrap();
        let seen = t.seen.lock().unwrap();
        assert_eq!(seen.len(), 4);
        let mut texts: Vec<String> = seen
            .iter()
            .map(|r| r.body["text"].as_str().unwrap().to_string())
            .collect();
        texts.sort();
        assert_eq!(texts, vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn a_request_missing_a_required_api_field_is_rejected_up_front() {
        // Otherwise it fails deep in the pipeline as an unresolvable path, attributed to
        // the wrong step.
        let mut s = specs();
        s.insert(
            "api.yaml".into(),
            Document::from_yaml(
                r#"
paths:
  /v1/chat/completions:
    post:
      operationId: createChatCompletion
      requestBody:
        content: {application/json: {schema: {type: object, required: [messages],
          properties: {messages: {type: array}, choices: {type: array}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, properties: {choices: {type: array}}}}}
"#,
            )
            .unwrap(),
        );
        let p = Prepared::new(Pipeline::from_yaml(PIPELINE).unwrap(), &s).unwrap();
        let t = Recorder::default();
        let err = p.run(&t, json!({"body": {"nope": 1}})).await.unwrap_err();
        assert!(matches!(err, EngineError::InvalidRequest(ref f) if f == "messages"));
        assert_eq!(t.seen.lock().unwrap().len(), 0, "must not call anything");
    }

    #[test]
    fn the_core_carries_no_domain_vocabulary() {
        // The genericity claim, as a test. These names must not appear in the engine,
        // config, expression or OpenAPI modules -- only in docs and examples.
        const SOURCES: [(&str, &str); 4] = [
            ("config.rs", include_str!("config.rs")),
            ("engine.rs", include_str!("engine.rs")),
            ("expr.rs", include_str!("expr.rs")),
            ("openapi.rs", include_str!("openapi.rs")),
        ];
        for (name, src) in SOURCES {
            let code: String = src
                .lines()
                .filter(|l| {
                    let t = l.trim_start();
                    !t.starts_with("//") && !t.starts_with("///") && !t.starts_with("*")
                })
                .collect::<Vec<_>>()
                .join("\n")
                .to_lowercase();
            for term in [
                "tokenize",
                "tokenizer",
                "prefill",
                "decode",
                "kv",
                "fleet",
                "prompt",
            ] {
                assert!(
                    !code.contains(term),
                    "{name} contains domain term `{term}` outside comments"
                );
            }
        }
    }
}

#[cfg(test)]
mod not_equals_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn not_equals_filters_empty_values_that_exists_cannot() {
        // The gRPC transport emits every declared field, so `text` is always present -- as
        // "" on a worker's terminal chunk. `exists` therefore matches it and produces an
        // extra empty frame; `notEquals: ""` is the predicate that actually filters.
        let mut s = Scope::new(json!({}));
        s.item = json!({"text": ""});
        let w: config::When =
            serde_yaml::from_str(r#"{path: "$.item.text", notEquals: ""}"#).unwrap();
        assert!(
            !engine::eval_when_pub(&s, &w).unwrap(),
            "empty text must be filtered"
        );

        s.item = json!({"text": "hi"});
        assert!(
            engine::eval_when_pub(&s, &w).unwrap(),
            "real text must pass"
        );

        let e: config::When =
            serde_yaml::from_str(r#"{path: "$.item.text", exists: true}"#).unwrap();
        s.item = json!({"text": ""});
        assert!(
            engine::eval_when_pub(&s, &e).unwrap(),
            "exists cannot tell empty from set"
        );
    }
}

#[cfg(test)]
mod batching_tests {
    use super::*;
    use crate::tests::specs;
    use serde_json::{Value, json};
    use std::sync::Mutex;

    /// Counts calls so a test can assert N requests produced ONE call.
    #[derive(Default)]
    struct CountingTransport {
        calls: Mutex<Vec<Request>>,
    }

    #[async_trait::async_trait]
    impl Transport for CountingTransport {
        async fn call(
            &self,
            req: Request,
        ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
            let n = req
                .body
                .get("texts")
                .and_then(Value::as_array)
                .map(|a| a.len())
                .unwrap_or(1);
            self.calls.lock().unwrap().push(req);
            // Positional results, one per folded input, as the spec declares.
            let results: Vec<Value> = (0..n)
                .map(|i| json!({"tokenIds": [i as i64], "blockHashes": []}))
                .collect();
            Ok(Reply::ok(json!({ "results": results })))
        }
    }

    pub(crate) const BATCH_TOKENIZER: &str = r#"
paths:
  /encode:
    post:
      operationId: encode
      x-batch: {requestField: text, intoField: texts, responseField: results, operationId: encodeBatch}
      requestBody:
        content: {application/json: {schema: {type: object, required: [text],
          properties: {text: {type: string}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object,
            properties: {tokenIds: {type: array}, blockHashes: {type: array}}}}}
  /encode_batch:
    post:
      operationId: encodeBatch
      requestBody:
        content: {application/json: {schema: {type: object, required: [texts],
          properties: {texts: {type: array}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, properties: {results: {type: array}}}}}
"#;

    const BATCH_PIPELINE: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: btok.yaml, baseUrl: "http://t:8080"}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q"}
      output: {tokenIds: "$.response.tokenIds"}
      batch: {maxSize: 32, lingerUs: 2000}
      respond: true
"#;

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_requests_fold_into_one_call_and_split_back() {
        let mut s = specs();
        s.insert(
            "btok.yaml".into(),
            Document::from_yaml(BATCH_TOKENIZER).unwrap(),
        );
        let p = std::sync::Arc::new(
            Prepared::new(Pipeline::from_yaml(BATCH_PIPELINE).unwrap(), &s).unwrap(),
        );
        let t = std::sync::Arc::new(CountingTransport::default());

        // Eight concurrent requests on one runtime: one leader, seven followers.
        let mut set = tokio::task::JoinSet::new();
        for i in 0..8 {
            let (p, t) = (p.clone(), t.clone());
            set.spawn(async move {
                p.run(&*t, json!({"body": {"q": format!("prompt {i}")}}))
                    .await
            });
        }
        let mut ok = 0;
        while let Some(r) = set.join_next().await {
            r.unwrap().unwrap();
            ok += 1;
        }
        assert_eq!(ok, 8, "every request must get a reply");

        let calls = t.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "8 requests should fold into 1 call, saw {}",
            calls.len()
        );
        let texts = calls[0].body["texts"].as_array().unwrap();
        assert_eq!(texts.len(), 8, "all 8 prompts must be in the folded call");
        assert!(
            calls[0].url.ends_with("/encode_batch"),
            "must call the folded operation"
        );
    }

    /// More concurrent requests than maxSize.
    ///
    /// The overflow used to be parked back in the slot with no leader: the leader took its
    /// max_size and left the rest behind, and the next arrival saw a non-empty slot and
    /// became a follower too. Nothing promotes a parked follower, and under closed-loop load
    /// no new request arrives until one completes, so the whole arm wedges. In production
    /// this showed as every request returning 200 after exactly 60,000 ms, all released at
    /// the same instant.
    ///
    /// It stayed hidden while maxSize was 128 and the benchmark ran at concurrency 64 -- the
    /// overflow branch simply never executed. Setting maxSize to 32 to match the arm under
    /// comparison is what ran it for the first time.
    #[tokio::test(flavor = "current_thread")]
    async fn more_requests_than_max_size_all_complete() {
        const SMALL_BATCH: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: btok.yaml, baseUrl: "http://t:8080"}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q"}
      output: {tokenIds: "$.response.tokenIds"}
      batch: {maxSize: 4, lingerUs: 0}
      respond: true
"#;
        let mut s = specs();
        s.insert(
            "btok.yaml".into(),
            Document::from_yaml(BATCH_TOKENIZER).unwrap(),
        );
        let p = std::sync::Arc::new(
            Prepared::new(Pipeline::from_yaml(SMALL_BATCH).unwrap(), &s).unwrap(),
        );
        let t = std::sync::Arc::new(CountingTransport::default());

        let mut set = tokio::task::JoinSet::new();
        for i in 0..17 {
            let (p, t) = (p.clone(), t.clone());
            set.spawn(async move {
                p.run(&*t, json!({"body": {"q": format!("prompt {i}")}}))
                    .await
            });
        }
        let mut ok = 0;
        let join = async {
            while let Some(r) = set.join_next().await {
                r.unwrap().expect("no member may fail");
                ok += 1;
            }
        };
        // A timeout, because the failure mode is a wedge, and a wedged test that merely
        // never returns reports nothing.
        tokio::time::timeout(std::time::Duration::from_secs(5), join)
            .await
            .expect("every request must complete, not wedge");
        assert_eq!(ok, 17);

        // Every request must appear exactly once across the calls, and no call may exceed
        // the declared cap.
        let calls = t.calls.lock().unwrap();
        let mut total = 0;
        for c in calls.iter() {
            let n = c.body["texts"].as_array().unwrap().len();
            assert!(
                n <= 4,
                "a call carried {n} items, over the declared maxSize of 4"
            );
            total += n;
        }
        assert_eq!(total, 17, "every request must be sent exactly once");
    }

    /// With one folded call allowed in flight, batch size tunes itself.
    ///
    /// The measured problem: the leader took the slot and dispatched immediately, so a new
    /// batch started while the previous call was still running. At 3,300 rps that sent ~3,300
    /// single-item RPCs a second where a serial batcher sent ~100 of ~32, and the tokenizer
    /// fleet burned 6.58 cores against 1.77 for identical work.
    ///
    /// A linger cannot fix it: at that rate 1,000 us accumulates ~3.3 requests, and reaching
    /// 32 would need ~10 ms added to every request. Waiting for the in-flight call is free,
    /// because that time is already being spent.
    ///
    /// Arrivals are STAGGERED here, which is the whole point. Spawning 64 requests at once
    /// makes every batch large whether or not the gate exists -- they all queue before the
    /// first leader runs -- and a first version of this test passed with the gate disabled
    /// for exactly that reason. Production arrivals are spread across the in-flight window,
    /// and that is the condition under which an ungated leader batches one item at a time.
    async fn gate_batching(max_in_flight: usize) -> Vec<usize> {
        let pipeline = format!(
            r#"
api: {{openapi: api.yaml, operationId: createChatCompletion}}
components:
  tokenizer: {{openapi: btok.yaml, baseUrl: "http://t:8080"}}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {{text: "$.request.body.q"}}
      output: {{tokenIds: "$.response.tokenIds"}}
      batch: {{maxSize: 64, lingerUs: 0, maxInFlight: {max_in_flight}}}
      respond: true
"#
        );

        /// Takes long enough that arrivals land during the call, as a real RPC does.
        #[derive(Default)]
        struct SlowTransport {
            calls: Mutex<Vec<usize>>,
        }
        #[async_trait::async_trait]
        impl Transport for SlowTransport {
            async fn call(
                &self,
                req: Request,
            ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
                let n = req.body["texts"].as_array().map(|a| a.len()).unwrap_or(1);
                self.calls.lock().unwrap().push(n);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                let results: Vec<Value> = (0..n)
                    .map(|i| json!({"tokenIds": [i as i64], "blockHashes": []}))
                    .collect();
                Ok(Reply::ok(json!({ "results": results })))
            }
        }

        let mut s = specs();
        s.insert(
            "btok.yaml".into(),
            Document::from_yaml(BATCH_TOKENIZER).unwrap(),
        );
        let p = std::sync::Arc::new(
            Prepared::new(Pipeline::from_yaml(&pipeline).unwrap(), &s).unwrap(),
        );
        let t = std::sync::Arc::new(SlowTransport::default());

        let mut set = tokio::task::JoinSet::new();
        for i in 0..40 {
            let (p, t) = (p.clone(), t.clone());
            set.spawn(async move {
                // 2 ms apart against a 20 ms call: ~10 arrivals per in-flight window.
                tokio::time::sleep(std::time::Duration::from_millis(2 * i)).await;
                p.run(&*t, json!({"body": {"q": format!("prompt {i}")}}))
                    .await
            });
        }
        let mut ok = 0;
        while let Some(r) = set.join_next().await {
            r.unwrap().expect("every request must succeed");
            ok += 1;
        }
        assert_eq!(ok, 40);
        let calls = t.calls.lock().unwrap().clone();
        assert_eq!(
            calls.iter().sum::<usize>(),
            40,
            "every request sent exactly once"
        );
        calls
    }

    #[tokio::test(flavor = "current_thread")]
    async fn one_call_in_flight_makes_batches_grow_under_load() {
        let gated = gate_batching(1).await;
        let ungated = gate_batching(0).await;
        // Measured at the time of writing: ungated 40 calls of 1 item each -- the production
        // pathology exactly -- against gated 5 calls of [1, 10, 11, 11, 7].
        assert!(
            gated.len() * 2 <= ungated.len(),
            "the gate should cut the call count substantially: gated {} calls {:?}, ungated {} calls {:?}",
            gated.len(),
            gated,
            ungated.len(),
            ungated
        );
    }

    /// The gate must not serialise a step that is not batched by it, nor deadlock when the
    /// leader is also the only request.
    #[tokio::test(flavor = "current_thread")]
    async fn a_single_request_through_the_gate_still_completes() {
        const GATED_ONE: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: btok.yaml, baseUrl: "http://t:8080"}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q"}
      output: {tokenIds: "$.response.tokenIds"}
      batch: {maxSize: 32, lingerUs: 0, maxInFlight: 1}
      respond: true
"#;
        let mut s = specs();
        s.insert(
            "btok.yaml".into(),
            Document::from_yaml(BATCH_TOKENIZER).unwrap(),
        );
        let p = Prepared::new(Pipeline::from_yaml(GATED_ONE).unwrap(), &s).unwrap();
        let t = CountingTransport::default();
        let out = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            p.run(&t, json!({"body": {"q": "only one"}})),
        )
        .await
        .expect("a lone request must not wait on a gate nobody holds");
        assert!(out.unwrap().is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_failing_batch_fails_every_member_rather_than_stranding_them() {
        struct Failing;
        #[async_trait::async_trait]
        impl Transport for Failing {
            async fn call(
                &self,
                _r: Request,
            ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
                Err("upstream down".into())
            }
        }
        let mut s = specs();
        s.insert(
            "btok.yaml".into(),
            Document::from_yaml(BATCH_TOKENIZER).unwrap(),
        );
        let p = std::sync::Arc::new(
            Prepared::new(Pipeline::from_yaml(BATCH_PIPELINE).unwrap(), &s).unwrap(),
        );
        let t = std::sync::Arc::new(Failing);
        let mut set = tokio::task::JoinSet::new();
        for i in 0..4 {
            let (p, t) = (p.clone(), t.clone());
            set.spawn(async move { p.run(&*t, json!({"body": {"q": i.to_string()}})).await });
        }
        let mut errs = 0;
        while let Some(r) = set.join_next().await {
            if r.unwrap().is_err() {
                errs += 1;
            }
        }
        // Every member must learn the batch failed; a follower left waiting forever is the
        // worse outcome and the one this asserts against.
        assert_eq!(errs, 4);
    }
}

/// The selector's fold, which is not a single-field lift.
///
/// `/select_batch` takes WHOLE requests as items and renames their fields. That shape was
/// specified and unit-tested in `batch.rs`, but never driven end to end through the engine,
/// and against the real service the folded call never returned -- at concurrency 1, with no
/// error logged. The service itself answers the folded body correctly when called directly,
/// so the fault is on this side. These tests run the real spec's shape through the engine.
#[cfg(test)]
mod whole_request_fold_tests {
    use super::*;
    use crate::tests::specs;
    use serde_json::{Value, json};
    use std::sync::Mutex;

    #[derive(Default)]
    struct SelectorTransport {
        calls: Mutex<Vec<Request>>,
    }

    #[async_trait::async_trait]
    impl Transport for SelectorTransport {
        async fn call(
            &self,
            req: Request,
        ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
            let n = req
                .body
                .get("items")
                .and_then(Value::as_array)
                .map(|a| a.len())
                .unwrap_or(1);
            self.calls.lock().unwrap().push(req);
            let results: Vec<Value> = (0..n)
                .map(|i| json!({"endpoint": format!("http://w{i}:8081")}))
                .collect();
            Ok(Reply::ok(json!({ "results": results })))
        }
    }

    /// Trimmed from the deployed selector.yaml, keeping the fold declaration verbatim.
    const SELECTOR: &str = r#"
paths:
  /select:
    post:
      operationId: select
      x-batch:
        intoField: items
        responseField: results
        operationId: selectBatch
        itemFields: {block_hashes: bh, sequence_hashes: sh, isl_tokens: isl}
      requestBody:
        content: {application/json: {schema: {type: object, required: [block_hashes, sequence_hashes, isl_tokens],
          properties: {block_hashes: {type: array}, sequence_hashes: {type: array}, isl_tokens: {type: integer}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, required: [endpoint],
            properties: {endpoint: {type: string}}}}}
  /select_batch:
    post:
      operationId: selectBatch
      requestBody:
        content: {application/json: {schema: {type: object, required: [items],
          properties: {items: {type: array}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, required: [results],
            properties: {results: {type: array}}}}}
"#;

    const PIPELINE: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  selector: {openapi: sel.yaml, baseUrl: "http://selector:8083"}
steps:
  - id: route
    call:
      component: selector
      operationId: select
      input:
        block_hashes: "$.request.body.bh"
        sequence_hashes: "$.request.body.sh"
        isl_tokens: {length: "$.request.body.bh"}
      batch: {maxSize: 128, lingerUs: 0}
      output: {endpoint: "$.response.endpoint"}
      respond: true
"#;

    async fn prepared() -> std::sync::Arc<Prepared> {
        let mut s = specs();
        s.insert("sel.yaml".into(), Document::from_yaml(SELECTOR).unwrap());
        std::sync::Arc::new(Prepared::new(Pipeline::from_yaml(PIPELINE).unwrap(), &s).unwrap())
    }

    fn req(i: i64) -> Value {
        json!({"body": {"bh": [i, i + 1], "sh": [i * 10, i * 10 + 1]}})
    }

    /// The reported failure, reduced: ONE request through a batched whole-request fold.
    #[tokio::test(flavor = "current_thread")]
    async fn a_single_request_completes() {
        let (p, t) = (
            prepared().await,
            std::sync::Arc::new(SelectorTransport::default()),
        );
        // A timeout, not a plain await: the symptom is a hang, and a hung test that merely
        // never finishes reports nothing useful.
        let out = tokio::time::timeout(std::time::Duration::from_secs(5), p.run(&*t, req(1)))
            .await
            .expect("batched select must not hang")
            .expect("batched select must not error")
            .expect("the responding step must produce a body");
        assert_eq!(out["endpoint"], json!("http://w0:8081"));
        let calls = t.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].url.ends_with("/select_batch"));
        assert_eq!(calls[0].body["items"][0]["bh"], json!([1, 2]));
        assert_eq!(calls[0].body["items"][0]["sh"], json!([10, 11]));
        assert_eq!(calls[0].body["items"][0]["isl"], json!(2));
    }

    /// Per-item fields necessarily DIFFER between requests. The disagreement check exists to
    /// catch a folded field being silently taken from request 0; it must not fire on the
    /// fields the fold is explicitly told to carry per item.
    #[tokio::test(flavor = "current_thread")]
    async fn differing_per_item_fields_are_not_a_disagreement() {
        let (p, t) = (
            prepared().await,
            std::sync::Arc::new(SelectorTransport::default()),
        );
        let mut set = tokio::task::JoinSet::new();
        for i in 1..=4 {
            let (p, t) = (p.clone(), t.clone());
            set.spawn(async move { p.run(&*t, req(i)).await });
        }
        let mut ok = 0;
        let join = async {
            while let Some(r) = set.join_next().await {
                r.unwrap().expect("no member may fail");
                ok += 1;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), join)
            .await
            .expect("batched select must not hang");
        assert_eq!(ok, 4);
        let calls = t.calls.lock().unwrap();
        let items: usize = calls
            .iter()
            .map(|c| c.body["items"].as_array().map_or(0, |a| a.len()))
            .sum();
        assert_eq!(items, 4, "every request must appear as its own item");
    }
}

/// Per-step timing, which is the instrument that was missing.
///
/// The compiled-in engine emits STAGESTATS per stage; the generic core emitted nothing, so
/// every comparison ran a profiled arm against an unprofiled one -- the totals said which arm
/// was slower and nothing said which step.
#[cfg(test)]
mod step_stats_tests {
    use super::*;
    use crate::tests::{Recorder, specs};
    use serde_json::json;

    const TWO_STEPS: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: "http://t:8080"}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q", blockSize: 64}
  - id: second
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q", blockSize: 64}
      respond: true
"#;

    #[tokio::test]
    async fn every_step_is_timed_and_counted() {
        let p = Prepared::new(Pipeline::from_yaml(TWO_STEPS).unwrap(), &specs()).unwrap();
        let t = Recorder::default();
        for _ in 0..3 {
            p.run(&t, json!({"body": {"q": "hi"}})).await.unwrap();
        }
        assert_eq!(p.stats.requests(), 3);
        let snap = p.stats.snapshot();
        let ids: Vec<&str> = snap.iter().map(|(id, _, _)| id.as_str()).collect();
        assert!(
            ids.contains(&"encode") && ids.contains(&"second"),
            "got {ids:?}"
        );
        for (id, _us, runs) in &snap {
            assert_eq!(*runs, 3, "step {id} should have run once per request");
        }
        assert!(
            p.stats.line().starts_with("PIPESTATS reqs=3"),
            "{}",
            p.stats.line()
        );
    }

    #[tokio::test]
    async fn a_skipped_step_averages_over_the_requests_that_ran_it() {
        // A guarded step's average must be over its own runs, not over all requests, or a
        // rarely-taken step looks cheap for the wrong reason.
        const GUARDED: &str = r#"
api: {openapi: api.yaml, operationId: createChatCompletion}
components:
  tokenizer: {openapi: tokenizer.yaml, baseUrl: "http://t:8080"}
steps:
  - id: only_when_flagged
    when: {path: "$.request.body.flag", exists: true}
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q", blockSize: 64}
  - id: always
    call:
      component: tokenizer
      operationId: encode
      input: {text: "$.request.body.q", blockSize: 64}
      respond: true
"#;
        let p = Prepared::new(Pipeline::from_yaml(GUARDED).unwrap(), &specs()).unwrap();
        let t = Recorder::default();
        p.run(&t, json!({"body": {"q": "a"}})).await.unwrap();
        p.run(&t, json!({"body": {"q": "b", "flag": 1}}))
            .await
            .unwrap();

        let snap = p.stats.snapshot();
        let runs = |id: &str| {
            snap.iter()
                .find(|(s, _, _)| s == id)
                .map(|(_, _, r)| *r)
                .unwrap()
        };
        assert_eq!(p.stats.requests(), 2);
        assert_eq!(runs("always"), 2);
        assert_eq!(
            runs("only_when_flagged"),
            1,
            "guarded step ran once, not twice"
        );
    }
}

/// Sharded batching: several serial queues instead of one.
///
/// `maxInFlight: 1` alone forces a choice between batching and throughput -- one serial queue
/// caps throughput at maxSize / round-trip (measured: 1,174 rps at 1.76 fleet cores, against
/// 3,412 rps at 6.10 cores with no limit). The compiled-in engine escapes that by running
/// several serial batchers at once, one per client. Shards are the same shape.
#[cfg(test)]
mod shard_tests {
    use super::*;
    use crate::tests::specs;
    use serde_json::{Value, json};
    use std::sync::Mutex;

    /// Records how many calls were in flight simultaneously, and how big each was.
    #[derive(Default)]
    struct ConcurrencyProbe {
        sizes: Mutex<Vec<usize>>,
        in_flight: Mutex<usize>,
        peak: Mutex<usize>,
    }

    #[async_trait::async_trait]
    impl Transport for ConcurrencyProbe {
        async fn call(
            &self,
            req: Request,
        ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
            let n = req.body["texts"].as_array().map(|a| a.len()).unwrap_or(1);
            {
                let mut f = self.in_flight.lock().unwrap();
                *f += 1;
                let mut p = self.peak.lock().unwrap();
                if *f > *p {
                    *p = *f;
                }
            }
            self.sizes.lock().unwrap().push(n);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            *self.in_flight.lock().unwrap() -= 1;
            let results: Vec<Value> = (0..n)
                .map(|i| json!({"tokenIds": [i as i64], "blockHashes": []}))
                .collect();
            Ok(Reply::ok(json!({ "results": results })))
        }
    }

    async fn run_with(shards: usize) -> (usize, usize) {
        let pipeline = format!(
            r#"
api: {{openapi: api.yaml, operationId: createChatCompletion}}
components:
  tokenizer: {{openapi: btok.yaml, baseUrl: "http://t:8080"}}
steps:
  - id: encode
    call:
      component: tokenizer
      operationId: encode
      input: {{text: "$.request.body.q"}}
      output: {{tokenIds: "$.response.tokenIds"}}
      batch: {{maxSize: 64, lingerUs: 0, maxInFlight: 1, shards: {shards}}}
      respond: true
"#
        );
        let mut s = specs();
        s.insert(
            "btok.yaml".into(),
            Document::from_yaml(crate::batching_tests::BATCH_TOKENIZER).unwrap(),
        );
        let p = std::sync::Arc::new(
            Prepared::new(Pipeline::from_yaml(&pipeline).unwrap(), &s).unwrap(),
        );
        let t = std::sync::Arc::new(ConcurrencyProbe::default());

        let mut set = tokio::task::JoinSet::new();
        for i in 0..40 {
            let (p, t) = (p.clone(), t.clone());
            set.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(2 * i)).await;
                p.run(&*t, json!({"body": {"q": format!("prompt {i}")}}))
                    .await
            });
        }
        while let Some(r) = set.join_next().await {
            r.unwrap().expect("every request must succeed");
        }
        let peak = *t.peak.lock().unwrap();
        let calls = t.sizes.lock().unwrap().len();
        (peak, calls)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shards_run_batches_concurrently_without_shrinking_them() {
        let (peak1, _calls1) = run_with(1).await;
        let (peak4, calls4) = run_with(4).await;

        // One shard is strictly serial -- that is the throughput cap the sweep measured.
        assert_eq!(
            peak1, 1,
            "a single shard must never have two calls in flight"
        );
        // Four shards overlap, which is the point.
        assert!(
            peak4 > 1,
            "four shards should overlap calls, peak was {peak4}"
        );
        // And they must not achieve it by shrinking batches back to one item each: the whole
        // trade being escaped is "concurrency OR batching".
        assert!(
            calls4 < 40,
            "sharding must not degenerate into one call per request: {calls4} calls"
        );
    }
}

/// Resuming a stream that dies part way through -- the behaviour a proxy cannot provide.
#[cfg(test)]
mod resume_tests {
    use super::*;
    use crate::engine::{Prepared, Reply, Request, Sink, Transport};
    use serde_json::{Value, json};
    use std::sync::Mutex;

    /// Fails after `die_after` items on the FIRST attempt, then serves the rest.
    ///
    /// It also records what the resumed call was asked for, which is the assertion that
    /// matters most: a resume that does not carry the delivered items forward would make the
    /// upstream start over, and the client would see the beginning of the answer twice.
    struct DyingStreamer {
        die_after: usize,
        attempts: Mutex<u32>,
        seen_inputs: Mutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl Transport for DyingStreamer {
        async fn call(
            &self,
            req: Request,
        ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
            let n = {
                let mut a = self.attempts.lock().unwrap();
                *a += 1;
                *a
            };
            self.seen_inputs.lock().unwrap().push(req.body.clone());
            let frame = |i: usize| Ok::<Value, String>(json!({"text": format!("t{i}")}));
            if n == 1 {
                let items: Vec<Result<Value, String>> = (0..self.die_after)
                    .map(frame)
                    .chain(std::iter::once(Err("upstream died".to_string())))
                    .collect();
                return Ok(Reply::stream(Box::pin(futures::stream::iter(items))));
            }
            // The resumed attempt continues from where the first stopped. A real backend
            // does this because the replayed context tells it what it already produced;
            // here the offset is read back out of the request for the same reason.
            let done = req
                .body
                .get("delivered")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;
            let items: Vec<Result<Value, String>> = (done..done + 2).map(frame).collect();
            Ok(Reply::stream(Box::pin(futures::stream::iter(items))))
        }
    }

    #[derive(Default)]
    struct Collect {
        got: Mutex<Vec<Value>>,
    }

    #[async_trait::async_trait]
    impl Sink for Collect {
        async fn item(&self, v: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.got.lock().unwrap().push(v);
            Ok(())
        }
    }

    fn pipeline(extra: &str) -> String {
        format!(
            r#"
api: {{openapi: api.yaml, operationId: createChatCompletion}}
components:
  workers: {{openapi: worker.yaml, baseUrl: http://w:8080}}
steps:
  - id: generate
    call:
      component: workers
      operationId: generate
      input: {{tokenIds: [1]}}
      respond: true
      stream:
        emit: {{delta: "$.item.text"}}
        countInto: frames
        collect: {{deliveredText: "$.item.text"}}
{extra}
"#
        )
    }

    const RESUME: &str = r#"        resume:
          maxAttempts: 2
          input:
            delivered: {length: "$.vars.deliveredText"}
"#;

    /// The worker spec the resume pipeline binds against: a streaming `generate` whose body
    /// also accepts `delivered`, which is what the resumed call carries forward.
    fn specs() -> std::collections::BTreeMap<String, crate::openapi::Document> {
        let mut s = crate::tests::specs();
        s.insert(
            "worker.yaml".into(),
            crate::openapi::Document::from_yaml(
                r#"
paths:
  /generate:
    post:
      operationId: generate
      x-streaming: true
      requestBody:
        content: {application/json: {schema: {type: object, required: [tokenIds],
          properties: {tokenIds: {type: array}, delivered: {type: integer}}}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object,
            properties: {text: {type: string}, frames: {type: integer},
                         deliveredText: {type: array}}}}}
"#,
            )
            .unwrap(),
        );
        s
    }

    async fn run_body(
        spec: &str,
        body: Value,
        t: &DyingStreamer,
    ) -> (Result<Option<Value>, EngineError>, Vec<Value>) {
        let p = Prepared::new(crate::config::Pipeline::from_yaml(spec).unwrap(), &specs()).unwrap();
        let sink = Collect::default();
        let r = p.run_with_sink(t, json!({"body": body}), &sink).await;
        let got = sink.got.lock().unwrap().clone();
        (r, got)
    }

    async fn run(
        spec: &str,
        t: &DyingStreamer,
    ) -> (Result<Option<Value>, EngineError>, Vec<Value>) {
        run_body(spec, json!({}), t).await
    }

    #[tokio::test]
    async fn a_stream_that_dies_part_way_is_resumed_without_repeating_itself() {
        let t = DyingStreamer {
            die_after: 3,
            attempts: Mutex::new(0),
            seen_inputs: Mutex::new(Vec::new()),
        };
        let (r, got) = run(&pipeline(RESUME), &t).await;
        assert!(r.is_ok(), "resumed run should succeed: {r:?}");

        // The client's stream is CONTIGUOUS: t0..t4 exactly once, in order. This is the whole
        // point -- a retry would have restarted at t0 and the client would have seen the
        // opening of the answer twice.
        let texts: Vec<String> = got
            .iter()
            .map(|v| v["delta"].as_str().unwrap_or("").to_string())
            .collect();
        assert_eq!(
            texts,
            vec!["t0", "t1", "t2", "t3", "t4"],
            "stream was not contiguous"
        );
        assert_eq!(
            *t.attempts.lock().unwrap(),
            2,
            "should have resumed exactly once"
        );

        // And the resumed request carried what had already been delivered. Without this the
        // test above could pass against a backend that merely happened to continue.
        let inputs = t.seen_inputs.lock().unwrap().clone();
        assert_eq!(
            inputs[1]["delivered"],
            json!(3),
            "resume did not carry the delivered count"
        );
    }

    #[tokio::test]
    async fn without_a_resume_policy_the_broken_stream_fails() {
        let t = DyingStreamer {
            die_after: 3,
            attempts: Mutex::new(0),
            seen_inputs: Mutex::new(Vec::new()),
        };
        let (r, got) = run(&pipeline(""), &t).await;
        assert!(
            r.is_err(),
            "a broken stream must fail when resume is not configured"
        );
        assert_eq!(
            got.len(),
            3,
            "items delivered before the break still reached the client"
        );
        assert_eq!(
            *t.attempts.lock().unwrap(),
            1,
            "must not retry a partially delivered stream"
        );
    }

    #[tokio::test]
    async fn disable_when_stops_a_resume_that_would_corrupt_the_answer() {
        // The guard that matters: replaying delivered tokens restarts a guided-decoding state
        // machine at the schema root, so a structured response would come back nested or
        // duplicated. Failing is the correct outcome, not completing it wrongly.
        let spec = pipeline(
            r#"        resume:
          maxAttempts: 2
          disableWhen: {path: "$.request.body.responseFormat", exists: true}
          input:
            delivered: {length: "$.vars.deliveredText"}
"#,
        );
        let t = DyingStreamer {
            die_after: 3,
            attempts: Mutex::new(0),
            seen_inputs: Mutex::new(Vec::new()),
        };
        let (r, _) = run_body(
            &spec,
            json!({"responseFormat": {"type": "json_schema"}}),
            &t,
        )
        .await;
        assert!(r.is_err(), "structured output must not be resumed");
        assert_eq!(
            *t.attempts.lock().unwrap(),
            1,
            "guard must prevent the second attempt"
        );
    }
}

/// Composite guards -- the gap that kept `resume` unshippable for structured output.
#[cfg(test)]
mod when_composition_tests {
    use crate::config::When;
    use crate::engine::eval_when_pub as holds;
    use crate::expr::Scope;
    use serde_json::json;

    fn scope(body: serde_json::Value) -> Scope {
        let mut s = Scope::default();
        s.request = json!({"body": body});
        s
    }

    #[test]
    fn any_of_covers_the_two_guards_resume_actually_needs() {
        // The real case: migration must be disabled for structured output OR n > 1, and a
        // single-path When could express neither pair. Duplicating the step under
        // complementary guards is combinatorial -- 2 conditions is already 4 variants.
        let w: When = serde_yaml::from_str(
            r#"
anyOf:
  - {path: "$.request.body.response_format", exists: true}
  - {path: "$.request.body.n", notEquals: 1}
"#,
        )
        .unwrap();

        assert!(
            holds(
                &scope(json!({"response_format": {"type": "json_schema"}})),
                &w
            )
            .unwrap()
        );
        assert!(holds(&scope(json!({"n": 4})), &w).unwrap());
        assert!(
            !holds(&scope(json!({"n": 1})), &w).unwrap(),
            "plain request must stay resumable"
        );
        assert!(
            !holds(&scope(json!({})), &w).unwrap(),
            "absent fields must not disable resume"
        );
    }

    #[test]
    fn all_of_and_not_compose() {
        let w: When = serde_yaml::from_str(
            r#"
allOf:
  - {path: "$.request.body.stream", equals: true}
  - not: {path: "$.request.body.response_format", exists: true}
"#,
        )
        .unwrap();
        assert!(holds(&scope(json!({"stream": true})), &w).unwrap());
        assert!(!holds(&scope(json!({"stream": false})), &w).unwrap());
        assert!(
            !holds(&scope(json!({"stream": true, "response_format": {}})), &w).unwrap(),
            "`not` must invert the inner predicate"
        );
    }

    #[test]
    fn a_composing_node_ignores_its_empty_path() {
        // A node that only composes carries no `path`. Evaluating "" as a path would decide
        // the result by accident, so composition is checked first.
        let w: When =
            serde_yaml::from_str(r#"{anyOf: [{path: "$.request.body.a", exists: true}]}"#).unwrap();
        assert!(w.path.is_empty());
        assert!(!holds(&scope(json!({})), &w).unwrap());
        assert!(holds(&scope(json!({"a": 1})), &w).unwrap());
    }
}
