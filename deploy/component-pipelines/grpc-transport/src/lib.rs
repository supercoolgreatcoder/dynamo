// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! A gRPC transport for `pipeline-core`, driven by protobuf descriptors.
//!
//! The orchestration core speaks OpenAPI operations and JSON values. Our components speak
//! gRPC: the tokenizer and the workers are tonic services, only the selector is HTTP. A
//! transport therefore has to bridge the two, and the interesting question is whether it
//! can do so **without either side learning about the other**.
//!
//! It can, because the binding is declared in the spec rather than compiled in:
//!
//! ```yaml
//! /encode:
//!   post:
//!     operationId: encode
//!     x-grpc: {service: tokenizer.Tokenizer, method: Encode}
//! ```
//!
//! The core passes every `x-` extension through untouched (it has no idea what `x-grpc`
//! means); this transport reads it, looks the method up in a `FileDescriptorSet`, converts
//! the JSON body into a `DynamicMessage` and back. Adding a third protocol means another
//! transport and another extension, not a change to the core.
//!
//! Using descriptors rather than generated stubs is what keeps it generic: a hand-written
//! adapter per service would work and would be faster to write, but it would reintroduce
//! exactly the compiled-in knowledge this design exists to remove -- a new component would
//! mean new gateway code.

use base64::Engine as _;
use bytes::{Buf, BufMut};
use dynamo_generic_pipeline::{Payload, Reply, Request, Transport};
use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MessageDescriptor, MethodDescriptor, SerializeOptions,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
const GRPC_STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const GRPC_CONNECTION_WINDOW_BYTES: u32 = 16 * 1024 * 1024;
const MAX_GRPC_CHANNELS_PER_ENDPOINT: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("operation has no `x-grpc` extension; this transport cannot route it")]
    NoBinding,
    #[error("`x-grpc` must supply both `service` and `method`")]
    BadBinding,
    #[error("method `{0}/{1}` not found in the descriptor pool")]
    NoSuchMethod(String, String),
    #[error("building request message: {0}")]
    Encode(String),
    #[error("decoding response message: {0}")]
    Decode(String),
    #[error("transport: {0}")]
    Transport(String),
}

/// What `x-grpc` says.
#[derive(Debug)]
struct Binding {
    service: String,
    method: String,
}

fn binding(req: &Request) -> Result<Binding, GrpcError> {
    let v = req.extensions.get("x-grpc").ok_or(GrpcError::NoBinding)?;
    let service = v
        .get("service")
        .and_then(Value::as_str)
        .ok_or(GrpcError::BadBinding)?;
    let method = v
        .get("method")
        .and_then(Value::as_str)
        .ok_or(GrpcError::BadBinding)?;
    Ok(Binding {
        service: service.to_string(),
        method: method.to_string(),
    })
}

/// Convert structured JSON values bound to protobuf `bytes` fields into their
/// protobuf-JSON base64 representation. This lets a graph bind an inbound JSON
/// object directly to a canonical `*_json` envelope without teaching the graph
/// runtime anything about protobuf or Dynamo.
fn encode_structured_bytes(
    value: &mut Value,
    descriptor: &MessageDescriptor,
) -> Result<(), GrpcError> {
    let Some(object) = value.as_object_mut() else {
        return Ok(());
    };
    for field in descriptor.fields() {
        let key = if object.contains_key(field.name()) {
            field.name()
        } else {
            field.json_name()
        };
        let Some(field_value) = object.get_mut(key) else {
            continue;
        };
        match field.kind() {
            Kind::Bytes if !field_value.is_string() => {
                let bytes = serde_json::to_vec(field_value)
                    .map_err(|error| GrpcError::Encode(error.to_string()))?;
                *field_value =
                    Value::String(base64::engine::general_purpose::STANDARD.encode(bytes));
            }
            Kind::Message(nested) if field.is_list() => {
                if let Some(items) = field_value.as_array_mut() {
                    for item in items {
                        encode_structured_bytes(item, &nested)?;
                    }
                }
            }
            Kind::Message(nested) => encode_structured_bytes(field_value, &nested)?,
            _ => {}
        }
    }
    Ok(())
}

/// Decode protobuf `bytes` fields containing canonical JSON when the operation
/// explicitly opts in with `x-grpc-json-bytes: true`.
fn decode_structured_bytes(value: &mut Value, descriptor: &MessageDescriptor) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    for field in descriptor.fields() {
        let key = if object.contains_key(field.name()) {
            field.name()
        } else {
            field.json_name()
        };
        let Some(field_value) = object.get_mut(key) else {
            continue;
        };
        match field.kind() {
            Kind::Bytes => {
                let Some(encoded) = field_value.as_str() else {
                    continue;
                };
                let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
                    continue;
                };
                if let Ok(decoded) = serde_json::from_slice(&bytes) {
                    *field_value = decoded;
                }
            }
            Kind::Message(nested) if field.is_list() => {
                if let Some(items) = field_value.as_array_mut() {
                    for item in items {
                        decode_structured_bytes(item, &nested);
                    }
                }
            }
            Kind::Message(nested) => decode_structured_bytes(field_value, &nested),
            _ => {}
        }
    }
}

fn response_json(message: &DynamicMessage) -> Result<Value, GrpcError> {
    let options = SerializeOptions::new()
        .use_proto_field_name(true)
        .skip_default_fields(false)
        .stringify_64_bit_integers(false);
    message
        .serialize_with_options(serde_json::value::Serializer, &options)
        .map_err(|error| GrpcError::Decode(error.to_string()))
}

fn transform_response(
    mut value: Value,
    descriptor: &MessageDescriptor,
    decode_json_bytes: bool,
    response_body: Option<&str>,
) -> Result<Value, GrpcError> {
    if decode_json_bytes {
        decode_structured_bytes(&mut value, descriptor);
    }
    if let Some(field) = response_body {
        value = value
            .get(field)
            .cloned()
            .ok_or_else(|| GrpcError::Decode(format!("response has no `{field}` field")))?;
    }
    Ok(value)
}

/// Encodes/decodes `DynamicMessage` so tonic can carry a message shape known only at run
/// time. tonic's generated code uses a `ProstCodec` bound to a concrete type; this is the
/// same contract with the descriptor supplied at construction.
#[derive(Clone)]
struct DynCodec;

struct DynEncoder;
struct DynDecoder;

impl tonic::codec::Encoder for DynEncoder {
    type Item = DynamicMessage;
    type Error = tonic::Status;
    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        let mut buf = Vec::with_capacity(item.encoded_len());
        item.encode(&mut buf)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        dst.put_slice(&buf);
        Ok(())
    }
}

impl tonic::codec::Decoder for DynDecoder {
    type Item = bytes::Bytes;
    type Error = tonic::Status;
    fn decode(
        &mut self,
        src: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<Self::Item>, Self::Error> {
        let len = src.remaining();
        let bytes = src.copy_to_bytes(len);
        Ok(Some(bytes))
    }
}

impl tonic::codec::Codec for DynCodec {
    type Encode = DynamicMessage;
    type Decode = bytes::Bytes;
    type Encoder = DynEncoder;
    type Decoder = DynDecoder;
    fn encoder(&mut self) -> Self::Encoder {
        DynEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        DynDecoder
    }
}

/// Extract a singular protobuf `bytes` field without materializing a reflective message.
///
/// Canonical worker responses declare `x-grpc-json-bytes` together with a response-body
/// field. That field contains the complete OpenAI chunk, while the remaining envelope fields
/// are discarded by the graph. Mooncake produces roughly 171 chunks per request, so building
/// a `DynamicMessage`, serializing every envelope field to JSON, base64-decoding the bytes,
/// and finally selecting one field was the dominant generic-path cost.
///
/// This parser handles all protobuf wire types needed to skip unrelated fields and declines
/// groups. The caller retains the descriptor-driven fallback, so unsupported or malformed
/// input cannot silently acquire different semantics.
fn decode_json_bytes_response_body(
    descriptor: &MessageDescriptor,
    bytes: &[u8],
    response_body: &str,
) -> Option<Value> {
    let field = descriptor.get_field_by_name(response_body).or_else(|| {
        descriptor
            .fields()
            .find(|field| field.json_name() == response_body)
    })?;
    if field.is_list() || !matches!(field.kind(), Kind::Bytes) {
        return None;
    }

    fn varint(bytes: &[u8], offset: &mut usize) -> Option<u64> {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = *bytes.get(*offset)?;
            *offset += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }

    let mut offset = 0usize;
    let mut body = None;
    while offset < bytes.len() {
        let key = varint(bytes, &mut offset)?;
        let number = (key >> 3) as u32;
        match (key & 7) as u8 {
            0 => {
                varint(bytes, &mut offset)?;
            }
            1 => offset = offset.checked_add(8)?,
            2 => {
                let len = usize::try_from(varint(bytes, &mut offset)?).ok()?;
                let end = offset.checked_add(len)?;
                let value = bytes.get(offset..end)?;
                if number == field.number() {
                    body = Some(value);
                }
                offset = end;
            }
            5 => offset = offset.checked_add(4)?,
            _ => return None,
        }
        if offset > bytes.len() {
            return None;
        }
    }

    let body = body.unwrap_or_default();
    Some(
        serde_json::from_slice(body).unwrap_or_else(|_| {
            Value::String(base64::engine::general_purpose::STANDARD.encode(body))
        }),
    )
}

fn decode_response(
    descriptor: &MessageDescriptor,
    bytes: &[u8],
    decode_json_bytes: bool,
    response_body: Option<&str>,
) -> Result<Value, GrpcError> {
    if decode_json_bytes {
        if let Some(response_body) = response_body {
            if let Some(value) = decode_json_bytes_response_body(descriptor, bytes, response_body) {
                return Ok(value);
            }
        }
    }
    let message = DynamicMessage::decode(descriptor.clone(), bytes)
        .map_err(|error| GrpcError::Decode(error.to_string()))?;
    transform_response(
        response_json(&message)?,
        descriptor,
        decode_json_bytes,
        response_body,
    )
}

/// gRPC transport over a descriptor pool.
///
/// Channels are cached per endpoint and cloned per call: tonic channels are cheap to clone
/// and multiplex over one HTTP/2 connection, whereas connecting per request costs a TCP and
/// h2 handshake every time -- a mistake this project has already paid for twice (the
/// selector hop and the detokenizer sidecar), so it is worth stating rather than assuming.
pub struct GrpcTransport {
    pool: DescriptorPool,
    channels: Arc<Mutex<HashMap<String, Arc<ChannelPool>>>>,
    channels_per_endpoint: usize,
}

struct ChannelPool {
    channels: Vec<tonic::transport::Channel>,
    next: AtomicUsize,
}

impl ChannelPool {
    fn next(&self) -> tonic::transport::Channel {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[index].clone()
    }
}

impl GrpcTransport {
    /// `descriptor_set` is a serialized `FileDescriptorSet` (`protoc --descriptor_set_out`).
    pub fn new(descriptor_set: &[u8]) -> Result<Self, GrpcError> {
        let channels_per_endpoint = std::env::var("DYN_GRPC_CHANNELS_PER_ENDPOINT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1usize);
        Self::with_connections(descriptor_set, channels_per_endpoint)
    }

    /// Build a transport with an explicit number of HTTP/2 connections per endpoint.
    ///
    /// Gateway hosts use this when their configuration owns connection-pool sizing;
    /// [`Self::new`] retains the environment-variable interface for standalone hosts.
    pub fn with_connections(
        descriptor_set: &[u8],
        channels_per_endpoint: usize,
    ) -> Result<Self, GrpcError> {
        let pool = DescriptorPool::decode(descriptor_set)
            .map_err(|e| GrpcError::Decode(format!("descriptor pool: {e}")))?;
        Ok(Self {
            pool,
            channels: Arc::new(Mutex::new(HashMap::new())),
            channels_per_endpoint: channels_per_endpoint.clamp(1, MAX_GRPC_CHANNELS_PER_ENDPOINT),
        })
    }

    fn method(&self, b: &Binding) -> Result<MethodDescriptor, GrpcError> {
        let svc = self
            .pool
            .get_service_by_name(&b.service)
            .ok_or_else(|| GrpcError::NoSuchMethod(b.service.clone(), b.method.clone()))?;
        let found = svc.methods().find(|m| m.name() == b.method);
        found.ok_or_else(|| GrpcError::NoSuchMethod(b.service.clone(), b.method.clone()))
    }

    /// Validate a configured binding at gateway startup instead of discovering a
    /// descriptor mismatch on the first request.
    pub fn validate_binding(&self, service: &str, method: &str) -> Result<(), GrpcError> {
        self.method(&Binding {
            service: service.to_string(),
            method: method.to_string(),
        })?;
        Ok(())
    }

    /// Whether the method bound by this graph request streams responses.
    pub fn is_server_streaming(&self, request: &Request) -> Result<bool, GrpcError> {
        Ok(self.method(&binding(request)?)?.is_server_streaming())
    }

    /// HTTP/2 `:path` for the method bound by this graph request.
    pub fn method_path(&self, request: &Request) -> Result<String, GrpcError> {
        let method = self.method(&binding(request)?)?;
        Ok(format!(
            "/{}/{}",
            method.parent_service().full_name(),
            method.name()
        ))
    }

    /// Encode a graph request into the bound method's protobuf wire payload.
    /// Envoy-owned transports use this to retain the exact same descriptor codec
    /// as the independent tonic path.
    pub fn encode_input(&self, request: &Request) -> Result<Vec<u8>, GrpcError> {
        let method = self.method(&binding(request)?)?;
        let mut body = request.body.clone();
        encode_structured_bytes(&mut body, &method.input())?;
        let message = DynamicMessage::deserialize(method.input(), &body)
            .map_err(|error| GrpcError::Encode(error.to_string()))?;
        let mut bytes = Vec::with_capacity(message.encoded_len());
        message
            .encode(&mut bytes)
            .map_err(|error| GrpcError::Encode(error.to_string()))?;
        Ok(bytes)
    }

    /// Decode one protobuf response frame using the graph request's declared
    /// JSON-bytes and response-body transformations.
    pub fn decode_output(&self, request: &Request, bytes: &[u8]) -> Result<Value, GrpcError> {
        let method = self.method(&binding(request)?)?;
        let decode_json_bytes = request
            .extensions
            .get("x-grpc-json-bytes")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let response_body = request
            .extensions
            .get("x-grpc-response-body")
            .and_then(Value::as_str);
        decode_response(&method.output(), bytes, decode_json_bytes, response_body)
    }

    async fn channel(&self, url: &str) -> Result<tonic::transport::Channel, GrpcError> {
        let mut pools = self.channels.lock().map_err(|error| {
            GrpcError::Transport(format!("channel pool lock poisoned: {error}"))
        })?;
        if let Some(pool) = pools.get(url) {
            return Ok(pool.next());
        }
        let ep = tonic::transport::Endpoint::from_shared(url.to_string())
            .map_err(|e| GrpcError::Transport(e.to_string()))?
            .tcp_nodelay(true)
            .initial_stream_window_size(Some(GRPC_STREAM_WINDOW_BYTES))
            .initial_connection_window_size(Some(GRPC_CONNECTION_WINDOW_BYTES));
        let pool = Arc::new(ChannelPool {
            channels: (0..self.channels_per_endpoint)
                .map(|_| ep.clone().connect_lazy())
                .collect(),
            next: AtomicUsize::new(0),
        });
        pools.insert(url.to_string(), pool.clone());
        Ok(pool.next())
    }
}

/// The URL the core built is `http://host:port/<openapi path>`; gRPC needs
/// `http://host:port` plus `/<package>.<Service>/<Method>` from the descriptor. Splitting
/// here rather than making the core emit gRPC paths keeps the core protocol-agnostic.
fn split_authority(url: &str) -> (String, ()) {
    let after_scheme = url.find("://").map(|i| i + 3).unwrap_or(0);
    match url[after_scheme..].find('/') {
        Some(i) => (url[..after_scheme + i].to_string(), ()),
        None => (url.to_string(), ()),
    }
}

#[async_trait::async_trait]
impl Transport for GrpcTransport {
    async fn call(&self, req: Request) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
        let b = binding(&req)?;
        let method = self.method(&b)?;
        let (authority, _) = split_authority(&req.url);
        let channel = self.channel(&authority).await?;

        // JSON -> DynamicMessage, driven entirely by the descriptor.
        let decode_json_bytes = req
            .extensions
            .get("x-grpc-json-bytes")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let response_body = req
            .extensions
            .get("x-grpc-response-body")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut body = req.body;
        encode_structured_bytes(&mut body, &method.input())?;
        let msg = DynamicMessage::deserialize(method.input(), &body)
            .map_err(|e| GrpcError::Encode(e.to_string()))?;

        let codec = DynCodec;
        let parent = method.parent_service();
        let full = format!("/{}/{}", parent.full_name(), method.name());
        let path = http::uri::PathAndQuery::from_maybe_shared(full)
            .map_err(|e| GrpcError::Transport(e.to_string()))?;

        let mut client = tonic::client::Grpc::new(channel);
        client
            .ready()
            .await
            .map_err(|e| GrpcError::Transport(e.to_string()))?;

        if method.is_server_streaming() {
            let resp = client
                .server_streaming(tonic::Request::new(msg), path, codec)
                .await
                .map_err(|e| GrpcError::Transport(e.to_string()))?;
            let inner = resp.into_inner();
            // Each streamed message becomes one item, converted to JSON. The core's
            // per-item projection then decides what is emitted -- the transport never
            // decides the public stream shape.
            let output = method.output();
            let stream = futures::StreamExt::map(inner, move |m| {
                m.map_err(|e| e.to_string()).and_then(|bytes| {
                    decode_response(&output, &bytes, decode_json_bytes, response_body.as_deref())
                        .map_err(|e| e.to_string())
                })
            });
            Ok(Reply {
                status: 200,
                payload: Payload::Stream(Box::pin(stream)),
            })
        } else {
            let resp = client
                .unary(tonic::Request::new(msg), path, codec)
                .await
                .map_err(|e| GrpcError::Transport(e.to_string()))?;
            let bytes = resp.into_inner();
            let v = decode_response(
                &method.output(),
                &bytes,
                decode_json_bytes,
                response_body.as_deref(),
            )?;
            Ok(Reply::ok(v))
        }
    }

    async fn sleep_ms(&self, ms: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn req(ext: Option<Value>) -> Request {
        let mut m: BTreeMap<String, Value> = BTreeMap::new();
        if let Some(e) = ext {
            m.insert("x-grpc".into(), e);
        }
        Request {
            method: "POST",
            url: "http://tok-svc:8090/encode".into(),
            headers: Default::default(),
            body: serde_json::json!({}),
            streaming: false,
            timeout_ms: None,
            extensions: Arc::new(m),
        }
    }

    #[test]
    fn reads_the_grpc_binding_from_the_operation_extension() {
        let b = binding(&req(Some(
            serde_json::json!({"service": "tokenizer.Tokenizer", "method": "Encode"}),
        )))
        .unwrap();
        assert_eq!(
            (b.service.as_str(), b.method.as_str()),
            ("tokenizer.Tokenizer", "Encode")
        );
    }

    #[test]
    fn an_operation_without_x_grpc_is_rejected_rather_than_guessed() {
        assert!(matches!(binding(&req(None)), Err(GrpcError::NoBinding)));
    }

    #[test]
    fn an_incomplete_binding_is_rejected() {
        let e = binding(&req(Some(serde_json::json!({"service": "s"})))).unwrap_err();
        assert!(matches!(e, GrpcError::BadBinding));
    }

    #[test]
    fn strips_the_openapi_path_to_an_authority() {
        // The core builds an HTTP-shaped URL; gRPC needs authority plus a descriptor path.
        assert_eq!(
            split_authority("http://tok-svc:8090/encode").0,
            "http://tok-svc:8090"
        );
        assert_eq!(split_authority("http://w:8081").0, "http://w:8081");
        assert_eq!(
            split_authority("http://kvworker3:8081/generate_tokens").0,
            "http://kvworker3:8081"
        );
    }

    #[test]
    fn resolves_every_dynamo_component_service_from_the_shared_descriptor() {
        let transport =
            GrpcTransport::new(dynamo_component_facades::proto::FILE_DESCRIPTOR_SET).unwrap();
        for (service, method) in [
            ("dynamo.components.v1.Preprocessor", "PrepareBatch"),
            ("dynamo.components.v1.Selector", "SelectBatch"),
            ("dynamo.components.v1.WorkerBridge", "Process"),
            ("dynamo.components.v1.ChatWorkerBridge", "Generate"),
            ("dynamo.components.v1.Postprocessor", "Process"),
        ] {
            transport.validate_binding(service, method).unwrap();
        }
    }

    #[test]
    fn structured_json_round_trips_through_a_canonical_bytes_field() {
        let pool =
            DescriptorPool::decode(dynamo_component_facades::proto::FILE_DESCRIPTOR_SET).unwrap();
        let descriptor = pool
            .get_message_by_name("dynamo.components.v1.PreprocessItem")
            .unwrap();
        let original = serde_json::json!({
            "item_id": "request-1",
            "openai_request_json": {
                "model": "test-model",
                "messages": [{"role": "user", "content": "hello"}]
            }
        });
        let mut encoded = original.clone();
        encode_structured_bytes(&mut encoded, &descriptor).unwrap();
        assert!(encoded["openai_request_json"].is_string());
        let wire_json = encoded.to_string();
        let mut de = serde_json::Deserializer::from_str(&wire_json);
        DynamicMessage::deserialize(descriptor.clone(), &mut de).unwrap();

        decode_structured_bytes(&mut encoded, &descriptor);
        assert_eq!(
            encoded["openai_request_json"],
            original["openai_request_json"]
        );
    }

    #[test]
    fn json_response_body_decodes_directly_from_protobuf_wire_bytes() {
        let transport =
            GrpcTransport::new(dynamo_component_facades::proto::FILE_DESCRIPTOR_SET).unwrap();
        let expected = serde_json::json!({
            "id": "request-1",
            "choices": [{"index": 0, "delta": {"content": "hello"}}]
        });
        fn put_varint(mut value: usize, output: &mut Vec<u8>) {
            loop {
                if value < 0x80 {
                    output.push(value as u8);
                    return;
                }
                output.push(((value as u8) & 0x7f) | 0x80);
                value >>= 7;
            }
        }

        let mut wire = Vec::new();
        let request_id = b"request-1";
        wire.push(0x0a);
        put_varint(request_id.len(), &mut wire);
        wire.extend_from_slice(request_id);
        let chunk = serde_json::to_vec(&expected).unwrap();
        wire.push(0x12);
        put_varint(chunk.len(), &mut wire);
        wire.extend_from_slice(&chunk);

        let mut extensions = BTreeMap::new();
        extensions.insert(
            "x-grpc".to_string(),
            serde_json::json!({
                "service": "dynamo.components.v1.ChatWorkerBridge",
                "method": "Generate"
            }),
        );
        extensions.insert("x-grpc-json-bytes".to_string(), Value::Bool(true));
        extensions.insert(
            "x-grpc-response-body".to_string(),
            Value::String("openai_chunk_json".to_string()),
        );
        let request = Request {
            method: "POST",
            url: "http://worker:50051/generate".to_string(),
            headers: Default::default(),
            body: serde_json::json!({}),
            streaming: true,
            timeout_ms: None,
            extensions: Arc::new(extensions),
        };

        assert_eq!(transport.decode_output(&request, &wire).unwrap(), expected);
    }
}
