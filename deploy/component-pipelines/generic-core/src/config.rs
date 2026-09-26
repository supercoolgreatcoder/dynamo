// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Generic pipeline configuration.
//!
//! Nothing here names a domain concept. There is no `Tokenize`, no `Select`, no `fleet`,
//! no `prefill`/`decode`. A step is "call operation X on component Y, binding these inputs
//! and capturing these outputs", and that is the whole vocabulary.
//!
//! The LLM pipeline the statically-linked core implements natively is expressed in this
//! vocabulary in `examples/kv-routing-agg.yaml`; the core cannot tell it apart from an
//! image-resize pipeline or an order-fulfilment pipeline.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A whole pipeline: the API it serves, the components it may call, and the steps.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Pipeline {
    /// The externally-facing API. Its OpenAPI document defines the request/response shape
    /// the pipeline is bound to, so the core validates bindings instead of trusting them.
    pub api: ApiBinding,
    /// Constants seeded into `$.vars` before the first step.
    ///
    /// Gives a deployment one place to state values several steps share -- a block size, a
    /// group name, a model id. The alternative is repeating a literal at every use, which
    /// is how the same value ends up configured twice and drifting.
    #[serde(default)]
    pub vars: BTreeMap<String, serde_json::Value>,
    /// Callable components, keyed by the name steps refer to.
    pub components: BTreeMap<String, Component>,
    /// Executed in order. A step may fan out via `parallel`.
    pub steps: Vec<Step>,
}

/// Binds the pipeline to one operation of an externally-facing OpenAPI document.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ApiBinding {
    /// Path to the OpenAPI document describing the API this pipeline serves.
    pub openapi: String,
    /// Which operation in that document.
    pub operation_id: String,
}

/// A callable dependency, described by its own OpenAPI document.
///
/// Describing components the same way as the external API is the point: an interface
/// between two of our own services is not a lesser contract than a public one, and the
/// recurring failures in this project came from contracts that were implied rather than
/// declared (a block size configured twice, a tokenizer identity assumed to match).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Component {
    pub openapi: String,
    /// Fixed endpoint. Mutually exclusive with `discovery`.
    #[serde(default)]
    pub base_url: Option<String>,
    /// Endpoint chosen at run time -- the target comes from a step binding, because some
    /// pipelines pick their callee (that is all "route to a worker" is, generically).
    #[serde(default)]
    pub discovery: Option<Discovery>,
}

/// How a component's concrete endpoint is found when it is not a fixed URL.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Discovery {
    /// Opaque to the core; the host environment resolves it (a k8s EndpointSlice, a
    /// service-mesh cluster, a static list).
    pub group: String,
}

/// One step: a call, or a fan-out over sub-pipelines.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Step {
    /// Identifies the step in errors and traces, and namespaces its outputs.
    pub id: String,
    /// Run this step only when the predicate holds.
    ///
    /// Without guards, every behavioural variant needs its own pipeline file -- which is
    /// how the statically-linked deployment ended up with separate aggregated and
    /// disaggregated configs that then drifted apart. A guard keeps one document.
    #[serde(default)]
    pub when: Option<When>,
    #[serde(flatten)]
    pub body: StepBody,
}

/// A minimal predicate over the scope. Deliberately not an expression language: it can
/// test a value, not compute one.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct When {
    /// Holds when EVERY listed predicate holds. `path` is then not required.
    ///
    /// Added because a single-path predicate could not express the guards this needs. Stream
    /// `resume` must be disabled for structured output AND for `n > 1`, and with one path per
    /// `When` the only way to cover both was to duplicate the step under complementary
    /// guards -- which is combinatorial in the number of conditions and is exactly how the
    /// aggregated and disaggregated configs drifted apart before they were unified.
    ///
    /// Still not an expression language: these compose predicates, they do not compute values.
    #[serde(default)]
    pub all_of: Vec<When>,
    /// Holds when ANY listed predicate holds.
    #[serde(default)]
    pub any_of: Vec<When>,
    /// Holds when the listed predicate does not.
    #[serde(default)]
    pub not: Option<Box<When>>,
    /// The path to test. Empty when this node only composes `allOf` / `anyOf` / `not`.
    #[serde(default)]
    pub path: String,
    /// Holds when the path resolves to exactly this value.
    #[serde(default)]
    pub equals: Option<serde_json::Value>,
    /// Holds when the path resolves at all (`true`) or does not (`false`).
    ///
    /// Beware with protobuf sources: the gRPC transport emits every declared field, so a
    /// field is always "present" and `exists` cannot distinguish empty from meaningful.
    /// `notEquals` is usually what a filter wants.
    #[serde(default)]
    pub exists: Option<bool>,
    /// Holds when the path resolves to anything other than this value.
    ///
    /// Added for exactly one real case: a worker's terminal chunk carries an empty `text`,
    /// and emitting it produced one extra frame per response -- a defect the gate missed
    /// because the gate counted only non-empty content.
    #[serde(default)]
    pub not_equals: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum StepBody {
    /// Invoke an operation on a component.
    #[serde(rename = "call")]
    Call(Call),
    /// Run sub-pipelines concurrently. Branches see the same scope and their outputs are
    /// merged; conflicting writes to one variable are a config error, caught at load.
    #[serde(rename = "parallel")]
    Parallel { branches: Vec<Vec<Step>> },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Call {
    /// Key into `Pipeline::components`.
    pub component: String,
    /// Operation within that component's OpenAPI document.
    pub operation_id: String,
    /// Endpoint expression, required when the component uses `discovery`.
    #[serde(default)]
    pub target: Option<Expr>,
    /// Inputs, by operation parameter / body-field name.
    #[serde(default)]
    pub input: BTreeMap<String, Expr>,
    /// Outputs to capture into scope: variable name -> expression over the response.
    #[serde(default)]
    pub output: BTreeMap<String, Expr>,
    /// Fold concurrent invocations of this step into one call.
    #[serde(default)]
    pub batch: Option<BatchPolicy>,
    /// Emit this step's result as the pipeline's response.
    #[serde(default)]
    pub respond: bool,
    /// Shape the response to the pipeline's external API instead of forwarding the
    /// callee's body verbatim. Without this the external contract is whatever the last
    /// component happens to return, which makes the API a downstream implementation
    /// detail rather than a declared interface.
    #[serde(default)]
    pub respond_with: BTreeMap<String, Expr>,
    /// Per-step resilience. Generic policy, so it belongs in config rather than in each
    /// call site.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
    /// Success criteria and failure handling.
    #[serde(default)]
    pub errors: Option<ErrorPolicy>,
    /// How to handle a streaming response. Only meaningful when the operation declares
    /// `x-streaming: true`.
    #[serde(default)]
    pub stream: Option<StreamPolicy>,
    /// Repeat this call once per element of a runtime collection.
    #[serde(default)]
    pub for_each: Option<ForEach>,
}

/// What to emit per streamed item, and what to keep when the stream ends.
///
/// Without this a streaming step could only forward the callee's items verbatim, which
/// makes the public stream shape a downstream implementation detail -- the same problem
/// `respondWith` solves for unary replies.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamPolicy {
    /// Per-item projection. The item is addressable as `$.item`. Empty forwards verbatim.
    #[serde(default)]
    pub emit: BTreeMap<String, Expr>,
    /// Whether accepted items are forwarded to the public sink. An internal
    /// streaming step can collect a summary for later steps without exposing
    /// its intermediate protocol to the caller.
    #[serde(default = "default_true")]
    pub emit_to_client: bool,
    /// Skip items where this predicate does not hold -- e.g. a chunk carrying no text.
    #[serde(default)]
    pub emit_when: Option<When>,
    /// Variables to accumulate across the stream, for a trailing summary.
    /// `count` counts items; `collect` gathers a per-item expression into an array.
    #[serde(default)]
    pub count_into: Option<String>,
    #[serde(default)]
    pub collect: BTreeMap<String, Expr>,
    /// Re-issue the call and continue the stream when it fails PART WAY THROUGH.
    #[serde(default)]
    pub resume: Option<ResumePolicy>,
}

fn default_true() -> bool {
    true
}

/// Continue a broken stream on a new upstream, carrying what was already delivered.
///
/// This is the one resilience behaviour a proxy cannot provide. A gateway retry is only
/// possible before response headers commit; once one item has reached the client, retrying is
/// impossible for Envoy, for any other proxy, and for [`RetryPolicy`] above -- all of which
/// treat a call as a single atomic attempt. But a long stream is exactly where an upstream is
/// most likely to die, and restarting it from nothing is not an option either: the client has
/// already seen part of the answer.
///
/// So the stream is resumed rather than retried. What was already delivered is accumulated by
/// the existing [`StreamPolicy::collect`] and fed back into the next attempt's inputs, which
/// makes the new upstream continue rather than start over. Nothing is re-emitted downstream.
///
/// Ported from Dynamo's `lib/llm/src/migration.rs` (`RetryManager`), which does this for LLM
/// generation by replaying already-generated `token_ids` as context. The algorithm is kept and
/// the domain knowledge is not: this core does not know what a token is. `input` says how to
/// rebuild the request and `disableWhen` says when resuming would corrupt the answer, both in
/// the pipeline document.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResumePolicy {
    /// How many times the stream may be resumed. Bounded because a pathologically failing
    /// fleet would otherwise let one client request cycle forever.
    pub max_attempts: u32,
    /// Inputs to override on the resumed call, merged over the original `input`.
    ///
    /// Evaluated against a scope where this stream's `collect` variables are already bound, so
    /// an expression can refer to what has been delivered so far.
    #[serde(default)]
    pub input: BTreeMap<String, Expr>,
    /// Do not resume when this predicate holds.
    ///
    /// Not optional in practice. Dynamo disables migration for two cases that would produce a
    /// WRONG answer rather than a failed one, and both are stated in the pipeline document
    /// because both are domain facts:
    ///
    /// * guided decoding / structured output -- the backend builds its grammar state machine
    ///   fresh per request and advances it only on newly generated tokens, so replaying the
    ///   delivered ones as context restarts it at the schema root and yields duplicated or
    ///   nested JSON;
    /// * `n > 1` -- per-choice generation state is not transferable.
    #[serde(default)]
    pub disable_when: Option<When>,
}

/// Fan out one call over a collection resolved at run time.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ForEach {
    /// Path to an array in scope.
    pub items: String,
    /// Name the current element takes, addressable as `$.vars.<as>`.
    #[serde(rename = "as")]
    pub bind_as: String,
    /// Maximum concurrent invocations. 1 is sequential.
    #[serde(default = "default_parallelism")]
    pub max_concurrent: usize,
    /// Collect each invocation's captured outputs into this array variable.
    #[serde(default)]
    pub collect_into: Option<String>,
}

fn default_parallelism() -> usize {
    1
}

/// Retries are only safe for operations without side effects, so the policy is opt-in per
/// step and the core does not infer it.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff_ms: u64,
    /// Statuses worth another attempt. Empty means transport failures only.
    ///
    /// Retrying a 400 is pointless and retrying a 409 can be harmful, so "which failures
    /// are transient" has to be stated rather than assumed -- without it a policy can only
    /// retry everything or nothing.
    #[serde(default)]
    pub retry_on: Vec<u16>,
}

/// What counts as success, and what to do when a call does not succeed.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ErrorPolicy {
    /// Statuses treated as success. Defaults to any 2xx.
    #[serde(default)]
    pub expect_status: Vec<u16>,
    /// What to do once retries are exhausted.
    #[serde(default)]
    pub on_failure: OnFailure,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OnFailure {
    /// Abort the pipeline. The default, because silently continuing past a failed step
    /// produces a response assembled from missing data.
    #[default]
    Fail,
    /// Skip the step and carry on; its `output` bindings are not applied.
    Skip,
    /// Continue using this value as the step's response, so `output` bindings still apply.
    Fallback(serde_json::Value),
}

/// Cross-request batching for a call.
///
/// Generic dispatch policy, not per-service logic: the cost of a callout is dominated by
/// round trips, so folding concurrent invocations into one call is the lever. Whether an
/// operation *may* be batched is a property of the operation and is declared in its
/// OpenAPI document (`x-batch`), not guessed here -- batching an operation whose result
/// depends on the previous call is silently wrong.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BatchPolicy {
    #[serde(default = "default_batch_max")]
    pub max_size: usize,
    /// Wait this long for more work before dispatching. 0 takes only what is already
    /// queued, so batch size tracks arrival concurrency and adds no latency.
    ///
    /// Default 0 deliberately. A linger raises batch size substantially, which helps
    /// fixed-cost work and can be severely counterproductive for variable-cost work; the
    /// measured case is in `../detok-sidecar/README.md`.
    #[serde(default)]
    pub linger_us: u64,
    /// How many folded calls for this step may be in flight at once. 0 means unlimited.
    ///
    /// Set to 1, batch size becomes SELF-TUNING and needs no linger: while a call is in
    /// flight the next leader waits, so everything arriving meanwhile accumulates and is sent
    /// as one batch. Under load batches grow; when idle a batch is one request and latency is
    /// unchanged. That is the property a linger tries to buy by guessing a duration.
    ///
    /// Unlimited was the original behaviour and measured badly: the leader took the slot and
    /// dispatched immediately, so at 3,300 rps the gateway sent ~3,300 single-item RPCs a
    /// second where a serial batcher sent ~100 of ~32 items, and the tokenizer fleet burned
    /// 6.58 cores against 1.77 for the same BPE work.
    ///
    /// Why a linger cannot fix that: at 3,300 rps a 1,000 us linger accumulates ~3.3
    /// requests. Reaching 32 would need ~10 ms of added latency on every request. Waiting for
    /// the in-flight call costs nothing extra, because that time is already being spent.
    #[serde(default)]
    pub max_in_flight: usize,
    /// Independent batch queues for this step. Default 1.
    ///
    /// `maxInFlight: 1` alone forces a choice between batching and throughput, because one
    /// serial queue caps throughput at maxSize / round-trip. Measured at ISL 4000: 1,174 rps
    /// at 1.76 fleet cores, against 3,412 rps at 6.10 cores unlimited. Neither is the
    /// compiled-in arm, which gets 3,647 rps at 1.56 cores.
    ///
    /// It does that by running SEVERAL serial batchers at once -- one per tokenizer client,
    /// round-robined. `shards: N` with `maxInFlight: 1` is the same shape: N queues, each
    /// batching properly, N calls in flight. Pair it with a transport that opens at least N
    /// connections, or the calls multiplex back onto one and land on one replica anyway.
    #[serde(default = "default_shards")]
    pub shards: usize,
}

fn default_shards() -> usize {
    1
}

fn default_batch_max() -> usize {
    32
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            max_size: default_batch_max(),
            linger_us: 0,
            max_in_flight: 0,
            shards: 1,
        }
    }
}

/// An expression: either a literal or a path into the execution scope.
///
/// Paths are `$.<root>.<segments>` where root is `request`, `response`, or `vars`.
/// Deliberately not a general expression language -- anything requiring computation
/// belongs in a component, not in the orchestrator, which is what keeps the core
/// stateless and its cost per step bounded.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Expr {
    Path(String),
    /// `{length: <expr>}` -- the number of elements in an array (or characters in a string).
    ///
    /// The ONLY computation in the language, and it was added because a real contract
    /// demanded it: the KV selector requires `isl_tokens`, a token COUNT, and rejects the
    /// request without it. The alternatives were worse -- send the raw token ids instead of
    /// hashes, which defeats the point of hash routing (O(blocks) on the wire, not
    /// O(tokens)), or bolt a general expression language onto an orchestrator.
    ///
    /// It stays defensible because it is O(1) on a parsed JSON array, total (no failure
    /// mode beyond a type mismatch), and closed -- `length` is the whole set. Anything
    /// needing more belongs in a component.
    Length {
        length: Box<Expr>,
    },
    /// `{optional: <expr>}` -- return null only when a referenced field is absent.
    /// Other expression errors still fail the pipeline.
    Optional {
        optional: Box<Expr>,
    },
    Literal(serde_json::Value),
}

impl Expr {
    pub fn as_path(&self) -> Option<&str> {
        match self {
            Expr::Path(s) if s.starts_with("$.") => Some(s),
            _ => None,
        }
    }
}

impl Pipeline {
    pub fn from_yaml(s: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(s)
    }

    /// Walks every step, including nested branches, in execution order.
    pub fn walk(&self) -> Vec<&Step> {
        fn rec<'a>(steps: &'a [Step], out: &mut Vec<&'a Step>) {
            for s in steps {
                out.push(s);
                if let StepBody::Parallel { branches } = &s.body {
                    for b in branches {
                        rec(b, out);
                    }
                }
            }
        }
        let mut out = Vec::new();
        rec(&self.steps, &mut out);
        out
    }
}
