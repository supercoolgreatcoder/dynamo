// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The interpreter.
//!
//! Read this file to check the genericity claim: it contains no domain vocabulary. It
//! resolves a step to an operation, builds a request from bindings, calls a `Transport`,
//! captures outputs, and repeats. Transport is a trait so the core has no HTTP or gRPC
//! dependency and is testable without a network -- and so that "which wire protocol" stops
//! being a property of the orchestrator.

use crate::config::{Call, Expr, Pipeline, Step, StepBody};
use crate::expr::{ExprError, Scope};
use crate::openapi::{Document, ResolvedOp, SpecError};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("step `{0}` names component `{1}`, which is not declared")]
    UnknownComponent(String, String),
    #[error("step `{0}` calls a discovery-based component without a `target` expression")]
    MissingTarget(String),
    #[error(transparent)]
    Spec(#[from] SpecError),
    #[error(transparent)]
    Expr(#[from] ExprError),
    #[error("transport error in step `{step}`: {source}")]
    Transport {
        step: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("two concurrent branches both wrote variable `{0}`")]
    ConflictingWrite(String),
    #[error("request is missing required field `{0}` of the pipeline's API operation")]
    InvalidRequest(String),
}

/// One outbound call, fully resolved. The core hands this to the host.
///
/// Parameters are placed by the spec, not by the caller: an input bound to a name the
/// operation declares `in: path` lands in the URL template, `in: query` in the query
/// string, `in: header` in the headers, and anything else in the body. That is what lets a
/// step call `GET /items/{id}?verbose=true` without the orchestrator knowing anything
/// about that API.
#[derive(Debug, Clone, PartialEq)]
pub struct Request {
    pub method: &'static str,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Value,
    /// Set when the operation declares `x-streaming: true`.
    pub streaming: bool,
    pub timeout_ms: Option<u64>,
    /// The operation's `x-` extensions, for the transport to interpret. Shared rather than
    /// cloned: this is per-request and the map is fixed at load.
    pub extensions: std::sync::Arc<BTreeMap<String, serde_json::Value>>,
}

/// A response: a status plus either one value or a stream of items.
///
/// The status is carried explicitly because "which outcomes are retryable" and "which are
/// errors at all" are policy the pipeline must be able to state. Without it a 503 and a 400
/// are indistinguishable to the core, so a config could only retry everything or nothing.
pub struct Reply {
    pub status: u16,
    pub payload: Payload,
}

/// Payload shape is decided by the operation's `x-streaming` declaration, not by the
/// orchestrator. Unary costs nothing extra.
pub enum Payload {
    Unary(Value),
    Stream(futures::stream::BoxStream<'static, Result<Value, String>>),
}

impl Reply {
    /// A 200 carrying one value -- the common case, and what a transport with no status
    /// concept (in-process, a mock) should return.
    pub fn ok(value: Value) -> Self {
        Self {
            status: 200,
            payload: Payload::Unary(value),
        }
    }

    pub fn stream(items: futures::stream::BoxStream<'static, Result<Value, String>>) -> Self {
        Self {
            status: 200,
            payload: Payload::Stream(items),
        }
    }
}

#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn call(&self, req: Request) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>>;

    /// Retry backoff. On the trait so the core needs no timer dependency and tests can
    /// make backoff instantaneous.
    async fn sleep_ms(&self, _ms: u64) {}
}

/// Where a streaming step's items go. The host supplies this; the core decides *what* to
/// emit per item from config, never *how* to write it, so SSE/chunked/websocket stays a
/// host concern.
#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn item(&self, value: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Turns a declared discovery group into a concrete endpoint.
///
/// Kept as a trait because *what* endpoints exist is environment state (an EndpointSlice,
/// a mesh cluster, a static list), not pipeline configuration. What IS configuration is
/// which group to draw from and, when the pipeline picks the callee itself, the expression
/// that produces it -- both of which live in the pipeline document.
#[async_trait::async_trait]
pub trait Resolver: Send + Sync {
    async fn resolve(
        &self,
        group: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;
}

/// Fails every lookup; used when a pipeline binds `target` explicitly for every
/// discovery-based call, which is the case for KV-style routing.
pub struct NoResolver;

#[async_trait::async_trait]
impl Resolver for NoResolver {
    async fn resolve(
        &self,
        group: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("no resolver configured for discovery group `{group}`").into())
    }
}

/// A sink that discards, for non-streaming runs and tests.
pub struct NullSink;

#[async_trait::async_trait]
impl Sink for NullSink {
    async fn item(&self, _v: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

impl Prepared {
    /// Decides whether a broken stream may be resumed, and builds the request that resumes it.
    ///
    /// Returns `None` to let the error propagate. Every `None` below is a case where resuming
    /// would be WORSE than failing, which is the part of this that deserves the care:
    ///
    /// * nothing delivered yet -- not a resume at all. An ordinary failed call, and the
    ///   caller's `retry` / `onFailure` already own it. Treating it as a resume would silently
    ///   bypass the policy the step actually declared.
    /// * budget exhausted -- a fleet failing pathologically would otherwise let one client
    ///   request cycle indefinitely.
    /// * `disableWhen` holds -- the answer would be corrupted rather than completed. See
    ///   [`ResumePolicy::disable_when`].
    fn plan_resume(
        &self,
        policy: &crate::config::StreamPolicy,
        resumes_left: &mut u32,
        delivered: u64,
        collected: &BTreeMap<String, Vec<Value>>,
        scope: &mut Scope,
        base: &Request,
    ) -> Result<Option<Request>, EngineError> {
        let Some(rp) = &policy.resume else {
            return Ok(None);
        };
        if delivered == 0 || *resumes_left == 0 {
            return Ok(None);
        }

        // The accumulation has to be visible to the expressions that rebuild the request,
        // because "what was already delivered" is the whole input to that decision. Bound
        // BEFORE `disableWhen` is evaluated so a guard can test it too.
        // `scope.vars` is a Value, so the object has to be reached explicitly.
        let saved: Vec<(String, Option<Value>)> = collected
            .keys()
            .map(|k| (k.clone(), scope.vars.get(k.as_str()).cloned()))
            .collect();
        if let Some(o) = scope.vars.as_object_mut() {
            for (name, vals) in collected {
                o.insert(name.clone(), Value::Array(vals.clone()));
            }
        }

        let decision = (|| -> Result<Option<Request>, EngineError> {
            if let Some(w) = &rp.disable_when {
                if eval_when(scope, w)? {
                    return Ok(None);
                }
            }
            let mut req = base.clone();
            let Value::Object(body) = &mut req.body else {
                return Ok(None);
            };
            for (field, expr) in &rp.input {
                body.insert(field.clone(), scope.eval(expr)?);
            }
            Ok(Some(req))
        })();

        // Restored so a resume decision leaves no trace in scope: these bindings exist only
        // to be read by `disableWhen` and `resume.input`, and the stream's real summary is
        // written once at the end of `drain_stream`.
        if let Some(o) = scope.vars.as_object_mut() {
            for (k, v) in saved {
                match v {
                    Some(v) => o.insert(k, v),
                    None => o.remove(&k),
                };
            }
        }
        let out = decision?;
        if out.is_some() {
            *resumes_left -= 1;
        }
        Ok(out)
    }

    /// Consumes a streaming reply, projecting each item per `stream.emit` and writing it to
    /// the sink. Returns a summary value so the step still has a `$.response` for its
    /// `output` bindings and for `respondWith`.
    ///
    /// The projection is what keeps the public stream shape declared rather than inherited:
    /// without it, a streaming step could only forward the callee's items verbatim, making
    /// the downstream component's chunk format the external contract.
    async fn drain_stream<T: Transport>(
        &self,
        step: &Step,
        call: &Call,
        mut items: futures::stream::BoxStream<'static, Result<Value, String>>,
        scope: &mut Scope,
        sink: &dyn Sink,
        transport: &T,
        req: &Request,
    ) -> Result<Value, EngineError> {
        use futures::StreamExt;
        let policy = call
            .stream
            .clone()
            .unwrap_or_else(|| crate::config::StreamPolicy {
                emit: Default::default(),
                emit_to_client: true,
                emit_when: None,
                count_into: None,
                collect: Default::default(),
                resume: None,
            });
        let mut count: u64 = 0;
        let mut collected: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        let saved_item = scope.item.clone();
        let mut resumes_left = policy.resume.as_ref().map(|r| r.max_attempts).unwrap_or(0);

        loop {
            while let Some(next) = items.next().await {
                let item = match next {
                    Ok(v) => v,
                    Err(e) => {
                        // The stream broke PART WAY THROUGH. `count` is the discriminator: at zero
                        // nothing has been delivered and this is an ordinary failed call, which
                        // the caller's retry/onFailure already covers. Past zero the client holds
                        // part of an answer and only resuming can complete it.
                        match self.plan_resume(
                            &policy,
                            &mut resumes_left,
                            count,
                            &collected,
                            scope,
                            req,
                        )? {
                            Some(next_req) => {
                                let reply = transport.call(next_req).await.map_err(|source| {
                                    EngineError::Transport {
                                        step: step.id.clone(),
                                        source,
                                    }
                                })?;
                                match reply.payload {
                                    Payload::Stream(s) => {
                                        items = s;
                                        // Nothing already emitted is emitted again: the new
                                        // upstream continues where the old one stopped because
                                        // `resume.input` told it what was already produced.
                                        continue;
                                    }
                                    // A resumed call that answers unary cannot continue a stream.
                                    Payload::Unary(_) => {
                                        return Err(EngineError::Transport {
                                            step: step.id.clone(),
                                            source: "resumed call did not return a stream".into(),
                                        });
                                    }
                                }
                            }
                            None => {
                                return Err(EngineError::Transport {
                                    step: step.id.clone(),
                                    source: e.into(),
                                });
                            }
                        }
                    }
                };
                // MOVED into the scope, not copied.
                //
                // This cloned the whole decoded chunk for every streamed item -- every field,
                // including its text -- so that `item` could be handed to the sink in the branch
                // where no projection is configured. That branch can take the value back out of
                // the scope instead, which costs nothing, and the projecting branch never wanted
                // the copy at all. At ~8,000 rps x 50 chunks this was a chunk-sized clone 400,000
                // times a second.
                scope.item = item;

                if let Some(w) = &policy.emit_when {
                    if !eval_when(scope, w)? {
                        continue;
                    }
                }
                count += 1;
                for (name, expr) in &policy.collect {
                    collected
                        .entry(name.clone())
                        .or_default()
                        .push(scope.eval(expr)?);
                }
                if !policy.emit_to_client {
                    continue;
                }
                let out = if policy.emit.is_empty() {
                    // No projection: the item IS the output. Taking it leaves `scope.item` null,
                    // which the next iteration overwrites before anything reads it.
                    std::mem::take(&mut scope.item)
                } else {
                    let mut m = serde_json::Map::new();
                    for (field, expr) in &policy.emit {
                        m.insert(field.clone(), scope.eval(expr)?);
                    }
                    Value::Object(m)
                };
                sink.item(out)
                    .await
                    .map_err(|source| EngineError::Transport {
                        step: step.id.clone(),
                        source,
                    })?;
            }
            break;
        }
        scope.item = saved_item;

        let mut summary = serde_json::Map::new();
        if let Some(name) = &policy.count_into {
            summary.insert(name.clone(), Value::from(count));
        }
        for (name, vals) in collected {
            summary.insert(name, Value::Array(vals));
        }
        Ok(Value::Object(summary))
    }
}

impl Prepared {
    /// Runs a call once, or once per element when `forEach` is set.
    ///
    /// `parallel` covers fan-out whose shape is known when the config is written;
    /// `forEach` covers fan-out over a collection that only exists at run time, which is
    /// the difference between "prefill alongside decode" and "one call per chunk".
    async fn run_call_maybe_foreach<T: Transport>(
        &self,
        step: &Step,
        call: &Call,
        transport: &T,
        scope: &mut Scope,
        sink: &dyn Sink,
    ) -> Result<Value, EngineError> {
        let Some(fe) = &call.for_each else {
            return self.run_call(step, call, transport, scope, sink).await;
        };
        let items = scope.eval_path(&fe.items)?;
        let items = items.as_array().cloned().unwrap_or_default();

        let saved = scope.vars.get(&fe.bind_as).cloned();
        let results = if fe.max_concurrent <= 1 {
            let mut out = Vec::with_capacity(items.len());
            for element in items {
                scope.set_var(&fe.bind_as, element);
                out.push(self.run_call(step, call, transport, scope, sink).await?);
            }
            out
        } else {
            // Each invocation owns a scope clone, because the element binding differs per
            // item and a shared scope would race. Results are collected in input order --
            // out-of-order completion must not reorder them, since a caller splitting them
            // back apart relies on position, exactly as batched responses do.
            use futures::StreamExt;
            let mut out =
                futures::stream::iter(items.into_iter().enumerate().map(|(i, element)| {
                    let mut sub = scope.clone();
                    sub.set_var(&fe.bind_as, element);
                    async move {
                        let mut sub = sub;
                        self.run_call(step, call, transport, &mut sub, sink)
                            .await
                            .map(|v| (i, v))
                    }
                }))
                .buffer_unordered(fe.max_concurrent)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            out.sort_by_key(|(i, _)| *i);
            out.into_iter().map(|(_, v)| v).collect()
        };
        match saved {
            Some(v) => scope.set_var(&fe.bind_as, v),
            None => {
                if let Some(o) = scope.vars.as_object_mut() {
                    o.remove(&fe.bind_as);
                }
            }
        }
        let all = Value::Array(results);
        if let Some(name) = &fe.collect_into {
            scope.set_var(name, all.clone());
        }
        Ok(all)
    }
}

impl Prepared {
    /// Applies a step's `output` bindings and any `respondWith` shaping. Shared by the
    /// batched and unbatched paths so a folded call behaves identically to a single one.
    fn finish_call(
        &self,
        _step: &Step,
        call: &Call,
        resp: Value,
        scope: &mut Scope,
    ) -> Result<Value, EngineError> {
        scope.response = resp;
        bind_outputs(call, scope)?;
        if call.respond && !call.respond_with.is_empty() {
            let mut shaped = serde_json::Map::new();
            for (field, expr) in &call.respond_with {
                shaped.insert(field.clone(), scope.eval(expr)?);
            }
            return Ok(Value::Object(shaped));
        }
        Ok(std::mem::take(&mut scope.response))
    }
}

/// Applies a step's `output` bindings.
///
/// When the step neither responds nor feeds a `forEach` aggregate, nothing reads
/// `$.response` after this point, so a binding of the form `$.response.<field>` can move the
/// subtree out instead of copying it. At ISL 4000 that field is a 4,000-element array and the
/// copy was the single largest remaining cost on the generic path. Anything else -- a deeper
/// path, an index, a literal, a `length` -- still evaluates by value.
fn bind_outputs(call: &Call, scope: &mut Scope) -> Result<(), EngineError> {
    let movable = !call.respond && call.for_each.is_none();
    for (name, expr) in &call.output {
        let v = match expr {
            Expr::Path(p) if movable && p.starts_with("$.response.") => scope.take_path(p)?,
            other => scope.eval(other)?,
        };
        scope.set_var(name, v);
    }
    Ok(())
}

/// For each variable, the step that performs its LAST read -- where the value may be moved
/// rather than copied.
///
/// `$.vars.tokenIds` at ISL 4000 is a 4,000-element array, and the binding that hands it to
/// the worker copies it. An exactly-once rule would not help: the real pipeline reads that
/// variable twice, once as `{length: ...}` for the selector and once as the worker's body.
/// The second read is the last one, and after it nothing can observe the variable, so that
/// read can take ownership. Steps run in order, so the last textual read is the last runtime
/// read.
///
/// A step qualifies only if it reads the variable exactly once, because within a step the
/// evaluation order of target, inputs, outputs and stream projections is not a contract.
///
/// Three shapes make a static analysis unsafe at all, so they disable the optimisation
/// outright rather than being reasoned about per-variable:
///   * `parallel` -- branches share a scope, so "later in the text" does not mean "later in
///     time", and one textual read can execute in several branches.
///   * `forEach` -- one textual read executes once per item.
///   * `errors.fallback` -- the fallback re-enters binding against a scope the failed
///     attempt may already have moved from.
/// Retry is fine: the body is built once, outside the retry loop, and re-sent as is.
pub fn last_reads(p: &Pipeline) -> BTreeMap<String, String> {
    let mut last: BTreeMap<String, String> = BTreeMap::new();
    let mut safe = true;

    fn count_expr(e: &Expr, reads: &mut BTreeMap<String, usize>) {
        match e {
            Expr::Path(p) => {
                if let Some(rest) = p.strip_prefix("$.vars.") {
                    // Any read counts, whole or partial: a partial read still needs the
                    // value present.
                    let name = rest.split(['.', '[']).next().unwrap_or(rest);
                    *reads.entry(name.to_string()).or_default() += 1;
                }
            }
            Expr::Length { length } => count_expr(length, reads),
            Expr::Optional { optional } => count_expr(optional, reads),
            Expr::Literal(_) => {}
        }
    }

    fn walk(steps: &[Step], last: &mut BTreeMap<String, String>, safe: &mut bool) {
        for step in steps {
            let mut reads: BTreeMap<String, usize> = BTreeMap::new();
            if let Some(w) = &step.when {
                count_expr(&Expr::Path(w.path.clone()), &mut reads);
            }
            match &step.body {
                StepBody::Parallel { branches } => {
                    *safe = false;
                    for b in branches {
                        walk(b, last, safe);
                    }
                }
                StepBody::Call(c) => {
                    if c.for_each.is_some() {
                        *safe = false;
                    }
                    if c.errors.as_ref().is_some_and(|e| {
                        matches!(e.on_failure, crate::config::OnFailure::Fallback { .. })
                    }) {
                        *safe = false;
                    }
                    if let Some(t) = &c.target {
                        count_expr(t, &mut reads);
                    }
                    for e in c.input.values() {
                        count_expr(e, &mut reads);
                    }
                    for e in c.output.values() {
                        count_expr(e, &mut reads);
                    }
                    for e in c.respond_with.values() {
                        count_expr(e, &mut reads);
                    }
                    if let Some(st) = &c.stream {
                        for e in st.emit.values().chain(st.collect.values()) {
                            count_expr(e, &mut reads);
                        }
                        if let Some(w) = &st.emit_when {
                            count_expr(&Expr::Path(w.path.clone()), &mut reads);
                        }
                    }
                }
            }
            for (name, n) in reads {
                if n == 1 {
                    last.insert(name, step.id.clone());
                } else {
                    // Read more than once here: no single site owns the last read, and a
                    // later step may still claim it.
                    last.remove(&name);
                }
            }
        }
    }

    walk(&p.steps, &mut last, &mut safe);
    if !safe {
        return Default::default();
    }
    last
}

/// A guard tests a value; an unresolvable path is `false` for `exists`, not an error,
/// because "is this field present" is the question being asked.
/// Test-visible wrapper; the predicate itself stays private.
pub fn eval_when_pub(scope: &Scope, w: &crate::config::When) -> Result<bool, EngineError> {
    eval_when(scope, w)
}

fn eval_when(scope: &Scope, w: &crate::config::When) -> Result<bool, EngineError> {
    // Composition first. A node that only composes carries no `path`, and evaluating an empty
    // path would otherwise decide the result by accident.
    if !w.any_of.is_empty() {
        for inner in &w.any_of {
            if eval_when(scope, inner)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if !w.all_of.is_empty() {
        for inner in &w.all_of {
            if !eval_when(scope, inner)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let Some(inner) = &w.not {
        return Ok(!eval_when(scope, inner)?);
    }
    let got = scope.eval_path(&w.path);
    if let Some(want_present) = w.exists {
        return Ok(got.is_ok() == want_present);
    }
    if let Some(expected) = &w.equals {
        return Ok(matches!(got, Ok(ref v) if v == expected));
    }
    if let Some(unwanted) = &w.not_equals {
        return Ok(matches!(got, Ok(ref v) if v != unwanted));
    }
    Ok(got.is_ok())
}

/// 2xx unless the step declares an explicit set. Making "what is success" configurable
/// matters for APIs that use 201/202/204 meaningfully.
fn is_success(status: u16, expect: &[u16]) -> bool {
    if expect.is_empty() {
        (200..300).contains(&status)
    } else {
        expect.contains(&status)
    }
}

/// Query/path/header values must be scalars; render them without JSON quoting.
fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A pipeline with its OpenAPI documents loaded and every binding validated.
///
/// Construction is where a bad config fails. Once built, execution cannot encounter an
/// unknown operation, an unbound required input, or a binding to a field the callee does
/// not define.
pub struct Prepared {
    pipeline: Pipeline,
    ops: BTreeMap<String, ResolvedOp>,
    resolver: std::sync::Arc<dyn Resolver>,
    api_request_schema: Option<crate::openapi::Schema>,
    /// Resolved batched operations, keyed by step id, for steps whose `x-batch` names a
    /// separate folded operation (e.g. encode -> encodeBatch).
    batch_ops: BTreeMap<String, ResolvedOp>,
    batchers: crate::batcher::Batchers,
    /// Variable -> the step whose read of it is the last one, and may therefore move the
    /// value out of scope instead of copying it. See `last_reads`.
    last_reads: BTreeMap<String, String>,
    /// Per-step wall time. See `StepStats`.
    pub stats: StepStats,
    /// Round-robin cursor across batch shards.
    shard_rr: AtomicU64,
}

impl Prepared {
    /// True when `path` is this step's whole read of a variable nothing reads afterwards.
    ///
    /// Only the whole variable moves: `$.vars.tokenIds` yes, `$.vars.tokenIds[0]` no. A
    /// partial read would have to leave the rest behind, which `take_path` does not do.
    fn is_movable_var(&self, step_id: &str, path: &str) -> bool {
        let Some(name) = path.strip_prefix("$.vars.") else {
            return false;
        };
        if name.contains('.') || name.contains('[') {
            return false;
        }
        self.last_reads.get(name).is_some_and(|s| s == step_id)
    }
}

/// Per-step wall time, accumulated across requests.
///
/// The compiled-in engine has emitted `STAGESTATS tokenize_us/req=... select_us/req=...
/// dispatch_us/req=...` for a long time. The generic core had nothing equivalent, so every
/// comparison between the two was a profiled arm against an unprofiled one: the totals said
/// the generic arm was slower and more expensive, and nothing said WHERE.
///
/// Wall time per step, not CPU: a step that waits -- on a batch gate, on a leader, on a round
/// trip -- contributes to request latency exactly as much as one that computes, and at fixed
/// concurrency latency is what sets throughput. A CPU profile would have shown the batch-gate
/// wait as free.
///
/// Counted for every request that RUNS the step, so a guarded step's average is over the
/// requests that took it, not over all of them.
#[derive(Debug, Default)]
pub struct StepStats {
    /// step id -> (total microseconds, times run)
    per_step: BTreeMap<String, (AtomicU64, AtomicU64)>,
    requests: AtomicU64,
}

impl StepStats {
    fn new(ids: impl Iterator<Item = String>) -> Self {
        Self {
            per_step: ids
                .map(|id| (id, (AtomicU64::new(0), AtomicU64::new(0))))
                .collect(),
            requests: AtomicU64::new(0),
        }
    }

    fn record(&self, step_id: &str, us: u64) {
        if let Some((total, runs)) = self.per_step.get(step_id) {
            total.fetch_add(us, Ordering::Relaxed);
            runs.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Requests completed so far.
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// `(step id, microseconds per run, runs)`, in pipeline order.
    pub fn snapshot(&self) -> Vec<(String, u64, u64)> {
        self.per_step
            .iter()
            .map(|(id, (total, runs))| {
                let r = runs.load(Ordering::Relaxed);
                (
                    id.clone(),
                    if r == 0 {
                        0
                    } else {
                        total.load(Ordering::Relaxed) / r
                    },
                    r,
                )
            })
            .collect()
    }

    /// One line in the same shape the compiled-in engine emits, so the two can be read
    /// side by side.
    pub fn line(&self) -> String {
        let mut out = format!("PIPESTATS reqs={}", self.requests());
        for (id, us, runs) in self.snapshot() {
            out.push_str(&format!(" {id}_us/req={us}(n={runs})"));
        }
        out
    }
}

impl std::fmt::Debug for Prepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prepared")
            .field("steps", &self.ops.keys())
            .finish()
    }
}

impl Prepared {
    /// Checks the inbound body against the external operation's declared required fields.
    ///
    /// Only required-field presence, deliberately: full JSON Schema validation belongs in a
    /// validating proxy, but a request missing a field every step binds would otherwise
    /// fail deep in the pipeline as an unresolvable path, attributed to the wrong step.
    fn validate_request(&self, request: &Value) -> Result<(), EngineError> {
        let Some(schema) = self.api_request_schema.as_ref() else {
            return Ok(());
        };
        let body = request.get("body").unwrap_or(&Value::Null);
        for field in &schema.required {
            if body.get(field).is_none() {
                return Err(EngineError::InvalidRequest(field.clone()));
            }
        }
        Ok(())
    }

    /// Attaches a resolver for discovery-based components that do not bind `target`.
    pub fn with_resolver(mut self, r: std::sync::Arc<dyn Resolver>) -> Self {
        self.resolver = r;
        self
    }

    /// `specs` maps a component's `openapi` path to its parsed document. Loading is the
    /// host's job so the core does no I/O.
    pub fn new(
        pipeline: Pipeline,
        specs: &BTreeMap<String, Document>,
    ) -> Result<Self, EngineError> {
        let mut ops = BTreeMap::new();

        // The externally-facing operation must exist. Without this the `api:` block is
        // decorative -- it was, until this check was added -- and a pipeline can claim to
        // serve an operation no document defines.
        let api_doc = specs
            .get(&pipeline.api.openapi)
            .ok_or_else(|| SpecError::NoSuchOperation(pipeline.api.operation_id.clone()))?;
        let api_op = api_doc.operation(&pipeline.api.operation_id)?;

        for step in pipeline.walk() {
            let StepBody::Call(call) = &step.body else {
                continue;
            };
            let comp = pipeline.components.get(&call.component).ok_or_else(|| {
                EngineError::UnknownComponent(step.id.clone(), call.component.clone())
            })?;
            if comp.base_url.is_none() && call.target.is_none() && comp.discovery.is_none() {
                return Err(EngineError::MissingTarget(step.id.clone()));
            }
            let doc = specs
                .get(&comp.openapi)
                .ok_or_else(|| SpecError::NoSuchOperation(call.operation_id.clone()))?;
            let op = doc.operation(&call.operation_id)?;
            op.validate(&step.id, &call.input, &call.output, call.batch.is_some())?;

            // A shaped response must match the external API's declared response schema,
            // so the public contract cannot drift from the document that describes it.
            if let Some(api_schema) = api_op.response_schema() {
                for field in call.respond_with.keys() {
                    if !api_schema.properties.contains_key(field) {
                        return Err(SpecError::UnknownOutput {
                            step: step.id.clone(),
                            op: pipeline.api.operation_id.clone(),
                            field: field.clone(),
                        }
                        .into());
                    }
                }
            }
            ops.insert(step.id.clone(), op);
        }
        let api_request_schema = api_op
            .op
            .request_body
            .as_ref()
            .and_then(|b| b.content.values().next())
            .map(|m| m.schema.clone());
        // Resolve the folded operation now, so a spec naming a batched operation that does
        // not exist fails at load rather than the first time a batch forms.
        let mut batch_ops = BTreeMap::new();
        for step in pipeline.walk() {
            let StepBody::Call(call) = &step.body else {
                continue;
            };
            if call.batch.is_none() {
                continue;
            }
            let op = &ops[&step.id];
            let Some(fold) = op.op.batch.as_ref().and_then(|b| b.fold()) else {
                continue;
            };
            let doc = &specs[&pipeline.components[&call.component].openapi];
            let id = fold
                .operation_id
                .clone()
                .unwrap_or_else(|| call.operation_id.clone());
            batch_ops.insert(step.id.clone(), doc.operation(&id)?);
        }
        let last_reads = last_reads(&pipeline);
        let stats = StepStats::new(pipeline.walk().into_iter().map(|s| s.id.clone()));
        Ok(Self {
            pipeline,
            ops,
            resolver: std::sync::Arc::new(NoResolver),
            api_request_schema,
            batch_ops,
            batchers: Default::default(),
            last_reads,
            stats,
            shard_rr: AtomicU64::new(0),
        })
    }

    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// Runs the pipeline. Returns the value of the step marked `respond`, if any.
    pub async fn run<T: Transport>(
        &self,
        transport: &T,
        request: Value,
    ) -> Result<Option<Value>, EngineError> {
        self.run_with_sink(transport, request, &NullSink).await
    }

    /// Streaming runs supply a sink; the core decides what to emit per item from config,
    /// never how to write it, so SSE vs chunked vs websocket stays a host concern.
    pub async fn run_with_sink<T: Transport>(
        &self,
        transport: &T,
        request: Value,
        sink: &dyn Sink,
    ) -> Result<Option<Value>, EngineError> {
        self.validate_request(&request)?;
        let mut scope = Scope::new(request);
        for (k, v) in &self.pipeline.vars {
            scope.set_var(k, v.clone());
        }
        let mut response = None;
        self.run_steps(
            &self.pipeline.steps,
            transport,
            &mut scope,
            &mut response,
            sink,
        )
        .await?;
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        Ok(response)
    }

    async fn run_steps<T: Transport>(
        &self,
        steps: &[Step],
        transport: &T,
        scope: &mut Scope,
        response: &mut Option<Value>,
        sink: &dyn Sink,
    ) -> Result<(), EngineError> {
        for step in steps {
            if let Some(w) = &step.when {
                if !eval_when(scope, w)? {
                    continue;
                }
            }
            match &step.body {
                StepBody::Call(call) => {
                    let t0 = std::time::Instant::now();
                    let out = self
                        .run_call_maybe_foreach(step, call, transport, scope, sink)
                        .await?;
                    self.stats.record(&step.id, t0.elapsed().as_micros() as u64);
                    if call.respond {
                        *response = Some(out);
                    }
                }
                StepBody::Parallel { branches } => {
                    // Branches read a common snapshot and write disjoint variables. Running
                    // them against a clone and merging keeps the ordering of writes from
                    // mattering, so a config cannot be order-dependent by accident.
                    let mut merged: BTreeMap<String, Value> = BTreeMap::new();
                    for branch in branches {
                        let mut sub = scope.clone();
                        let mut sub_resp = None;
                        Box::pin(self.run_steps(branch, transport, &mut sub, &mut sub_resp, sink))
                            .await?;
                        if sub_resp.is_some() {
                            *response = sub_resp;
                        }
                        if let Some(obj) = sub.vars.as_object() {
                            for (k, v) in obj {
                                let before = scope.vars.get(k);
                                if before == Some(v) {
                                    continue;
                                }
                                if merged.insert(k.clone(), v.clone()).is_some() {
                                    return Err(EngineError::ConflictingWrite(k.clone()));
                                }
                            }
                        }
                    }
                    for (k, v) in merged {
                        scope.set_var(&k, v);
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_call<T: Transport>(
        &self,
        step: &Step,
        call: &Call,
        transport: &T,
        scope: &mut Scope,
        sink: &dyn Sink,
    ) -> Result<Value, EngineError> {
        let op = self.ops.get(&step.id).expect("validated in Prepared::new");
        let comp = &self.pipeline.components[&call.component];

        let base = match (&comp.base_url, &call.target) {
            (Some(b), _) => b.clone(),
            (None, Some(t)) => scope
                .eval(t)?
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| EngineError::MissingTarget(step.id.clone()))?,
            // No explicit target: ask the environment for one from the declared group.
            (None, None) => {
                let group = comp
                    .discovery
                    .as_ref()
                    .map(|d| d.group.clone())
                    .ok_or_else(|| EngineError::MissingTarget(step.id.clone()))?;
                self.resolver
                    .resolve(&group)
                    .await
                    .map_err(|source| EngineError::Transport {
                        step: step.id.clone(),
                        source,
                    })?
            }
        };

        // Placement is driven entirely by the operation's declared parameters.
        let mut body = serde_json::Map::new();
        let mut headers = BTreeMap::new();
        let mut query: Vec<(String, String)> = Vec::new();
        let mut path = op.path.clone();
        for (field, expr) in &call.input {
            let v = match expr {
                Expr::Path(p) if self.is_movable_var(&step.id, p) => scope.take_path(p)?,
                other => scope.eval(other)?,
            };
            match op.parameter(field).map(|p| p.location) {
                Some(crate::openapi::ParamIn::Path) => {
                    path = path.replace(&format!("{{{field}}}"), &scalar(&v));
                }
                Some(crate::openapi::ParamIn::Query) => query.push((field.clone(), scalar(&v))),
                Some(crate::openapi::ParamIn::Header) | Some(crate::openapi::ParamIn::Cookie) => {
                    headers.insert(field.clone(), scalar(&v));
                }
                None => {
                    body.insert(field.clone(), v);
                }
            }
        }
        let mut url = format!("{}{}", base.trim_end_matches('/'), path);
        if !query.is_empty() {
            let qs: Vec<String> = query.into_iter().map(|(k, v)| format!("{k}={v}")).collect();
            url.push('?');
            url.push_str(&qs.join("&"));
        }

        // Batched path: fold this request together with whatever else is waiting for the
        // same step and endpoint, issue ONE call, and split the reply back apart. Only taken
        // when the callee declared HOW to fold; `x-batch: true` alone is validatable but not
        // executable, so such a step runs unbatched rather than guessing.
        if let (Some(policy), Some(bop)) = (call.batch.as_ref(), self.batch_ops.get(&step.id)) {
            let fold_spec = op
                .op
                .batch
                .as_ref()
                .and_then(|b| b.fold())
                .expect("batch_ops only holds steps with a fold spec")
                .clone();
            // One queue per shard. Requests spread across them, so several batches fill and
            // dispatch concurrently instead of one queue serialising everything.
            let shard = if policy.shards > 1 {
                (self.shard_rr.fetch_add(1, Ordering::Relaxed) as usize) % policy.shards
            } else {
                0
            };
            let key = format!("{}|{}|{}", step.id, base, shard);
            // Take, not clone: every path below this point returns, so `body` has no later
            // reader. At ISL 4000 it carries the whole prompt text.
            let single = Value::Object(std::mem::take(&mut body));
            match self.batchers.join(&key, single).await {
                crate::batcher::Join::Follower(rx) => {
                    let v = rx.await.map_err(|_| EngineError::Transport {
                        step: step.id.clone(),
                        source: "batch leader dropped without replying".into(),
                    })?;
                    let resp = v.map_err(|e| EngineError::Transport {
                        step: step.id.clone(),
                        source: e.into(),
                    })?;
                    return self.finish_call(step, call, resp, scope);
                }
                crate::batcher::Join::Leader(leader) => {
                    // Acquired BEFORE collecting, and held across the calls: while a folded
                    // call is in flight the next leader waits here, and everything arriving
                    // meanwhile joins its batch. Batch size then tracks load with no
                    // configured wait. Dropped at the end of this block.
                    let _gate = match policy.max_in_flight {
                        0 => None,
                        n => Some(self.batchers.gate(&key, n).await),
                    };
                    let (reqs, txs) = leader.collect(policy.linger_us).await;
                    // The leader owns everything it collected, and `maxSize` bounds the
                    // CALL, not the collection. Splitting here rather than in `collect` is
                    // what guarantees no request is left without a leader.
                    let max = policy.max_size.max(1);
                    let url = format!("{}{}", base.trim_end_matches('/'), bop.path);
                    let chunks: Vec<_> = {
                        let mut reqs = reqs;
                        let mut txs = txs;
                        let mut out = Vec::new();
                        while !reqs.is_empty() {
                            let take = max.min(reqs.len());
                            let rest_r = reqs.split_off(take);
                            let rest_t = txs.split_off(take);
                            out.push((reqs, txs));
                            reqs = rest_r;
                            txs = rest_t;
                        }
                        out
                    };
                    // Concurrently, so the members of chunk 2 do not wait out chunk 1's
                    // round trip. Nothing is spawned; these borrow the caller's transport.
                    let calls = chunks.into_iter().map(|(creqs, ctxs)| {
                        let fold_spec = &fold_spec;
                        let url = url.clone();
                        async move {
                            let folded = match crate::batcher::fold_checked_owned(creqs, fold_spec)
                            {
                                Ok(f) => f,
                                Err(e) => {
                                    // Fold failure is this chunk's failure, not the whole
                                    // batch's, and every member must hear about it.
                                    crate::batcher::distribute(ctxs, Err(e.to_string()), fold_spec);
                                    return;
                                }
                            };
                            let breq = Request {
                                method: bop.method,
                                url,
                                headers: BTreeMap::new(),
                                body: folded,
                                streaming: false,
                                timeout_ms: call.timeout_ms,
                                extensions: std::sync::Arc::new(bop.op.extensions.clone()),
                            };
                            match transport.call(breq).await {
                                Ok(r) => {
                                    let v = match r.payload {
                                        Payload::Unary(v) => v,
                                        Payload::Stream(_) => Value::Null,
                                    };
                                    crate::batcher::distribute(ctxs, Ok(v), fold_spec);
                                }
                                Err(e) => {
                                    crate::batcher::distribute(ctxs, Err(e.to_string()), fold_spec);
                                }
                            }
                        }
                    });
                    futures::future::join_all(calls).await;
                    let mine = leader.into_receiver().await;
                    let resp = mine
                        .map_err(|_| EngineError::Transport {
                            step: step.id.clone(),
                            source: "batch reply channel closed".into(),
                        })?
                        .map_err(|e| EngineError::Transport {
                            step: step.id.clone(),
                            source: e.into(),
                        })?;
                    return self.finish_call(step, call, resp, scope);
                }
            }
        }

        let req = Request {
            method: op.method,
            url,
            headers,
            body: Value::Object(body),
            streaming: op.op.streaming,
            timeout_ms: call.timeout_ms,
            extensions: std::sync::Arc::new(op.op.extensions.clone()),
        };

        // Retries are opt-in per step because they are only safe without side effects, and
        // a status is only retryable if the policy says so: retrying a 400 is pointless and
        // retrying a 409 can be harmful.
        let attempts = call
            .retry
            .as_ref()
            .map(|r| r.max_attempts.max(1))
            .unwrap_or(1);
        let policy = call.errors.clone().unwrap_or_default();
        let mut last: Option<Box<dyn std::error::Error + Send + Sync>> = None;
        let mut reply: Option<Reply> = None;

        for attempt in 0..attempts {
            match transport.call(req.clone()).await {
                Ok(r) if is_success(r.status, &policy.expect_status) => {
                    reply = Some(r);
                    break;
                }
                Ok(r) => {
                    let retryable = call
                        .retry
                        .as_ref()
                        .is_some_and(|p| p.retry_on.contains(&r.status));
                    last = Some(format!("status {}", r.status).into());
                    if !retryable {
                        reply = None;
                        break;
                    }
                }
                Err(e) => last = Some(e),
            }
            if attempt + 1 < attempts {
                if let Some(b) = call.retry.as_ref().map(|r| r.backoff_ms) {
                    if b > 0 {
                        transport.sleep_ms(b).await;
                    }
                }
            }
        }

        let reply = match reply {
            Some(r) => r,
            None => {
                let source = last.unwrap_or_else(|| "call failed".into());
                match &policy.on_failure {
                    crate::config::OnFailure::Fail => {
                        return Err(EngineError::Transport {
                            step: step.id.clone(),
                            source,
                        });
                    }
                    // Skip: no response, so `output` bindings are deliberately not applied.
                    crate::config::OnFailure::Skip => return Ok(Value::Null),
                    crate::config::OnFailure::Fallback(v) => Reply::ok(v.clone()),
                }
            }
        };

        let resp = match reply.payload {
            Payload::Unary(v) => v,
            Payload::Stream(s) => {
                self.drain_stream(step, call, s, scope, sink, transport, &req)
                    .await?
            }
        };

        // Move the response into scope rather than snapshotting a copy. At ISL 4000 the
        // response carries a 4,000-element token array, so a clone here is ~40 us of pure
        // memcpy per step -- and it bought nothing, since the value is handed straight back
        // at the end of this function.
        scope.response = resp;
        bind_outputs(call, scope)?;

        // Shape the response to the declared external API when asked, rather than letting
        // the callee's body become the public contract by default.
        if call.respond && !call.respond_with.is_empty() {
            let mut shaped = serde_json::Map::new();
            for (field, expr) in &call.respond_with {
                shaped.insert(field.clone(), scope.eval(expr)?);
            }
            return Ok(Value::Object(shaped));
        }
        Ok(std::mem::take(&mut scope.response))
    }
}

/// Convenience for hosts: the set of distinct OpenAPI documents a pipeline references.
pub fn referenced_specs(p: &Pipeline) -> Vec<String> {
    let mut v: Vec<String> = p.components.values().map(|c| c.openapi.clone()).collect();
    v.push(p.api.openapi.clone());
    v.sort();
    v.dedup();
    v
}

#[allow(dead_code)]
fn _assert_expr_used(_: &Expr) {}

#[cfg(test)]
mod move_semantics_tests {
    use super::*;

    /// `steps:` plus a first step that binds `ids`; each test appends its own readers.
    const HEAD: &str = r#"
api: {openapi: api.yaml, operationId: chat}
components:
  tok: {openapi: tok.yaml, baseUrl: "http://tok"}
  w:   {openapi: w.yaml,   baseUrl: "http://w"}
steps:
  - id: t
    call: {component: tok, operationId: encode, input: {text: "$.request.body.q"}, output: {ids: "$.response.token_ids"}}
"#;

    fn last_of(tail: &str) -> BTreeMap<String, String> {
        let yaml = format!("{HEAD}{tail}");
        last_reads(&serde_yaml::from_str::<Pipeline>(&yaml).expect("pipeline"))
    }

    #[test]
    fn the_only_reader_is_the_last_reader() {
        let m = last_of(
            r#"  - id: g
    call: {component: w, operationId: gen, input: {token_ids: "$.vars.ids"}, respond: true}
"#,
        );
        assert_eq!(m.get("ids").map(String::as_str), Some("g"));
    }

    #[test]
    fn the_later_of_two_readers_wins() {
        // This is the real aggregated pipeline: the selector reads the ids as a `length`,
        // then the worker reads them as its body. An exactly-once rule would refuse to move
        // the one array whose copy actually costs anything.
        let m = last_of(
            r#"  - id: s
    call:
      component: w
      operationId: sel
      input: {isl_tokens: {length: "$.vars.ids"}}
  - id: g
    call: {component: w, operationId: gen, input: {token_ids: "$.vars.ids"}, respond: true}
"#,
        );
        assert_eq!(m.get("ids").map(String::as_str), Some("g"));
    }

    #[test]
    fn a_step_reading_twice_owns_no_last_read() {
        // Within one step the evaluation order of inputs is not a contract, so neither read
        // can be declared the last one.
        let m = last_of(
            r#"  - id: g
    call:
      component: w
      operationId: gen
      input: {token_ids: "$.vars.ids", n: {length: "$.vars.ids"}}
      respond: true
"#,
        );
        assert_eq!(m.get("ids"), None);
    }

    #[test]
    fn a_later_single_read_reclaims_it() {
        let m = last_of(
            r#"  - id: s
    call:
      component: w
      operationId: sel
      input: {token_ids: "$.vars.ids", n: {length: "$.vars.ids"}}
  - id: g
    call: {component: w, operationId: gen, input: {token_ids: "$.vars.ids"}, respond: true}
"#,
        );
        assert_eq!(m.get("ids").map(String::as_str), Some("g"));
    }

    #[test]
    fn for_each_disables_the_optimisation_entirely() {
        // One textual read, many executions. "Last read" is not a position in the text here.
        let m = last_of(
            r#"  - id: g
    call:
      component: w
      operationId: gen
      input: {token_ids: "$.vars.ids"}
      forEach: {items: "$.request.body.batch", as: chunk}
      respond: true
"#,
        );
        assert!(m.is_empty());
    }

    #[test]
    fn parallel_disables_the_optimisation_entirely() {
        let m = last_of(
            r#"  - id: p
    parallel:
      branches:
        - - id: a
            call: {component: w, operationId: gen, input: {token_ids: "$.vars.ids"}}
"#,
        );
        assert!(m.is_empty());
    }

    #[test]
    fn only_a_whole_read_by_the_owning_step_moves() {
        let yaml = format!(
            "{HEAD}{}",
            r#"  - id: g
    call: {component: w, operationId: gen, input: {token_ids: "$.vars.ids"}, respond: true}
"#
        );
        let p: Pipeline = serde_yaml::from_str(&yaml).unwrap();
        let prep = Prepared {
            pipeline: p.clone(),
            ops: Default::default(),
            resolver: std::sync::Arc::new(NoResolver),
            api_request_schema: None,
            batch_ops: Default::default(),
            batchers: Default::default(),
            last_reads: last_reads(&p),
            stats: Default::default(),
            shard_rr: AtomicU64::new(0),
        };
        assert!(prep.is_movable_var("g", "$.vars.ids"));
        assert!(!prep.is_movable_var("t", "$.vars.ids")); // not the last reader
        assert!(!prep.is_movable_var("g", "$.vars.ids[0]")); // partial
        assert!(!prep.is_movable_var("g", "$.vars.ids.inner")); // partial
        assert!(!prep.is_movable_var("g", "$.response.token_ids"));
        assert!(!prep.is_movable_var("g", "$.vars.absent"));
    }

    #[test]
    fn a_responding_step_keeps_its_response_intact() {
        // `finish_call` returns the response when there is no `respondWith`, so a moving
        // output binding there would hollow out the body the caller receives.
        let call: Call = serde_yaml::from_str(
            r#"{component: w, operationId: gen, input: {}, output: {ids: "$.response.token_ids"}, respond: true}"#,
        )
        .unwrap();
        let mut scope = Scope::new(serde_json::json!({}));
        scope.response = serde_json::json!({"token_ids": [1, 2, 3]});
        bind_outputs(&call, &mut scope).unwrap();
        assert_eq!(
            scope.eval_path("$.vars.ids").unwrap(),
            serde_json::json!([1, 2, 3])
        );
        assert_eq!(
            scope.eval_path("$.response.token_ids").unwrap(),
            serde_json::json!([1, 2, 3])
        );
    }

    #[test]
    fn a_non_responding_step_moves_its_response_out() {
        let call: Call = serde_yaml::from_str(
            r#"{component: w, operationId: gen, input: {}, output: {ids: "$.response.token_ids"}}"#,
        )
        .unwrap();
        let mut scope = Scope::new(serde_json::json!({}));
        scope.response = serde_json::json!({"token_ids": [1, 2, 3], "other": 9});
        bind_outputs(&call, &mut scope).unwrap();
        assert_eq!(
            scope.eval_path("$.vars.ids").unwrap(),
            serde_json::json!([1, 2, 3])
        );
        assert!(scope.eval_path("$.response.token_ids").is_err());
        // Siblings are untouched; only the bound field leaves.
        assert_eq!(
            scope.eval_path("$.response.other").unwrap(),
            serde_json::json!(9)
        );
    }
}
