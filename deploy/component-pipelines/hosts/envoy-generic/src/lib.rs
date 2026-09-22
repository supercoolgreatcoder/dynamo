// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The generic orchestration core, hosted inside Envoy 1.39 as a dynamic module.
//!
//! The point of this crate is that it contains no orchestration. `pipeline-core` is the same
//! crate agentgateway hosts, depended on by path, running the same pipeline YAML against the
//! same OpenAPI documents and the same protobuf descriptor set. Porting the interpreter would
//! have made a gateway-to-gateway comparison into a comparison of two implementations of one
//! idea; this way the interpreter is a constant and the host is the variable.
//!
//! **I/O is configurable, and defaults to our own.** With no `upstream_clusters` map the
//! pipeline's callouts go out over tonic and reqwest rather than through Envoy clusters; map
//! an authority to a cluster and that hop is issued with `send_http_callout` (plain HTTP) or
//! `start_http_stream` (gRPC) instead, picking up the cluster's load balancing, outlier
//! detection, circuit breakers, retries and `upstream_rq_*` stats. See
//! `k8s/host-managed-transport.md`.
//!
//! Direct I/O remains the default, and it is what every benchmark to date measured:
//!
//!   * it is what the agentgateway host already does, so the two hosts embed the interpreter
//!     identically and the delta is the request/response path rather than two different
//!     connection pools;
//!   * `Transport::call` is an `async fn` taking `&self`, with no Envoy handle. Driving it
//!     from Envoy callouts means a hand-rolled executor whose transport queues requests for a
//!     driver holding `&mut EnvoyHttpFilter` to issue -- which is the deeper integration and
//!     worth doing, but it changes what is being measured.
//!
//! So: this measures "what does it cost to host a generic orchestrator in Envoy", not "how
//! fast are Envoy's clusters". The distinction is recorded here because the number is
//! meaningless without it.
//!
//! Streaming works through the scheduler. The pipeline runs on a tokio runtime off the Envoy
//! worker thread; each projected item is pushed onto a queue and the filter is woken with
//! `EnvoyHttpFilterScheduler::commit`, which the SDK marks `Send` for exactly this. The
//! Envoy thread then drains the queue with `send_response_data`. Nothing touches an Envoy
//! handle from the runtime thread.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dynamo_generic_pipeline::{
    referenced_specs, Document, Payload, Pipeline, Prepared, Reply, Request, Sink, Transport,
};
use dynamo_pipeline_grpc::GrpcTransport;
use envoy_proxy_dynamic_modules_rust_sdk::*;
use serde::Deserialize;
use serde_json::Value;

declare_init_functions!(
    init,
    new_http_filter_config_fn,
    new_http_filter_per_route_config_fn
);

/// Woken when the pipeline has produced output or finished.
const PIPELINE_EVENT: u64 = 1;

/// Woken when the pipeline has parked an upstream call for the Envoy thread to issue.
const OUTBOUND_EVENT: u64 = 2;

/// How many bytes of SSE may be parked while the client is not reading, before the stream is
/// ended.
///
/// Sized against the workload rather than picked: mooncake's median generation is ~171 tokens
/// and an SSE frame is a few hundred bytes, so a whole response is tens of kilobytes. 4 MB is
/// therefore ~100 complete responses of slack -- generous for a client that is merely slow,
/// and still bounded for one that has stopped reading entirely.
const MAX_PARKED_BYTES: usize = 4 * 1024 * 1024;

/// How many upstream frames may be parked when the consumer is behind.
///
/// One frame is one token, so this is ~6 complete mooncake-sized generations of slack. Large
/// enough that an ordinary scheduling hiccup never trips it, small enough that a consumer which
/// has genuinely stopped is caught quickly.
const MAX_BACKLOG_FRAMES: usize = 1024;

/// Emit `PIPESTATS` every N completed requests; 0 disables it.
///
/// Read once at load rather than per request: `std::env::var` on the request path would be a
/// syscall-shaped cost inside the thing being measured.
static PIPESTATS_EVERY: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::env::var("GENERIC_PIPELINE_STATS_EVERY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
});
/// Per-process entropy keeps IDs distinct across gateway replicas; the sequence keeps the
/// request path deterministic, allocation-only, and unique under concurrency.
///
/// IDs used to be `(unix_seconds, body_length)`. Identical benchmark prompts therefore
/// collided inside a preprocessor batch, which correctly rejected them as duplicate
/// `item_id` values. The resulting empty SSE streams looked like gateway overload.
static REQUEST_ID_PREFIX: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    use std::io::Read as _;

    let mut bytes = [0u8; 8];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .is_ok()
    {
        u64::from_ne_bytes(bytes)
    } else {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        nanos ^ ((std::process::id() as u64) << 32)
    }
});
static REQUEST_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn next_request_id() -> String {
    let sequence = REQUEST_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("chatcmpl-{:016x}-{sequence:016x}", *REQUEST_ID_PREFIX)
}

fn init() -> bool {
    maybe_start_profiler();
    true
}

/// Sample this process's CPU for N seconds and print where it actually goes.
///
/// This exists because attribution by every cheaper means was exhausted. The generic arm costs
/// ~2.1x the compiled-in arm per request at ISL 4000, and varying one parameter at a time
/// localised that to per-token and per-chunk work:
///
///   generic = 615 us + 0.243 us/token + 21.3 us/chunk
///   static  = 540 us + 0.045 us/token + 10.6 us/chunk
///
/// but component microbenchmarks account for only ~25% of either scaling term, and five
/// mechanisms were measured and eliminated (interpretation, JSON<->protobuf mapping, wire
/// reflection, tokenizer-fleet capacity, SSE wakeups). Knowing the cost scales per token is not
/// knowing which code spends it, and guessing produced five wrong answers.
///
/// Signal-based, so it samples every thread including Envoy's workers -- which matters, since
/// the pipeline runs on its own runtime and the response is written on Envoy's thread, and the
/// cost could be on either.
fn maybe_start_profiler() {
    let secs: u64 = std::env::var("GENERIC_PIPELINE_PROFILE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if secs == 0 {
        return;
    }
    std::thread::spawn(move || {
        let guard = match pprof::ProfilerGuardBuilder::default()
            .frequency(199)
            // Frames inside the allocator and the unwinder itself are noise here.
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
        {
            Ok(g) => g,
            Err(e) => {
                eprintln!("generic_pipeline: profiler failed to start: {e}");
                return;
            }
        };
        eprintln!("generic_pipeline: PROFILE sampling for {secs}s at 199 Hz");
        std::thread::sleep(std::time::Duration::from_secs(secs));
        let report = match guard.report().build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("generic_pipeline: profiler report failed: {e}");
                return;
            }
        };

        let mut by_leaf: std::collections::HashMap<String, isize> = Default::default();
        // Rolled up by crate as well as by leaf: a leaf table scatters one logical cost across
        // twenty inlined helpers, and the question here is which LAYER owns the time.
        let mut by_area: std::collections::HashMap<&str, isize> = Default::default();
        let mut total: isize = 0;
        for (frames, count) in report.data.iter() {
            total += *count;
            let leaf = frames
                .frames
                .first()
                .and_then(|f| f.first())
                .map(|sym| sym.name())
                .unwrap_or_else(|| "?".to_string());
            *by_leaf.entry(leaf).or_default() += *count;

            // Attribute the whole stack to the outermost area it touches, so nested calls are
            // counted once rather than smeared.
            let joined: String = frames
                .frames
                .iter()
                .flatten()
                .map(|sym| sym.name())
                .collect::<Vec<_>>()
                .join(";");
            let area = if joined.contains("serde_json") {
                "serde_json"
            } else if joined.contains("prost_reflect") {
                "prost_reflect"
            } else if joined.contains("pipeline_core") {
                "pipeline_core"
            } else if joined.contains("pipeline_grpc") {
                "pipeline_grpc"
            } else if joined.contains("tonic") || joined.contains("h2") || joined.contains("hyper")
            {
                "grpc/http2 client"
            } else if joined.contains("envoy") || joined.contains("Envoy") {
                "envoy"
            } else if joined.contains("tokio") {
                "tokio"
            } else if joined.contains("alloc")
                || joined.contains("malloc")
                || joined.contains("free")
            {
                "allocator"
            } else {
                "other"
            };
            *by_area.entry(area).or_default() += *count;
        }

        let pct = |c: isize| 100.0 * c as f64 / total.max(1) as f64;
        let mut areas: Vec<_> = by_area.into_iter().collect();
        areas.sort_by_key(|(_, c)| -*c);
        eprintln!("generic_pipeline: PROFILE total_samples={total}");
        for (a, c) in areas {
            eprintln!(
                "generic_pipeline: PROFILE area {a:<20} {:>6.2}%  ({c})",
                pct(c)
            );
        }
        let mut leaves: Vec<_> = by_leaf.into_iter().collect();
        leaves.sort_by_key(|(_, c)| -*c);
        for (name, c) in leaves.into_iter().take(30) {
            let short: String = name.chars().take(96).collect();
            eprintln!("generic_pipeline: PROFILE leaf {:>6.2}%  {short}", pct(c));
        }
        eprintln!("generic_pipeline: PROFILE done");
    });
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModuleConfig {
    /// Path to the pipeline YAML. The same file agentgateway loads.
    pipeline_file: String,
    /// Path to the protobuf descriptor set backing `x-grpc` bindings.
    grpc_descriptor_file: String,
    /// Connections opened per gRPC authority. One channel pins the whole fleet to a single
    /// replica behind a ClusterIP, because HTTP/2 multiplexes onto one connection -- measured
    /// on the agentgateway host as 6.58/0.00/0.00/0.00 cores across a four-replica fleet.
    #[serde(default = "default_conns")]
    grpc_conns: usize,
    #[serde(default = "default_max_body")]
    max_request_bytes: usize,
    /// Selects either module-owned tonic sockets or Envoy-owned clusters. The
    /// callout mode is fail-closed: no unmapped hop falls back to direct I/O.
    #[serde(default)]
    transport_mode: TransportMode,
    /// Path to the worker-registration document. See [`Registration`].
    #[serde(default)]
    registration_file: Option<String>,
    /// Upstream authority -> Envoy cluster name, e.g.
    /// `{"selector-decode:8083": "selector_decode"}`.
    ///
    /// An authority listed here has its calls issued with `send_http_callout` against that
    /// cluster, so the hop gets the cluster's load balancing, outlier detection, circuit
    /// breakers, retries and stats. An authority NOT listed keeps going out over our own
    /// sockets. Empty by default, which is exactly the behaviour before this existed -- so the
    /// old configuration stays runnable and each hop can be moved over and measured alone.
    #[serde(default)]
    upstream_clusters: std::collections::HashMap<String, String>,
    /// Derive the cluster name from the authority instead of looking it up.
    ///
    /// `kvworker7:8081` -> `kvworker7`. Set this and `upstream_clusters` becomes unnecessary for
    /// every authority whose cluster is named after its host.
    ///
    /// This exists for CDS. The map is STATIC CONFIG listing every worker, so a fleet that
    /// changes at run time leaves it stale -- and a stale entry does not fail, it silently falls
    /// back to the module's own sockets. That failure once reproduced a previous benchmark
    /// almost exactly while three hops had quietly stopped going through Envoy. A rule cannot go
    /// stale, so the control plane can add and remove clusters freely and nothing here has to
    /// be told.
    ///
    /// The map still wins where it has an entry, so a hop whose cluster is NOT named after its
    /// host keeps working.
    #[serde(default)]
    cluster_from_authority: bool,
    /// Per-callout timeout. Nothing bounded these calls before; `pipeline-core` supports a
    /// per-step `timeout_ms` and no deployed spec sets one.
    #[serde(default = "default_callout_timeout")]
    callout_timeout_ms: u64,
    /// Retry policy for callouts, in a route's `retry_on` vocabulary. Requires an Envoy
    /// carrying `patches/envoy-callout-options.patch`; against a stock Envoy the optional
    /// callback is absent and the SDK falls back to the plain callout, so leaving this set is
    /// safe but does nothing.
    ///
    /// Empty by default. Retries are NOT a free win here: a callout targets a cluster, so a
    /// retry re-picks a host, and for a KV-routed worker that discards the prefix-cache hit
    /// the architecture exists to produce. Hence `retry_clusters` rather than a global switch.
    #[serde(default)]
    callout_retry_on: String,
    #[serde(default)]
    callout_num_retries: u32,
    #[serde(default)]
    callout_per_try_timeout_ms: u64,
    /// Cluster-name prefixes the retry policy applies to. A hop whose cluster does not match
    /// any prefix is issued exactly as before. Listed explicitly rather than inferred: which
    /// hops are safe to retry is a property of the PIPELINE, not of the transport.
    #[serde(default)]
    retry_clusters: Vec<String>,
    /// Carry unary gRPC over `send_http_callout` instead of `start_http_stream`.
    ///
    /// The stream API was only ever used for unary gRPC because it is the one that delivers
    /// trailers, and gRPC reports status in `grpc-status` there. With
    /// `on_http_callout_done_with_trailers` available, the unary call can use the unary API.
    /// Off by default: on a stock Envoy the optional symbol is never called and this would
    /// silently lose gRPC status.
    #[serde(default)]
    unary_grpc_via_callout: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TransportMode {
    #[default]
    Independent,
    EnvoyCallouts,
}

fn default_callout_timeout() -> u64 {
    120_000
}

/// Which workers exist, and which selector indexes them.
///
/// This exists because the arm was not self-sufficient. The pipeline ROUTES through a
/// selector, but a selector only knows a worker after something has registered it -- and this
/// module registered nothing. It worked anyway for an entire benchmark matrix because
/// agentgateway ran first in every cell and registered the same fleet with the same selector;
/// Envoy was routing against an index another gateway had populated.
///
/// Adding a full teardown between cells is what exposed it: the selectors came up empty and
/// every request failed with `transport error in step 'route': status 503`. The dependency was
/// real the whole time and invisible only because of ordering.
///
/// The body posted here is byte-for-byte what agentgateway posts (`worker_id`, `endpoint`,
/// `kv_events_endpoints`, `block_size`, `max_num_batched_tokens`, `total_kv_blocks`), so both
/// hosts hand the selector the same fleet description and the comparison stays on the request
/// path where it belongs.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    groups: Vec<WorkerGroup>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerGroup {
    /// Base URL of the selection service that indexes this fleet.
    selector: String,
    block_size: u32,
    max_num_batched_tokens: u64,
    total_kv_blocks: u64,
    workers: Vec<KvWorker>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct KvWorker {
    worker_id: u64,
    endpoint: String,
    /// data-parallel rank -> ZMQ endpoint. The selector's embedded indexer subscribes to
    /// these, which is how it learns what each worker has cached.
    kv_events_endpoints: std::collections::BTreeMap<String, String>,
}

/// Registers every fleet with its selector, retrying until the selectors are reachable.
///
/// Failure is fatal to config load rather than logged and ignored: a module that comes up
/// without registering produces a gateway that answers 503 to everything while looking
/// healthy, which is exactly the failure that cost a benchmark pass.
async fn register_workers(http: &reqwest::Client, reg: &Registration) -> Result<(), String> {
    for g in &reg.groups {
        let url = format!("{}/workers", g.selector.trim_end_matches('/'));
        for w in &g.workers {
            let body = serde_json::json!({
                "worker_id": w.worker_id,
                "endpoint": w.endpoint,
                "kv_events_endpoints": w.kv_events_endpoints,
                "block_size": g.block_size,
                "max_num_batched_tokens": g.max_num_batched_tokens,
                "total_kv_blocks": g.total_kv_blocks,
            });
            let mut last = String::new();
            let mut ok = false;
            // ~120 s: cold_everything restarts the selectors immediately before Envoy, so the
            // first few attempts legitimately race their startup.
            for attempt in 0..60 {
                match http.post(&url).json(&body).send().await {
                    Ok(r) if r.status().is_success() => {
                        ok = true;
                        break;
                    }
                    Ok(r) => last = format!("status {}", r.status()),
                    Err(e) => last = e.to_string(),
                }
                if attempt == 0 {
                    eprintln!(
                        "generic_pipeline: waiting for selector {}: {last}",
                        g.selector
                    );
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            if !ok {
                return Err(format!(
                    "register worker {} with {}: {last}",
                    w.worker_id, g.selector
                ));
            }
        }
        eprintln!(
            "generic_pipeline: registered {} workers with {}",
            g.workers.len(),
            g.selector
        );
    }
    Ok(())
}

fn default_conns() -> usize {
    4
}
fn default_max_body() -> usize {
    32 * 1024 * 1024
}

/// Loaded once per filter config, shared by every request.
///
/// Held behind a single Arc so creating a filter is one atomic increment rather than a fresh
/// allocation: `new_http_filter` runs on every request, and the previous version rebuilt this
/// struct and wrapped it in a new Arc each time.
struct Shared {
    prepared: Arc<Prepared>,
    transport: Arc<CompositeTransport>,
    runtime: Arc<tokio::runtime::Runtime>,
    max_request_bytes: usize,
    transport_mode: TransportMode,
    /// Empty means "do all I/O ourselves", which is the behaviour this module shipped with.
    clusters: Arc<std::collections::HashMap<String, String>>,
    /// See [`ModuleConfig::cluster_from_authority`].
    cluster_from_authority: bool,
    callout_timeout_ms: u64,
    /// Retry policy for callouts, and the cluster prefixes it applies to. Both empty by
    /// default; see [`ModuleConfig::callout_retry_on`] for why this is not a global switch.
    callout_retry_on: String,
    #[cfg(feature = "envoy-callout-options")]
    callout_num_retries: u32,
    #[cfg(feature = "envoy-callout-options")]
    callout_per_try_timeout_ms: u64,
    retry_clusters: Vec<String>,
    unary_grpc_via_callout: bool,
}

struct GenericConfig {
    shared: Arc<Shared>,
}

fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    _cfg: &mut EC,
    name: &str,
    config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    if name != "generic_pipeline" {
        return None;
    }
    let cfg: ModuleConfig = serde_json::from_slice(config)
        .map_err(|e| eprintln!("generic_pipeline: bad config: {e}"))
        .ok()?;
    if cfg.transport_mode == TransportMode::EnvoyCallouts
        && cfg.upstream_clusters.is_empty()
        && !cfg.cluster_from_authority
    {
        eprintln!(
            "generic_pipeline: envoy_callouts requires upstream_clusters or cluster_from_authority"
        );
        return None;
    }
    #[cfg(not(feature = "envoy-callout-options"))]
    if cfg.unary_grpc_via_callout || !cfg.callout_retry_on.is_empty() {
        eprintln!(
            "generic_pipeline: unary gRPC callouts/retries require the envoy-callout-options feature and pinned Envoy ABI patch"
        );
        return None;
    }
    #[cfg(feature = "envoy-callout-options")]
    if (cfg.unary_grpc_via_callout || !cfg.callout_retry_on.is_empty())
        && !callout_with_options_available()
    {
        eprintln!("generic_pipeline: Envoy does not provide the required callout-options ABI");
        return None;
    }

    let prepared = load_pipeline(&cfg.pipeline_file)
        .map_err(|e| eprintln!("generic_pipeline: {e}"))
        .ok()?;
    let descriptor = std::fs::read(&cfg.grpc_descriptor_file)
        .map_err(|e| {
            eprintln!(
                "generic_pipeline: descriptor {}: {e}",
                cfg.grpc_descriptor_file
            )
        })
        .ok()?;
    let grpc = GrpcTransport::new(&descriptor)
        .map_err(|e| eprintln!("generic_pipeline: descriptor pool: {e}"))
        .ok()?;
    let http = reqwest::Client::builder()
        // Same shape the agentgateway host uses for its selector leg, so the HTTP leg is not
        // the slower one by accident.
        .http2_prior_knowledge()
        .pool_max_idle_per_host(8192)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .build()
        .map_err(|e| eprintln!("generic_pipeline: http client: {e}"))
        .ok()?;

    // Its own runtime: Envoy's worker threads must never block, and the pipeline awaits
    // network I/O. Threads are bounded rather than defaulted so the module cannot quietly
    // take the whole box from Envoy's own workers.
    let workers: usize = std::env::var("GENERIC_PIPELINE_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("generic-pipeline")
        .enable_all()
        .build()
        .map_err(|e| eprintln!("generic_pipeline: runtime: {e}"))
        .ok()?;

    // Before any traffic: the selector must know the fleet, or every route step 503s.
    if let Some(path) = cfg.registration_file.as_deref() {
        let text = std::fs::read_to_string(path)
            .map_err(|e| eprintln!("generic_pipeline: registration {path}: {e}"))
            .ok()?;
        let reg: Registration = serde_json::from_str(&text)
            .map_err(|e| eprintln!("generic_pipeline: registration {path}: {e}"))
            .ok()?;
        runtime
            .block_on(register_workers(&http, &reg))
            .map_err(|e| eprintln!("generic_pipeline: {e}"))
            .ok()?;
    }

    eprintln!(
        "generic_pipeline: loaded {} ({} steps), {} grpc conns, {workers} runtime threads",
        cfg.pipeline_file,
        prepared.pipeline().steps.len(),
        cfg.grpc_conns
    );
    Some(Box::new(GenericConfig {
        shared: Arc::new(Shared {
            prepared: Arc::new(prepared),
            transport: Arc::new(CompositeTransport { grpc, http }),
            runtime: Arc::new(runtime),
            max_request_bytes: cfg.max_request_bytes,
            transport_mode: cfg.transport_mode,
            clusters: Arc::new(cfg.upstream_clusters),
            cluster_from_authority: cfg.cluster_from_authority,
            callout_timeout_ms: cfg.callout_timeout_ms,
            callout_retry_on: cfg.callout_retry_on,
            #[cfg(feature = "envoy-callout-options")]
            callout_num_retries: cfg.callout_num_retries,
            #[cfg(feature = "envoy-callout-options")]
            callout_per_try_timeout_ms: cfg.callout_per_try_timeout_ms,
            retry_clusters: cfg.retry_clusters,
            unary_grpc_via_callout: cfg.unary_grpc_via_callout,
        }),
    }))
}

fn new_http_filter_per_route_config_fn(_n: &str, _c: &[u8]) -> Option<Box<dyn std::any::Any>> {
    None
}

/// Resolves every `openapi:` the pipeline references, relative to the pipeline file, and
/// validates all bindings. Anything wrong fails here, at load, rather than per request.
fn load_pipeline(path: &str) -> Result<Prepared, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("pipeline {path}: {e}"))?;
    let pipeline: Pipeline =
        serde_yaml::from_str(&text).map_err(|e| format!("pipeline {path}: {e}"))?;
    let dir = std::path::Path::new(path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut specs = std::collections::BTreeMap::new();
    for rel in referenced_specs(&pipeline) {
        let p = dir.join(&rel);
        let s = std::fs::read_to_string(&p).map_err(|e| format!("spec {}: {e}", p.display()))?;
        let doc = Document::from_yaml(&s).map_err(|e| format!("spec {}: {e}", p.display()))?;
        specs.insert(rel, doc);
    }
    Prepared::new(pipeline, &specs).map_err(|e| format!("prepare: {e}"))
}

/// gRPC or HTTP according to the operation's own contract: a component declares `x-grpc` or
/// it does not, and the pipeline never says which protocol anything speaks.
struct CompositeTransport {
    grpc: GrpcTransport,
    http: reqwest::Client,
}

impl CompositeTransport {
    /// The protobuf codec, for a host that carries gRPC itself. See `HostTransport::call_grpc`.
    fn grpc(&self) -> &GrpcTransport {
        &self.grpc
    }
}

#[async_trait::async_trait]
impl Transport for CompositeTransport {
    async fn call(&self, req: Request) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
        if req.extensions.contains_key("x-grpc") {
            return self.grpc.call(req).await;
        }
        let resp = self.http.post(&req.url).json(&req.body).send().await?;
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        Ok(Reply {
            status,
            payload: Payload::Unary(body),
        })
    }

    async fn sleep_ms(&self, ms: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

/// One upstream call, parked by the runtime for the Envoy worker thread to issue.
///
/// The pipeline's calls cannot be made from the runtime thread: `send_http_callout` needs
/// `&mut EnvoyHttpFilter`, which is neither `Send` nor available off the worker. So the call
/// is described here, handed over through [`Bridge`], and the answer comes back on a oneshot.
struct Pending {
    cluster: String,
    path: String,
    authority: String,
    body: Vec<u8>,
    timeout_ms: u64,
    reply: tokio::sync::oneshot::Sender<Result<(u16, Vec<u8>), String>>,
}

/// One gRPC call carried over an Envoy cluster, parked for the Envoy thread to start.
///
/// gRPC goes through `start_http_stream` rather than `send_http_callout` for one reason:
/// only the stream API delivers TRAILERS, and gRPC reports its status in trailers. The unary
/// callout hands back headers and a body and nothing else -- checked in `abi.h`, not just in
/// the Rust SDK -- so a unary gRPC callout would have no way to read `grpc-status`.
struct PendingGrpc {
    cluster: String,
    path: String,
    authority: String,
    /// Length-prefixed gRPC frame: 1 compression byte + 4-byte big-endian length + protobuf.
    body: Vec<u8>,
    reply: GrpcReply,
}

/// Where a gRPC response goes: one message, or many as they arrive.
///
/// The decode worker streams ~50 token frames per request, so a streaming reply cannot be
/// accumulated and handed back at the end -- the interpreter projects each item as it arrives
/// and the client is waiting on it. Raw frame BYTES are what crosses this boundary, not
/// decoded `Value`s, so protobuf decoding happens on the runtime side rather than on Envoy's
/// worker thread, which must not do avoidable work.
enum GrpcReply {
    Unary(tokio::sync::oneshot::Sender<Result<Vec<u8>, String>>),
    Stream(tokio::sync::mpsc::Sender<Result<Vec<u8>, String>>),
}

/// Response state for one in-flight gRPC stream, accumulated across callbacks.
#[derive(Default)]
struct GrpcStream {
    /// Body bytes so far. gRPC frames can split across data callbacks, so this is reassembled
    /// rather than assumed to arrive whole.
    buf: Vec<u8>,
    /// Frames received while the consumer's channel was full.
    ///
    /// Held rather than dropped, and drained in order before any newer frame, because a token
    /// stream delivered out of order is a scrambled answer.
    backlog: std::collections::VecDeque<Vec<u8>>,
    /// Set only when the BACKLOG exceeded its cap -- i.e. the consumer stopped for good, not
    /// merely fell behind. Reported as an error rather than silently truncating a generation.
    overflowed: bool,
    /// `grpc-status` from headers or trailers; a gRPC error is a 200 with a non-zero status,
    /// so ignoring this would turn every upstream failure into an empty success.
    status: Option<i32>,
    message: String,
}

/// The runtime <-> Envoy-thread channel for upstream calls.
///
/// The reverse direction already existed -- SSE frames go back via `scheduler.commit` -- and
/// this is the same mechanism pointed the other way.
#[derive(Default)]
struct Bridge {
    /// Parked plain-HTTP calls the Envoy thread has not issued yet.
    outbox: Vec<Pending>,
    /// Parked gRPC calls.
    outbox_grpc: Vec<PendingGrpc>,
    /// Issued calls, keyed by the handle Envoy gave us, awaiting their response.
    inflight: std::collections::HashMap<
        u64,
        tokio::sync::oneshot::Sender<Result<(u16, Vec<u8>), String>>,
    >,
    /// Issued gRPC streams, keyed by stream handle.
    streams: std::collections::HashMap<u64, (GrpcStream, GrpcReply)>,
    /// Unary gRPC issued over the CALLOUT api rather than the stream api, keyed by callout
    /// handle. Separate from `inflight` because the answer needs unframing and a `grpc-status`
    /// check, and its reply channel carries bytes rather than `(status, body)`.
    inflight_grpc:
        std::collections::HashMap<u64, tokio::sync::oneshot::Sender<Result<Vec<u8>, String>>>,
}

/// Starts a child span for one hop, if tracing is on, and returns the W3C `traceparent` that
/// continues it upstream.
///
/// The header is built here rather than left to Envoy because a CALLOUT is not a routed
/// request: `AsyncClient::StreamOptions` can carry a parent span (`setParentSpan`,
/// `setChildSpanName`) and would then inject context itself, but the dynamic-module ABI does
/// not expose those. Without this the upstream service starts a NEW trace, so a trace shows the
/// gateway's view and the worker's view as two unrelated things.
///
/// `'static` is manufactured here deliberately; see [`GenericFilter::spans`] for why it is
/// sound and what upholds it.
fn hop_span<EHF: EnvoyHttpFilter>(
    envoy: &EHF,
    op: &str,
    cluster: &str,
    path: &str,
) -> (Option<Box<dyn EnvoyChildSpan>>, Option<String>) {
    let Some(s) = envoy.spawn_child_span(op) else {
        return (None, None);
    };
    s.set_tag("upstream.cluster", cluster);
    s.set_tag("http.path", path);
    s.set_tag("component", "generic_pipeline");
    // Built from the ACTIVE span, not from the child just created: the SDK exposes
    // `get_trace_id`/`get_span_id` on `EnvoySpan` and not on `EnvoyChildSpan`, so a child's own
    // ids are not readable. The consequence is that the upstream's span parents to the REQUEST
    // span rather than to this hop's span -- the trace is connected and complete, one level
    // flatter than ideal. Reading a child's ids would need one more ABI accessor.
    //
    // `01` = sampled. The span exists because Envoy decided to trace this request, so
    // propagating "not sampled" would ask the upstream to discard the half of the trace that
    // explains what the gateway was waiting for.
    let tp = envoy
        .get_active_span()
        .and_then(|a| match (a.get_trace_id(), a.get_span_id()) {
            (Some(t), Some(p)) => Some(format!("00-{t}-{p}-01")),
            _ => None,
        });
    let span =
        unsafe { std::mem::transmute::<Box<dyn EnvoyChildSpan + '_>, Box<dyn EnvoyChildSpan>>(s) };
    (Some(span), tp)
}

/// Ends a hop's span, recording how it went.
fn finish_span(span: Option<Box<dyn EnvoyChildSpan>>, outcome: &str, detail: Option<&str>) {
    let Some(mut s) = span else { return };
    s.set_tag("outcome", outcome);
    if let Some(d) = detail {
        // Truncated: a span tag is not the place for a whole upstream body, and some
        // backends drop an entire span whose tag exceeds their limit.
        s.set_tag("error", &d.chars().take(256).collect::<String>());
    }
    s.finish();
}

/// Delivers an error to whichever reply shape a parked gRPC call is waiting on.
fn fail_grpc(reply: GrpcReply, msg: String) {
    match reply {
        GrpcReply::Unary(tx) => {
            let _ = tx.send(Err(msg));
        }
        GrpcReply::Stream(tx) => {
            let _ = tx.try_send(Err(msg));
        }
    }
}

/// Reads `grpc-status` / `grpc-message` out of headers or trailers, if present.
fn read_grpc_status(hs: &[(EnvoyBuffer, EnvoyBuffer)], st: &mut GrpcStream) {
    for (k, v) in hs {
        match k.as_slice() {
            b"grpc-status" => {
                if let Ok(s) = String::from_utf8_lossy(v.as_slice()).parse::<i32>() {
                    st.status = Some(s);
                }
            }
            b"grpc-message" => st.message = String::from_utf8_lossy(v.as_slice()).into_owned(),
            _ => {}
        }
    }
}

/// Wraps a protobuf message in a gRPC length-prefixed frame.
fn grpc_frame(msg: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(5 + msg.len());
    b.push(0); // not compressed
    b.extend_from_slice(&(msg.len() as u32).to_be_bytes());
    b.extend_from_slice(msg);
    b
}

/// Length of the first COMPLETE gRPC frame's payload, if one has fully arrived.
fn grpc_frame_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 5 {
        return None;
    }
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    (buf.len() >= 5 + len).then_some(len)
}

/// Takes the first complete gRPC frame's payload out of a response buffer.
///
/// Returns None when fewer than a full frame has arrived; the caller keeps accumulating.
fn grpc_unframe(buf: &[u8]) -> Option<&[u8]> {
    grpc_frame_len(buf).map(|n| &buf[5..5 + n])
}

/// Routes the pipeline's calls through the HOST's cluster machinery instead of our own sockets.
///
/// This is the whole point of the exercise: a call issued with `send_http_callout` against a
/// named cluster gets that cluster's load balancing, outlier detection, circuit breakers,
/// retries, per-try timeouts, upstream TLS and `upstream_rq_*` stats. A call issued over our
/// own reqwest client gets none of them, and does not even appear in Envoy's stats.
///
/// `Transport` does not change, and neither does the interpreter: the pipeline still says
/// "call this operation on this target" and the host still decides how. Only the answer to
/// "how" moves.
///
/// An authority with no cluster mapped falls back to direct I/O, so hops can be moved over one
/// at a time and each one measured on its own.
struct HostTransport {
    bridge: Arc<Mutex<Bridge>>,
    scheduler: Arc<Mutex<Option<Box<dyn EnvoyHttpFilterScheduler>>>>,
    /// authority ("selector-decode:8083") -> Envoy cluster name.
    clusters: Arc<std::collections::HashMap<String, String>>,
    /// Derive the cluster from the authority when the map has no entry. See
    /// [`ModuleConfig::cluster_from_authority`].
    cluster_from_authority: bool,
    /// Used for any hop not mapped to a cluster, and for gRPC, which needs the stream API
    /// rather than the callout API because only that one delivers trailers -- and gRPC carries
    /// its status in trailers.
    fallback: Arc<CompositeTransport>,
    timeout_ms: u64,
    /// Callout mode never silently escapes to module-owned sockets.
    require_cluster: bool,
}

impl HostTransport {
    /// Which Envoy cluster carries this authority.
    ///
    /// The explicit map wins, then the naming rule (`kvworker7:8081` -> `kvworker7`), then
    /// nothing -- and nothing means the hop goes out over our own sockets. That fallback is the
    /// dangerous one: it does not fail, it just stops using Envoy, which is why the harness
    /// asserts `upstream_rq_total` rather than trusting this to be right.
    fn cluster_for(&self, authority: &str) -> Option<String> {
        if let Some(c) = self.clusters.get(authority) {
            return Some(c.clone());
        }
        if self.cluster_from_authority {
            let host = authority.split(':').next().unwrap_or(authority);
            if !host.is_empty() {
                return Some(host.to_string());
            }
        }
        None
    }
}

/// Splits `http://host:port/path` into (authority, path) without pulling in a URL parser.
fn split_url(url: &str) -> (String, String) {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    match rest.find('/') {
        Some(i) => (rest[..i].to_string(), rest[i..].to_string()),
        None => (rest.to_string(), "/".to_string()),
    }
}

#[async_trait::async_trait]
impl Transport for HostTransport {
    async fn call(&self, req: Request) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
        if req.extensions.contains_key("x-grpc") {
            return self.call_grpc(req).await;
        }
        let (authority, path) = split_url(&req.url);
        let Some(cluster) = self.cluster_for(&authority) else {
            if self.require_cluster {
                return Err(format!("no Envoy cluster mapped for authority `{authority}`").into());
            }
            return self.fallback.call(req).await;
        };

        let body = serde_json::to_vec(&req.body)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut b = self.bridge.lock().expect("bridge mutex");
            b.outbox.push(Pending {
                cluster: cluster.clone(),
                path,
                authority,
                body,
                timeout_ms: self.timeout_ms,
                reply: tx,
            });
        }
        if let Some(s) = self.scheduler.lock().expect("scheduler mutex").as_ref() {
            s.commit(OUTBOUND_EVENT);
        }
        // If the filter is gone the sender is dropped and this resolves to an error rather
        // than hanging, which is also how a cancelled request stops doing work.
        let (status, bytes) = rx
            .await
            .map_err(|_| "filter went away before the callout completed")??;
        let payload: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        Ok(Reply {
            status,
            payload: Payload::Unary(payload),
        })
    }

    async fn sleep_ms(&self, ms: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

impl HostTransport {
    /// Carries one gRPC call over an Envoy cluster.
    ///
    /// Unary only, deliberately. A server-streaming reply (the decode worker's token stream)
    /// arrives as many frames across many `on_http_stream_data` callbacks and has to be fed
    /// into the interpreter's `Payload::Stream` as it arrives; that is a larger change than
    /// this one and is staged after it. Streaming methods fall back to tonic, so the pipeline
    /// runs either way and the hops that HAVE moved can be measured on their own.
    async fn call_grpc(
        &self,
        req: Request,
    ) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>> {
        let b = req
            .extensions
            .get("x-grpc")
            .ok_or("x-grpc binding missing")?;
        let (Some(_service), Some(_method)) = (
            b.get("service").and_then(Value::as_str),
            b.get("method").and_then(Value::as_str),
        ) else {
            return Err("x-grpc binding malformed".into());
        };
        let (authority, _) = split_url(&req.url);
        let grpc = self.fallback.grpc();
        let Some(cluster) = self.cluster_for(&authority) else {
            if self.require_cluster {
                return Err(format!("no Envoy cluster mapped for authority `{authority}`").into());
            }
            return self.fallback.call(req).await;
        };
        let streaming = grpc.is_server_streaming(&req)?;

        // This crate still owns protobuf; only the carrying moves. The codec is tested to
        // produce the same bytes the transport would have sent.
        let path = grpc.method_path(&req)?;
        let msg = grpc.encode_input(&req)?;

        // Bounded, for the same reason the SSE channel is: an unbounded queue lets a fast
        // worker outrun a slow client and buffer a whole generation in memory.
        let (stx, srx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, String>>(256);
        let (utx, urx) = tokio::sync::oneshot::channel();
        {
            let mut br = self.bridge.lock().expect("bridge mutex");
            br.outbox_grpc.push(PendingGrpc {
                cluster: cluster.clone(),
                path,
                authority,
                body: grpc_frame(&msg),
                reply: if streaming {
                    GrpcReply::Stream(stx)
                } else {
                    GrpcReply::Unary(utx)
                },
            });
        }
        if let Some(s) = self.scheduler.lock().expect("scheduler mutex").as_ref() {
            s.commit(OUTBOUND_EVENT);
        }

        if !streaming {
            let payload = urx
                .await
                .map_err(|_| "filter went away before the gRPC call completed")??;
            let value = grpc.decode_output(&req, &payload)?;
            return Ok(Reply {
                status: 200,
                payload: Payload::Unary(value),
            });
        }

        // Decoding happens HERE, on the runtime, not on Envoy's worker thread: the worker
        // forwards raw frames and must not do work it can hand off.
        let codec = self.fallback.clone();
        let graph_request = req.clone();
        let stream = futures::stream::unfold(srx, move |mut rx| {
            let codec = codec.clone();
            let graph_request = graph_request.clone();
            async move {
                let item = rx.recv().await?;
                let out = match item {
                    Ok(bytes) => codec
                        .grpc()
                        .decode_output(&graph_request, &bytes)
                        .map_err(|e| format!("decode: {e}")),
                    Err(e) => Err(e),
                };
                Some((out, rx))
            }
        });
        Ok(Reply {
            status: 200,
            payload: Payload::Stream(Box::pin(stream)),
        })
    }
}

/// What the runtime thread hands back to the Envoy thread.
#[derive(Default)]
struct Outbox {
    /// Frames emitted so far. The trailing usage chunk reports this as completion_tokens.
    n_out: u64,
    /// Accumulated completion text, for a non-streaming request.
    text: String,
    /// Accumulated reasoning, kept apart from `text` for the same reason it is split upstream.
    reasoning: String,
    /// Parsed tool calls, as the JSON array the sidecar produced. Empty when there were none.
    tool_calls: String,
    /// SSE frames ready to write.
    chunks: Vec<Vec<u8>>,
    /// Set once the pipeline has finished; carries an error message if it failed.
    done: Option<Option<String>>,
    /// Set once headers have been written, so they are written exactly once.
    headers_sent: bool,
    /// The DOWNSTREAM write buffer is over its high watermark: the client is not draining.
    ///
    /// Envoy reports this through `on_downstream_above_write_buffer_high_watermark`. While it
    /// holds, frames are kept here instead of being handed to Envoy, so its buffer gets a
    /// chance to drain rather than being fed from a producer that never pauses.
    congested: bool,
    /// Bytes parked in `chunks` while congested.
    ///
    /// Tracked because the queue must be BOUNDED. Pausing writes turns a downstream stall into
    /// memory growth in this process, and unbounded growth under load is a worse failure than
    /// the one being fixed -- one slow client could take the gateway down for everyone.
    parked_bytes: usize,
}

/// Receives the interpreter's per-item projections and turns them into SSE frames.
///
/// Framing lives here rather than in the pipeline for the same reason it does in the
/// agentgateway host: the interpreter decides WHAT to emit, the host decides how it is
/// framed on the wire. Keeping the framing identical between hosts is what makes a
/// per-token streaming benchmark comparable.
/// Everything before the delta. Constant.
///
/// Key order is ALPHABETICAL, matching what `serde_json::json!` emitted before: its default
/// map is a BTreeMap, so the bytes on the wire were sorted. Preserving that exactly means this
/// change is invisible to every client rather than merely equivalent -- asserted byte-for-byte
/// in the tests.
const FRAME_PREFIX: &[u8] = br#"data: {"choices":[{"delta":{"content":"#;

/// Everything after the delta, built once per request: the fields that never vary within a
/// response, with the id and model escaped here so the per-chunk path escapes only the delta.
fn frame_suffix(id: &str, created: u64, model: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(120 + id.len() + model.len());
    p.extend_from_slice(br#"},"finish_reason":null,"index":0}],"created":"#);
    p.extend_from_slice(created.to_string().as_bytes());
    p.extend_from_slice(br#","id":"#);
    serde_json::to_writer(&mut p, id).expect("writing to a Vec cannot fail");
    p.extend_from_slice(br#","model":"#);
    serde_json::to_writer(&mut p, model).expect("writing to a Vec cannot fail");
    p.extend_from_slice(br#","object":"chat.completion.chunk"}"#);
    p.extend_from_slice(b"\n\n");
    p
}

struct SseSink {
    /// False when the client asked for a single JSON object; the sink then accumulates the
    /// deltas and the Envoy thread emits one response at completion.
    streaming: bool,
    outbox: Arc<Mutex<Outbox>>,
    /// Precomputed per request; see [`frame_suffix`].
    frame_suffix: Vec<u8>,
    scheduler: Arc<Mutex<Option<Box<dyn EnvoyHttpFilterScheduler>>>>,
    id: String,
    created: u64,
    model: String,
}

impl SseSink {
    fn wake(&self) {
        if let Some(s) = self.scheduler.lock().expect("scheduler mutex").as_ref() {
            s.commit(PIPELINE_EVENT);
        }
    }
}

#[async_trait::async_trait]
impl Sink for SseSink {
    async fn item(&self, v: Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.streaming {
            // Dynamo's ChatWorkerFacade already applied the canonical response
            // generator, reasoning/tool parsers, finish reasons, and usage policy.
            // The host owns only SSE framing; it must not reconstruct an OpenAI
            // chunk or reinterpret model output.
            let mut frame = Vec::with_capacity(256);
            frame.extend_from_slice(b"data: ");
            serde_json::to_writer(&mut frame, &v)?;
            frame.extend_from_slice(b"\n\n");
            {
                let mut outbox = self.outbox.lock().expect("outbox mutex");
                outbox.n_out += 1;
                outbox.chunks.push(frame);
            }
            self.wake();
            return Ok(());
        }
        let delta = v.get("delta").and_then(Value::as_str).unwrap_or_default();
        let reasoning = v
            .get("reasoning")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let tool_calls = v
            .get("toolCalls")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // The frame is assembled from a prefix computed ONCE per request, the escaped delta,
        // and a constant suffix -- no `Value` tree per chunk.
        //
        // Every field but `content` is identical in all ~50 frames of a response, and
        // building a nested serde_json object and Display-formatting it per chunk was the
        // largest remaining cost: a profile with the token array already removed put
        // serde_json at 44.5% of gateway CPU, and at 6,890 rps x 50 chunks this path was
        // constructing ~345,000 JSON trees a second to express a shape that never changes.
        //
        // `to_writer` on the &str is what keeps this correct rather than clever: it emits the
        // quotes and the escaping serde_json itself would, so a delta containing a quote, a
        // backslash or a control character is encoded identically to before.
        if !self.streaming {
            // No framing and no wakeup per token: nothing can be sent until the whole
            // completion exists, so waking the Envoy thread per delta would be pure cost.
            let mut o = self.outbox.lock().expect("outbox mutex");
            o.n_out += 1;
            o.text.push_str(delta);
            o.reasoning.push_str(reasoning);
            if !tool_calls.is_empty() {
                o.tool_calls = tool_calls.to_string();
            }
            return Ok(());
        }
        // The fast path below builds one shape with no JSON tree per chunk, and it is the
        // overwhelmingly common one: reasoning and tool calls are absent from almost every
        // frame. When either IS present the frame is assembled the ordinary way -- correctness
        // over a micro-optimisation for a case that does not dominate.
        if !reasoning.is_empty() || !tool_calls.is_empty() {
            let mut d = serde_json::Map::new();
            if !delta.is_empty() {
                d.insert("content".into(), Value::String(delta.into()));
            }
            if !reasoning.is_empty() {
                d.insert("reasoning_content".into(), Value::String(reasoning.into()));
            }
            if !tool_calls.is_empty() {
                // Emitted as the parsed array the sidecar produced, not as a string.
                d.insert(
                    "tool_calls".into(),
                    serde_json::from_str(tool_calls).unwrap_or(Value::Null),
                );
            }
            let chunk = serde_json::json!({
                "choices": [{"delta": Value::Object(d), "finish_reason": Value::Null, "index": 0}],
                "created": self.created,
                "id": self.id,
                "model": self.model,
                "object": "chat.completion.chunk"
            });
            let mut frame = Vec::with_capacity(160);
            frame.extend_from_slice(b"data: ");
            serde_json::to_writer(&mut frame, &chunk)?;
            frame.extend_from_slice(b"\n\n");
            {
                let mut o = self.outbox.lock().expect("outbox mutex");
                o.n_out += 1;
                o.chunks.push(frame);
            }
            self.wake();
            return Ok(());
        }
        let mut frame =
            Vec::with_capacity(FRAME_PREFIX.len() + delta.len() + self.frame_suffix.len() + 8);
        frame.extend_from_slice(FRAME_PREFIX);
        serde_json::to_writer(&mut frame, delta)?;
        frame.extend_from_slice(&self.frame_suffix);
        {
            let mut o = self.outbox.lock().expect("outbox mutex");
            o.n_out += 1;
            o.chunks.push(frame);
        }
        // Wake on EVERY frame. Two attempts to avoid it have now failed, differently.
        //
        // The first COALESCED, delaying a frame in the hope another followed, and measured
        // worse -- 3,780 rps without, 3,488 with -- because at fixed client concurrency
        // throughput is concurrency/latency, so holding a token back trades the wrong currency.
        //
        // The second suppressed only REDUNDANT commits -- one already issued and not yet
        // drained -- so nothing was delayed and ~250,000 cross-thread commits a second went
        // away. It changed nothing measurable. A/B at ISL 4000, us/req:
        //
        //   OSL      1        50       200
        //   wake     1,265    2,779    6,781     slope 27.7 us/chunk
        //   suppress 1,259    2,690    6,816     slope 27.9 us/chunk
        //
        // The slopes are identical, which is the number that matters: a per-chunk effect has
        // to GROW with output length, and this does not. The -3.2% at OSL 50 is noise, as the
        // OSL 200 cell shows.
        //
        // So the per-chunk cost -- 21.3 us against the compiled-in arm's 10.6 us, of which
        // only ~0.9 us is conversion -- is not the wakeup. It is still unattributed.
        self.wake();
        Ok(())
    }
}

struct GenericFilter {
    cfg: Arc<Shared>,
    outbox: Arc<Mutex<Outbox>>,
    scheduler: Arc<Mutex<Option<Box<dyn EnvoyHttpFilterScheduler>>>>,
    started: AtomicBool,
    model: String,
    /// The client asked for a trailing usage chunk via stream_options.include_usage.
    ///
    /// WITHOUT it a load generator running --use-server-token-count counts zero output
    /// tokens. The dangerous part is that the arm still produces an rps number: a plausible
    /// figure measuring an unequal comparison, because the agentgateway host DOES emit this
    /// chunk. Here it surfaced as osl=None and an empty results cell -- 15,658 requests, zero
    /// errors, and no throughput number at all.
    want_usage: bool,
    /// Echoed into every chunk, including the usage one.
    chunk_id: String,
    created: u64,
    /// True when this filter answers the request itself rather than letting it route.
    owned: bool,
    /// Upstream calls in flight through Envoy's clusters, if host-managed transport is on.
    bridge: Arc<Mutex<Bridge>>,
    /// One tracing child span per in-flight hop, keyed the same way the hop is.
    ///
    /// Without these a trace shows the whole orchestration as ONE span: five upstream calls,
    /// no per-hop timing, no way to tell a slow tokenizer from a slow worker. Envoy's tracing
    /// ABI supported this all along (`spawn_child_span`); the module never used it.
    ///
    /// These live on the FILTER and not on the shared `Bridge`, which is not a detail. A span
    /// holds raw pointers into the Envoy filter, so it may only be touched on the Envoy worker
    /// thread -- and `Bridge` is shared with the tokio runtime. The compiler enforced this:
    /// putting them in `Bridge` makes it non-`Send` and `runtime.spawn` stops compiling. Here
    /// every access is through `&mut self` in an Envoy-thread callback, so the rule is
    /// structural rather than remembered.
    ///
    /// Storing them across callbacks is sound despite the SDK's borrowing signature:
    /// `EnvoyChildSpanImpl` holds two raw pointers and a flag, and the `'a` is an SDK safety
    /// convention rather than a real borrow. What upholds it is that no span outlives its
    /// filter -- each is finished in one of this filter's callbacks, and `Drop` finishes the
    /// rest.
    spans: std::collections::HashMap<u64, Box<dyn EnvoyChildSpan>>,
    stream_spans: std::collections::HashMap<u64, Box<dyn EnvoyChildSpan>>,
    /// The request's token budget, when it set one. Used to decide `finish_reason`.
    max_tokens: Option<u64>,
    /// False when the client asked for `stream: false`.
    ///
    /// The module used to answer SSE regardless, which is a protocol defect: a caller that
    /// asked for one JSON object got an event stream. It also made the ordinary readiness
    /// probe report this arm dead while it was serving perfectly, so the harness needed a
    /// second, SSE-aware probe to work around a bug rather than fix it.
    streaming: bool,
}

impl Drop for GenericFilter {
    /// Finishes any span still open when the request ends.
    ///
    /// This is what makes storing spans with a manufactured `'static` sound (see
    /// [`Bridge::spans`]): a span holds a raw pointer to the Envoy filter, so none may outlive
    /// it. A cancelled request is the case that matters -- the client disconnects, hops are
    /// still in flight, and their spans would otherwise be both leaked in the tracer and left
    /// holding a pointer to a filter that is going away.
    fn drop(&mut self) {
        for (_, s) in self.spans.drain().chain(self.stream_spans.drain()) {
            finish_span(Some(s), "cancelled", None);
        }
    }
}

impl GenericFilter {
    /// Why the generation ended.
    ///
    /// `length` when the token budget was spent, `stop` otherwise. This was a hardcoded "stop",
    /// which reports a truncated answer as a complete one -- see `k8s/CONFORMANCE.md`.
    fn finish_reason(&self, produced: u64) -> &'static str {
        match self.max_tokens {
            Some(cap) if produced >= cap => "length",
            _ => "stop",
        }
    }

    /// Does this hop get a retry policy?
    ///
    /// Only hops whose cluster matches a configured prefix. The worker clusters deliberately
    /// do not: KV routing CHOSE that worker because it holds the prefix, so a retry both
    /// discards the cache hit and, on the decode stream, could duplicate tokens already
    /// delivered to the client.
    fn retryable(&self, cluster: &str) -> bool {
        !self.cfg.callout_retry_on.is_empty()
            && self
                .cfg
                .retry_clusters
                .iter()
                .any(|p| cluster.starts_with(p.as_str()))
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for GenericConfig {
    fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(GenericFilter {
            cfg: self.shared.clone(),
            outbox: Arc::new(Mutex::new(Outbox::default())),
            scheduler: Arc::new(Mutex::new(None)),
            started: AtomicBool::new(false),
            model: String::new(),
            want_usage: false,
            chunk_id: String::new(),
            created: 0,
            owned: false,
            bridge: Arc::new(Mutex::new(Bridge::default())),
            max_tokens: None,
            spans: std::collections::HashMap::new(),
            stream_spans: std::collections::HashMap::new(),
            streaming: true,
        })
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for GenericFilter {
    fn on_request_headers(
        &mut self,
        envoy: &mut EHF,
        _end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_headers_status {
        // StopIteration, not Continue. The router filter runs last but acts on HEADERS, so a
        // route with direct_response answers before the body ever arrives -- which is exactly
        // what happened: every request came back 404 from the catch-all route while this
        // filter was still waiting for a body it would never be given.
        //
        // Stopping here keeps the router out of it entirely for the paths this module owns,
        // and lets everything else fall through to normal routing.
        let path = envoy
            .get_request_header_value(":path")
            .map(|v| String::from_utf8_lossy(v.as_slice()).into_owned())
            .unwrap_or_default();
        if path.starts_with("/v1/chat/completions") {
            self.owned = true;
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
        } else {
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
        }
    }

    fn on_request_body(
        &mut self,
        envoy: &mut EHF,
        end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_body_status {
        if !self.owned {
            return abi::envoy_dynamic_module_type_on_http_filter_request_body_status::Continue;
        }
        if !end_of_stream {
            return abi::envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationAndBuffer;
        }
        if self.started.swap(true, Ordering::SeqCst) {
            return abi::envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }

        let size = envoy.get_buffered_request_body_size() + envoy.get_received_request_body_size();
        if size > self.cfg.max_request_bytes {
            envoy.send_response(
                413,
                &[],
                Some(b"request body is too large"),
                Some("generic_pipeline.too_large"),
            );
            return abi::envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }

        // Buffered body: StopIterationAndBuffer above means Envoy has accumulated the whole
        // request here by end_of_stream.
        let mut body = Vec::with_capacity(size);
        if let Some(bufs) = envoy.get_buffered_request_body() {
            for b in bufs {
                body.extend_from_slice(b.as_slice());
            }
        }
        let parsed: Value = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => {
                envoy.send_response(
                    400,
                    &[],
                    Some(b"invalid JSON body"),
                    Some("generic_pipeline.invalid_body"),
                );
                return abi::envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
            }
        };
        self.model = parsed
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.streaming = parsed
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.want_usage = parsed
            .get("stream_options")
            .and_then(|o| o.get("include_usage"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        // Needed to tell "the model stopped" from "it ran out of budget". Without it
        // `finish_reason` was the constant "stop", which tells a client the answer is complete
        // when it was truncated -- and a client uses exactly that field to decide whether to
        // continue. Found by diffing against Dynamo's frontend, not by reading this code: a
        // hardcoded "stop" reads fine until you compare it with a request that hit the cap.
        self.max_tokens = parsed
            .get("max_completion_tokens")
            .or_else(|| parsed.get("max_tokens"))
            .and_then(Value::as_u64);

        // A malformed request is the CLIENT's error, and must not be reported as a gateway
        // failure: a 502 invites a retry, which is exactly wrong here. The body follows
        // OpenAI's `{error: {message, type, code}}` so a client parsing errors structurally
        // sees what it expects.
        let bad = if !parsed.is_object() {
            Some("request body must be a JSON object")
        } else if !parsed
            .get("messages")
            .map(|m| m.is_array())
            .unwrap_or(false)
        {
            Some("`messages` is required and must be an array")
        } else if self.model.is_empty() {
            Some("`model` is required")
        } else {
            None
        };
        if let Some(msg) = bad {
            let body = serde_json::json!({
                "error": {"message": msg, "type": "invalid_request_error", "code": 400}
            });
            envoy.send_response(
                400,
                &[("content-type", b"application/json".as_slice())],
                Some(body.to_string().as_bytes()),
                Some("generic_pipeline.invalid_request"),
            );
            return abi::envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer;
        }

        // The scheduler is the only thing handed to the runtime thread; it is the SDK's
        // supported cross-thread wakeup and is marked Send for that purpose.
        // new_scheduler returns `impl EnvoyHttpFilterScheduler`, so it is boxed here to be
        // stored and shared with the runtime thread.
        *self.scheduler.lock().expect("scheduler mutex") =
            Some(Box::new(envoy.new_scheduler()) as Box<dyn EnvoyHttpFilterScheduler>);

        let prepared = self.cfg.prepared.clone();
        let transport = self.cfg.transport.clone();
        // Host-managed transport when any authority is mapped to a cluster, our own sockets
        // otherwise. Built per request because the bridge back to this filter is per request:
        // a callout belongs to the filter that issued it, which is also what makes a
        // destroyed filter cancel its own in-flight calls.
        // With `clusterFromAuthority` there may be no map at all and still be clusters to use,
        // which is the point of the rule: the control plane owns the fleet, not this config.
        let host_tx: Option<Arc<HostTransport>> =
            if self.cfg.transport_mode == TransportMode::Independent {
                None
            } else {
                Some(Arc::new(HostTransport {
                    bridge: self.bridge.clone(),
                    scheduler: self.scheduler.clone(),
                    clusters: self.cfg.clusters.clone(),
                    cluster_from_authority: self.cfg.cluster_from_authority,
                    fallback: transport.clone(),
                    timeout_ms: self.cfg.callout_timeout_ms,
                    require_cluster: true,
                }))
            };
        let outbox = self.outbox.clone();
        let scheduler = self.scheduler.clone();
        let model = self.model.clone();
        let streaming = self.streaming;
        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let id = next_request_id();
        self.chunk_id = id.clone();
        self.created = created;

        self.cfg.runtime.spawn(async move {
            let sink = SseSink {
                streaming,
                frame_suffix: frame_suffix(&id, created, &model),
                outbox: outbox.clone(),
                scheduler: scheduler.clone(),
                id: id.clone(),
                created,
                model,
            };
            // The interpreter's request shape is `{body: ...}`; the pipeline's `api:` block
            // names the operation this body belongs to.
            let request = serde_json::json!({ "id": id, "body": parsed });
            // One `Transport` or the other, chosen once. `run_with_sink` takes `&dyn
            // Transport`, so this costs a vtable dispatch per call and nothing per request.
            let outcome = match host_tx.as_deref() {
                Some(t) => prepared.run_with_sink(t, request, &sink).await,
                None => prepared.run_with_sink(&*transport, request, &sink).await,
            };
            let err = match outcome {
                Ok(_) => None,
                // Logged, not swallowed: a swallowed failure becomes a 200 carrying only
                // `data: [DONE]`, which is indistinguishable from a model that generated
                // nothing.
                Err(e) => {
                    eprintln!("generic_pipeline: pipeline failed: {e}");
                    Some(e.to_string())
                }
            };
            // Per-step timing, emitted periodically.
            //
            // `StepStats` has always been collected and never printed -- it appeared only in a
            // unit test -- so the interpreter's own profile was unavailable for the entire
            // optimisation campaign, and attribution was done by arithmetic on microbenchmarks
            // instead. That is how ~200 us of measured conversion cost came to be offered as an
            // explanation for a ~1,370 us per-request difference.
            //
            // Off by default and sampled by count rather than by time: a println on the
            // request path at 5,000 rps is itself a measurable cost, which is the mistake the
            // tokenizer counters already made twice.
            let every = *PIPESTATS_EVERY;
            if every > 0 {
                let n = prepared.stats.requests();
                if n > 0 && n % every == 0 {
                    eprintln!("generic_pipeline: {}", prepared.stats.line());
                }
            }
            outbox.lock().expect("outbox mutex").done = Some(err);
            if let Some(s) = scheduler.lock().expect("scheduler mutex").as_ref() {
                s.commit(PIPELINE_EVENT);
            }
        });

        abi::envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationNoBuffer
    }

    /// The same event with TRAILERS, which is where gRPC reports its status.
    ///
    /// Envoy calls this instead of [`Self::on_http_callout_done`] only when it carries
    /// `patches/envoy-callout-options.patch`; the symbol is optional and a stock Envoy never
    /// looks it up. A callout registered in `inflight_grpc` is answered here and nowhere else,
    /// which is why `unary_grpc_via_callout` must stay off on a stock binary.
    #[cfg(feature = "envoy-callout-options")]
    fn on_http_callout_done_with_trailers(
        &mut self,
        envoy: &mut EHF,
        callout_id: u64,
        result: abi::envoy_dynamic_module_type_http_callout_result,
        response_headers: Option<&[(EnvoyBuffer, EnvoyBuffer)]>,
        response_body: Option<&[EnvoyBuffer]>,
        response_trailers: Option<&[(EnvoyBuffer, EnvoyBuffer)]>,
    ) {
        let grpc_tx = self
            .bridge
            .lock()
            .expect("bridge mutex")
            .inflight_grpc
            .remove(&callout_id);
        let Some(tx) = grpc_tx else {
            // Not a gRPC callout -- an ordinary hop, handled by the trailer-less path.
            return self.on_http_callout_done(
                envoy,
                callout_id,
                result,
                response_headers,
                response_body,
            );
        };
        let span = self.spans.remove(&callout_id);
        if result != abi::envoy_dynamic_module_type_http_callout_result::Success {
            finish_span(span, "failed", Some(&format!("{result:?}")));
            let _ = tx.send(Err(format!("grpc callout failed: {result:?}")));
            return;
        }
        // Status can arrive in the HEADERS (a trailers-only response) or in the trailers, and
        // both are read for the same reason the stream path reads both: a gRPC error is a 200.
        let mut st = GrpcStream::default();
        if let Some(hs) = response_headers {
            read_grpc_status(hs, &mut st);
        }
        if let Some(ts) = response_trailers {
            read_grpc_status(ts, &mut st);
        }
        if let Some(sp) = &span {
            sp.set_tag("grpc.status", &st.status.unwrap_or(0).to_string());
        }
        if st.status.unwrap_or(0) != 0 {
            finish_span(span, "failed", Some(&st.message));
            let _ = tx.send(Err(format!(
                "grpc status {} {}",
                st.status.unwrap_or(-1),
                st.message
            )));
            return;
        }
        for chunk in response_body.unwrap_or(&[]) {
            st.buf.extend_from_slice(chunk.as_slice());
        }
        let unframed = grpc_unframe(&st.buf);
        finish_span(span, if unframed.is_some() { "ok" } else { "failed" }, None);
        let _ = tx.send(match unframed {
            Some(msg) => Ok(msg.to_vec()),
            None => Err(format!(
                "grpc callout returned an incomplete frame ({} bytes)",
                st.buf.len()
            )),
        });
    }

    /// Delivers a host-managed upstream call's answer back to the parked pipeline step.
    fn on_http_callout_done(
        &mut self,
        _envoy: &mut EHF,
        callout_id: u64,
        result: abi::envoy_dynamic_module_type_http_callout_result,
        response_headers: Option<&[(EnvoyBuffer, EnvoyBuffer)]>,
        response_body: Option<&[EnvoyBuffer]>,
    ) {
        let reply = self
            .bridge
            .lock()
            .expect("bridge mutex")
            .inflight
            .remove(&callout_id);
        let span = self.spans.remove(&callout_id);
        let Some(reply) = reply else {
            finish_span(span, "orphaned", None);
            return;
        };
        if result != abi::envoy_dynamic_module_type_http_callout_result::Success {
            finish_span(span, "failed", Some(&format!("{result:?}")));
            // Reset, timeout or no healthy host. Reported rather than turned into an empty
            // 200: a step that "succeeded" with no body is the silent-success shape that has
            // cost this project several debugging rounds.
            let _ = reply.send(Err(format!("callout failed: {result:?}")));
            return;
        }
        // `:status` is a header like any other here.
        let status = response_headers
            .and_then(|hs| {
                hs.iter()
                    .find(|(k, _)| k.as_slice() == b":status")
                    .map(|(_, v)| {
                        String::from_utf8_lossy(v.as_slice())
                            .parse::<u16>()
                            .unwrap_or(0)
                    })
            })
            .unwrap_or(0);
        let mut body = Vec::new();
        for chunk in response_body.unwrap_or(&[]) {
            body.extend_from_slice(chunk.as_slice());
        }
        if let Some(s) = &span {
            s.set_tag("http.status_code", &status.to_string());
            s.set_tag("http.response_size", &body.len().to_string());
        }
        finish_span(span, if status < 400 { "ok" } else { "http_error" }, None);
        let _ = reply.send(Ok((status, body)));
    }

    /// A gRPC error is a 200 whose `grpc-status` is non-zero, and it can arrive in the
    /// HEADERS (a trailers-only response) rather than the trailers. Missing that would turn
    /// every upstream gRPC failure into an empty success, which is the silent-success shape
    /// this project keeps paying for.
    fn on_http_stream_headers(
        &mut self,
        _envoy: &mut EHF,
        stream_handle: u64,
        response_headers: &[(EnvoyBuffer, EnvoyBuffer)],
        _end_stream: bool,
    ) {
        let mut b = self.bridge.lock().expect("bridge mutex");
        if let Some((st, _)) = b.streams.get_mut(&stream_handle) {
            read_grpc_status(response_headers, st);
        }
    }

    fn on_http_stream_data(
        &mut self,
        _envoy: &mut EHF,
        stream_handle: u64,
        response_data: &[EnvoyBuffer],
        _end_stream: bool,
    ) {
        let mut b = self.bridge.lock().expect("bridge mutex");
        let Some((st, reply)) = b.streams.get_mut(&stream_handle) else {
            return;
        };
        // Accumulated rather than assumed whole: a gRPC frame can split across callbacks, and
        // one callback can carry several.
        for chunk in response_data {
            st.buf.extend_from_slice(chunk.as_slice());
        }
        // A streaming reply is forwarded as it arrives. Holding it to the end would serialise
        // a ~50-frame generation behind its own completion, which is the whole point of
        // streaming. A unary reply is left in the buffer for `on_http_stream_complete`.
        if let GrpcReply::Stream(tx) = reply {
            // Anything parked by an earlier callback goes first, or frames would be delivered
            // out of order -- which for a token stream means a scrambled answer.
            while let Some(front) = st.backlog.front() {
                match tx.try_send(Ok(front.clone())) {
                    Ok(()) => {
                        st.backlog.pop_front();
                    }
                    Err(_) => break,
                }
            }
            while let Some(n) = grpc_frame_len(&st.buf) {
                let frame: Vec<u8> = st.buf[5..5 + n].to_vec();
                st.buf.drain(..5 + n);
                // `try_send` rather than `send`: this is the Envoy worker thread and it must
                // never block.
                //
                // A full channel used to mean DROP THE STREAM. That is wrong: the channel
                // fills whenever the consumer is momentarily behind, which a slow client makes
                // routine, and the result was a truncated generation reported as an error. The
                // frame is parked instead, and the backlog is what bounds the memory.
                //
                // There is no way to push back on the UPSTREAM here -- the dynamic-module ABI
                // has no read-disable for an http stream (`send_data`/`send_trailers` are the
                // only module->Envoy stream calls), so the worker cannot be told to slow down.
                // Parking with a cap is the best available behaviour, and the cap failing
                // loudly beats silently losing tokens.
                if !st.backlog.is_empty() || tx.try_send(Ok(frame.clone())).is_err() {
                    st.backlog.push_back(frame);
                    if st.backlog.len() > MAX_BACKLOG_FRAMES {
                        st.overflowed = true;
                        break;
                    }
                }
            }
        }
    }

    fn on_http_stream_trailers(
        &mut self,
        _envoy: &mut EHF,
        stream_handle: u64,
        response_trailers: &[(EnvoyBuffer, EnvoyBuffer)],
    ) {
        let mut b = self.bridge.lock().expect("bridge mutex");
        if let Some((st, _)) = b.streams.get_mut(&stream_handle) {
            read_grpc_status(response_trailers, st);
        }
    }

    fn on_http_stream_complete(&mut self, _envoy: &mut EHF, stream_handle: u64) {
        let entry = self
            .bridge
            .lock()
            .expect("bridge mutex")
            .streams
            .remove(&stream_handle);
        let span = self.stream_spans.remove(&stream_handle);
        let Some((st, reply)) = entry else {
            finish_span(span, "orphaned", None);
            return;
        };
        let failed = if st.status.unwrap_or(0) != 0 {
            Some(format!(
                "grpc status {} {}",
                st.status.unwrap_or(-1),
                st.message
            ))
        } else if st.overflowed {
            Some("grpc stream: consumer fell behind and frames were dropped".to_string())
        } else {
            None
        };
        if let Some(s) = &span {
            s.set_tag("grpc.status", &st.status.unwrap_or(0).to_string());
        }
        finish_span(
            span,
            if failed.is_none() { "ok" } else { "failed" },
            failed.as_deref(),
        );
        match reply {
            GrpcReply::Unary(tx) => {
                let r = match failed {
                    Some(e) => Err(e),
                    // A clean stream with no complete frame is not an empty success.
                    None => match grpc_unframe(&st.buf) {
                        Some(msg) => Ok(msg.to_vec()),
                        None => Err(format!(
                            "grpc stream ended with an incomplete frame ({} bytes)",
                            st.buf.len()
                        )),
                    },
                };
                let _ = tx.send(r);
            }
            GrpcReply::Stream(tx) => {
                // Frames were forwarded as they arrived; completion only reports a failure.
                // Dropping the sender is what ends the consumer's stream.
                if let Some(e) = failed {
                    let _ = tx.try_send(Err(e));
                }
            }
        }
    }

    fn on_http_stream_reset(
        &mut self,
        _envoy: &mut EHF,
        stream_handle: u64,
        reset_reason: abi::envoy_dynamic_module_type_http_stream_reset_reason,
    ) {
        let entry = self
            .bridge
            .lock()
            .expect("bridge mutex")
            .streams
            .remove(&stream_handle);
        finish_span(
            self.stream_spans.remove(&stream_handle),
            "reset",
            Some(&format!("{reset_reason:?}")),
        );
        if let Some((_, reply)) = entry {
            let e = format!("grpc stream reset: {reset_reason:?}");
            match reply {
                GrpcReply::Unary(tx) => {
                    let _ = tx.send(Err(e));
                }
                GrpcReply::Stream(tx) => {
                    let _ = tx.try_send(Err(e));
                }
            }
        }
    }

    /// The client has stopped draining: Envoy's downstream write buffer is over its high
    /// watermark.
    ///
    /// Before this existed the module had no idea, and its response was to fill a bounded
    /// channel and then DROP token frames -- surfacing as "consumer fell behind and frames
    /// were dropped", i.e. a truncated generation caused by a slow reader. Slow clients on long
    /// generations are ordinary, not exceptional, so that was a defect rather than an edge
    /// case; and mock backends hide it completely.
    fn on_downstream_above_write_buffer_high_watermark(&mut self, _envoy: &mut EHF) {
        self.outbox.lock().expect("outbox mutex").congested = true;
    }

    /// The client is reading again.
    fn on_downstream_below_write_buffer_low_watermark(&mut self, envoy: &mut EHF) {
        {
            let mut o = self.outbox.lock().expect("outbox mutex");
            o.congested = false;
        }
        // Flush what accumulated while paused, on this thread -- we are already on the Envoy
        // worker. Waiting for the next pipeline event would stall the stream for as long as
        // the next token takes.
        self.on_scheduled(envoy, PIPELINE_EVENT);
    }

    /// The downstream stream ended -- normally, or because the client disconnected.
    ///
    /// Envoy calls this before destroying the filter, and unlike `Drop` it hands us `envoy`, so
    /// this is the one place upstream work can be cancelled EXPLICITLY.
    ///
    /// Without it, cancellation was a side effect: the filter was destroyed, reply channels
    /// dropped, the pipeline's awaits errored, and Envoy reset the streams during teardown.
    /// That works, but it waits for teardown. A worker generating tokens for a client that has
    /// already gone is burning GPU, so the gap between "client left" and "worker stopped" is
    /// worth closing directly.
    ///
    /// `reset_http_stream` was already in the ABI -- I added a duplicate before finding it,
    /// because I searched for the name I expected (`..._http_stream_reset`) rather than the one
    /// it has (`..._http_filter_reset_http_stream`). Third time this session that a capability
    /// looked missing only because of how it was spelled.
    fn on_stream_complete(&mut self, envoy: &mut EHF) {
        let live: Vec<u64> = self
            .bridge
            .lock()
            .expect("bridge mutex")
            .streams
            .keys()
            .copied()
            .collect();
        for sid in live {
            // Safe: `sid` came from `streams`, which is only populated with handles
            // `start_http_stream` returned, and entries are removed on complete/reset. A handle
            // for an already-finished stream is the ordinary race and Envoy ignores it.
            unsafe { envoy.reset_http_stream(sid) };
        }
    }

    /// Runs on the Envoy worker thread. Everything that touches `envoy` happens here.
    fn on_scheduled(&mut self, envoy: &mut EHF, event_id: u64) {
        if event_id == OUTBOUND_EVENT {
            // Issue every call the runtime has parked. Draining first and releasing the lock
            // before touching `envoy` keeps the runtime from blocking on the Envoy thread.
            let parked: Vec<Pending> =
                std::mem::take(&mut self.bridge.lock().expect("bridge mutex").outbox);
            for p in parked {
                let (span, traceparent) = hop_span(envoy, &p.cluster, &p.cluster, &p.path);
                let mut headers: Vec<(&str, &[u8])> = vec![
                    (":method", b"POST".as_slice()),
                    (":path", p.path.as_bytes()),
                    ("host", p.authority.as_bytes()),
                    ("content-type", b"application/json".as_slice()),
                ];
                if let Some(tp) = &traceparent {
                    headers.push(("traceparent", tp.as_bytes()));
                }
                let headers = headers.as_slice();
                let (init, id) = if self.retryable(&p.cluster) {
                    #[cfg(feature = "envoy-callout-options")]
                    {
                        envoy.send_http_callout_with_options(
                            &p.cluster,
                            &headers,
                            Some(&p.body),
                            p.timeout_ms,
                            &self.cfg.callout_retry_on,
                            self.cfg.callout_num_retries,
                            self.cfg.callout_per_try_timeout_ms,
                        )
                    }
                    #[cfg(not(feature = "envoy-callout-options"))]
                    {
                        unreachable!("retry configuration is rejected at startup")
                    }
                } else {
                    envoy.send_http_callout(&p.cluster, &headers, Some(&p.body), p.timeout_ms)
                };
                if init != abi::envoy_dynamic_module_type_http_callout_init_result::Success {
                    // ClusterNotFound is the likely one and it is a CONFIG error -- the
                    // authority was mapped to a cluster that the bootstrap does not define.
                    // Failing the step loudly beats retrying into the same wall.
                    let _ = p.reply.send(Err(format!("callout init failed: {init:?}")));
                    continue;
                }
                if let Some(s) = span {
                    self.spans.insert(id, s);
                }
                self.bridge
                    .lock()
                    .expect("bridge mutex")
                    .inflight
                    .insert(id, p.reply);
            }

            // gRPC, over the STREAM api -- only it delivers trailers, and gRPC's status lives
            // there. See k8s/host-managed-transport.md.
            let parked_grpc: Vec<PendingGrpc> =
                std::mem::take(&mut self.bridge.lock().expect("bridge mutex").outbox_grpc);
            for p in parked_grpc {
                let (span, traceparent) = hop_span(envoy, &p.cluster, &p.cluster, &p.path);
                let mut headers: Vec<(&str, &[u8])> = vec![
                    (":method", b"POST".as_slice()),
                    (":path", p.path.as_bytes()),
                    ("host", p.authority.as_bytes()),
                    ("content-type", b"application/grpc".as_slice()),
                    // Without `te: trailers` a gRPC server may refuse the request outright.
                    ("te", b"trailers".as_slice()),
                ];
                if let Some(tp) = &traceparent {
                    headers.push(("traceparent", tp.as_bytes()));
                }
                let headers = headers.as_slice();
                // A unary gRPC call can go over the unary api once trailers are delivered --
                // which is what `on_http_callout_done_with_trailers` adds. The stream api was
                // only ever used here to reach `grpc-status`.
                if self.cfg.unary_grpc_via_callout && matches!(p.reply, GrpcReply::Unary(_)) {
                    {
                        let GrpcReply::Unary(tx) = p.reply else {
                            unreachable!()
                        };
                        let (init, id) = if self.retryable(&p.cluster) {
                            #[cfg(feature = "envoy-callout-options")]
                            {
                                envoy.send_http_callout_with_options(
                                    &p.cluster,
                                    &headers,
                                    Some(&p.body),
                                    self.cfg.callout_timeout_ms,
                                    &self.cfg.callout_retry_on,
                                    self.cfg.callout_num_retries,
                                    self.cfg.callout_per_try_timeout_ms,
                                )
                            }
                            #[cfg(not(feature = "envoy-callout-options"))]
                            {
                                unreachable!("retry configuration is rejected at startup")
                            }
                        } else {
                            envoy.send_http_callout(
                                &p.cluster,
                                &headers,
                                Some(&p.body),
                                self.cfg.callout_timeout_ms,
                            )
                        };
                        if init != abi::envoy_dynamic_module_type_http_callout_init_result::Success
                        {
                            let _ = tx.send(Err(format!("grpc callout init failed: {init:?}")));
                        } else {
                            self.bridge
                                .lock()
                                .expect("bridge mutex")
                                .inflight_grpc
                                .insert(id, tx);
                        }
                        continue;
                    }
                }
                // Body and end_stream go in the start call: a unary gRPC request is exactly
                // one frame, so there is nothing to send afterwards.
                let (init, sid) = envoy.start_http_stream(
                    &p.cluster,
                    &headers,
                    Some(&p.body),
                    true,
                    self.cfg.callout_timeout_ms,
                );
                if init != abi::envoy_dynamic_module_type_http_callout_init_result::Success {
                    // ClusterNotFound here is a CONFIG error: an authority was mapped to a
                    // cluster the bootstrap does not define. Fail the step loudly.
                    fail_grpc(p.reply, format!("grpc stream init failed: {init:?}"));
                    continue;
                }
                if let Some(s) = span {
                    self.stream_spans.insert(sid, s);
                }
                self.bridge
                    .lock()
                    .expect("bridge mutex")
                    .streams
                    .insert(sid, (GrpcStream::default(), p.reply));
            }
            return;
        }
        if event_id != PIPELINE_EVENT {
            return;
        }
        let (chunks, done, need_headers, overrun) = {
            let mut o = self.outbox.lock().expect("outbox mutex");
            let need_headers = !o.headers_sent;
            // While the downstream buffer is over its high watermark, frames stay in `chunks`.
            // Handing Envoy more bytes for a buffer it has just told us is full only moves the
            // queue; leaving them here lets that buffer drain.
            //
            // `done` is still read, so a pipeline that finishes during congestion is not lost:
            // the terminal write below is what flushes everything once the client catches up.
            if o.congested {
                o.parked_bytes = o.chunks.iter().map(|c| c.len()).sum();
                // Bounded, because pausing a producer that cannot be paused upstream turns a
                // stalled client into unbounded memory here. At the cap the request is failed
                // EXPLICITLY -- a clear error beats an out-of-memory that takes every other
                // request with it.
                let overrun = o.parked_bytes > MAX_PARKED_BYTES;
                (Vec::new(), o.done.clone(), false, overrun)
            } else {
                if need_headers {
                    o.headers_sent = true;
                }
                o.parked_bytes = 0;
                (
                    std::mem::take(&mut o.chunks),
                    o.done.clone(),
                    need_headers,
                    false,
                )
            }
        };
        if overrun {
            // Ending the stream is the only honest option: headers are long since sent, so
            // there is no status left to report with, and continuing would mean growing
            // without limit on behalf of a client that has stopped reading.
            eprintln!(
                "generic_pipeline: downstream stalled with >{MAX_PARKED_BYTES} bytes parked;                  ending the stream"
            );
            envoy.send_response_data(b"", true);
            return;
        }

        // A non-streaming request produces nothing until the pipeline finishes, so the whole
        // incremental path below is skipped: no early headers, no per-token writes.
        if !self.streaming {
            if let Some(err) = done {
                let o = self.outbox.lock().expect("outbox mutex");
                if err.is_none() {
                    let body = serde_json::json!({
                        "id": self.chunk_id,
                        "object": "chat.completion",
                        "created": self.created,
                        "model": self.model,
                        "system_fingerprint": Value::Null,
                        "service_tier": Value::Null,
                        "choices": [{
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": o.text,
                                "reasoning_content": if o.reasoning.is_empty() {
                                    Value::Null
                                } else {
                                    Value::String(o.reasoning.clone())
                                },
                                "tool_calls": if o.tool_calls.is_empty() {
                                    Value::Null
                                } else {
                                    serde_json::from_str(&o.tool_calls).unwrap_or(Value::Null)
                                }
                            },
                            "logprobs": Value::Null,
                            "finish_reason": self.finish_reason(o.n_out)
                        }],
                        "usage": {
                            "prompt_tokens": 0,
                            "completion_tokens": o.n_out,
                            "total_tokens": o.n_out
                        }
                    });
                    envoy.send_response(
                        200,
                        &[("content-type", b"application/json".as_slice())],
                        Some(body.to_string().as_bytes()),
                        None,
                    );
                } else {
                    // Nothing has been written yet, so unlike the streaming path this CAN
                    // report the failure properly instead of ending an already-open stream.
                    envoy.send_response(
                        502,
                        &[("content-type", b"application/json".as_slice())],
                        Some(br#"{"error":"pipeline failed"}"#.as_slice()),
                        Some("generic_pipeline.failed"),
                    );
                }
            }
            return;
        }

        if need_headers {
            envoy.send_response_headers(
                &[
                    (":status", b"200".as_slice()),
                    ("content-type", b"text/event-stream".as_slice()),
                    ("cache-control", b"no-cache".as_slice()),
                ],
                false,
            );
        }
        // One write per drain, not one per frame. This does NOT collapse SSE frames: each
        // `data: ...\n\n` stays delimited, so the client still parses exactly one frame per
        // token and a per-token benchmark still counts what it should. It only stops crossing
        // the module/Envoy boundary once per token to hand over bytes that are already
        // contiguous.
        if !chunks.is_empty() {
            let mut buf = Vec::with_capacity(chunks.iter().map(|c| c.len()).sum());
            for c in &chunks {
                buf.extend_from_slice(c);
            }
            envoy.send_response_data(&buf, false);
        }
        if let Some(err) = done {
            if err.is_none() {
                // One frame per token, then usage, then the terminator -- byte-identical to
                // the agentgateway host's framing, so a per-token streaming benchmark
                // compares the same thing on both.
                envoy.send_response_data(b"data: [DONE]\n\n", true);
            } else {
                // The stream is already open by this point, so the only honest way to signal
                // failure is to end it; the error is logged above rather than invented into
                // a chunk the client would render as model output.
                envoy.send_response_data(b"", true);
            }
        }
    }
}

#[cfg(test)]
mod request_id_tests {
    use super::*;

    #[test]
    fn request_ids_are_unique_across_concurrent_gateway_threads() {
        let threads = 16;
        let ids_per_thread = 1_000;
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                std::thread::spawn(move || {
                    (0..ids_per_thread)
                        .map(|_| next_request_id())
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let ids: std::collections::HashSet<_> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("ID generator thread"))
            .collect();

        assert_eq!(ids.len(), threads * ids_per_thread);
    }
}
#[cfg(test)]
mod grpc_framing_tests {
    use super::*;

    /// Frames must survive arriving in arbitrarily bad chunks.
    ///
    /// Envoy delivers response bytes in whatever sizes the network produced: a frame can split
    /// across callbacks and a callback can carry several. Reassembly that assumed either shape
    /// would truncate a generation while every throughput number improved -- the response would
    /// simply be short, which no counter reports.
    #[test]
    fn frames_reassemble_across_any_chunking() {
        let msgs: Vec<Vec<u8>> = vec![
            b"a".to_vec(),
            b"hello world".to_vec(),
            vec![0u8; 300],
            b"z".to_vec(),
        ];
        let mut wire = Vec::new();
        for m in &msgs {
            wire.extend_from_slice(&grpc_frame(m));
        }

        // Every chunk size from 1 byte to the whole body at once.
        for chunk in 1..=wire.len() {
            let mut buf: Vec<u8> = Vec::new();
            let mut got: Vec<Vec<u8>> = Vec::new();
            for piece in wire.chunks(chunk) {
                buf.extend_from_slice(piece);
                while let Some(n) = grpc_frame_len(&buf) {
                    got.push(buf[5..5 + n].to_vec());
                    buf.drain(..5 + n);
                }
            }
            assert_eq!(got, msgs, "chunk size {chunk} lost or corrupted frames");
            assert!(
                buf.is_empty(),
                "chunk size {chunk} left {} bytes over",
                buf.len()
            );
        }
    }

    /// A partial frame yields nothing rather than a short read.
    #[test]
    fn an_incomplete_frame_is_not_a_frame() {
        let full = grpc_frame(b"abcdef");
        for n in 0..full.len() {
            assert_eq!(
                grpc_frame_len(&full[..n]),
                None,
                "{n} bytes must not parse as a frame"
            );
        }
        assert_eq!(grpc_frame_len(&full), Some(6));
    }

    /// An empty protobuf message is a legitimate frame, not an absent one.
    #[test]
    fn an_empty_message_is_still_a_frame() {
        let f = grpc_frame(b"");
        assert_eq!(f.len(), 5);
        assert_eq!(grpc_frame_len(&f), Some(0));
        assert_eq!(grpc_unframe(&f), Some(&[][..]));
    }
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    /// The assembled frame must be byte-identical to the `serde_json::json!` one it replaced.
    ///
    /// This is a hand-built JSON document on the response path, so "it looks right" is not
    /// enough: a client parses every frame, and an escaping difference on a delta containing a
    /// quote or a control character would corrupt a response while every throughput number
    /// improved. Compared against the original construction for a set of deltas chosen to
    /// break naive escaping.
    #[test]
    fn the_assembled_frame_matches_the_json_macro_byte_for_byte() {
        let id = "chatcmpl-1789843935-74";
        let created = 1789843935u64;
        let model = "Qwen/Qwen2.5-0.5B-Instruct";

        for delta in [
            "",
            "hello",
            " world",
            "a \"quoted\" word",
            "back\\slash",
            "new\nline\tand\ttabs",
            "control\u{0001}char",
            "unicode \u{1f600} and \u{00e9}",
            "</script>",
        ] {
            let old = {
                let chunk = serde_json::json!({
                    "id": id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": null}],
                });
                format!("data: {chunk}\n\n").into_bytes()
            };

            let mut new = FRAME_PREFIX.to_vec();
            serde_json::to_writer(&mut new, delta).expect("write");
            new.extend_from_slice(&frame_suffix(id, created, model));

            assert_eq!(
                String::from_utf8_lossy(&new),
                String::from_utf8_lossy(&old),
                "frame differs for delta {delta:?}"
            );
        }
    }

    /// And the result must actually parse back to the same content.
    #[test]
    fn the_assembled_frame_parses_back() {
        let mut f = FRAME_PREFIX.to_vec();
        serde_json::to_writer(&mut f, "a \"b\" c").expect("write");
        f.extend_from_slice(&frame_suffix("id-1", 7, "m"));
        let text = String::from_utf8(f).expect("utf8");
        let json = text.strip_prefix("data: ").expect("prefix").trim_end();
        let v: Value = serde_json::from_str(json).expect("valid json");
        assert_eq!(
            v["choices"][0]["delta"]["content"],
            Value::from("a \"b\" c")
        );
        assert_eq!(v["object"], Value::from("chat.completion.chunk"));
        assert!(v["choices"][0]["finish_reason"].is_null());
    }
}
