// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand};
use dynamo_backend_common::{DisaggregationMode, EngineAdapter, LLMEngine};
use dynamo_component_facades::{
    chat_worker::ChatWorkerFacade,
    postprocess::PostprocessorFacade,
    preprocess::PreprocessorFacade,
    proto::{
        FILE_DESCRIPTOR_SET, chat_worker_bridge_server::ChatWorkerBridgeServer,
        postprocessor_server::PostprocessorServer, preprocessor_server::PreprocessorServer,
        selector_server::SelectorServer,
    },
    selector::SelectorFacade,
    worker::CanonicalBackendEngine,
};
use dynamo_ext_proc::{PodDiscovery, PodDiscoveryConfig, RegistrationDefaults, TopologyAdapter};
use dynamo_kv_router::{
    WorkerType, config::KvRouterConfig, plugins::RouterPluginRegistry,
    services::selection::SelectionServiceBuilder,
};
use dynamo_llm::{
    backend::Backend,
    model_card::ModelDeploymentCard,
    preprocessor::{BackendOutput, OpenAIPreprocessor, PreprocessedRequest},
    protocols::common::llm_backend::{FinishReason, LLMEngineOutput},
};
use dynamo_runtime::{
    pipeline::{
        AsyncEngine, AsyncEngineContextProvider, Context as EngineContext, Error, ManyOut,
        Operator, ResponseStream, ServiceBackend, ServiceEngine, ServiceFrontend, SingleIn, Source,
        async_trait,
    },
    protocols::annotated::Annotated,
};
use futures::stream;
use serde::Deserialize;
use tonic::transport::Server;

const GRPC_STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;
const GRPC_CONNECTION_WINDOW_BYTES: u32 = 16 * 1024 * 1024;
const GRPC_MAX_CONCURRENT_STREAMS: u32 = 4096;

fn server_builder() -> Server {
    Server::builder()
        .initial_stream_window_size(Some(GRPC_STREAM_WINDOW_BYTES))
        .initial_connection_window_size(Some(GRPC_CONNECTION_WINDOW_BYTES))
        .max_concurrent_streams(Some(GRPC_MAX_CONCURRENT_STREAMS))
        .tcp_nodelay(true)
}

#[derive(Parser)]
#[command(name = "dynamo-component-facade")]
struct Args {
    #[arg(long, env = "DYN_COMPONENT_LISTEN", default_value = "0.0.0.0:50051")]
    listen: SocketAddr,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write the descriptor used by descriptor-driven gateway hosts.
    Descriptor(DescriptorArgs),
    Preprocessor(ModelArgs),
    Postprocessor(PostprocessorArgs),
    Selector(SelectorArgs),
    /// CPU-only deterministic worker for facade/orchestrator benchmarks.
    BenchmarkWorker(ModelArgs),
    /// Aggregate vLLM worker facade using Dynamo's native sidecar engine.
    VllmWorker(SidecarWorkerArgs),
    /// Aggregate SGLang worker facade using Dynamo's native sidecar engine.
    SglangWorker(SidecarWorkerArgs),
}

#[derive(clap::Args)]
struct DescriptorArgs {
    #[arg(long, short)]
    output: PathBuf,
}

#[derive(clap::Args)]
struct ModelArgs {
    #[arg(long, env = "DYN_COMPONENT_MODEL_PATH")]
    model_path: PathBuf,
    #[arg(long, env = "DYN_COMPONENT_CHAT_TEMPLATE")]
    chat_template: Option<PathBuf>,
    #[arg(long, default_value_t = 128)]
    max_batch_items: usize,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_batch_bytes: usize,
    #[arg(long, default_value_t = 8)]
    max_concurrency: usize,
}

#[derive(clap::Args)]
struct PostprocessorArgs {
    #[command(flatten)]
    model: ModelArgs,
    #[arg(long, default_value_t = 4096)]
    max_active_requests: usize,
    #[arg(long, default_value_t = 64)]
    session_queue_capacity: usize,
    #[arg(long, default_value_t = 1024)]
    output_queue_capacity: usize,
    #[arg(long, default_value_t = 1024 * 1024)]
    max_chunk_bytes: usize,
}

#[derive(clap::Args)]
struct SidecarWorkerArgs {
    #[command(flatten)]
    model: ModelArgs,
    /// Native Dynamo sidecar options, supplied after `--`.
    #[arg(last = true, allow_hyphen_values = true)]
    sidecar_args: Vec<String>,
}

#[derive(clap::Args)]
struct SelectorArgs {
    #[arg(long, env = "DYN_COMPONENT_ROUTER_CONFIG")]
    router_config: Option<PathBuf>,
    /// JSON file declaring one or more InferencePools to watch.
    #[arg(long, env = "DYN_COMPONENT_DISCOVERY_CONFIG")]
    discovery_config: Option<PathBuf>,
    #[arg(long, default_value = "aggregated")]
    worker_type: WorkerType,
    #[arg(long, default_value_t = 4)]
    indexer_threads: usize,
    #[arg(long, default_value_t = 128)]
    max_batch_items: usize,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_batch_bytes: usize,
    #[arg(long, default_value_t = 32)]
    max_concurrency: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectorDiscoveryFile {
    pools: Vec<PoolRegistration>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolRegistration {
    namespace: String,
    inference_pool_name: String,
    model_name: String,
    #[serde(default = "default_block_size")]
    block_size: u32,
    #[serde(default)]
    total_kv_blocks: Option<u64>,
    #[serde(default)]
    max_num_batched_tokens: Option<u64>,
    #[serde(default = "default_data_parallel_size")]
    data_parallel_size: u32,
    #[serde(default = "default_kv_event_port")]
    kv_event_port: u16,
    #[serde(default = "default_kv_event_port_stride")]
    kv_event_port_stride: u16,
    #[serde(default)]
    replay_port: Option<u16>,
}

fn default_block_size() -> u32 {
    16
}
fn default_data_parallel_size() -> u32 {
    1
}
fn default_kv_event_port() -> u16 {
    5557
}
fn default_kv_event_port_stride() -> u16 {
    1
}

struct BenchmarkBackend;

#[async_trait]
impl AsyncEngine<SingleIn<PreprocessedRequest>, ManyOut<Annotated<BackendOutput>>, Error>
    for BenchmarkBackend
{
    async fn generate(
        &self,
        request: EngineContext<PreprocessedRequest>,
    ) -> Result<ManyOut<Annotated<BackendOutput>>, Error> {
        let token_count = request
            .stop_conditions
            .max_tokens
            .unwrap_or(1)
            .clamp(1, 2048);
        let context = request.context();
        let outputs = (0..token_count).map(move |index| {
            Annotated::from_data(BackendOutput {
                token_ids: vec![42],
                tokens: vec![Some("x".into())],
                text: Some("x".into()),
                cum_log_probs: None,
                log_probs: None,
                top_logprobs: None,
                finish_reason: (index + 1 == token_count).then_some(FinishReason::Length),
                stop_reason: None,
                index: Some(0),
                completion_usage: None,
                disaggregated_params: None,
                encoder_result: None,
                worker_trace_link: None,
                engine_data: None,
                routing_data: None,
                jailed_text: None,
            })
        });
        Ok(ResponseStream::new(
            Box::pin(stream::iter(outputs)),
            context,
        ))
    }
}

fn load_preprocessor(args: &ModelArgs) -> anyhow::Result<Arc<OpenAIPreprocessor>> {
    let model_card =
        ModelDeploymentCard::load_from_disk(&args.model_path, args.chat_template.as_deref())
            .with_context(|| format!("load model assets from {}", args.model_path.display()))?;
    OpenAIPreprocessor::new(model_card)
}

fn real_worker_pipeline(
    engine: Arc<dyn LLMEngine>,
    model_card: &ModelDeploymentCard,
) -> anyhow::Result<CanonicalBackendEngine> {
    // This is Dynamo's normal worker adapter and Backend detokenizer. Only
    // their transport boundary changes; no engine or token policy is copied.
    let tokenizer = model_card.tokenizer()?;
    let frontend =
        ServiceFrontend::<SingleIn<PreprocessedRequest>, ManyOut<Annotated<BackendOutput>>>::new();
    let backend = Backend::from_tokenizer(tokenizer).into_operator();
    let inner: ServiceEngine<SingleIn<PreprocessedRequest>, ManyOut<Annotated<LLMEngineOutput>>> =
        Arc::new(EngineAdapter::new(engine, DisaggregationMode::Aggregated));
    let inner = ServiceBackend::from_engine(inner);
    Ok(frontend
        .link(backend.forward_edge())?
        .link(inner)?
        .link(backend.backward_edge())?
        .link_terminal(frontend)?)
}

async fn serve_real_worker(
    engine: Arc<dyn LLMEngine>,
    config: SidecarWorkerArgs,
    listen: SocketAddr,
) -> anyhow::Result<()> {
    let model_card = ModelDeploymentCard::load_from_disk(
        &config.model.model_path,
        config.model.chat_template.as_deref(),
    )
    .with_context(|| {
        format!(
            "load model assets from {}",
            config.model.model_path.display()
        )
    })?;
    let processor = OpenAIPreprocessor::new(model_card.clone())?;
    let pipeline = real_worker_pipeline(engine.clone(), &model_card)?;
    // Native sidecar engines discover their runtime metadata and connect to
    // the engine here. The gRPC health service becomes Ready only afterward.
    if let Err(error) = engine.start(0).await {
        // The backend-common lifecycle contract also requires cleanup after
        // a partial start; do not leave an engine connection behind.
        if let Err(cleanup_error) = engine.cleanup().await {
            tracing::warn!(%cleanup_error, "native sidecar cleanup after failed start failed");
        }
        return Err(error.into());
    }
    let result = async {
        let service = ChatWorkerFacade::new(
            pipeline,
            processor,
            config.model.max_batch_bytes,
            config.model.max_batch_bytes,
        )?;
        let (reporter, health) = tonic_health::server::health_reporter();
        reporter
            .set_serving::<ChatWorkerBridgeServer<ChatWorkerFacade>>()
            .await;
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()
            .context("build component reflection service")?;
        server_builder()
            .add_service(health)
            .add_service(reflection)
            .add_service(ChatWorkerBridgeServer::new(service))
            .serve_with_shutdown(listen, shutdown_signal())
            .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = engine.cleanup().await.map_err(anyhow::Error::from);
    result.and(cleanup)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = terminate.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    match args.command {
        Command::Descriptor(config) => {
            std::fs::write(&config.output, FILE_DESCRIPTOR_SET)
                .with_context(|| format!("write descriptor to {}", config.output.display()))?;
        }
        Command::Preprocessor(config) => {
            let service = PreprocessorFacade::new(
                load_preprocessor(&config)?,
                config.max_batch_items,
                config.max_batch_bytes,
                config.max_concurrency,
            )?;
            let (reporter, health) = tonic_health::server::health_reporter();
            reporter
                .set_serving::<PreprocessorServer<PreprocessorFacade>>()
                .await;
            let reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
                .build_v1()
                .context("build component reflection service")?;
            server_builder()
                .add_service(health)
                .add_service(reflection)
                .add_service(PreprocessorServer::new(service))
                .serve(args.listen)
                .await?;
        }
        Command::Postprocessor(config) => {
            let service = PostprocessorFacade::new(
                load_preprocessor(&config.model)?,
                config.max_active_requests,
                config.session_queue_capacity,
                config.output_queue_capacity,
                config.max_chunk_bytes,
            )?;
            let (reporter, health) = tonic_health::server::health_reporter();
            reporter
                .set_serving::<PostprocessorServer<PostprocessorFacade>>()
                .await;
            let reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
                .build_v1()
                .context("build component reflection service")?;
            server_builder()
                .add_service(health)
                .add_service(reflection)
                .add_service(PostprocessorServer::new(service))
                .serve(args.listen)
                .await?;
        }
        Command::BenchmarkWorker(config) => {
            let engine: CanonicalBackendEngine = Arc::new(BenchmarkBackend);
            let service = ChatWorkerFacade::new(
                engine,
                load_preprocessor(&config)?,
                config.max_batch_bytes,
                config.max_batch_bytes,
            )?;
            let (reporter, health) = tonic_health::server::health_reporter();
            reporter
                .set_serving::<ChatWorkerBridgeServer<ChatWorkerFacade>>()
                .await;
            let reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
                .build_v1()
                .context("build component reflection service")?;
            server_builder()
                .add_service(health)
                .add_service(reflection)
                .add_service(ChatWorkerBridgeServer::new(service))
                .serve(args.listen)
                .await?;
        }
        Command::VllmWorker(config) => {
            let mut argv = vec!["dynamo-vllm-sidecar".to_string()];
            argv.extend(config.sidecar_args.iter().cloned());
            let (engine, sidecar_config) = tokio::task::spawn_blocking(move || {
                dynamo_vllm_sidecar::VllmSidecarEngine::try_from_args(argv)
            })
            .await??;
            anyhow::ensure!(
                sidecar_config.disaggregation_mode == DisaggregationMode::Aggregated,
                "vLLM gRPC facade currently supports only aggregated workers"
            );
            serve_real_worker(Arc::new(engine), config, args.listen).await?;
        }
        Command::SglangWorker(config) => {
            let mut argv = vec!["dynamo-sglang-sidecar".to_string()];
            argv.extend(config.sidecar_args.iter().cloned());
            let (engine, sidecar_config) = tokio::task::spawn_blocking(move || {
                dynamo_sglang_sidecar::SglangSidecarEngine::try_from_args(argv)
            })
            .await??;
            anyhow::ensure!(
                sidecar_config.disaggregation_mode == DisaggregationMode::Aggregated,
                "SGLang gRPC facade currently supports only aggregated workers"
            );
            serve_real_worker(Arc::new(engine), config, args.listen).await?;
        }
        Command::Selector(config) => {
            let router_config = match config.router_config {
                Some(path) => serde_json::from_slice::<KvRouterConfig>(
                    &std::fs::read(&path)
                        .with_context(|| format!("read router config {}", path.display()))?,
                )
                .with_context(|| format!("parse router config {}", path.display()))?,
                None => KvRouterConfig::default(),
            };
            let selection = Arc::new(
                SelectionServiceBuilder::new(
                    router_config,
                    config.worker_type,
                    RouterPluginRegistry::default(),
                )
                .indexer_threads(config.indexer_threads)
                .build()
                .await?,
            );
            let mut topology_adapters = Vec::new();
            if let Some(path) = config.discovery_config.as_ref() {
                let discovery: SelectorDiscoveryFile = serde_json::from_slice(
                    &std::fs::read(path)
                        .with_context(|| format!("read discovery config {}", path.display()))?,
                )
                .with_context(|| format!("parse discovery config {}", path.display()))?;
                anyhow::ensure!(
                    !discovery.pools.is_empty(),
                    "discovery config must declare at least one pool"
                );
                for pool in discovery.pools {
                    let (reflector, _ready) = PodDiscovery::spawn_with_config(PodDiscoveryConfig {
                        namespace: pool.namespace,
                        inference_pool_name: pool.inference_pool_name,
                        data_parallel_size: pool.data_parallel_size,
                        kv_event_port_stride: pool.kv_event_port_stride,
                        kv_event_port: pool.kv_event_port,
                        replay_port: pool.replay_port,
                    })
                    .await?;
                    topology_adapters.push(TopologyAdapter::spawn_service(
                        reflector,
                        Arc::clone(&selection),
                        RegistrationDefaults {
                            model_name: pool.model_name,
                            block_size: pool.block_size,
                            total_kv_blocks: pool.total_kv_blocks,
                            max_num_batched_tokens: pool.max_num_batched_tokens,
                        },
                    ));
                }
            }
            let service = SelectorFacade::new(
                selection,
                config.max_batch_items,
                config.max_batch_bytes,
                config.max_concurrency,
            )?;
            let (reporter, health) = tonic_health::server::health_reporter();
            reporter
                .set_serving::<SelectorServer<SelectorFacade>>()
                .await;
            let reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
                .build_v1()
                .context("build component reflection service")?;
            server_builder()
                .add_service(health)
                .add_service(reflection)
                .add_service(SelectorServer::new(service))
                .serve(args.listen)
                .await?;
        }
    }
    Ok(())
}
