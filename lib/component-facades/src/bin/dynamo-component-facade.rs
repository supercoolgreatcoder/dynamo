// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::{Parser, Subcommand};
use dynamo_component_facades::{
    postprocess::PostprocessorFacade,
    preprocess::PreprocessorFacade,
    proto::{
        postprocessor_server::PostprocessorServer, preprocessor_server::PreprocessorServer,
        selector_server::SelectorServer, FILE_DESCRIPTOR_SET,
    },
    selector::SelectorFacade,
};
use dynamo_kv_router::{
    WorkerType, config::KvRouterConfig, plugins::RouterPluginRegistry,
    services::selection::SelectionServiceBuilder,
};
use dynamo_llm::{model_card::ModelDeploymentCard, preprocessor::OpenAIPreprocessor};
use tonic::transport::Server;

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
    Preprocessor(ModelArgs),
    Postprocessor(PostprocessorArgs),
    Selector(SelectorArgs),
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
struct SelectorArgs {
    #[arg(long, env = "DYN_COMPONENT_ROUTER_CONFIG")]
    router_config: Option<PathBuf>,
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

fn load_preprocessor(args: &ModelArgs) -> anyhow::Result<Arc<OpenAIPreprocessor>> {
    let model_card =
        ModelDeploymentCard::load_from_disk(&args.model_path, args.chat_template.as_deref())
            .with_context(|| format!("load model assets from {}", args.model_path.display()))?;
    OpenAIPreprocessor::new(model_card)
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
            Server::builder()
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
            Server::builder()
                .add_service(health)
                .add_service(reflection)
                .add_service(PostprocessorServer::new(service))
                .serve(args.listen)
                .await?;
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
            Server::builder()
                .add_service(health)
                .add_service(reflection)
                .add_service(SelectorServer::new(service))
                .serve(args.listen)
                .await?;
        }
    }
    Ok(())
}
