// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Publish the native engine's post-start registration snapshot for the
//! EPP-derived InferencePool Pod watcher. Kubernetes is only a metadata
//! transport here; no Dynamo runtime or engine-specific parser is involved.

use anyhow::{Context, ensure};
use dynamo_backend_common::EngineConfig;
use dynamo_ext_proc::{WORKER_METADATA_ANNOTATION, WorkerMetadata};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
};
use serde_json::json;

pub struct PodPublicationTarget {
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

fn metadata_from_engine(config: &EngineConfig, pod_uid: &str) -> WorkerMetadata {
    let llm = config.llm.as_ref();
    WorkerMetadata {
        schema_version: "v1".to_string(),
        model_name: Some(
            config
                .served_model_name
                .clone()
                .unwrap_or_else(|| config.model.clone()),
        ),
        block_size: llm.and_then(|value| value.kv_cache_block_size),
        total_kv_blocks: llm.and_then(|value| value.total_kv_blocks),
        max_num_batched_tokens: llm.and_then(|value| value.max_num_batched_tokens),
        stable_routing_id: Some(pod_uid.to_string()),
        ..Default::default()
    }
}

/// Patch this process's own Pod only after `LLMEngine::start` has discovered
/// real cache capacity. A UID check prevents a stale process from publishing
/// metadata to a different Pod that happens to reuse its name.
pub async fn publish_engine_metadata(
    target: &PodPublicationTarget,
    engine_config: &EngineConfig,
) -> anyhow::Result<WorkerMetadata> {
    ensure!(!target.namespace.is_empty(), "Pod namespace is empty");
    ensure!(!target.name.is_empty(), "Pod name is empty");
    ensure!(!target.uid.is_empty(), "Pod UID is empty");
    ensure!(
        !engine_config.model.is_empty(),
        "engine model identity is empty"
    );
    let metadata = metadata_from_engine(engine_config, &target.uid);
    let serialized = serde_json::to_string(&metadata).context("serialize worker metadata")?;
    let client = Client::try_default()
        .await
        .context("create Kubernetes client")?;
    let pods: Api<Pod> = Api::namespaced(client, &target.namespace);
    for attempt in 0..3 {
        let pod = pods
            .get(&target.name)
            .await
            .with_context(|| format!("read own Pod {}/{}", target.namespace, target.name))?;
        ensure!(
            pod.metadata.uid.as_deref() == Some(target.uid.as_str()),
            "Pod UID changed before worker metadata publication"
        );
        let patch = json!({
            "metadata": {
                "resourceVersion": pod.metadata.resource_version,
                "annotations": {(WORKER_METADATA_ANNOTATION): serialized}
            }
        });
        match pods
            .patch(&target.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => {
                tracing::info!(
                    pod = %target.name,
                    model = ?metadata.model_name,
                    block_size = ?metadata.block_size,
                    total_kv_blocks = ?metadata.total_kv_blocks,
                    "published native engine metadata on worker Pod"
                );
                return Ok(metadata);
            }
            Err(kube::Error::Api(response)) if response.code == 409 && attempt < 2 => {
                // The API's resourceVersion precondition caught a concurrent
                // Pod update. Re-read before retrying so we never overwrite
                // another publisher's metadata snapshot.
                continue;
            }
            Err(error) => return Err(error).context("patch worker Pod annotation"),
        }
    }
    unreachable!("conflict retry loop returns on its final attempt")
}

#[cfg(test)]
mod tests {
    use dynamo_backend_common::LlmRegistration;

    use super::*;

    #[test]
    fn native_engine_capacity_maps_without_guessing() {
        let config = EngineConfig {
            model: "model-path".to_string(),
            served_model_name: Some("served-model".to_string()),
            llm: Some(LlmRegistration {
                kv_cache_block_size: Some(64),
                total_kv_blocks: Some(2401),
                max_num_batched_tokens: Some(4096),
                ..Default::default()
            }),
            ..Default::default()
        };
        let metadata = metadata_from_engine(&config, "pod-uid-1");
        assert_eq!(metadata.model_name.as_deref(), Some("served-model"));
        assert_eq!(metadata.block_size, Some(64));
        assert_eq!(metadata.total_kv_blocks, Some(2401));
        assert_eq!(metadata.max_num_batched_tokens, Some(4096));
        assert_eq!(metadata.stable_routing_id.as_deref(), Some("pod-uid-1"));
    }

    #[test]
    fn missing_capacity_remains_unpublished() {
        let config = EngineConfig {
            model: "model".to_string(),
            llm: Some(LlmRegistration::default()),
            ..Default::default()
        };
        let metadata = metadata_from_engine(&config, "pod-uid-2");
        assert_eq!(metadata.model_name.as_deref(), Some("model"));
        assert_eq!(metadata.block_size, None);
        assert_eq!(metadata.total_kv_blocks, None);
    }
}
