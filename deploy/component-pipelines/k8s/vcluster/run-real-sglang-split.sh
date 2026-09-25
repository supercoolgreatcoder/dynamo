#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Deliberately never reads the ambient Kubernetes context. This deploys only
# the isolated real SGLang aggregate correctness fixture, not benchmark pods.
set -euo pipefail

required=(
  VCLUSTER_KUBECONFIG VCLUSTER_EXPECTED_SERVER VCLUSTER_NAMESPACE
  FACADE_STORE_PATH SGLANG_WORKER_ENV_PATH GATEWAY_BUNDLE_PATH
  NIX_STORE_NFS_SERVER NIX_STORE_NFS_PATH MODEL_NFS_SERVER MODEL_NFS_PATH
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "missing required setting: $name" >&2
    exit 2
  fi
done

kubectl_bin=${KUBECTL_BIN:-kubectl}
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$kubectl_bin" >/dev/null
command -v "$envsubst_bin" >/dev/null

actual_server=$(
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" \
    config view --minify --output jsonpath='{.clusters[0].cluster.server}'
)
if [[ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]]; then
  echo "refusing non-vCluster API: expected $VCLUSTER_EXPECTED_SERVER, got $actual_server" >&2
  exit 2
fi
case "$FACADE_STORE_PATH" in
  /nix/store/*) ;;
  *) echo "FACADE_STORE_PATH must be an immutable Nix store path" >&2; exit 2 ;;
esac
case "$SGLANG_WORKER_ENV_PATH" in
  /nix/store/*) ;;
  *) echo "SGLANG_WORKER_ENV_PATH must be an immutable Nix store path" >&2; exit 2 ;;
esac
case "$GATEWAY_BUNDLE_PATH" in
  /nix/store/*) ;;
  *) echo "GATEWAY_BUNDLE_PATH must be an immutable Nix store path" >&2; exit 2 ;;
esac

staged_facade="/shared/nix/store/${FACADE_STORE_PATH#/nix/store/}/bin/dynamo-component-facade"
staged_engine="/shared/nix/store/${SGLANG_WORKER_ENV_PATH#/nix/store/}/bin/python"
staged_gateway="/shared/nix/store/${GATEWAY_BUNDLE_PATH#/nix/store/}/bin/agentgateway"
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  exec dynamo-component-store-stager -- test -x "$staged_facade"
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  exec dynamo-component-store-stager -- test -x "$staged_engine"
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  exec dynamo-component-store-stager -- test -x "$staged_gateway"

manifest="$(dirname "$0")/real-sglang-split.yaml.tmpl"
substitutions='${VCLUSTER_NAMESPACE} ${FACADE_STORE_PATH} ${SGLANG_WORKER_ENV_PATH} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH} ${MODEL_NFS_SERVER} ${MODEL_NFS_PATH}'
"$envsubst_bin" "$substitutions" < "$manifest" |
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
    apply --dry-run=server -f - >/dev/null
"$envsubst_bin" "$substitutions" < "$manifest" |
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
    apply -f -

"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-sglang-split --timeout=900s
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-qwen3-preprocessor --timeout=300s

# This second manifest adds a separate InferencePool, selector, and gateway.
# It reuses only the already-proven immutable OpenAPI/descriptor ConfigMap;
# the benchmark routing graph and its Deployments are not modified.
route_manifest="$(dirname "$0")/real-sglang-route.yaml.tmpl"
route_substitutions='${VCLUSTER_NAMESPACE} ${FACADE_STORE_PATH} ${GATEWAY_BUNDLE_PATH} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}'
"$envsubst_bin" "$route_substitutions" < "$route_manifest" |
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
    apply --dry-run=server -f - >/dev/null
"$envsubst_bin" "$route_substitutions" < "$route_manifest" |
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
    apply -f -
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-qwen3-selector --timeout=300s
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-qwen3-agw-generic --timeout=300s
