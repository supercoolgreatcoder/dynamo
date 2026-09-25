#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Deploy the isolated real-vLLM aggregate correctness fixture inside vCluster.
set -euo pipefail

required=(
  VCLUSTER_KUBECONFIG VCLUSTER_EXPECTED_SERVER VCLUSTER_NAMESPACE
  FACADE_STORE_PATH VLLM_WORKER_ENV_PATH VLLM_RS_PATH GATEWAY_BUNDLE_PATH
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
    config view --minify -o jsonpath='{.clusters[0].cluster.server}'
)
if [[ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]]; then
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
fi
for name in FACADE_STORE_PATH VLLM_WORKER_ENV_PATH VLLM_RS_PATH GATEWAY_BUNDLE_PATH; do
  case "${!name}" in
    /nix/store/*) ;;
    *) echo "$name must be an immutable Nix store path" >&2; exit 2 ;;
  esac
done
vc=("$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "refusing real-worker deployment during an active benchmark" >&2
    exit 2
  }
for inactive in agw-static agw-generic envoy-independent envoy-callouts \
  dynamo-frontend-reference dynamo-reference-worker \
  real-vllm real-sglang real-sglang-split; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null || {
      echo "$inactive is active; refusing confounded real-worker test" >&2
      exit 2
    }
done
"${vc[@]}" get serviceaccount component-selector >/dev/null
"${vc[@]}" get configmap component-pipeline >/dev/null
for artifact in \
  "${FACADE_STORE_PATH#/nix/store/}/bin/dynamo-component-facade" \
  "${VLLM_WORKER_ENV_PATH#/nix/store/}/bin/python" \
  "${VLLM_RS_PATH#/nix/store/}/bin/vllm-rs" \
  "${GATEWAY_BUNDLE_PATH#/nix/store/}/bin/agentgateway"; do
  "${vc[@]}" exec dynamo-component-store-stager -- \
    test -x "/shared/nix/store/$artifact"
done

manifest="$(dirname "$0")/real-vllm-split.yaml.tmpl"
substitutions='${VCLUSTER_NAMESPACE} ${FACADE_STORE_PATH} ${VLLM_WORKER_ENV_PATH} ${VLLM_RS_PATH} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH} ${MODEL_NFS_SERVER} ${MODEL_NFS_PATH}'
"$envsubst_bin" "$substitutions" < "$manifest" |
  "${vc[@]}" apply --dry-run=server -f - >/dev/null
"$envsubst_bin" "$substitutions" < "$manifest" |
  "${vc[@]}" apply -f -

"${vc[@]}" rollout status deployment/real-vllm-split --timeout=900s
"${vc[@]}" rollout status deployment/real-qwen3-preprocessor --timeout=300s

route_manifest="$(dirname "$0")/real-vllm-route.yaml.tmpl"
route_substitutions='${VCLUSTER_NAMESPACE} ${FACADE_STORE_PATH} ${GATEWAY_BUNDLE_PATH} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}'
"$envsubst_bin" "$route_substitutions" < "$route_manifest" |
  "${vc[@]}" apply --dry-run=server -f - >/dev/null
"$envsubst_bin" "$route_substitutions" < "$route_manifest" |
  "${vc[@]}" apply -f -
"${vc[@]}" rollout status deployment/real-vllm-selector --timeout=300s
"${vc[@]}" rollout status deployment/real-vllm-agw-generic --timeout=300s
