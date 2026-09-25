#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Replace the split-pipeline binaries with one staged, ABI-matched Nix bundle.
# Only the explicit vCluster kubeconfig and namespace are ever used.
set -euo pipefail

if [ "$#" -ne 1 ] || [[ "$1" != /nix/store/* ]] || [[ "${1#/nix/store/}" == */* ]]; then
  echo "usage: $0 /nix/store/<component-pipeline-bundle>" >&2
  exit 2
fi
bundle=$1
basename=${bundle##*/}
: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig path}"
: "${VCLUSTER_EXPECTED_SERVER:?set the expected vCluster API server URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
test -f "$VCLUSTER_KUBECONFIG"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "kubeconfig server $actual_server does not match expected vCluster server" >&2
  exit 2
fi
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")

"${vc[@]}" get jobs -o json | jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
  echo "refusing to roll out while a benchmark Job is active" >&2
  exit 2
}
"${vc[@]}" get deployment envoy-independent agw-static agw-generic envoy-callouts -o json |
  jq -e 'all(.items[]; if .metadata.name == "envoy-independent" then
      .spec.replicas == 1 and .status.readyReplicas == 1
    else .spec.replicas == 0 end)' >/dev/null || {
    echo "expected one Ready Envoy-direct gateway and all other gateways scaled to zero" >&2
    exit 2
  }
"${vc[@]}" get deployment dynamo-preprocessor dynamo-selector dynamo-benchmark-worker -o json |
  jq -e 'all(.items[]; .spec.replicas == .status.readyReplicas)' >/dev/null || {
    echo "facade Deployments are not fully Ready" >&2
    exit 2
  }
for artifact in bin/agentgateway bin/dynamo-component-facade bin/envoy-static lib/libgeneric_pipeline.so; do
  "${vc[@]}" exec dynamo-component-store-stager -- test -f "/shared/nix/store/$basename/$artifact" || {
    echo "missing staged artifact: $bundle/$artifact" >&2
    exit 2
  }
done

patch_command() {
  local deployment=$1 binary=$2 patch
  patch=$(jq -nc --arg binary "$binary" '[
    {op:"replace",path:"/spec/template/spec/containers/0/command/0",value:$binary}
  ]')
  "${vc[@]}" patch deployment "$deployment" --type=json -p "$patch"
}

patch_envoy() {
  local deployment=$1 index patch
  index=$("${vc[@]}" get deployment "$deployment" -o json | jq -r '
    [.spec.template.spec.containers[0].env | to_entries[] |
      select(.value.name == "ENVOY_DYNAMIC_MODULES_SEARCH_PATH") | .key][0] // empty')
  [[ "$index" =~ ^[0-9]+$ ]] || {
    echo "$deployment has no Envoy dynamic-module search path" >&2
    exit 2
  }
  patch=$(jq -nc --arg binary "$bundle/bin/envoy-static" --arg module "$bundle/lib" \
    --argjson index "$index" '[
      {op:"replace",path:"/spec/template/spec/containers/0/command/0",value:$binary},
      {op:"replace",path:("/spec/template/spec/containers/0/env/"+($index|tostring)+"/value"),value:$module}
    ]')
  "${vc[@]}" patch deployment "$deployment" --type=json -p "$patch"
}

# Inactive arms first; then move the live serving graph from its leaves upward.
patch_command agw-static "$bundle/bin/agentgateway"
patch_command agw-generic "$bundle/bin/agentgateway"
patch_envoy envoy-callouts
for deployment in dynamo-benchmark-worker dynamo-preprocessor dynamo-selector; do
  patch_command "$deployment" "$bundle/bin/dynamo-component-facade"
  "${vc[@]}" rollout status "deployment/$deployment" --timeout=300s
  if [ "$deployment" = dynamo-benchmark-worker ]; then
    bash "$(dirname "$0")/refresh-envoy-callout-worker-map.sh" \
      "$VCLUSTER_KUBECONFIG" "$VCLUSTER_NAMESPACE"
  fi
done
patch_envoy envoy-independent
"${vc[@]}" rollout status deployment/envoy-independent --timeout=300s
echo "rolled out $bundle to all split-pipeline Deployments in $VCLUSTER_NAMESPACE"
