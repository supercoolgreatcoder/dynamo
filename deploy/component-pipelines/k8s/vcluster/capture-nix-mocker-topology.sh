#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Save the benchmark's applied topology as a portable, secret-free Kubernetes
# List. Call after gateway rotations end; the dynamic Envoy worker map must be
# refreshed after recreating worker Pods from this snapshot.
set -euo pipefail

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${RESULT_DIR:?set the local result directory}"
test -f "$VCLUSTER_KUBECONFIG"
test -d "$RESULT_DIR"
server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $server == "$VCLUSTER_EXPECTED_SERVER" ]] || {
  echo "refusing non-vCluster API: $server" >&2
  exit 2
}
out=$RESULT_DIR/benchmark_topology.json
[[ ! -e $out ]] || { echo "refusing to overwrite $out" >&2; exit 2; }
tmp=$(mktemp "$RESULT_DIR/.benchmark-topology.XXXXXXXX")
cleanup() { rm -f -- "$tmp"; }
trap cleanup EXIT

vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
resources=(
  deployment/agw-static deployment/agw-generic
  deployment/envoy-independent deployment/envoy-callouts
  deployment/dynamo-preprocessor deployment/dynamo-selector
  deployment/dynamo-benchmark-worker
  service/agw-static service/agw-generic
  service/envoy-independent service/envoy-callouts
  service/dynamo-preprocessor service/dynamo-selector service/dynamo-worker
  configmap/component-pipeline configmap/benchmark-model
  configmap/selector-discovery configmap/agw-static configmap/agw-generic
  configmap/envoy-independent configmap/envoy-callouts
  inferencepool/benchmark-workers
  serviceaccount/component-selector role/component-selector
  rolebinding/component-selector
)

"${vc[@]}" get "${resources[@]}" -o json |
  jq --arg namespace "$VCLUSTER_NAMESPACE" '
    {apiVersion:"v1",kind:"List",items:[.items[] |
      {apiVersion,kind,metadata:{name:.metadata.name,namespace:$namespace}}
      + (if .kind == "ConfigMap" then
           {data:(.data // {}),binaryData:(.binaryData // {})}
         elif .kind == "ServiceAccount" then {}
         elif .kind == "Role" then {rules:.rules}
         elif .kind == "RoleBinding" then
           {roleRef:.roleRef,subjects:.subjects}
         else {spec:.spec} end)
      | if .kind == "Service" then
          .spec |= del(.clusterIP,.clusterIPs,.ipFamilies,.ipFamilyPolicy)
        else . end
    ]}
  ' > "$tmp"

jq -e '.items | length == 25' "$tmp" >/dev/null || {
  echo "topology snapshot is incomplete" >&2
  exit 1
}
mv -- "$tmp" "$out"
echo "captured 25 secret-free benchmark resources in $out"
