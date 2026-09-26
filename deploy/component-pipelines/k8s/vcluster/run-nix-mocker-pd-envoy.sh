#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Clone the existing Nix-built Envoy generic host against the already-smoked
# synthetic P/D graph. Every write is guarded by the exact vCluster server.
set -euo pipefail
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
bundle=${PD_ENVOY_BUNDLE_PATH:-/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699}
[[ $bundle == /nix/store/* && ${bundle#/nix/store/} != */* ]] || exit 2
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
"${vc[@]}" exec "$stager_pod" -- test -x "/shared/nix${bundle#/nix}/bin/envoy-static"
"${vc[@]}" exec "$stager_pod" -- test -f "/shared/nix${bundle#/nix}/lib/libgeneric_pipeline.so"
"${vc[@]}" get configmap dynamo-pd-contracts -o json |
  jq -e '.data["disaggregated.yaml"] and .binaryData["components_descriptor.bin"]' >/dev/null

dry_run=${PD_DRY_RUN:-0}
[[ $dry_run == 0 || $dry_run == 1 ]] || exit 2
apply_json() {
  local body=$1
  printf '%s\n' "$body" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
  if [[ $dry_run == 0 ]]; then
    printf '%s\n' "$body" | "${vc[@]}" apply -f -
  fi
}

source_config=$("${vc[@]}" get configmap envoy-independent -o json)
config=$(jq -c '
  {apiVersion,kind,metadata:{name:"dynamo-pd-envoy-generic"},data:.data}
  | .data["envoy.yaml"] |= gsub("graphs/aggregate.yaml";"graphs/disaggregated.yaml")
' <<<"$source_config")
apply_json "$config"

source_deployment=$("${vc[@]}" get deployment envoy-independent -o json)
deployment=$(jq -c --arg bundle "$bundle" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-envoy-generic"},spec:.spec}
  | .spec.replicas=1
  | .spec.selector.matchLabels.app="dynamo-pd-envoy-generic"
  | .spec.template.metadata.labels.app="dynamo-pd-envoy-generic"
  | .spec.template.spec.containers[0].command[0]=($bundle+"/bin/envoy-static")
  | .spec.template.spec.containers[0].env |= map(
      if .name == "ENVOY_DYNAMIC_MODULES_SEARCH_PATH"
      then .value=($bundle+"/lib") else . end)
  | .spec.template.spec.volumes |= map(
      if .name == "config" then .configMap.name="dynamo-pd-envoy-generic"
      elif .name == "pipeline" then
        .configMap.name="dynamo-pd-contracts"
        | .configMap.items |= map(if .key == "aggregate.yaml" then
            .key="disaggregated.yaml" | .path="graphs/disaggregated.yaml"
          else . end)
      else . end)
' <<<"$source_deployment")
apply_json "$deployment"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"dynamo-pd-envoy-generic"},
  spec:{selector:{app:"dynamo-pd-envoy-generic"},
  ports:[{name:"http",port:8080,targetPort:8080}]}}')"

if [[ $dry_run == 1 ]]; then
  echo "Envoy P/D fixture passed server dry-run in $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
  exit 0
fi
"${vc[@]}" rollout status deployment/dynamo-pd-envoy-generic --timeout=900s
echo "Envoy P/D fixture ready in $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
