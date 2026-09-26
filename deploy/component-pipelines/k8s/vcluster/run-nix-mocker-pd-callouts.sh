#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Benchmark-only static Envoy callout snapshot of the Ready P/D decode Pods.
# Production should publish these clusters with CDS/xDS on Pod churn.
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
dry_run=${PD_DRY_RUN:-0}
[[ $dry_run == 0 || $dry_run == 1 ]] || exit 2
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
"${vc[@]}" exec "$stager_pod" -- test -x "/shared/nix${bundle#/nix}/bin/envoy-static"
"${vc[@]}" exec "$stager_pod" -- test -f "/shared/nix${bundle#/nix}/lib/libgeneric_pipeline.so"
"${vc[@]}" get configmap dynamo-pd-contracts -o json |
  jq -e '.data["disaggregated.yaml"] and .binaryData["components_descriptor.bin"]' >/dev/null
for component in dynamo-pd-preprocessor:4 dynamo-pd-selector:4 dynamo-pd-prefill:4 dynamo-pd-decode:16; do
  name=${component%:*}
  replicas=${component#*:}
  "${vc[@]}" get deployment "$name" -o json |
    jq -e --argjson replicas "$replicas" '
      .spec.replicas == $replicas and .status.readyReplicas == $replicas
    ' >/dev/null
done
pods=$("${vc[@]}" get pods -l app=dynamo-pd-decode -o json)
ready_pods=$(jq -c '[.items[] | select(.metadata.deletionTimestamp == null) |
  select(.status.phase == "Running") |
  select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
  .status.podIP]' <<<"$pods")
jq -e 'length == 16 and all(.[]; test("^[0-9]{1,3}(\\.[0-9]{1,3}){3}$")) and (unique|length) == 16' \
  <<<"$ready_pods" >/dev/null

# The module maps http://IP:50051 to a cluster named IP. This preserves the
# selector's exact Pod target, unlike mapping every IP to a load-balanced Service.
decode_clusters=$(jq -r '[.[] |
  "    - name: \"" + . + "\"\n" +
  "      type: STATIC\n" +
  "      connect_timeout: 2s\n" +
  "      typed_extension_protocol_options:\n" +
  "        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:\n" +
  "          \"@type\": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions\n" +
  "          explicit_http_config:\n" +
  "            http2_protocol_options:\n" +
  "              initial_stream_window_size: 8388608\n" +
  "              initial_connection_window_size: 16777216\n" +
  "              max_concurrent_streams: 4096\n" +
  "      load_assignment:\n" +
  "        cluster_name: \"" + . + "\"\n" +
  "        endpoints:\n" +
  "          - lb_endpoints:\n" +
  "              - endpoint: {address: {socket_address: {address: " + . + ", port_value: 50051}}}"
 ] | join("\n")' <<<"$ready_pods")
repo_root=$(git rev-parse --show-toplevel)
source_config=$repo_root/deploy/component-pipelines/hosts/envoy-generic/envoy-callouts.yaml
test -s "$source_config"
rendered=$(jq -nr --rawfile source "$source_config" --arg clusters "$decode_clusters" '
  $source
  | gsub("graphs/aggregate.yaml"; "graphs/disaggregated.yaml")
  | gsub("dynamo-preprocessor"; "dynamo-pd-preprocessor")
  | gsub("dynamo-selector"; "dynamo-pd-selector")
  | gsub("dynamo-worker"; "dynamo-pd-prefill")
  | . + "\n" + $clusters + "\n"
')
[[ $rendered == *'"cluster_from_authority":true'* ]] || {
  echo "callout config must derive decode cluster names from selected authorities" >&2
  exit 2
}
apply_json() {
  local body=$1
  printf '%s\n' "$body" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
  if [[ $dry_run == 0 ]]; then
    printf '%s\n' "$body" | "${vc[@]}" apply -f -
  fi
}
config=$(jq -nc --arg data "$rendered" '{apiVersion:"v1",kind:"ConfigMap",
  metadata:{name:"dynamo-pd-envoy-callouts"},data:{"envoy.yaml":$data}}')
apply_json "$config"
source_deployment=$("${vc[@]}" get deployment envoy-callouts -o json)
deployment=$(jq -c --arg bundle "$bundle" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-envoy-callouts"},spec:.spec}
  | .spec.replicas=1
  | .spec.selector.matchLabels.app="dynamo-pd-envoy-callouts"
  | .spec.template.metadata.labels.app="dynamo-pd-envoy-callouts"
  | .spec.template.spec.containers[0].command[0]=($bundle+"/bin/envoy-static")
  | .spec.template.spec.containers[0].args |=
      map(if . == "20" then "6" else . end)
  | .spec.template.spec.containers[0].env |=
      map(if .name == "GENERIC_PIPELINE_THREADS" then .value="8"
          elif .name == "ENVOY_DYNAMIC_MODULES_SEARCH_PATH"
          then .value=($bundle+"/lib") else . end)
  | .spec.template.spec.volumes |= map(
      if .name == "config" then .configMap.name="dynamo-pd-envoy-callouts"
      elif .name == "pipeline" then
        .configMap.name="dynamo-pd-contracts"
        | .configMap.items |= map(if .key == "aggregate.yaml" then
            .key="disaggregated.yaml" | .path="graphs/disaggregated.yaml"
          else . end)
      else . end)
' <<<"$source_deployment")
apply_json "$deployment"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"dynamo-pd-envoy-callouts"},
  spec:{selector:{app:"dynamo-pd-envoy-callouts"},
  ports:[{name:"http",port:8080,targetPort:8080}]}}')"
if [[ $dry_run == 1 ]]; then
  echo "P/D callout fixture passed vCluster server dry-run with 16 distinct decode Pod clusters"
  exit 0
fi
# A ConfigMap update does not change the Deployment template. Restart Envoy so
# cluster and graph snapshots change atomically after a Pod-IP refresh.
"${vc[@]}" rollout restart deployment/dynamo-pd-envoy-callouts
"${vc[@]}" rollout status deployment/dynamo-pd-envoy-callouts --timeout=900s
echo "P/D callout fixture ready with 16 selected-Pod clusters in $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
