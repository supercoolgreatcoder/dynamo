#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Clone the compiled AGW static host for the synthetic P/D graph. The core
# invokes the existing Dynamo facade GenerateRaw/Generate RPCs; only transport
# order and handoff plumbing live in the static host.
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
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
dry_run=${PD_DRY_RUN:-0}
[[ $dry_run == 0 || $dry_run == 1 ]] || exit 2
preprocess_linger_us=${PD_PREPROCESS_BATCH_LINGER_US:-200}
[[ $preprocess_linger_us =~ ^[0-9]+$ ]] &&
  (( preprocess_linger_us <= 1000000 )) || exit 2
worker_threads=${PD_WORKER_THREADS:-12}
[[ $worker_threads =~ ^[1-9][0-9]*$ ]] &&
  (( worker_threads <= 128 )) || exit 2
grpc_channels=${PD_GRPC_CHANNELS_PER_ENDPOINT:-}
[[ -z $grpc_channels || ( $grpc_channels =~ ^[1-9][0-9]*$ && $grpc_channels -le 256 ) ]] || exit 2
stage_timing_every=${PD_STAGE_TIMING_EVERY:-0}
[[ $stage_timing_every =~ ^[0-9]+$ ]] &&
  (( stage_timing_every <= 1000000 )) || exit 2
rust_log=${PD_RUST_LOG:-}
[[ -z $rust_log || $rust_log == warn ]] || exit 2
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
for component in dynamo-pd-preprocessor:4 dynamo-pd-selector:4 dynamo-pd-prefill:4 dynamo-pd-decode:16; do
  name=${component%:*}
  replicas=${component#*:}
  "${vc[@]}" get deployment "$name" -o json |
    jq -e --argjson replicas "$replicas" \
      '.spec.replicas == $replicas and .status.readyReplicas == $replicas' >/dev/null
done

binary=${PD_AGW_BINARY:-/nix/store/j19wni2yhzxbim3h29bcxpq4rwgv6qj4-agentgateway-component-pipeline-0.0.0-b14ca87d0a/bin/agentgateway}
[[ $binary == /nix/store/*/bin/agentgateway ]] || exit 2
"${vc[@]}" exec "$stager_pod" -- test -x \
  "/shared/nix${binary#/nix}"

apply_json() {
  local body=$1
  printf '%s\n' "$body" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
  if [[ $dry_run == 0 ]]; then
    printf '%s\n' "$body" | "${vc[@]}" apply -f -
  fi
}
source_config=$("${vc[@]}" get configmap agw-static -o json)
config=$(jq -c --arg threads "$worker_threads" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-agw-static"},data:.data}
  | .data["config.yaml"] |= (
      gsub("dynamo-preprocessor";"dynamo-pd-preprocessor")
      | gsub("dynamo-selector";"dynamo-pd-selector")
      | gsub("dynamo-worker";"dynamo-pd-decode")
      | gsub("workerThreads: [0-9]+"; "workerThreads: " + $threads))
' <<<"$source_config")
[[ $(jq -r '.data["config.yaml"]' <<<"$config") == *'mode: static'* ]] || exit 2
apply_json "$config"

source_deployment=$("${vc[@]}" get deployment agw-static -o json)
deployment=$(jq -c --arg binary "$binary" --arg linger "$preprocess_linger_us" --arg channels "$grpc_channels" --arg timing "$stage_timing_every" --arg rust_log "$rust_log" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-agw-static"},spec:.spec}
  | .spec.replicas=1
  | .spec.selector.matchLabels.app="dynamo-pd-agw-static"
  | .spec.template.metadata.labels.app="dynamo-pd-agw-static"
  | .spec.template.spec.containers[0].command[0]=$binary
  | .spec.template.spec.containers[0].env +=
      [{name:"DYN_PREFILL_ENDPOINT",value:"http://dynamo-pd-prefill:50051"}]
  | .spec.template.spec.containers[0].env +=
      (if $timing == "0" then []
       else [{name:"DYN_STATIC_STAGE_TIMING_EVERY",value:$timing}] end)
  | .spec.template.spec.containers[0].env +=
      (if $rust_log == "" then []
       else [{name:"RUST_LOG",value:$rust_log}] end)
  | .spec.template.spec.containers[0].env |= map(
      if .name == "DYN_PREPROCESS_BATCH_LINGER_US"
      then .value=$linger else . end)
  | .spec.template.spec.containers[0].env |= (
      if $channels == "" then .
      else map(select(.name != "DYN_GRPC_CHANNELS_PER_ENDPOINT")) +
        [{name:"DYN_GRPC_CHANNELS_PER_ENDPOINT",value:$channels}]
      end)
  | .spec.template.spec.volumes |= map(
      if .name == "config" then .configMap.name="dynamo-pd-agw-static" else . end)
' <<<"$source_deployment")
apply_json "$deployment"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"dynamo-pd-agw-static"},
  spec:{selector:{app:"dynamo-pd-agw-static"},
  ports:[{name:"http",port:8080,targetPort:8080}]}}')"
if [[ $dry_run == 1 ]]; then
  echo "AGW static P/D fixture passed vCluster server dry-run"
  exit 0
fi
"${vc[@]}" rollout status deployment/dynamo-pd-agw-static --timeout=900s
echo "AGW static P/D fixture ready in $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
