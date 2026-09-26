#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Clone the previously verified Qwen3/vLLM aggregate fixture into an isolated
# two-GPU P/D fixture. Every Kubernetes request uses the explicit vCluster API.
set -euo pipefail

for name in VCLUSTER_KUBECONFIG VCLUSTER_EXPECTED_SERVER VCLUSTER_NAMESPACE \
  FACADE_STORE_PATH GATEWAY_BUNDLE_PATH; do
  [[ -n ${!name:-} ]] || { echo "missing $name" >&2; exit 2; }
done
[[ $FACADE_STORE_PATH == /nix/store/* ]] || exit 2
[[ $GATEWAY_BUNDLE_PATH == /nix/store/* ]] || exit 2
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || {
  echo "this fixture is restricted to the gateway-poc vCluster" >&2
  exit 2
}
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || {
  echo "this fixture is restricted to dynamo-components-v2" >&2
  exit 2
}

kubectl_bin=${KUBECTL_BIN:-kubectl}
actual_server=$(
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" \
    config view --minify -o jsonpath='{.clusters[0].cluster.server}'
)
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || {
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
}
vc=("$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
dry_run=${PD_DRY_RUN:-0}
[[ $dry_run == 0 || $dry_run == 1 ]] || {
  echo "PD_DRY_RUN must be 0 or 1" >&2
  exit 2
}
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
for inactive in agw-static agw-generic envoy-independent envoy-callouts \
  dynamo-frontend-reference real-vllm-split real-vllm-agw-generic \
  real-sglang-split real-qwen3-agw-generic; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null
done
if [[ $dry_run == 0 ]]; then
  for basename in \
    "${FACADE_STORE_PATH#/nix/store/}/bin/dynamo-component-facade" \
    "${GATEWAY_BUNDLE_PATH#/nix/store/}/bin/agentgateway"; do
    "${vc[@]}" exec "$stager_pod" -- \
      test -x "/shared/nix/store/$basename"
  done
fi

repo_root=$(git rev-parse --show-toplevel)
spec_dir=$repo_root/deploy/component-pipelines/specs
graph_file=$repo_root/deploy/component-pipelines/graphs/disaggregated.yaml
tmp_dir=$(mktemp -d -t dynamo-real-vllm-pd.XXXXXXXX)
cleanup() { rm -r -- "$tmp_dir"; }
trap cleanup EXIT
"$FACADE_STORE_PATH/bin/dynamo-component-facade" descriptor \
  --output "$tmp_dir/components_descriptor.bin"

apply_json() {
  local body=$1
  printf '%s\n' "$body" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
  if [[ $dry_run == 0 ]]; then
    printf '%s\n' "$body" | "${vc[@]}" apply -f -
  fi
}

# Mount the descriptor emitted by this exact facade, not the old benchmark
# descriptor. The AGW and Envoy transports both use this protobuf contract.
contracts=$(
  "${vc[@]}" create configmap real-vllm-pd-contracts \
    --from-file="components_descriptor.bin=$tmp_dir/components_descriptor.bin" \
    --from-file="openai-chat.yaml=$spec_dir/openai-chat.yaml" \
    --from-file="preprocessor.yaml=$spec_dir/preprocessor.yaml" \
    --from-file="selector.yaml=$spec_dir/selector.yaml" \
    --from-file="chat-worker.yaml=$spec_dir/chat-worker.yaml" \
    --dry-run=client -o json
)
apply_json "$contracts"

graph=$(
  jq -n --rawfile graph "$graph_file" '
    {apiVersion:"v1",kind:"ConfigMap",metadata:{name:"real-vllm-pd-graph"},
     data:{"disaggregated.yaml":($graph
       | gsub("http://dynamo-preprocessor:50051";"http://real-vllm-pd-preprocessor:50051")
       | gsub("http://dynamo-selector:50051";"http://real-vllm-pd-selector:50051")
       | gsub("http://dynamo-prefill:50051";"http://real-vllm-pd-prefill:50051"))}}
  '
)
apply_json "$graph"

base_worker=$("${vc[@]}" get deployment real-vllm-split -o json)
for role in prefill decode; do
  name=real-vllm-pd-$role
  if [[ $role == prefill ]]; then kv_role=kv_producer; else kv_role=kv_consumer; fi
  kv_config=$(jq -nc --arg role "$kv_role" '{kv_connector:"NixlConnector",kv_role:$role,
    kv_buffer_device:"cuda",kv_connector_extra_config:{backends:["UCX"],num_threads:8}}')
  worker=$(jq -c --arg name "$name" --arg role "$role" \
    --arg kv_config "$kv_config" --arg facade "$FACADE_STORE_PATH" '
    {apiVersion,kind,metadata:{name:$name},spec:.spec}
    | .spec.replicas=1
    | .spec.selector.matchLabels.app=$name
    | .spec.template.metadata.labels.app=$name
    | .spec.template.spec.containers |= map(
        if .name == "engine" then
          .args += ["--kv-transfer-config",$kv_config]
          | .env = ((.env // []) + [{name:"VLLM_NIXL_SIDE_CHANNEL_HOST",
              valueFrom:{fieldRef:{fieldPath:"status.podIP"}}}])
        elif .name == "facade" then
          .command[0]=($facade+"/bin/dynamo-component-facade")
          | .args += ["--disaggregation-mode",$role]
        else . end)
  ' <<<"$base_worker")
  apply_json "$worker"
  service=$(jq -nc --arg name "$name" '{apiVersion:"v1",kind:"Service",
    metadata:{name:$name},spec:{selector:{app:$name},
    ports:[{name:"grpc",port:50051,targetPort:"grpc"}]}}')
  apply_json "$service"
done

base_preprocessor=$("${vc[@]}" get deployment real-qwen3-preprocessor -o json)
preprocessor=$(jq -c --arg facade "$FACADE_STORE_PATH" '
  {apiVersion,kind,metadata:{name:"real-vllm-pd-preprocessor"},spec:.spec}
  | .spec.replicas=1
  | .spec.selector.matchLabels.app="real-vllm-pd-preprocessor"
  | .spec.template.metadata.labels.app="real-vllm-pd-preprocessor"
  | .spec.template.spec.containers[0].command[0]=($facade+"/bin/dynamo-component-facade")
' <<<"$base_preprocessor")
apply_json "$preprocessor"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"real-vllm-pd-preprocessor"},
  spec:{selector:{app:"real-vllm-pd-preprocessor"},
  ports:[{name:"grpc",port:50051,targetPort:"grpc"}]}}')"

base_pool=$("${vc[@]}" get inferencepool real-vllm-split -o json)
pool=$(jq -c '
  {apiVersion,kind,metadata:{name:"real-vllm-pd-decode"},spec:.spec}
  | .spec.selector.matchLabels.app="real-vllm-pd-decode"
  | .spec.endpointPickerRef.name="real-vllm-pd-selector"
' <<<"$base_pool")
apply_json "$pool"

selector_config=$(jq -nc --arg ns "$VCLUSTER_NAMESPACE" '
  {apiVersion:"v1",kind:"ConfigMap",metadata:{name:"real-vllm-pd-selector"},
   data:{"discovery.json":({pools:[{namespace:$ns,
     inference_pool_name:"real-vllm-pd-decode",model_name:"Qwen/Qwen3-0.6B",
     block_size:16}]}|tojson)}}')
apply_json "$selector_config"
base_selector=$("${vc[@]}" get deployment real-vllm-selector -o json)
selector=$(jq -c --arg facade "$FACADE_STORE_PATH" '
  {apiVersion,kind,metadata:{name:"real-vllm-pd-selector"},spec:.spec}
  | .spec.replicas=1
  | .spec.selector.matchLabels.app="real-vllm-pd-selector"
  | .spec.template.metadata.labels.app="real-vllm-pd-selector"
  | .spec.template.spec.containers[0].command[0]=($facade+"/bin/dynamo-component-facade")
  | .spec.template.spec.volumes |= map(if .name == "config" then
      .configMap.name="real-vllm-pd-selector" else . end)
' <<<"$base_selector")
apply_json "$selector"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"real-vllm-pd-selector"},
  spec:{selector:{app:"real-vllm-pd-selector"},
  ports:[{name:"grpc",port:50051,targetPort:"grpc"}]}}')"

base_gateway_config=$("${vc[@]}" get configmap real-vllm-agw-generic -o json)
gateway_config=$(jq -c '
  {apiVersion,kind,metadata:{name:"real-vllm-pd-agw"},data:.data}
  | .data["config.yaml"] |= (
      gsub("real-vllm-split:50051";"real-vllm-pd-decode:50051")
      | gsub("graphs/aggregate.yaml";"graphs/disaggregated.yaml"))
' <<<"$base_gateway_config")
apply_json "$gateway_config"
base_gateway=$("${vc[@]}" get deployment real-vllm-agw-generic -o json)
gateway=$(jq -c --arg bundle "$GATEWAY_BUNDLE_PATH" '
  {apiVersion,kind,metadata:{name:"real-vllm-pd-agw"},spec:.spec}
  | .spec.replicas=1
  | .spec.selector.matchLabels.app="real-vllm-pd-agw"
  | .spec.template.metadata.labels.app="real-vllm-pd-agw"
  | .spec.template.spec.containers[0].command[0]=($bundle+"/bin/agentgateway")
  | .spec.template.spec.volumes |= map(
      if .name == "config" then .configMap.name="real-vllm-pd-agw"
      elif .name == "pipeline" then
        .projected.sources |= map(
          if .configMap.name == "component-pipeline" then
            .configMap.name="real-vllm-pd-contracts"
          elif .configMap.name == "real-vllm-graph" then
            .configMap.name="real-vllm-pd-graph"
            | .configMap.items |= map(if .key == "aggregate.yaml" then
                .key="disaggregated.yaml" | .path="graphs/disaggregated.yaml"
              else . end)
          else . end)
      else . end)
' <<<"$base_gateway")
apply_json "$gateway"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"real-vllm-pd-agw"},
  spec:{selector:{app:"real-vllm-pd-agw"},
  ports:[{name:"http",port:8080,targetPort:"http"}]}}')"

if [[ $dry_run == 1 ]]; then
  echo "real vLLM P/D fixture passed server-side dry-run inside $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
  exit 0
fi
for deployment in real-vllm-pd-prefill real-vllm-pd-decode \
  real-vllm-pd-preprocessor real-vllm-pd-selector real-vllm-pd-agw; do
  "${vc[@]}" rollout status "deployment/$deployment" --timeout=900s
done
echo "real vLLM P/D fixture ready inside $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
