#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Derive an isolated synthetic P/D fixture from the verified aggregate Pods.
# No production engine is replaced: benchmark-only workers exercise the same
# Dynamo-backed facades, gRPC contract, and generic graph as real P/D workers.
set -euo pipefail

for name in VCLUSTER_KUBECONFIG VCLUSTER_EXPECTED_SERVER VCLUSTER_NAMESPACE \
  FACADE_STORE_PATH GATEWAY_BUNDLE_PATH; do
  [[ -n ${!name:-} ]] || { echo "missing $name" >&2; exit 2; }
done
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
[[ $FACADE_STORE_PATH == /nix/store/* ]] || exit 2
[[ $GATEWAY_BUNDLE_PATH == /nix/store/* ]] || exit 2
dry_run=${PD_DRY_RUN:-0}
[[ $dry_run == 0 || $dry_run == 1 ]] || exit 2
prefill_replicas=${PD_PREFILL_REPLICAS:-4}
decode_replicas=${PD_DECODE_REPLICAS:-16}
preprocessor_replicas=${PD_PREPROCESSOR_REPLICAS:-4}
selector_replicas=${PD_SELECTOR_REPLICAS:-4}
for count in "$prefill_replicas" "$decode_replicas" "$preprocessor_replicas" "$selector_replicas"; do
  [[ $count =~ ^[1-9][0-9]*$ ]] || { echo "invalid replica count: $count" >&2; exit 2; }
done

actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || {
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
}
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
for basename in \
  "${FACADE_STORE_PATH#/nix/store/}/bin/dynamo-component-facade" \
  "${GATEWAY_BUNDLE_PATH#/nix/store/}/bin/agentgateway"; do
  if [[ $dry_run == 0 ]]; then
    "${vc[@]}" exec "$stager_pod" -- \
      test -x "/shared/nix/store/$basename"
  fi
done

apply_json() {
  local body=$1
  printf '%s\n' "$body" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
  if [[ $dry_run == 0 ]]; then
    printf '%s\n' "$body" | "${vc[@]}" apply -f -
  fi
}

repo_root=$(git rev-parse --show-toplevel)
spec_dir=$repo_root/deploy/component-pipelines/specs
graph_file=$repo_root/deploy/component-pipelines/graphs/disaggregated.yaml
tmp_dir=$(mktemp -d -t dynamo-mocker-pd.XXXXXXXX)
trap 'rm -r -- "$tmp_dir"' EXIT
"$FACADE_STORE_PATH/bin/dynamo-component-facade" descriptor \
  --output "$tmp_dir/components_descriptor.bin"
graph=$(jq -nr --rawfile graph "$graph_file" '$graph
  | gsub("http://dynamo-preprocessor:50051";"http://dynamo-pd-preprocessor:50051")
  | gsub("http://dynamo-selector:50051";"http://dynamo-pd-selector:50051")
  | gsub("http://dynamo-prefill:50051";"http://dynamo-pd-prefill:50051")')
contracts=$("${vc[@]}" create configmap dynamo-pd-contracts \
  --from-file="components_descriptor.bin=$tmp_dir/components_descriptor.bin" \
  --from-file="openai-chat.yaml=$spec_dir/openai-chat.yaml" \
  --from-file="preprocessor.yaml=$spec_dir/preprocessor.yaml" \
  --from-file="selector.yaml=$spec_dir/selector.yaml" \
  --from-file="chat-worker.yaml=$spec_dir/chat-worker.yaml" \
  --dry-run=client -o json)
contracts=$(jq -c --arg graph "$graph" '.data["disaggregated.yaml"]=$graph' \
  <<<"$contracts")
apply_json "$contracts"

base_worker=$("${vc[@]}" get deployment dynamo-benchmark-worker -o json)
for role in prefill decode; do
  name=dynamo-pd-$role
  if [[ $role == prefill ]]; then replicas=$prefill_replicas; else replicas=$decode_replicas; fi
  worker=$(jq -c --arg name "$name" --arg role "$role" \
    --arg facade "$FACADE_STORE_PATH" --argjson replicas "$replicas" '
    {apiVersion,kind,metadata:{name:$name},spec:.spec}
    | .spec.replicas=$replicas
    | .spec.selector.matchLabels.app=$name
    | .spec.template.metadata.labels.app=$name
    | .spec.template.metadata.annotations["dynamo.nvidia.com/worker-metadata"] |=
      (fromjson | .router_hint_worker_type=$role | tojson)
    | .spec.template.spec.containers[0].command[0]=($facade+"/bin/dynamo-component-facade")
    | .spec.template.spec.containers[0].args += ["--benchmark-mode",$role]
  ' <<<"$base_worker")
  apply_json "$worker"
  service=$(jq -nc --arg name "$name" '{apiVersion:"v1",kind:"Service",
    metadata:{name:$name},spec:{selector:{app:$name},
    ports:[{name:"grpc",port:50051,targetPort:50051}]}}')
  apply_json "$service"
done

base_pool=$("${vc[@]}" get inferencepool benchmark-workers -o json)
pool=$(jq -c '{apiVersion,kind,metadata:{name:"dynamo-pd-decode"},spec:.spec}
  | .spec.selector.matchLabels.app="dynamo-pd-decode"
  | .spec.endpointPickerRef.name="dynamo-pd-selector"' <<<"$base_pool")
apply_json "$pool"
selector_config=$(jq -nc --arg ns "$VCLUSTER_NAMESPACE" '
  {apiVersion:"v1",kind:"ConfigMap",metadata:{name:"dynamo-pd-selector-config"},
   data:{"discovery.json":({pools:[{namespace:$ns,
     inference_pool_name:"dynamo-pd-decode",model_name:"Qwen/Qwen2.5-0.5B-Instruct",
     block_size:512,total_kv_blocks:32768,max_num_batched_tokens:8192}]}|tojson)}}')
apply_json "$selector_config"
base_selector=$("${vc[@]}" get deployment dynamo-selector -o json)
selector=$(jq -c --arg facade "$FACADE_STORE_PATH" \
  --argjson replicas "$selector_replicas" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-selector"},spec:.spec}
  | .spec.replicas=$replicas
  | .spec.selector.matchLabels.app="dynamo-pd-selector"
  | .spec.template.metadata.labels.app="dynamo-pd-selector"
  | .spec.template.spec.nodeSelector={}
  | .spec.template.spec.containers[0].command[0]=($facade+"/bin/dynamo-component-facade")
  | .spec.template.spec.volumes |= map(if .name == "config" then
      .configMap.name="dynamo-pd-selector-config" else . end)
' <<<"$base_selector")
apply_json "$selector"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"dynamo-pd-selector"},spec:{selector:{app:"dynamo-pd-selector"},
  ports:[{name:"grpc",port:50051,targetPort:50051}]}}')"

base_preprocessor=$("${vc[@]}" get deployment dynamo-preprocessor -o json)
preprocessor=$(jq -c --arg facade "$FACADE_STORE_PATH" \
  --argjson replicas "$preprocessor_replicas" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-preprocessor"},spec:.spec}
  | .spec.replicas=$replicas
  | .spec.selector.matchLabels.app="dynamo-pd-preprocessor"
  | .spec.template.metadata.labels.app="dynamo-pd-preprocessor"
  | .spec.template.spec.containers[0].command[0]=($facade+"/bin/dynamo-component-facade")
' <<<"$base_preprocessor")
apply_json "$preprocessor"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"dynamo-pd-preprocessor"},spec:{selector:{app:"dynamo-pd-preprocessor"},
  ports:[{name:"grpc",port:50051,targetPort:50051}]}}')"

base_gateway_config=$("${vc[@]}" get configmap agw-generic -o json)
gateway_config=$(jq -c '
  {apiVersion,kind,metadata:{name:"dynamo-pd-agw-generic"},data:.data}
  | .data["config.yaml"] |=
    (gsub("dynamo-worker:50051";"dynamo-pd-decode:50051")
     | gsub("graphs/aggregate.yaml";"graphs/disaggregated.yaml"))
' <<<"$base_gateway_config")
apply_json "$gateway_config"
base_gateway=$("${vc[@]}" get deployment agw-generic -o json)
gateway=$(jq -c --arg bundle "$GATEWAY_BUNDLE_PATH" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-agw-generic"},spec:.spec}
  | .spec.replicas=1
  | .spec.selector.matchLabels.app="dynamo-pd-agw-generic"
  | .spec.template.metadata.labels.app="dynamo-pd-agw-generic"
  | .spec.template.spec.containers[0].command[0]=($bundle+"/bin/agentgateway")
  | .spec.template.spec.volumes |= map(
      if .name == "config" then .configMap.name="dynamo-pd-agw-generic"
      elif .name == "pipeline" then
        .configMap.name="dynamo-pd-contracts"
        | .configMap.items |= map(if .key == "aggregate.yaml" then
            .key="disaggregated.yaml" | .path="graphs/disaggregated.yaml"
          else . end)
      else . end)
' <<<"$base_gateway")
apply_json "$gateway"
apply_json "$(jq -nc '{apiVersion:"v1",kind:"Service",
  metadata:{name:"dynamo-pd-agw-generic"},spec:{selector:{app:"dynamo-pd-agw-generic"},
  ports:[{name:"http",port:8080,targetPort:8080}]}}')"

if [[ $dry_run == 1 ]]; then
  echo "mock P/D fixture passed server dry-run inside $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
  exit 0
fi
# A ConfigMap refresh does not change the Deployment template. Restart AGW so
# its startup validator sees this exact graph/spec/descriptor snapshot.
"${vc[@]}" rollout restart deployment/dynamo-pd-agw-generic
for deployment in dynamo-pd-prefill dynamo-pd-decode dynamo-pd-preprocessor \
  dynamo-pd-selector dynamo-pd-agw-generic; do
  "${vc[@]}" rollout status "deployment/$deployment" --timeout=900s
done
echo "mock P/D fixture ready inside $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
