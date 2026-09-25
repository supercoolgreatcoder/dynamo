#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Scale AIPerf clients without changing the frozen short request bodies or the
# single Envoy-direct gateway. Keep this series distinct from the six-client
# parity matrix: offered load intentionally changes.
set -euo pipefail
shopt -s nullglob

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ] || ! [[ "$1" =~ ^[0-9]+$ ]] ||
  ! [[ "$2" =~ ^r[1-9][0-9]*$ ]] || ! [[ "${3:-base}" =~ ^(base|selector4|preprocessor8|gateway12|isolated|envoy12)$ ]]; then
  echo "usage: $0 CLIENTS rN [base|selector4|preprocessor8|gateway12|isolated|envoy12]" >&2
  exit 2
fi
clients=$1
trial=$2
variant=${3:-base}
selector_replicas=1
preprocessor_replicas=4
gateway_threads=8
envoy_workers=6
if [ "$variant" = selector4 ]; then
  selector_replicas=4
elif [ "$variant" = preprocessor8 ]; then
  preprocessor_replicas=8
elif [ "$variant" = gateway12 ]; then
  gateway_threads=12
elif [ "$variant" = envoy12 ]; then
  envoy_workers=12
fi
if [ "$clients" -lt 6 ] || [ "$clients" -gt 24 ] || [ $((clients % 3)) -ne 0 ]; then
  echo "CLIENTS must be a multiple of 3 between 6 and 24" >&2
  exit 2
fi

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig path}"
: "${VCLUSTER_EXPECTED_SERVER:?set the expected vCluster API server URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${AIPERF_NODE_A:?set dedicated load-generator node A}"
: "${AIPERF_NODE_B:?set dedicated load-generator node B}"
: "${AIPERF_NODE_C:?set dedicated load-generator node C}"
: "${NIX_STORE_NFS_SERVER:?set the existing vCluster NFS server}"
: "${NIX_STORE_NFS_PATH:?set the existing vCluster NFS export}"
: "${TOKENIZER_STORE_BASENAME:?set the staged tokenizer basename}"
: "${RESULT_DIR:?set the local result directory}"
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$envsubst_bin" >/dev/null
test -f "$VCLUSTER_KUBECONFIG"
test -d "$RESULT_DIR"
test "$AIPERF_NODE_A" != "$AIPERF_NODE_B"
test "$AIPERF_NODE_A" != "$AIPERF_NODE_C"
test "$AIPERF_NODE_B" != "$AIPERF_NODE_C"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "kubeconfig server $actual_server does not match expected vCluster server" >&2
  exit 2
fi
kubectl_vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
for node in "$AIPERF_NODE_A" "$AIPERF_NODE_B" "$AIPERF_NODE_C"; do
  kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" get node "$node" -o json |
    jq -e '
      (.metadata.labels["topology.unikorn-cloud.org/node-pool"] == "cpu-pool") and
      ((.spec.unschedulable // false) == false) and
      ((.spec.taints // []) | all(.[]; .effect != "NoSchedule")) and
      any(.status.conditions[]; .type == "Ready" and .status == "True")
    ' >/dev/null || {
      echo "load-generator node $node is not schedulable and Ready" >&2
      exit 2
    }
done

job="ceilv1-short-envoy-direct-c${clients}-${trial}"
if [ "$variant" != base ]; then
  job="${job}-${variant}"
fi
if "${kubectl_vc[@]}" get job "$job" >/dev/null 2>&1; then
  echo "refusing to reuse Job $job" >&2
  exit 2
fi
"${kubectl_vc[@]}" get jobs -o json |
  jq -e '[.items[] | select(.metadata.name | startswith("ceilv1-")) | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "another ceiling Job is active" >&2
    exit 2
  }
"${kubectl_vc[@]}" get deployment envoy-independent -o json |
  jq -e --arg threads "$gateway_threads" --arg workers "$envoy_workers" '
    (.spec.replicas == 1) and (.status.readyReplicas == 1) and
    (.spec.template.spec.containers[0].args[3] == $workers) and
    any(.spec.template.spec.containers[].env[]?;
      .name == "GENERIC_PIPELINE_THREADS" and .value == $threads)
  ' >/dev/null || {
    echo "envoy-independent must be 1/1 Ready with $gateway_threads Tokio threads and $envoy_workers Envoy workers" >&2
    exit 2
  }
for component in "dynamo-preprocessor:${preprocessor_replicas}" "dynamo-selector:${selector_replicas}" dynamo-benchmark-worker:16; do
  name=${component%:*}
  expected=${component#*:}
  "${kubectl_vc[@]}" get deployment "$name" -o json |
    jq -e --argjson expected "$expected" '(.spec.replicas == $expected) and (.status.readyReplicas == $expected)' >/dev/null || {
      echo "deployment $name must be $expected/$expected Ready" >&2
      exit 2
    }
done
if [ "$variant" = isolated ] || [ "$variant" = envoy12 ]; then
  "${kubectl_vc[@]}" get pods -l app=dynamo-benchmark-worker -o json |
    jq -e --arg node "$AIPERF_NODE_C" '
      (.items | length == 16) and
      all(.items[]; .metadata.deletionTimestamp == null and
          .spec.nodeName != $node and .status.containerStatuses[0].ready == true)
    ' >/dev/null || {
      echo "all 16 Ready synthetic workers must be off client node C" >&2
      exit 2
    }
fi
"${kubectl_vc[@]}" get deployment agw-static agw-generic envoy-callouts dynamo-frontend-reference -o json |
  jq -e 'all(.items[]; .spec.replicas == 0)' >/dev/null || {
    echo "another benchmark gateway has replicas" >&2
    exit 2
  }

export RUN_NAME=$job ARM_SERVICE=envoy-independent DATASET=short-claude-sonnet-raw.jsonl
export VCLUSTER_NAMESPACE AIPERF_NODE_A AIPERF_NODE_B TOKENIZER_STORE_BASENAME
export NIX_STORE_NFS_SERVER NIX_STORE_NFS_PATH
export BENCHMARK_START_UNIX=$(( $(date -u +%s) + 120 ))
"$envsubst_bin" '${RUN_NAME} ${VCLUSTER_NAMESPACE} ${ARM_SERVICE} ${DATASET} ${AIPERF_NODE_A} ${AIPERF_NODE_B} ${BENCHMARK_START_UNIX} ${TOKENIZER_STORE_BASENAME} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
  < "$(dirname "$0")/frozen-raw-capacity-job.yaml.tmpl" |
  "${kubectl_vc[@]}" create --dry-run=client -f - -o json |
  jq --argjson clients "$clients" --arg node_c "$AIPERF_NODE_C" '
    .spec.completions = $clients |
    .spec.parallelism = $clients |
    .spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values += [$node_c]
  ' |
  "${kubectl_vc[@]}" apply -f -

echo "waiting for $job ($clients clients, barrier $BENCHMARK_START_UNIX)" >&2
"${kubectl_vc[@]}" wait --for=condition=complete "job/$job" --timeout=900s
bash "$(dirname "$0")/collect-nix-mocker-ceiling.sh" "$job" "$clients"
