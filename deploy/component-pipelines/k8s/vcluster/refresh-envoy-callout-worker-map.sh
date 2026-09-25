#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Refresh the static benchmark-only authority map after mock-worker Pod churn.
# The production path should use CDS/xDS instead of a ConfigMap of Pod IPs.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 VCLUSTER_KUBECONFIG NAMESPACE" >&2
  exit 2
fi

vcluster_kubeconfig=$1
namespace=$2
test -s "$vcluster_kubeconfig"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
actual_server=$(kubectl --kubeconfig "$vcluster_kubeconfig" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
fi
expected_workers=${EXPECTED_WORKERS:-16}
[[ "$expected_workers" =~ ^[1-9][0-9]*$ ]] || {
  echo "EXPECTED_WORKERS must be a positive integer" >&2
  exit 2
}

pods=$(kubectl --kubeconfig "$vcluster_kubeconfig" -n "$namespace" \
  get pods -l app=dynamo-benchmark-worker -o json)
ready_count=$(jq '[.items[] | select(.metadata.deletionTimestamp == null) |
  select(.status.phase == "Running") |
  select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length' <<<"$pods")
if [ "$ready_count" -ne "$expected_workers" ]; then
  echo "expected $expected_workers ready mock workers, found $ready_count; refusing to update callout map" >&2
  exit 1
fi

worker_map=$(jq -c '[.items[] | select(.metadata.deletionTimestamp == null) |
  select(.status.phase == "Running") |
  select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
  {key: (.status.podIP + ":50051"), value: "dynamo-worker"}] | from_entries' <<<"$pods")

kubectl --kubeconfig "$vcluster_kubeconfig" -n "$namespace" \
  get configmap envoy-callouts -o json |
  jq -e --arg worker_map "$worker_map" '
    if (.data["envoy.yaml"] | test("\"upstream_clusters\":\\{[^}]*\\}")) then
      .data["envoy.yaml"] |= sub("\"upstream_clusters\":\\{[^}]*\\}";
        "\"upstream_clusters\":" + $worker_map)
    else
      error("Envoy callout config has no upstream_clusters map")
    end
    | del(.metadata.managedFields, .status)
  ' |
  kubectl --kubeconfig "$vcluster_kubeconfig" -n "$namespace" replace -f -
