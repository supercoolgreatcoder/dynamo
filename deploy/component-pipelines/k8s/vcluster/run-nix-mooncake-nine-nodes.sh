#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Select the frozen nine-node plan and delegate to the guarded benchmark runner.
# Only the explicit vCluster kubeconfig supplied by the caller may be used.
set -euo pipefail

if [ "$#" -ne 3 ] || ! [[ "$1" =~ ^(c512|c512-grace)$ ]] || ! [[ "$2" =~ ^(12|14|15|16|24)$ ]] || ! [[ "$3" =~ ^r[1-9][0-9]*$ ]] || { [ "$1" = c512-grace ] && [ "$2" = 24 ]; } || { [ "$1" = c512 ] && [ "$2" = 15 ]; }; then
  echo "usage: $0 c512 {12|14|16|24} rN | c512-grace {12|14|15|16} rN" >&2
  exit 2
fi

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${NIX_STORE_NFS_SERVER:?set the vCluster NFS server}"
: "${NIX_STORE_NFS_PATH:?set the vCluster NFS export}"
: "${ENVSUBST_BIN:?set the Nix gettext envsubst executable}"

case "$2" in
  24) series=2026-09-26-accf6af-mooncake-client-nine-nodes-c512 ;;
  *) series="2026-09-26-accf6af-mooncake-client-nine-nodes-c512-$2" ;;
esac
runner_mode=fixed9c512
if [ "$1" = c512-grace ]; then
  series="${series}-grace"
  runner_mode=fixed9c512grace
fi
script_dir=$(cd "$(dirname "$0")" && pwd)
export RESULT_DIR="$script_dir/results/$series"
plan="$RESULT_DIR/benchmark_plan.json"
test -s "$plan"
mapfile -t nodes < <(jq -r '.placement.nodes[]' "$plan")
test "${#nodes[@]}" -eq 9
suffixes=(A B C D E F G H I)
for index in "${!suffixes[@]}"; do
  variable="AIPERF_NODE_${suffixes[$index]}"
  printf -v "$variable" '%s' "${nodes[$index]}"
  export "$variable"
done

exec bash "$script_dir/run-nix-mooncake-ceiling.sh" "$runner_mode" "$2" "$3"
