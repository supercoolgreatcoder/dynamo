#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Stage only closure paths absent from the vCluster store. The host writes to
# its NFS bridge; a vCluster Job copies bridge NFS -> store NFS. Never kubectl cp.
set -euo pipefail

if [[ "$#" != 2 ]] || [[ "$1" != /nix/store/* ]] ||
  [[ "${1#/nix/store/}" == */* ]]; then
  echo "usage: $0 /nix/store/<closure> <unique-stage-id>" >&2
  exit 2
fi
closure=$1
stage_id=$2
[[ "$stage_id" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] &&
  [[ ${#stage_id} -le 40 ]] || {
    echo "stage ID must be a DNS-safe name of at most 40 characters" >&2
    exit 2
  }
test -d "$closure"
: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${NIX_STORE_NFS_SERVER:?set the vCluster Nix store NFS server}"
: "${NIX_STORE_NFS_PATH:?set the vCluster Nix store NFS path}"
: "${NIX_BRIDGE_NFS_SERVER:?set the host-visible bridge NFS server}"
: "${NIX_BRIDGE_NFS_PATH:?set the host-visible bridge NFS path}"
: "${NIX_BRIDGE_HOST_PATH:?set the host-visible bridge mount}"
test -d "$NIX_BRIDGE_HOST_PATH"
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$envsubst_bin" >/dev/null

actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [[ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]]; then
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
fi
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
"${vc[@]}" wait --for=condition=Ready "pod/$stager_pod" --timeout=120s >/dev/null
job_name="nixstage-$stage_id"
if "${vc[@]}" get job "$job_name" >/dev/null 2>&1; then
  echo "refusing to reuse staging Job $job_name" >&2
  exit 2
fi
bridge_stage_id=${SOURCE_STAGE_ID:-$stage_id}
[[ "$bridge_stage_id" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] &&
  [[ ${#bridge_stage_id} -le 40 ]] || {
    echo "SOURCE_STAGE_ID must be a DNS-safe name of at most 40 characters" >&2
    exit 2
  }
stage_dir="$NIX_BRIDGE_HOST_PATH/nixstage-$bridge_stage_id"
if [[ -z "${SOURCE_STAGE_ID:-}" && -e "$stage_dir" ]]; then
  echo "refusing to reuse bridge directory $stage_dir" >&2
  exit 2
fi

mapfile -t missing < <(
  comm -23 \
    <(nix-store -qR "$closure" | sed 's,.*/,,' | sort) \
    <("${vc[@]}" exec "$stager_pod" -- ls /shared/nix/store | sort)
)
if [[ ${#missing[@]} == 0 ]]; then
  echo "closure already staged: $closure"
  exit 0
fi
if [[ -z "${SOURCE_STAGE_ID:-}" ]]; then
  mkdir -p "$stage_dir"
  for basename in "${missing[@]}"; do
    cp -a "/nix/store/$basename" "$stage_dir/"
  done
else
  for basename in "${missing[@]}"; do
    test -e "$stage_dir/$basename" || {
      echo "bridge directory lacks $basename: $stage_dir" >&2
      exit 2
    }
  done
fi

export JOB_NAME="$job_name" BRIDGE_STAGE_ID="$bridge_stage_id" VCLUSTER_NAMESPACE
export NIX_STORE_NFS_SERVER NIX_STORE_NFS_PATH
export NIX_BRIDGE_NFS_SERVER NIX_BRIDGE_NFS_PATH
"$envsubst_bin" '${JOB_NAME} ${BRIDGE_STAGE_ID} ${VCLUSTER_NAMESPACE} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH} ${NIX_BRIDGE_NFS_SERVER} ${NIX_BRIDGE_NFS_PATH}' \
  < "$(dirname "$0")/nix-closure-stage-job.yaml.tmpl" |
  "${vc[@]}" apply -f -
"${vc[@]}" wait --for=condition=complete "job/$job_name" --timeout=900s

mapfile -t remaining < <(
  comm -23 \
    <(nix-store -qR "$closure" | sed 's,.*/,,' | sort) \
    <("${vc[@]}" exec "$stager_pod" -- ls /shared/nix/store | sort)
)
if [[ ${#remaining[@]} != 0 ]]; then
  printf 'Nix paths still missing from vCluster store: %s\n' "${remaining[@]}" >&2
  exit 1
fi
echo "staged $closure with ${#missing[@]} new Nix paths; bridge copy retained at $stage_dir"
