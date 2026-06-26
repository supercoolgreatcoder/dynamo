#!/usr/bin/env bash
# Run the actual pytest shadow-failover e2e (test_shadow_failover.py) on Kubernetes inside
# one store-native busybox container — the k8s analogue of the single-host test. The test
# drives the failover choreography itself, so this exercises the SAME fast pause/resume
# failover path; the only difference from the single-host run is the container boundary.
#
#   ./run-pytest-failover-k8s.sh <engine> [namespace]
#       engine    = sglang | vllm | trtllm   (sglang + vllm validated; recovery ~2-3s)
#       namespace = k8s namespace (default $K8S_NS or mkhadkevich-dev)
#
# Prereqs (see README.md): rootfs PVC (Nix store @ subPath=store; tests synced @ subPath=dsrc;
# gms_kv_ring @ subPath=pylib) NFS-mounted on the host at $ROOTFS; shared-model-cache PVC;
# runtimeClassName nvidia; built artifacts + /tmp/repro-<engine> out-links.
set -uo pipefail
ENGINE="${1:?usage: run-pytest-failover-k8s.sh <sglang|vllm|trtllm> [namespace]}"
NS="${2:-${K8S_NS:-mkhadkevich-dev}}"
ROOTFS="${ROOTFS:-/rootfs}"; MODEL="${FAULT_TOLERANCE_MODEL_NAME:-Qwen/Qwen3-0.6B}"
HERE="$(cd "$(dirname "$0")" && pwd)"; FLAKE="${DYNAMO_FLAKE:-/nix/envs}"
SRCROOT="${SRCROOT:-$FLAKE/../dynamo}"; P="bulwark-pytest-$ENGINE"

# Resolve store paths (override via env for a different build).
GMS_SP="$(nix path-info "$FLAKE#dynamo-gms" 2>/dev/null)"
RT_SP="$(nix path-info "$FLAKE#dynamo-runtime" 2>/dev/null)"
RING_SP="$(nix path-info "$FLAKE#gms-rust-ring" 2>/dev/null)"
ENGINE_VENV="$(readlink -f "/tmp/repro-$ENGINE")"; SVC="$(readlink -f /tmp/repro-services)"
PYT="$(readlink -f /tmp/repro-pyt)"
g(){ ls -d /nix/store/*-"$1"-*/ 2>/dev/null | grep -E "$2" | head -1 | sed 's#/$##'; }
CCW="${CCW:-$(g gcc-wrapper .)}"; CUDA="${CUDA:-$(g cuda-merged .)}"
GNUSED="${GNUSED:-$(g gnused .)}"; COREUTILS="${COREUTILS:-$(g coreutils .)}"
BLAKE3="${BLAKE3:-$(g blake3 'blake3-[0-9]')}"; OPENMPI="${OPENMPI:-$(g openmpi .)}"
LDC="${LDC:-$(ls /nix/store/*-glibc-*-bin/bin/ldconfig 2>/dev/null | head -1)}"
: "${GMS_SP:?build .#dynamo-gms first}" "${ENGINE_VENV:?missing /tmp/repro-$ENGINE}" \
  "${CCW:?no gcc-wrapper}" "${CUDA:?no cuda}" "${LDC:?no ldconfig}" \
  "${GNUSED:?no gnused (needed: busybox sed lacks -u)}" "${COREUTILS:?no coreutils}"
sp(){ echo "$1/lib/python3.12/site-packages"; }

# PATH: GNU sed+coreutils FIRST (fix #1), then wrapper-nvcc dir (fix #2), then the rest.
PATH_V="$GNUSED/bin:$COREUTILS/bin:/tmp/cudawrap/bin:$CCW/bin:$CUDA/bin:$ENGINE_VENV/bin:$SVC/bin:/bin:/sbin:/usr/bin"
# PYTHONPATH: rebased runtime/_core first, then gms, ring, engine venv, base py env, pylib, tests, blake3.
PP="$(sp "$RT_SP"):$(sp "$GMS_SP"):$(sp "$RING_SP"):$(sp "$ENGINE_VENV"):$(sp "$PYT"):/gms-pylib:/dsrc:$(sp "$BLAKE3")"

echo "[k8s-pytest-fo] engine=$ENGINE ns=$NS model=$MODEL"
# 1) Sync the closure delta + the tests/ tree + gms_kv_ring into the cluster store (NFS).
echo "[k8s-pytest-fo] syncing store closures -> $ROOTFS/store ..."
nix path-info -r "$GMS_SP" "$RT_SP" "$RING_SP" "$ENGINE_VENV" "$SVC" "$PYT" "$CCW" "$CUDA" \
  "$GNUSED" "$COREUTILS" "$BLAKE3" "$OPENMPI" "$(dirname "$(dirname "$LDC")")" 2>/dev/null | sort -u | while read -r p; do
  b="$(basename "$p")"; [ -e "$ROOTFS/store/$b" ] && continue
  t="$ROOTFS/store/.tmp.$b"; rm -rf "$t" 2>/dev/null
  cp -a "$p" "$t" 2>/dev/null && mv "$t" "$ROOTFS/store/$b" 2>/dev/null || rm -rf "$t"
done
# The test imports the tests/ tree directly (no symlinks) + the gms_kv_ring pylib.
echo "[k8s-pytest-fo] syncing tests/ + gms_kv_ring ..."
mkdir -p "$ROOTFS/dsrc/tests"; cp -aL "$SRCROOT/tests/." "$ROOTFS/dsrc/tests/" 2>/dev/null || true
mkdir -p "$ROOTFS/pylib/gms_kv_ring"; cp -a "$SRCROOT/lib/gms_kv_ring/." "$ROOTFS/pylib/gms_kv_ring/" 2>/dev/null || true

# 2) Render the manifest.
M="/tmp/$P.yaml"
sed -e "s#__ENGINE__#$ENGINE#g" -e "s#__MODEL__#$MODEL#g" \
    -e "s#__ENGINE_VENV_PYTHON__#$ENGINE_VENV/bin/python#g" \
    -e "s#__PATH__#$PATH_V#g" -e "s#__PYTHONPATH__#$PP#g" \
    -e "s#__OPENMPI_LIB__#$OPENMPI/lib#g" \
    -e "s#__LDCONFIG__#$LDC#g" -e "s#__CCW__#$CCW#g" -e "s#__CUDA__#$CUDA#g" \
    "$HERE/pytest-failover-pod.yaml.tmpl" > "$M"

# 3) Apply + stream until the pytest result line appears.
kubectl -n "$NS" delete pod "$P" --ignore-not-found --wait=true >/dev/null 2>&1
kubectl -n "$NS" apply -f "$M"
echo "[k8s-pytest-fo] applied; waiting for pytest result (model load + JIT ~4-5 min)..."
for i in $(seq 1 40); do
  sleep 20
  phase="$(kubectl -n "$NS" get pod "$P" -o jsonpath='{.status.phase}' 2>/dev/null)"
  res="$(kubectl -n "$NS" logs "$P" 2>/dev/null | grep -aoE '[0-9]+ (passed|failed)' | tail -1)"
  printf '  @%ds phase=%s %s\n' $((i*20)) "$phase" "$res"
  [ -n "$res" ] && break
  [ "$phase" = "Succeeded" ] || [ "$phase" = "Failed" ] && break
done
echo "[k8s-pytest-fo] result: $(kubectl -n "$NS" logs "$P" 2>/dev/null | grep -aE '[0-9]+ (passed|failed)' | tail -1)"
echo "[k8s-pytest-fo] full log: kubectl -n $NS logs $P | teardown: kubectl -n $NS delete pod $P"
