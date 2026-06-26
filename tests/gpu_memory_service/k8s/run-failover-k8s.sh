#!/usr/bin/env bash
# Run the single-pod Bulwark GMS shadow-failover e2e on Kubernetes, store-native
# (busybox + NFS-mounted Nix store). Kubernetes analogue of
# tests/gpu_memory_service/test_shadow_failover.py. See README.md.
#
#   ./run-failover-k8s.sh <engine> [namespace]
#       engine    = vllm | sglang | trtllm
#       namespace = k8s namespace (default: $K8S_NS or mkhadkevich-dev)
#
# Prereqs (see README.md):
#   - kubectl context with a `rootfs` PVC (Nix store at subPath=store) and
#     `shared-model-cache` PVC, GPU nodes, runtimeClassName `nvidia`.
#   - The host has the rootfs PVC mounted at $ROOTFS (default /rootfs) so the
#     store closures can be synced. Set ROOTFS= if mounted elsewhere.
set -uo pipefail
ENGINE="${1:?usage: run-failover-k8s.sh <vllm|sglang|trtllm> [namespace]}"
NS="${2:-${K8S_NS:-mkhadkevich-dev}}"
ROOTFS="${ROOTFS:-/rootfs}"
MODEL="${FAULT_TOLERANCE_MODEL_NAME:-Qwen/Qwen3-0.6B}"
GPU_INDEX="${GPU_INDEX:-0}"
HERE="$(cd "$(dirname "$0")" && pwd)"
FLAKE="${DYNAMO_FLAKE:-/nix/envs}"

# Resolve the store paths to sync + reference (override via env for a different build).
GMS_SP="$(nix path-info "$FLAKE#dynamo-gms" 2>/dev/null)"
RT_SP="$(nix path-info "$FLAKE#dynamo-runtime" 2>/dev/null)"
RING_SP="$(nix path-info "$FLAKE#gms-rust-ring" 2>/dev/null)"
ENGINE_VENV="$(readlink -f "/tmp/repro-$ENGINE" 2>/dev/null)"
SERVICES="$(readlink -f /tmp/repro-services 2>/dev/null)"
: "${GMS_SP:?build .#dynamo-gms first}" "${ENGINE_VENV:?missing /tmp/repro-$ENGINE out-link}"

echo "[k8s-fo] engine=$ENGINE ns=$NS model=$MODEL gpu=$GPU_INDEX"

# 1) Sync the closure delta into the cluster store (host-side; rootfs PVC is NFS-mounted).
echo "[k8s-fo] syncing store closures -> $ROOTFS/store ..."
nix path-info -r "$GMS_SP" "$RT_SP" "$RING_SP" "$ENGINE_VENV" "$SERVICES" 2>/dev/null | sort -u | while read -r p; do
  b="$(basename "$p")"; [ -e "$ROOTFS/store/$b" ] && continue
  t="$ROOTFS/store/.tmp.$b"; rm -rf "$t" 2>/dev/null
  cp -a "$p" "$t" 2>/dev/null && mv "$t" "$ROOTFS/store/$b" 2>/dev/null || rm -rf "$t"
done
echo "[k8s-fo] store synced."

# 2) Render the pod manifest.
sp() { echo "$1/lib/python3.12/site-packages"; }
MANIFEST="/tmp/bulwark-failover-$ENGINE.yaml"
sed -e "s#__ENGINE__#$ENGINE#g" \
    -e "s#__MODEL__#$MODEL#g" \
    -e "s#__GPU_INDEX__#$GPU_INDEX#g" \
    -e "s#__ENGINE_VENV__#$ENGINE_VENV#g" \
    -e "s#__RT_SP__#$(sp "$RT_SP")#g" \
    -e "s#__GMS_SP__#$(sp "$GMS_SP")#g" \
    -e "s#__RING_SP__#$(sp "$RING_SP")#g" \
    -e "s#__SRC_SP__#${DYNAMO_SRC_SP:-$ENGINE_VENV/lib/python3.12/site-packages}#g" \
    -e "s#__SERVICES_BIN__#$SERVICES/bin#g" \
    "$HERE/bulwark-failover-pod.yaml.tmpl" > "$MANIFEST"
echo "[k8s-fo] manifest -> $MANIFEST"

# 3) Apply + wait for the engines to register with the frontend.
kubectl -n "$NS" delete pod "bulwark-failover-$ENGINE" --ignore-not-found --wait=true >/dev/null 2>&1
kubectl -n "$NS" apply -f "$MANIFEST"
echo "[k8s-fo] waiting for pod Ready + model registration (up to 10m)..."
kubectl -n "$NS" wait --for=condition=Ready "pod/bulwark-failover-$ENGINE" --timeout=600s || {
  echo "[k8s-fo] pod not Ready; recent logs:"; kubectl -n "$NS" logs "bulwark-failover-$ENGINE" --all-containers --tail=40; exit 1; }

PF_PID=""; cleanup() { [ -n "$PF_PID" ] && kill "$PF_PID" 2>/dev/null; }; trap cleanup EXIT
kubectl -n "$NS" port-forward "pod/bulwark-failover-$ENGINE" 8080:8080 >/dev/null 2>&1 & PF_PID=$!
sleep 5

# 4) Drive the failover: serve, kill primary (engine-0), serve again via the shadow.
curlc() { curl -fsS -m 30 "http://localhost:8080/v1/chat/completions" -H 'content-type: application/json' \
  -d "{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"$1\"}],\"max_tokens\":16}"; }
echo "[k8s-fo] PRIMARY serve:"; curlc "Primary test" | head -c 300; echo
echo "[k8s-fo] killing primary engine-0..."
kubectl -n "$NS" exec "bulwark-failover-$ENGINE" -c engine-0 -- sh -c 'kill -9 -1' 2>/dev/null || true
sleep 8
echo "[k8s-fo] POST-FAILOVER serve (shadow):"; curlc "Post failover" | head -c 300; echo

echo "[k8s-fo] container states:"; kubectl -n "$NS" get pod "bulwark-failover-$ENGINE" -o jsonpath='{range .status.containerStatuses[*]}{.name}={.state}{"\n"}{end}'
echo "[k8s-fo] done. Inspect: kubectl -n $NS logs bulwark-failover-$ENGINE --all-containers"
echo "[k8s-fo] teardown: kubectl -n $NS delete pod bulwark-failover-$ENGINE"
