#!/usr/bin/env bash
# Run the single-pod Bulwark GMS shadow-failover e2e on Kubernetes, store-native
# (busybox + NFS-mounted Nix store), GPU shared across containers via DRA. No
# operator/Grove needed — failover is local (engines coordinate via the flock +
# the shared GMS KV pool). Kubernetes analogue of test_shadow_failover.py.
#
#   ./run-failover-k8s.sh <engine> [namespace]
#       engine    = vllm | sglang | trtllm      (vllm is validated; see README)
#       namespace = k8s namespace (default $K8S_NS or mkhadkevich-dev)
#
# Prereqs (see README.md): rootfs PVC (Nix store @ subPath=store) NFS-mounted on the
# host at $ROOTFS; shared-model-cache PVC; DRA DeviceClass gpu.nvidia.com;
# runtimeClassName nvidia; built artifacts + /tmp/repro-<engine> out-links.
set -uo pipefail
ENGINE="${1:?usage: run-failover-k8s.sh <vllm|sglang|trtllm> [namespace]}"
NS="${2:-${K8S_NS:-mkhadkevich-dev}}"
ROOTFS="${ROOTFS:-/rootfs}"; MODEL="${FAULT_TOLERANCE_MODEL_NAME:-Qwen/Qwen3-0.6B}"
HERE="$(cd "$(dirname "$0")" && pwd)"; FLAKE="${DYNAMO_FLAKE:-/nix/envs}"; P="bulwark-failover-$ENGINE"

# Resolve store paths (override via env for a different build).
GMS_SP="$(nix path-info "$FLAKE#dynamo-gms" 2>/dev/null)"
RT_SP="$(nix path-info "$FLAKE#dynamo-runtime" 2>/dev/null)"
RING_SP="$(nix path-info "$FLAKE#gms-rust-ring" 2>/dev/null)"
ENGINE_VENV="$(readlink -f "/tmp/repro-$ENGINE")"; SVC="$(readlink -f /tmp/repro-services)"
# Busybox-env toolchain (override via env). These ship in the engine-venv closure.
CCW="${CCW:-$(ls -d /nix/store/*-gcc-wrapper-*/ 2>/dev/null | head -1 | sed 's#/$##')}"
CUDA="${CUDA:-$(ls -d /nix/store/*-cuda-merged-*/ 2>/dev/null | head -1 | sed 's#/$##')}"
LDC="${LDC:-$(ls /nix/store/*-glibc-*-bin/bin/ldconfig 2>/dev/null | head -1)}"
# vllm/GMS coexistence args (per-engine; sglang/trtllm differ — see runtime.py).
ENGINE_ARGS="${ENGINE_ARGS:---enforce-eager --enable-sleep-mode --max-num-seqs 1}"
VLLM_UTIL="${VLLM_UTIL:-0.45}"
: "${GMS_SP:?build .#dynamo-gms first}" "${ENGINE_VENV:?missing /tmp/repro-$ENGINE}" "${CCW:?no gcc-wrapper}" "${CUDA:?no cuda}" "${LDC:?no ldconfig}"
sp(){ echo "$1/lib/python3.12/site-packages"; }

echo "[k8s-fo] engine=$ENGINE ns=$NS model=$MODEL"
# 1) Sync the closure delta into the cluster store (host-side; rootfs PVC is NFS-mounted).
echo "[k8s-fo] syncing store closures -> $ROOTFS/store ..."
nix path-info -r "$GMS_SP" "$RT_SP" "$RING_SP" "$ENGINE_VENV" "$SVC" "$CCW" "$CUDA" "$(dirname "$(dirname "$LDC")")" 2>/dev/null | sort -u | while read -r p; do
  b="$(basename "$p")"; [ -e "$ROOTFS/store/$b" ] && continue
  t="$ROOTFS/store/.tmp.$b"; rm -rf "$t" 2>/dev/null
  cp -a "$p" "$t" 2>/dev/null && mv "$t" "$ROOTFS/store/$b" 2>/dev/null || rm -rf "$t"
done
# Sync the gms_kv_ring source pylib (importable by the GMS vllm integration).
mkdir -p "$ROOTFS/pylib/gms_kv_ring"; cp -a "$FLAKE/../dynamo/lib/gms_kv_ring/." "$ROOTFS/pylib/gms_kv_ring/" 2>/dev/null || true

# 2) Render the manifest.
M="/tmp/$P.yaml"
sed -e "s#__ENGINE__#$ENGINE#g" -e "s#__MODEL__#$MODEL#g" -e "s#__VLLM_UTIL__#$VLLM_UTIL#g" \
    -e "s#__ENGINE_ARGS__#$ENGINE_ARGS#g" -e "s#__ENGINE_VENV__#$ENGINE_VENV#g" \
    -e "s#__RT_SP__#$(sp "$RT_SP")#g" -e "s#__GMS_SP__#$(sp "$GMS_SP")#g" -e "s#__RING_SP__#$(sp "$RING_SP")#g" \
    -e "s#__SRC_SP__#$(sp "$ENGINE_VENV")#g" -e "s#__SERVICES_BIN__#$SVC/bin#g" \
    -e "s#__LDCONFIG__#$LDC#g" -e "s#__CCW__#$CCW#g" -e "s#__CUDA__#$CUDA#g" \
    "$HERE/bulwark-failover-pod.yaml.tmpl" > "$M"

# 3) Apply + wait (log-based) for primary then shadow to register.
kubectl -n "$NS" delete pod "$P" --ignore-not-found --wait=true >/dev/null 2>&1
kubectl -n "$NS" delete resourceclaimtemplate "bulwark-gpu-$ENGINE" --ignore-not-found >/dev/null 2>&1
kubectl -n "$NS" apply -f "$M"
reg(){ kubectl -n "$NS" logs "$P" -c "$1" 2>/dev/null | grep -c "Registered base model"; }
echo "[k8s-fo] waiting for primary (engine-0)..."; for i in $(seq 1 45); do [ "$(reg engine-0)" -ge 1 ] && break; sleep 15; done
echo "[k8s-fo] waiting for shadow (engine-1) to attach + register..."; for i in $(seq 1 40); do [ "$(reg engine-1)" -ge 1 ] && break; sleep 15; done
echo "[k8s-fo] engine-0 reg=$(reg engine-0)  engine-1 reg=$(reg engine-1)"

# 4) Drive failover: serve -> crash the primary's worker -> serve via the shadow.
serve(){ kubectl -n "$NS" exec "$P" -c frontend -- wget -qO- --header='Content-Type: application/json' \
  --post-data="{\"model\":\"$MODEL\",\"messages\":[{\"role\":\"user\",\"content\":\"$1\"}],\"max_tokens\":16}" \
  http://localhost:8080/v1/chat/completions 2>/dev/null; }
echo "[k8s-fo] PRIMARY serve:"; serve "Primary test"; echo
echo "[k8s-fo] crashing primary engine-0 (kill EngineCore)..."
kubectl -n "$NS" exec "$P" -c engine-0 -- sh -c 'for p in $(pgrep -f EngineCore); do kill -9 $p; done' 2>/dev/null || true
sleep 20
echo "[k8s-fo] engine-0 state: $(kubectl -n "$NS" get pod "$P" -o jsonpath='{range .status.containerStatuses[?(@.name=="engine-0")]}{.state}{end}' | head -c 60)"
echo "[k8s-fo] POST-FAILOVER serve (shadow):"; serve "Post failover"; echo
echo "[k8s-fo] done. Logs: kubectl -n $NS logs $P --all-containers | teardown: kubectl -n $NS delete pod $P"
