# GMS shadow-failover on Kubernetes (store-native / busybox)

Kubernetes analogue of the single-host shadow-failover e2e
(`tests/gpu_memory_service/test_shadow_failover.py`). It reproduces the **single-pod
Bulwark** topology on a real cluster GPU using the **store-native busybox** strategy:
a `busybox` image with the Nix store mounted from an NFS PVC, running the
store-native CPython/engine binaries directly (no container image build).

## Why this exists

The single-host e2e proves the failover *mechanism* (KV stays RW-owned across a
primary crash; the shadow serves; single-writer-per-segment holds). This harness
confirms the **same mechanism in Kubernetes**, in the same multi-container shape the
production Bulwark operator deploys.

## Topology — one pod, five containers, one shared GPU

| Container | Role | Command / key env |
|---|---|---|
| `gms-weights` | GMS weights daemon | `gpu_memory_service.cli.server`, `GMS_SERVER_TAGS=weights` |
| `gms-kv-cache` | GMS kv_cache daemon | `gpu_memory_service.cli.server`, `GMS_SERVER_TAGS=kv_cache` |
| `frontend` | OpenAI HTTP ingress | nats + etcd + `dynamo.frontend` |
| `engine-0` | primary | `ENGINE_ID=0`, `*_GMS_LOCK_BEFORE_INIT=1` |
| `engine-1` | shadow | `ENGINE_ID=1`, `DYN_GMS_FAILOVER_SHADOW_MODE=true` |

Shared in-pod: `gms-shared` (`/run/gms/shared` — UDS sockets + `failover.lock`),
`/dev/shm` (KV-lease table), the Nix store (`/nix/store`), the model cache. The
container/volume/env layout mirrors the tested operator source
[`deploy/operator/internal/dynamo/failover.go`](../../../deploy/operator/internal/dynamo/failover.go)
(intra-pod failover mode). The engines auto-coordinate through the flock — no external
orchestrator — so failover is triggered simply by **killing `engine-0`** and confirmed
by the frontend continuing to serve via `engine-1`.

**GPU sharing:** all GPU containers share one physical GPU via the nvidia runtime
(`NVIDIA_VISIBLE_DEVICES=all` + `CUDA_VISIBLE_DEVICES` pinning), *not* per-container
`nvidia.com/gpu` requests (which would assign distinct GPUs and defeat GMS sharing).
Pin to a node with a free GPU index (`GPU_INDEX`, `nodeName`/`nodeSelector`).

## Prerequisites

- A kubectl context with: a `rootfs` PVC (Nix store under `subPath: store`), a
  `shared-model-cache` PVC, GPU nodes, and `runtimeClassName: nvidia`.
- The `rootfs` PVC mounted on the build host (default `/rootfs`) so closures can be
  synced host-side. Set `ROOTFS=` otherwise.
- The GMS + engine artifacts built (`nix build .#dynamo-gms .#dynamo-runtime
  .#gms-rust-ring` and the `/tmp/repro-<engine>` engine-venv out-links).

## Run

```bash
# foundation check (store-native CPython + _core + torch.cuda + 16-byte lease record)
kubectl -n <ns> apply -f foundation-probe.yaml   # edit store-path placeholders first
kubectl -n <ns> logs gms-foundation-probe

# full single-pod failover for one engine
./run-failover-k8s.sh vllm   <ns>
./run-failover-k8s.sh sglang <ns>
./run-failover-k8s.sh trtllm <ns>   # weights-only GMS (no kv_cache daemon); V2 KV only
```

The runner syncs the store delta, renders `bulwark-failover-pod.yaml.tmpl`, applies it,
waits for model registration, serves a request, kills `engine-0`, and serves again —
the shadow should answer with KV preserved.

## Validation status (be honest about what's proven)

- **Foundation — VALIDATED in-cluster** (ns `mkhadkevich-dev`): the busybox+store pod
  runs store-native CPython on a cluster GPU; `torch` sees CUDA; the rebuilt
  `dynamo._core` loads; `gpu_memory_service` imports; `LEASE_RECORD_SIZE == 16` (the
  repacked record). See `foundation-probe.yaml`.
- **Store sync — VALIDATED**: host-side closure sync into the `rootfs` PVC store
  (287-path delta) used by `run-failover-k8s.sh`.
- **Full 5-container failover green-run — PENDING**: the manifest + driver are derived
  from the tested operator failover source and the proven single-host e2e, but the
  end-to-end green run on a shareable-GPU node has not yet been completed here (engine
  cold-start from NFS is slow; GPU-sharing across containers needs a node with a free
  index). Run via `run-failover-k8s.sh` and iterate on your cluster.

## Relationship to the operator

For production, the Bulwark operator (`deploy/operator`, `DynamoGraphDeployment` with
failover mode) generates this topology — including the inter-pod variant that shares
GPU memory across pods via DRA ResourceClaims. This harness is the lightweight,
store-native test path for the intra-pod (single-pod) case.
