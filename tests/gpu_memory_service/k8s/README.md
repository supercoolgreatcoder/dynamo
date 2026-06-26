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

**GPU sharing (DRA):** the four GPU containers share ONE physical GPU via a native
DRA `ResourceClaim` (DeviceClass `gpu.nvidia.com`) referenced by each — the same
mechanism Bulwark uses. Requesting `nvidia.com/gpu` per-container would assign
distinct GPUs and defeat GMS memory sharing; DRA exposes the same device to all
referencing containers (as `cuda:0`) and the scheduler places the pod on a free-GPU
node. No `NVIDIA_VISIBLE_DEVICES`/UUID juggling, no operator.

**Shadow attach:** the shadow must *attach* the primary's published KV pool rather
than allocate its own. That path (`use_existing_shared_geometry()`) is enabled by
`GMS_VLLM_SHARED_KV=1`; the shadow then waits for and reattaches the primary's
`kv_pool:v2:*` allocation (`shared=True`). Without it the shadow sizes a fresh KV
tensor and OOMs against the per-process cap even with the GPU mostly free.

**Busybox toolchain:** store-native engines need a few host tools the busybox image
lacks — provided via env in the template: `ldconfig` (symlinked from glibc-bin), a C
compiler + `nvcc -ccbin` (gcc-wrapper + cuda-merged) and `ninja` for triton/flashinfer
JIT, `TRITON_LIBCUDA_PATH`/`LIBRARY_PATH` pointing at where the nvidia runtime injects
`libcuda.so.1` (`/usr/lib/x86_64-linux-gnu`) and the nix CUDA `lib`/`lib/stubs`.

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

## Validation status

**VALIDATED end-to-end in-cluster** (ns `mkhadkevich-dev`, vLLM, Qwen3-0.6B), on the
rebased `dynamo._core` + 16-byte KV lease record:

- 5 containers in one pod, **one GPU shared via DRA** (both GMS daemons + both engines
  on the same `GPU-*`); no operator, no `NVIDIA_VISIBLE_DEVICES`/UUID hacks.
- Primary (engine-0) loaded through the full GMS vLLM integration and **served real
  tokens**; created the GMS persistent KV pool (`kv_pool:v2:*`).
- Shadow (engine-1) **attached the primary's pool** (`Reattached … shared=True`, same
  tag), not a fresh allocation.
- Primary **crashed** (EngineCore killed → engine-0 `terminated`); the daemon **released
  the primary's persistent claims**; the **shadow served the next request** with KV
  preserved — i.e. the same shadow-failover behavior as the single-host e2e.

Notes: engine cold-start from the NFS store + flashinfer JIT is slow (~3–6 min/engine);
the `--max-num-seqs 1` / util `0.45` knobs match the single-host failover config.
sglang/trtllm reuse the same pod template (their per-engine args differ — see
`tests/gpu_memory_service/common/runtime.py`); only vLLM has been run green here.

## Relationship to the operator

For production, the Bulwark operator (`deploy/operator`, `DynamoGraphDeployment` with
failover mode) generates this topology — including the inter-pod variant that shares
GPU memory across pods via DRA ResourceClaims. This harness is the lightweight,
store-native test path for the intra-pod (single-pod) case.
