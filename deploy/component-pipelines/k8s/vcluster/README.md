<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# vCluster validation

These assets keep the prototype deployment and all benchmark load inside a vCluster.
Use the vCluster kubeconfig explicitly for every command; do not apply these resources
to the host-cluster context.

`package.nix` assembles the thin Dynamo facade, patched Agentgateway host, and Envoy
dynamic module into one relocatable Nix-store output. The selector itself uses the
vCluster Kubernetes API to watch `InferencePool` objects and annotated worker Pods.
It does not connect to the Dynamo runtime.

The reproducible zero-delay benchmark is rendered from
`benchmark-pod.yaml.tmpl`. It runs three interleaved trials for all four gateway arms
and the stock Dynamo frontend reference at concurrency 1, 16, and 64. Each cell has
200 warmup requests and 2,000 measured requests.

```bash
export KUBECONFIG=/path/to/vcluster.kubeconfig
export VCLUSTER_NAMESPACE=dynamo-components-v2
export PD_STORE_PATH=/nix/store/...-gateway-pd-mock-0.1.0
export NIX_STORE_NFS_SERVER=192.0.2.1
export NIX_STORE_NFS_PATH=/path/to/shared/nix

envsubst '${VCLUSTER_NAMESPACE} ${PD_STORE_PATH} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
  < deploy/component-pipelines/k8s/vcluster/benchmark-pod.yaml.tmpl \
  | kubectl --kubeconfig "$KUBECONFIG" apply -f -
kubectl --kubeconfig "$KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  logs -f component-benchmark | tee benchmark.jsonl
```

The Envoy-callout arm must have a cluster name for every authority returned by the
selector. For a static benchmark deployment, map the current worker Pod authorities
to `dynamo-worker` in `upstream_clusters`. A production installation should publish
those endpoints through CDS/xDS so Pod churn updates Envoy and selector discovery
atomically; hard-coded Pod IPs are intentionally not checked into this repository.

GPU-backed vLLM or SGLang correctness testing also remains inside the vCluster. If its
virtual nodes do not advertise GPU resources, that test is unavailable there and must
not be silently moved to the host cluster.
`real-workers.yaml` follows the slim-container deployment pattern: BusyBox supplies
only the root filesystem, while immutable Python/engine, CUDA, compiler, and support
closures are mounted from the shared read-only Nix store. Publish the complete closure
to the NFS export's `store/` subdirectory before applying the manifest. The init
container builds only writable `/sbin` and `/cuda` compatibility views; engine code is
never copied into the container image. Nix packages `nvcc` separately from the merged
CUDA toolkit, so `CPATH`, `CPLUS_INCLUDE_PATH`, and `NVCC_PREPEND_FLAGS` explicitly point
at `/cuda/include` for runtime JIT compilation.

The audited AIPerf campaign and real-engine correctness evidence are under
`results/2026-09-22-campaign/`.
