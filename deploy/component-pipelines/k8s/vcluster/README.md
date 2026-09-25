<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# vCluster validation

These assets keep the prototype deployment and all benchmark load inside a vCluster.
Use the vCluster kubeconfig explicitly for every command; do not apply these resources
to the host-cluster context.

The preferred source-native build is `gateway-pipeline#component-pipeline-v2` in the
`dynamo-nix-envs` repository, branch `feat/dynamo-component-pipeline-builds`
(full-bundle build tested at commit `8a612b7`). It assembles the thin Dynamo facade, patched Agentgateway
host, patched Envoy executable, and Envoy dynamic module into one Nix-store output.
`package.nix` is the older prototype packaging reference. The source pins and
build instructions are in the envs flake's `gateway-pipeline/README.md`; the
component code and patch sources are pinned to this Dynamo branch at `98676215fa`.
The standalone Envoy module from the current Nix pin builds and passes a streaming
smoke test. The complete bundle also builds to
`/nix/store/g7achknzv9ibixmfdaxgjy4a3pp33dp5-dynamo-component-pipelines-98676215fa`;
repeat the build after changing any source pin.
The selector itself uses the
vCluster Kubernetes API to watch `InferencePool` objects and annotated worker Pods.
It does not connect to the Dynamo runtime.

Build the bundle from a checkout of that envs branch, then publish only its runtime
closure to the vCluster's existing NFS store export. Use the explicit vCluster
kubeconfig for every Kubernetes write; the temporary stager is itself a vCluster Pod.

```bash
cd /work/envs/gateway-pipeline
nix build .#component-pipeline-v2
COMPONENT_BUNDLE=$(nix path-info .#component-pipeline-v2)
cd /work/dynamo-epp
export KUBECONFIG=/path/to/vcluster.kubeconfig
export VCLUSTER_NAMESPACE=dynamo-components-v2
export NIX_STORE_NFS_SERVER=192.0.2.1
export NIX_STORE_NFS_PATH=/path/to/shared/nix
envsubst '${VCLUSTER_NAMESPACE} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
  < deploy/component-pipelines/k8s/vcluster/store-stager.yaml.tmpl |
  kubectl --kubeconfig "$KUBECONFIG" apply -f -
kubectl --kubeconfig "$KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  wait --for=condition=Ready pod/dynamo-component-store-stager --timeout=120s
nix-store -qR "$COMPONENT_BUNDLE" | sed 's#^/nix/store/##' |
  tar -C /nix/store -cf - -T - |
  kubectl --kubeconfig "$KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
    exec -i dynamo-component-store-stager -- tar -C /shared/nix/store -xf -
```

After replacing the mock-worker Pods, refresh the static Envoy-callout authority map
before restarting that gateway:

```bash
bash deploy/component-pipelines/k8s/vcluster/refresh-envoy-callout-worker-map.sh \
  "$KUBECONFIG" "$VCLUSTER_NAMESPACE"
```

The script refuses to update unless all 16
parity-topology mock workers are Ready. This ConfigMap is benchmark-only; production
worker churn needs CDS/xDS rather than hard-coded Pod IPs.

For short and ISL4000 capacity, render
`frozen-raw-capacity-job.yaml.tmpl` with a unique `RUN_NAME`, an
`ARM_SERVICE` of `agw-static`, `agw-generic`, `envoy-independent`,
`envoy-callouts`, or `dynamo-frontend-reference`, and a `DATASET` of
`short-claude-sonnet-raw.jsonl` or `isl4000-claude-sonnet-raw.jsonl`.
Set `BENCHMARK_START_UNIX` at least 60 seconds ahead and provide
`AIPERF_NODE_A`, `AIPERF_NODE_B`, and
`TOKENIZER_STORE_BASENAME` (the basename, without `/nix/store/`).
Restrict `envsubst` to these render variables and the namespace/NFS variables, so
the Job's runtime `$(JOB_COMPLETION_INDEX)` reference is preserved. Each Job runs
six AIPerf 0.12.0 clients at concurrency 128 for 45 seconds, 3/3 across the two
load-generator nodes. The measured clients replay frozen raw OpenAI bodies and do
not synthesize or tokenize prompts on the hot path.

`run-nix-mocker-trial.sh` renders either frozen-raw or cached-Mooncake template,
waits for all six clients, and copies their JSON/CSV/console summaries into a
local result directory. It refuses to reuse a Job name and requires the selected
gateway to be exactly 1/1 Ready. Scale one gateway arm up at a time; for the
Dynamo reference arm, also scale its four reference-worker Pods to 4/4 Ready.
The script never scales Deployments or chooses a Kubernetes context implicitly:
set `VCLUSTER_KUBECONFIG`, `VCLUSTER_EXPECTED_SERVER` (the API URL from the
vCluster kubeconfig), `VCLUSTER_NAMESPACE`, `AIPERF_NODE_A/B`,
`NIX_STORE_NFS_SERVER/PATH`, `TOKENIZER_STORE_BASENAME`, and `RESULT_DIR`
explicitly. Set `ENVSUBST_BIN` if `envsubst` is not on `PATH`. Then run, for
example:

```bash
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-trial.sh \
  envoy-callouts short r3
```

The runner still requires the vCluster-only `dynamo-component-store-stager` Pod
for evidence collection. A successful Job alone is not accepted as a result:
the script also parses all six exports and reports errors and cancellations.
After one valid first pass per cell, `run-nix-mocker-matrix.sh` runs the two
remaining interleaved passes for all four gateway arms and three workloads,
plus the stock Dynamo reference on short and ISL4000. It verifies
the expected vCluster API URL, refuses to overlap an active `nixv2-*` Job,
scales only the six benchmark gateway/reference Deployments inside that
vCluster, and skips only already-collected, six-client, error-free cells. AGW
static Mooncake receives an extra `r4` because its original `r1` failed before
traffic. Stock Dynamo Mooncake `r2` and `r3` completed with request errors;
their exports are retained and excluded while a separate diagnostic investigates
the intermittent empty-content responses. The script scales the
gateway/reference Deployments back to zero at completion.

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

The Mooncake campaign separates dataset preparation from load generation. Render
`mooncake-prepare-job.yaml.tmpl` once to normalize the 512-token-block trace and
populate AIPerf's content-addressed mmap cache on shared NFS. The preparation job
uses a 100 ms profile only to make AIPerf publish the cache; its preparation probes
is not benchmark data. After it completes, render `mooncake-job.yaml.tmpl` for each
orchestrator arm. Its six clients hard-link the same prepared mmap, so they send
the same OpenAI chat text without rebuilding approximately 700 MiB of prompts per
client. AIPerf still resolves the pinned Qwen tokenizer identity for the cache key,
but prompt synthesis and token-count metrics are absent from the measured client
path (`--use-server-token-count`). Seed 42 makes both the cache key and synthesized
text deterministic. Keep the AIPerf image, tokenizer revision, trace, and prompt
flags identical: changing any of them deliberately produces a cache
miss.

For comparison with the retained Claude prototype, apply the two strategic-merge
patch templates before starting an arm. They reproduce its processor and mock-worker
budgets rather
than merely using the same replica count: four render/tokenize pods, four Tokio
threads and four active operations per pod, two pods on each of two 32-core nodes,
the `fastokens` backend with fallback disabled, a 64 MiB cache per pod, 32 gateway
connections, and a 32-item/200-us cross-request batch. Pinning the backend matters:
Dynamo's default HuggingFace backend is correct but is not performance-comparable to
the reference service's `dynamo_tokenizers::FastTokenizer`. The facade still invokes
Dynamo's canonical `OpenAIPreprocessor`; the old standalone tokenizer implementation
is a benchmark reference, not production code.

```bash
export PROCESSOR_NODE_A=node-a PROCESSOR_NODE_B=node-b
export COMPONENT_BUNDLE=/nix/store/...-dynamo-component-pipelines
export TOKENIZER_PATH=/nix/store/...-qwen-tokenizer

envsubst '${PROCESSOR_NODE_A} ${PROCESSOR_NODE_B} ${COMPONENT_BUNDLE} ${TOKENIZER_PATH}' \
  < deploy/component-pipelines/k8s/vcluster/claude-parity-preprocessor-patch.yaml.tmpl \
  > /tmp/claude-parity-preprocessor-patch.yaml
kubectl --kubeconfig "$KUBECONFIG" -n "$VCLUSTER_NAMESPACE" patch \
  deployment dynamo-preprocessor --type=strategic \
  --patch-file=/tmp/claude-parity-preprocessor-patch.yaml

envsubst '${COMPONENT_BUNDLE}' \
  < deploy/component-pipelines/k8s/vcluster/claude-parity-gateway-patch.yaml.tmpl \
  > /tmp/claude-parity-gateway-patch.yaml
kubectl --kubeconfig "$KUBECONFIG" -n "$VCLUSTER_NAMESPACE" patch \
  deployment agw-static --type=strategic \
  --patch-file=/tmp/claude-parity-gateway-patch.yaml
```

Set `AIPERF_NODE_A` and `AIPERF_NODE_B` when rendering `mooncake-job.yaml.tmpl`.
The six cached-text load generators are spread 3/3 across those dedicated CPU nodes;
without this constraint Kubernetes may place all six on one 32-core node and make the
client the bottleneck. Keep those nodes distinct from the gateway, processor, selector,
and mock-worker nodes. Set `BENCHMARK_START_UNIX` far enough in the future for all six
Pods to become Running. They wait on that shared wall-clock barrier so an image pull or
scheduler delay cannot stagger the trace and silently lower the offered load.
Each indexed client writes its summary and `profile_export.jsonl` under
`/shared/aiperf/results/$JOB_NAME/$JOB_COMPLETION_INDEX`; retain that directory
with the benchmark record so the analysis is audit-grade. Restrict `envsubst` to
the documented render-time variables so it does not consume the runtime
`$JOB_COMPLETION_INDEX` reference.

Render `claude-parity-worker-patch.yaml.tmpl` with `WORKER_NODE_A` through
`WORKER_NODE_D` and apply it as a strategic patch to `dynamo-benchmark-worker`.
This keeps the 16 zero-GPU mock workers evenly spread over the same four-node budget
used by the reference campaign.

The Envoy-callout arm must have a cluster name for every authority returned by the
selector. For a static benchmark deployment, map the current worker Pod authorities
to `dynamo-worker` in `upstream_clusters`. A production installation should publish
those endpoints through CDS/xDS so Pod churn updates Envoy and selector discovery
atomically; hard-coded Pod IPs are intentionally not checked into this repository.

Kubernetes Services load-balance TCP connections, while gRPC multiplexes many streams
over each HTTP/2 connection. Set `DYN_GRPC_CHANNELS_PER_ENDPOINT` on the AGW static
and AGW generic hosts when a facade Service has multiple replicas. For independent
Envoy, set `grpc_conns` in the dynamic-module `filter_config` instead: the host
calls `GrpcTransport::with_connections` and ignores that environment variable.
The channel pool is clamped to 1–256. Record the actual module startup log and
replica counts with each benchmark. This knob is unnecessary for Envoy callouts
because Envoy owns and balances those upstream HTTP/2 connections.

Also record `GENERIC_PIPELINE_THREADS` and Envoy's `--concurrency` separately.
The original Nix-built mocker comparison gave Envoy direct four Tokio threads
and six Envoy workers but Envoy callouts eight of each. Matching the direct
Tokio budget to eight raised its ISL4000 median from 5,820 to 9,681 RPS;
raising its Envoy workers from six to eight did not materially improve a
follow-up diagnostic. The [thread-budget result](results/2026-09-25-envoy-direct-threads/README.md)
includes the frozen short, ISL4000, and Mooncake checks. Do not infer a
transport penalty from a comparison with unequal module runtime threads.

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

The retained evidence is organized as follows:

- `results/2026-09-22-campaign/` contains the first audited short, ISL4000, and
  Mooncake campaign plus real-engine correctness evidence.
- `results/2026-09-23-claude-parity/` records the four-orchestrator Mooncake
  reconstruction against the preserved Claude prototype.
- `results/2026-09-23-callout-parity.md` records the corrected short, ISL4000,
  and Mooncake Envoy-callout parity runs, including frozen-dataset hashes and the
  exact persistent-volume artifact locations.
- `results/2026-09-23-mooncake-ceiling/` contains the initial Mooncake saturation
  analysis.
- `results/2026-09-23-mooncake-single-gateway-ceiling/` contains the plans,
  normalized analyses, and 78 raw AIPerf exports used to isolate the ceiling of
  one Envoy-callout gateway while scaling the other components.
- `results/2026-09-24-nix-build-mocker-parity/` retains the source-pinned
  four-gateway/three-workload matrix and stock-frontend reference.
- `results/2026-09-25-agw-static-packed-ab/` records the packed-token static
  gateway improvement, with no short or Mooncake regression.
- `results/2026-09-25-envoy-direct-threads/` records Envoy-direct parity after
  matching the module runtime thread budget; adjacent profile and channel
  probe directories retain the diagnostic evidence and failed hypotheses.

The repository intentionally retains normalized and raw benchmark evidence, but not
machine-local `result` symlinks or Nix store closures. Rebuild the refactored bundle
with the envs flake above; a `/nix/store/...` symlink from the machine that performed the run is
not portable evidence.
