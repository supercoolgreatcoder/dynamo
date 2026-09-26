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
(current validated package commit `7024cac`). It assembles the thin Dynamo facade, patched Agentgateway
host, patched Envoy executable, and Envoy dynamic module into one Nix-store output.
`package.nix` is the older prototype packaging reference. The source pins and
build instructions are in the envs flake's `gateway-pipeline/README.md`; the
gateway graph and facade sources are pinned to Dynamo `accf6af699`, while the
unchanged Envoy ABI patch is independently pinned to `98676215fa`.
The standalone Envoy module from the current Nix pin builds and passes a streaming
smoke test. The current complete bundle builds to
`/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699`;
repeat the build after changing any source pin.
The selector itself uses the
vCluster Kubernetes API to watch `InferencePool` objects and annotated worker Pods.
It does not connect to the Dynamo runtime.

The real SGLang split-worker fixture uses a newer facade-only Nix pin:
`8468212130` in the same envs branch (envs commit `dd53aa3`). The gateway
source pin is deliberately unchanged, so upstream facade updates do not
rebuild the patched gateway unnecessarily. The Nix-built facade is
`/nix/store/gig2imm94kjrv2jbdqkyysfpgqnc1bmv-dynamo-component-facade-1.6.0-8468212130`;
the complete bundle also builds as
`/nix/store/j7n9hn14s34c2595q1nr4h9w0055y24x-dynamo-component-pipelines-8468212130`.
That older bundle produced the retained three-run aggregate matrix; it is not
the `accf6af699` bundle used by the subsequent real-vLLM P/D smoke and
fresh benchmark recheck.

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

For the existing mocker-parity topology, roll the staged bundle through all
split-pipeline Deployments with the guarded vCluster-only helper:

```bash
export VCLUSTER_KUBECONFIG="$KUBECONFIG"
export VCLUSTER_EXPECTED_SERVER=https://your-vcluster-api.example:443
bash deploy/component-pipelines/k8s/vcluster/rollout-nix-bundle.sh \
  "$COMPONENT_BUNDLE"
```

The helper requires an exact vCluster API-server match, no active benchmark
Jobs, all staged artifacts, and the expected gateway replica topology. It
rolls worker, preprocessor, selector, and Envoy direct in order, updates the
inactive AGW/callout arms, and refreshes the benchmark-only Envoy-callout
worker map after worker Pod churn. The full-bundle smoke and frozen-workload
evidence is under
[`results/2026-09-25-full-bundle-parity/`](results/2026-09-25-full-bundle-parity/).

If replacing mock-worker Pods without the rollout helper, refresh the static
Envoy-callout authority map before restarting that gateway:

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

The runner still requires a vCluster-only Nix-store helper Pod for evidence
collection. It defaults to `dynamo-component-store-stager`; set
`NIX_STAGER_POD` to a new Ready Pod with the same NFS mount if the original
24-hour helper has completed. A successful Job alone is not accepted as a result:
the script also parses all six exports and reports errors and cancellations.
For a newly pinned bundle, use `run-nix-mocker-recheck.sh` with the same
environment and a fresh `RESULT_DIR`, for example `TRIAL=r21 WORKLOAD=short`.
It runs four gateway arms one at a time with a rotating order for `r21`–`r23`,
refuses active Jobs or an active real-model/stock-reference fixture, and skips
only complete six-client error-free local exports. Job names must be unused;
it never overwrites a prior Kubernetes Job. The
[facade 846821 recheck](results/2026-09-25-facade-846821-recheck/README.md)
retains the first clean short series and the later workload results.
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

## Reproduce the current Nix-bundle mocker recheck

Use source branch `feat/dynamo-component-pipelines-repro` and envs branch
`feat/dynamo-component-pipeline-builds` at or after the commits above. Build
`#component-pipeline-v2` from the envs `gateway-pipeline` flake and record
`nix path-info .#component-pipeline-v2`; for the pinned source it resolves to
the `accf6af699` bundle above. Its four outputs are the Dynamo facade,
patched AGW, patched Envoy executable, and generic Envoy module. Check
`nix flake check --no-build --impure` before staging. Do not build on the
benchmark nodes while measuring.

Point `VCLUSTER_KUBECONFIG` to the issued vCluster kubeconfig and set
`VCLUSTER_EXPECTED_SERVER` to its exact API URL. The helpers compare these
before every Kubernetes write. The 2026-09-25 campaign used namespace
`dynamo-components-v2`, six AIPerf 0.12.0 clients split 3/3 over nodes
`cluster-0967a26d-pool-1f83edbe-mj5s4-lhwhj` and
`cluster-0967a26d-pool-1f83edbe-mj5s4-dlq67`, four Dynamo `fastokens`
preprocessors, one InferencePool selector, 16 synthetic facade workers, and
one gateway replica per cell. The gateway rotation runs short, ISL4000, and
Mooncake separately. Short and ISL4000 replay frozen raw OpenAI payloads;
Mooncake reuses the prepared content-addressed mmap. Clients do not
synthesize prompts or compute token counts in the measured phase.

Stage the complete Nix closure with `stage-nix-closure.sh` and a fresh DNS-safe
stage ID. It requires the vCluster store NFS server/path and the host-visible
bridge NFS server/path/mount (`NIX_STORE_NFS_SERVER/PATH`,
`NIX_BRIDGE_NFS_SERVER/PATH`, `NIX_BRIDGE_HOST_PATH`). The 2026-09-25 run used
store server `192.168.0.220`, store export
`/unikorn-identity-03cfadd31877-551/pvc-19aeb650-5f77-45c4-8b8b-0a3b88542f39`,
bridge server `192.168.0.220`, bridge export
`/unikorn-identity-03cfadd31877-551/pvc-e3aa3d24-3aa3-4459-a23f-dfd5bc12990d`,
and host mount `/data`. Set `ENVSUBST_BIN` if `envsubst` is not on `PATH`.
Never point these helpers at the host-cluster kubeconfig.

```bash
# From the Dynamo repository root; COMPONENT_BUNDLE is the flake output path.
export COMPONENT_BUNDLE=/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699
export VCLUSTER_KUBECONFIG=/path/to/issued-vcluster.kubeconfig
export VCLUSTER_EXPECTED_SERVER=https://gateway-poc.mkhadkevich-dev:443
export VCLUSTER_NAMESPACE=dynamo-components-v2
export NIX_STORE_NFS_SERVER=192.168.0.220
export NIX_STORE_NFS_PATH=/unikorn-identity-03cfadd31877-551/pvc-19aeb650-5f77-45c4-8b8b-0a3b88542f39
export NIX_BRIDGE_NFS_SERVER=192.168.0.220
export NIX_BRIDGE_NFS_PATH=/unikorn-identity-03cfadd31877-551/pvc-e3aa3d24-3aa3-4459-a23f-dfd5bc12990d
export NIX_BRIDGE_HOST_PATH=/data
bash deploy/component-pipelines/k8s/vcluster/stage-nix-closure.sh \
  "$COMPONENT_BUNDLE" accf6af-repro-unique1
```

Use a fresh stage ID for every run. The store stager Pod and bridge mount must
already exist in the vCluster environment; the first section shows how to
create the Pod. The helper copies only missing closure paths and verifies them
in the vCluster store after its Job completes.

Scale real-worker fixtures and all benchmark gateways to zero; verify their
Pods are gone and no Job is active. Bring up only `envoy-independent` to 1/1,
then run `rollout-nix-bundle.sh "$COMPONENT_BUNDLE"`. That helper rolls the
facade workers, preprocessor, selector, AGW and Envoy binaries, and refreshes
the Envoy-callout worker map after Pod IPs change. Smoke one streamed request
before load. For the exact frozen benchmark topology, set:

```bash
export VCLUSTER_KUBECONFIG=/path/to/issued-vcluster.kubeconfig
export VCLUSTER_EXPECTED_SERVER=https://gateway-poc.mkhadkevich-dev:443
export VCLUSTER_NAMESPACE=dynamo-components-v2
export AIPERF_NODE_A=cluster-0967a26d-pool-1f83edbe-mj5s4-lhwhj
export AIPERF_NODE_B=cluster-0967a26d-pool-1f83edbe-mj5s4-dlq67
export NIX_STORE_NFS_SERVER=192.168.0.220
export NIX_STORE_NFS_PATH=/unikorn-identity-03cfadd31877-551/pvc-19aeb650-5f77-45c4-8b8b-0a3b88542f39
export TOKENIZER_STORE_BASENAME=wjq1b3wfjpzak4yd4rmj9arwqk1gkiir-qwen-tokenizer
export RESULT_DIR="$PWD/deploy/component-pipelines/k8s/vcluster/results/2026-09-25-accf6af-bundle-recheck"
mkdir -p "$RESULT_DIR"
for workload in short isl4000 mooncake; do
  for trial in r28 r29 r30; do
    TRIAL="$trial" WORKLOAD="$workload" \
      bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-recheck.sh
  done
done
jobs=("$RESULT_DIR"/raw_aiperf/nixv2-*)
JOB_NAMES=$(printf '%s\n' "${jobs[@]##*/}" | paste -sd, -) \
  bash deploy/component-pipelines/k8s/vcluster/capture-nix-mocker-execution.sh
SERIES_ID=nix-component-pipeline-accf6af-2026-09-25 \
  bash deploy/component-pipelines/k8s/vcluster/summarize-nix-mocker-results.sh
bash deploy/component-pipelines/k8s/vcluster/capture-nix-mocker-topology.sh
```

Run from the Dynamo repository root, with `COMPONENT_BUNDLE` set to the Nix
output and `ENVSUBST_BIN` set if needed. The runner refuses active Jobs,
active real-model fixtures, incomplete facade replicas, existing partial
exports, and reused Job names; it rotates arm order and scales exactly one
gateway at a time. Each Job has a 90-second shared start barrier, concurrency
128 per client, and a 45-second measured interval. It accepts a cell only
when all six AIPerf JSON exports exist with no reported request errors or
cancellations. Preserve the complete `raw_aiperf/` tree, Jobs' commands and
Pod placement, bundle path, and excluded runs in the result record. Report
the median of three aggregate Job RPS values, not a mean of client RPS
values. The source script `capture-nix-mocker-execution.sh` captures Job
commands and placement; `summarize-nix-mocker-results.sh` normalizes exports.
`capture-nix-mocker-topology.sh` saves the 25 applied, secret-free Kubernetes
resources as an installable JSON List. Rebuild/stage the referenced Nix
closures and prepared datasets before applying that snapshot in a fresh
vCluster; refresh the Envoy-callout worker map after its Pod IPs change.
The [earlier 846821 recheck](results/2026-09-25-facade-846821-recheck/README.md)
is the comparison series. Mooncake's approximately 3,026 RPS offered rate
is not a saturation ceiling.

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
scheduler delay does not start a client before the others are ready. For both
raw-payload and Mooncake Jobs, the barrier precedes `aiperf profile` initialization;
it does **not** guarantee synchronized measured start times. Audit each client's
exported `start_time` and keep runs with large start spread out of paired
gateway comparisons. The 2026-09-26 synthetic P/D ISL4000 repeats show why:
AGW's valid clients started 1.7–2.9 seconds apart, while Envoy's valid clients
started nearly together, making summed-client and global-window rates disagree.
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

For the new aggregate native-SGLang correctness path, stage the closure of the
Nix-built facade and the Nix SGLang worker environment on the same vCluster NFS
store. Set the explicit `VCLUSTER_KUBECONFIG`, exact
`VCLUSTER_EXPECTED_SERVER`, `VCLUSTER_NAMESPACE`, `FACADE_STORE_PATH`,
`SGLANG_WORKER_ENV_PATH`, `GATEWAY_BUNDLE_PATH`, and both NFS server/path pairs.
`ENVSUBST_BIN` may point to a Nix `gettext` executable. Then run:

```bash
bash deploy/component-pipelines/k8s/vcluster/run-real-sglang-split.sh
GRPCURL_BIN=/path/to/grpcurl \
  bash deploy/component-pipelines/k8s/vcluster/smoke-real-sglang-split.sh
```

The deploy helper refuses a non-matching vCluster API and checks that all
three immutable Nix executables are staged before it applies anything. It
creates a separate `real-sglang-split` InferencePool, `real-qwen3-selector`,
`real-qwen3-preprocessor`, and `real-qwen3-agw-generic`; the mocker pool and
benchmark gateway remain unchanged. The selector's Pod watch supplies the
actual worker Pod endpoint. After native engine startup, the facade patches its
own Pod annotation with the engine's discovered KV block size, block count,
batch-token limit, model name, and stable Pod UID. It fails closed if this
opt-in publication fails; the fixture grants only Pod get/patch to its worker
ServiceAccount. The smoke script checks the annotation and compares the
selector endpoint with the Ready Pod IP, calls the preprocessor and native
worker over gRPC, then asserts
an OpenAI-compatible streamed response through AGW with a `stop` finish
reason and `[DONE]`. The model's `chat_template_kwargs.enable_thinking=false`
keeps this correctness check about the final answer rather than truncated
thinking tokens. The SGLang fixture is aggregate-only; SGLang disaggregated
real-worker verification remains pending. Its selector sees the runtime KV
capacity, but this fixture does not yet enable native SGLang KV-event emission.
See [the captured real-SGLang run](results/2026-09-25-real-sglang-split/README.md).

The real-vLLM path uses the same preprocessor, selector, and worker-facade
contract, but the engine frontend must be upstream `vllm-rs` from the exact
revision as its headless Python vLLM engine. The Python
`vllm.entrypoints.grpc_server` does **not** expose the `vllm.Control` service
required by Dynamo's sidecar. From the dedicated `/work/envs` Nix branch,
build and stage `#vllm-rs` and `#vllm`; set `VLLM_RS_PATH` and
`VLLM_WORKER_ENV_PATH` in addition
to the shared vCluster, facade, gateway, model, and NFS settings above. Then run:

```bash
bash deploy/component-pipelines/k8s/vcluster/run-real-vllm-split.sh
GRPCURL_BIN=/path/to/grpcurl \
  bash deploy/component-pipelines/k8s/vcluster/smoke-real-vllm-split.sh
```

The [captured real-vLLM run](results/2026-09-25-real-vllm-split/README.md)
proves aggregate streaming through AGW and runtime KV metadata publication.

For real vLLM prefill/decode, stage the current `accf6af699` facade output
and complete bundle in the same vCluster NFS store, set their absolute store
paths as `FACADE_STORE_PATH` and `GATEWAY_BUNDLE_PATH`, and use the exact
vCluster variables above. Run `run-real-vllm-pd.sh` and then
`smoke-real-vllm-pd.sh`. The fixture clones the aggregate vLLM worker into
separate `kv_producer` and `kv_consumer` Pods, gives each NIXL side channel
the routable `status.podIP`, and routes the prefill handoff over the graph's
batched gRPC facades to the decode worker. The smoke validates both workers'
runtime KV annotations and an OpenAI-compatible streamed answer with a
terminal finish reason. This correctness check passed on 2026-09-25 with
real Qwen3-0.6B GPU workers in the vCluster; it is not a P/D throughput
result. The NIXL side-channel pod-IP setting follows
`lib/sidecar/vllm/deploy/disagg.yaml` and is required when bypassing the
`dynamo.vllm` wrapper.

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
- `results/2026-09-25-accf6af-mooncake-ceiling-fixed-reader/` and
  `results/2026-09-25-accf6af-mooncake-client-placement/` retain the later
  Mooncake capacity probes and their corrected replay-fidelity audits. Their
  high-load AIPerf schedules degraded, so the reported sums of client rates
  are diagnostics, not a proven single-gateway ceiling.
- `results/2026-09-26-accf6af-mooncake-distributed-v013/` retains the frozen
  AIPerf 0.13.0/JobSet experiment, its source manifest and ten
  failed preparation audits. It never produced a benchmark measurement: the
  single dataset manager reached about 39 GB and exited 137 while composing
  544,776 prompts. The JobSet controller was installed only in the vCluster;
  its upstream v0.12.0 manifest has SHA-256
  `a41aaf12dd0b7b0a3d626b8d6107f32c3dc889d7e7830adc46b8cdb77a8963bb`.
  Mechanical generated JobSet manifests are kept locally but ignored by Git;
  regenerate them from `mooncake-distributed-aiperfjob.yaml` using pinned
  `aiperf==0.13.0` and `aiperf kube generate --no-operator`.
- `results/2026-09-26-accf6af-mooncake-client-nine-nodes*/` retains the
  follow-up nine-node client-placement plans, complete per-client cache-hit
  proofs, execution manifests, and streamed request-record audits. Every
  plan is frozen before its Job starts. A capacity number is valid only when
  all scheduled requests complete, every client's replay lag and start-spread
  pass, and errors are zero. The sum of per-client RPS is never used as a
  synchronized gateway-capacity number.

The 46-second grace-series sweep uses the same 22,699-row trace per client,
one Envoy-callout gateway, eight preprocessors, eight selectors, and 32 mock
workers; only the number of AIPerf clients changes. Each client reads its
prebuilt mmap prompt cache, so measured load generation skips tokenization and
prompt composition. The extra second lets the final 45,000-ms arrival drain.
The globally normalized successful RPS divides the committed successful
request count by the global first-start to last-finish window, rather than
summing independent client rates:

| Clients | Scheduled / successful | Global successful RPS | Max p99 replay lag | Strict gate |
| ---: | ---: | ---: | ---: | :--- |
| 12 | 272,388 / 272,388 | 5,921.33 | 118.99 ms | pass |
| 14 | 317,786 / 317,786 | 6,907.32 | 130.99 ms | pass |
| 14 repeat | 317,786 / 317,786 | 6,906.35 | 123.03 ms | pass |
| 15 | 340,485 / 304,942 | invalid as capacity | 5,746.09 ms | fail |
| 16 | 363,184 / 306,744 | invalid as capacity | 7,858.78 ms | fail |

These trials bracket the strict single-gateway-topology Mooncake capacity between the
14- and 15-client offered loads (7,061.91 and 7,566.33 nominal RPS), with
6,906–6,907 globally normalized RPS reproduced at the passing point. The
failed 15-/16-client sums are not throughput ceilings because replay fell
behind. This sweep isolates one gateway replica while scaling the other
stages, but its pass/fail boundary alone does not prove which shared serving
stage saturates first.

For the nine-node sweep, set `VCLUSTER_KUBECONFIG`,
`VCLUSTER_EXPECTED_SERVER=https://gateway-poc.mkhadkevich-dev:443`,
`VCLUSTER_NAMESPACE=dynamo-components-v2`, `NIX_STORE_NFS_SERVER`,
`NIX_STORE_NFS_PATH`, and `ENVSUBST_BIN` (a Nix `gettext` executable). Then run:

```bash
bash deploy/component-pipelines/k8s/vcluster/run-nix-mooncake-nine-nodes.sh c512-grace 12 r1
```

Replace `12` with `14`, `15`, or `16` for the other 46-second grace-series load points;
`c512` selects the earlier 45-second control plans, including 24 clients. The
trace still sends at its original 0–45-second timestamps; the grace variant
only lets AIPerf record the final requests after timestamp 45,000 ms. The
wrapper reads the exact node list from each plan and the
runner rejects any non-vCluster API server or active benchmark Job. Follow a
completed Job with `python3 audit-mooncake-request-records.py RESULT_DIR JOB
--clients 12 > RESULT_DIR/request-audit-JOB.json`, using the actual client
count. The raw `profile_export.jsonl` files are
large (about 0.9 GB for one 24-client run) and remain under
`/shared/nix/aiperf/results/JOB` in the vCluster NFS volume; their per-file
SHA-256 hashes and globally normalized metrics are committed in the audit.
Local `raw_aiperf/` copies are intentionally ignored by Git, not deleted.

The repository retains normalized evidence and either raw exports or their
vCluster NFS locations and hashes, but not machine-local `result` symlinks or
Nix store closures. Rebuild the refactored bundle
with the envs flake above; a `/nix/store/...` symlink from the machine that performed the run is
not portable evidence.

## Static AGW disaggregated prefill/decode reproduction (2026-09-26)

The compiled static host now invokes the Dynamo facade's `GenerateRaw` prefill
RPC, forwards only its opaque disaggregated handoff, and invokes the facade's
decode `Generate` RPC. It does not copy Dynamo's engine, tokenizer, selector,
or postprocessor logic. The first build (`743fe6254f`) multiplexed all
prefill traffic over one HTTP/2 connection. Its short run had 9,067 request
errors; [the audit](results/2026-09-26-mocker-pd-agw-static/audit-nixpds-short-pd-agw-static-r1.json)
marks it invalid, so its apparent throughput must not be compared.

The corrected source is Dynamo commit `1dcc540c6b`, built by the envs flake
`gateway-pipeline#component-pipeline-agentgateway` on branch
`feat/dynamo-component-pipeline-static-pd`. Its Nix output is
`/nix/store/g8mr1z39cy1pnb75rvxvxs2gbf58cr2h-agentgateway-component-pipeline-0.0.0-b14ca87d0a`.
The host opens 32 independent prefill gRPC connections via
`DYN_GRPC_CHANNELS_PER_ENDPOINT=32`, allowing the Kubernetes Service to spread
streams across four prefill replicas. One AGW replica, four preprocessors,
four selectors, four prefill workers, and 16 decode workers ran in
`dynamo-components-v2` on the explicit vCluster API. All workers are synthetic
Dynamo `AsyncEngine` benchmark fixtures behind the production facade, not GPU
measurements. The prefill marker is mandatory at decode, so the
`smoke-nix-mocker-pd.sh` streamed OpenAI response proves the handoff path.

The [frozen plan](results/2026-09-26-mocker-pd-agw-static-pool/benchmark_plan.json)
has SHA-256 `57863f8e4666996e82db06e2728da738c6845387b625139f8f6af7c58f079240`.
Its three separate, zero-error audits report:

| Workload | Summed client RPS | Globally normalized successful RPS | Audit |
| --- | ---: | ---: | --- |
| Short | 9,870.96 | 9,569.90 | valid |
| ISL4000 | 5,851.48 | 5,712.81 | valid |
| Mooncake | 3,024.04 | 2,965.97 | valid; full trace and mmap cache hits |

These are one-pass, summary-level characterizations. Earlier generic and
callout P/D series used distinct plans and run windows; their numbers are
context, not promotion-grade paired deltas. In particular, ISL4000 is below
the earlier generic and callout results, so static P/D parity is not yet
established. Mooncake is near the six-client offered-load ceiling; this is not
a gateway-capacity measurement.

A separate [selector-pool candidate](results/2026-09-26-mocker-pd-agw-static-selector-pool/benchmark_plan.json)
(`a48d8a30d0`, Nix output
`/nix/store/mqziwjkv1jmnnp84q5ajq67vmqa96jm6-agentgateway-component-pipeline-0.0.0-b14ca87d0a`)
also pooled 32 independent selector connections. Its first ISL4000 trial
passed all audit gates at 6,043.03 summed and 5,957.69 globally normalized
successful RPS. This small gain does not explain the main static-versus-generic
gap; short and Mooncake were not run on that candidate.

The next [worker-pool candidate](results/2026-09-26-mocker-pd-agw-static-worker-pool/benchmark_plan.json)
(`dafb4898e4`, Nix output
`/nix/store/nddp59k8czydjigx61bcil2z8ixfmsv6-agentgateway-component-pipeline-0.0.0-b14ca87d0a`)
matched the generic transport's 32 lazy gRPC connections per selected worker
endpoint. Its first ISL4000 trial also passed all audit gates at 6,104.74
summed and 5,974.62 globally normalized successful RPS. The negligible
increase over selector pooling rules out connection fan-out as the primary
remaining static P/D bottleneck. Short and Mooncake were not run on this
diagnostic candidate; the earlier prefill-pool series is the only complete
three-workload static P/D characterization so far.

Further single-pass ISL4000 diagnostics kept the same six-client dataset,
vCluster nodes, and facade workers; the static setting trials also kept the
same AGW binary. Each Job completed with six
exports, zero errors and cancellations, and a valid frozen-plan audit:

| AGW arm / setting | Global successful RPS | Mean request latency across clients | Interpretation |
| --- | ---: | ---: | --- |
| Static, 12 threads, 200 µs batch linger | 5,974.62 | 120.41 ms | Pooled worker baseline |
| Static, 12 threads, 100 µs batch linger | 5,841.16 | — | Timer amount did not close gap |
| Static, 12 threads, zero batch linger | 5,848.82 | — | Eliminating timer did not close gap |
| Static, 16 threads, 200 µs batch linger | 5,897.06 | 121.43 ms | Matching generic thread count did not close gap |
| Generic, 16 threads, fresh repeat | 9,372.84 | 38.19 ms | Current generic gateway still reproduces earlier result |

The generic repeat uses the existing generic AGW binary and the same worker
graph, but is a separate benchmark series. These single-pass runs isolate
likely causes; they are not an interleaved, three-repetition promotion test.
Static's mean AIPerf time to first token is about 121 ms versus 38 ms for
the fresh generic run. Connection pooling, batch linger, and gateway worker
thread count have not explained that wait. Opt-in per-stage timing in the
static facade is the next diagnostic; it leaves Dynamo core unchanged.

To reproduce, build the pinned envs flake, stage its output with
`stage-nix-closure.sh` and a fresh stage ID, then set the explicit
`VCLUSTER_KUBECONFIG`, `VCLUSTER_EXPECTED_SERVER`, `VCLUSTER_NAMESPACE`,
`NIX_STAGER_POD`, `NIX_STORE_NFS_SERVER/PATH`, and `ENVSUBST_BIN` used above.
Use a Ready helper Pod with `/shared/nix` mounted from the same vCluster NFS
store. Run `run-nix-mocker-pd-static.sh`, then
`PD_GATEWAY_SERVICE=dynamo-pd-agw-static smoke-nix-mocker-pd.sh`. With every
other gateway scaled to zero and no active Job, run a fresh trial ID for each
workload:

```bash
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh short r3 static
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh isl4000 r3 static
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh mooncake r3 static
```

Audit each with `python3 audit-nix-mocker-pd.py RESULT_DIR WORKLOAD r3
--static`, where `RESULT_DIR` is the static-pool plan directory. The raw
six-client exports remain in its ignored `raw_aiperf/` folder; committed
summaries retain their hashes, plan identity, vCluster Job identities,
occupancy, and cache-hit evidence.
