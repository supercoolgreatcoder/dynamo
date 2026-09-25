# Exact-Nix-bundle single-gateway Mooncake capacity check

This is a separate capacity series from the six-client, four-gateway parity
campaign. It uses the same Nix-built Dynamo component/gateway bundle
`/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699`,
one Envoy generic-with-callouts gateway, 20 Envoy workers plus 20 pipeline
threads, eight selectors, eight preprocessors, and 32 synthetic workers. All
components and AIPerf Jobs ran **only inside the guarded vCluster**. The two
load-generator nodes were separate from the gateway node.

The original pre-synthesized Mooncake trace (SHA256
`28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550`)
was replayed once by each of 12 AIPerf 0.12.0 clients over 45 seconds, at
6,053.07 nominal offered requests/s. Every client in every attempt logged an
mmap-cache HIT and explicitly skipped tokenizer and composer work. The frozen
workload contract is [benchmark_plan.json](benchmark_plan.json).

| Attempt | Successful RPS | Output tokens/s | AIPerf errors | Eligibility |
|---|---:|---:|---:|---|
| r1 | 6,043.92 | 1,034,721.22 | 0 | Clean summary-level throughput point |
| r2 | 6,043.85 | 1,034,719.74 | 2 | Ineligible: load-generator mmap errors |
| r3 | 6,043.37 | 1,034,625.37 | 1 | Ineligible: load-generator mmap error |

All three Kubernetes Jobs completed 12/12 clients, with six clients on each
of two CPU nodes. The r2 and r3 errors are `MemoryMapSerializationError` /
truncated JSON while AIPerf reads its cached conversation data. They are not
HTTP failures, but the zero-error gate correctly excludes those attempts.
Inspection of the pinned AIPerf image confirmed that
`aiperf/dataset/memory_map_utils.py` implements
`MemoryMapDatasetClient.get_conversation()` as `data_mmap.seek(offset)`
followed by `data_mmap.read(size)` on the same object. This is not an atomic
offset read; concurrent access can interleave the two operations. Its
`get_payload_bytes()` path already uses cursor-free mmap slicing. This
diagnosis explains the observed truncated JSON, but the failing attempts
remain ineligible until a new, separately identified instrument series
actually validates a fix.
Their numerical agreement is diagnostic only; it must not be used to claim a
three-run clean median or production SLO. Raw AIPerf JSON/CSV/console exports,
the captured Job/Pod placement, and cache-hit evidence are retained separately
for each attempt. The launcher preserves a completed run even when validation
rejects it.

The clean r1 point is 99.85% of this offered schedule. Consequently it
establishes **at least 6,043.92 RPS**, not the hard ceiling of this gateway.
The prior clean 20+20 run on an earlier build reported 4,671.15 RPS, so this
result is 29.39% higher as descriptive historical context. The builds were
not interleaved, and source/environment changes between them have not been
isolated; no causal speedup claim is supported. The output-length shape is
similar (about 171 tokens per completed request), while one representative
client's mean request latency fell from about 299 ms to 28 ms. AIPerf's
server-reported prompt-token count is zero in both runs, so these exports do
not independently audit prompt-token accounting or response-content parity.

Reproduce from the repo root by setting `VCLUSTER_KUBECONFIG`,
`VCLUSTER_EXPECTED_SERVER`, `VCLUSTER_NAMESPACE`, `AIPERF_NODE_A`,
`AIPERF_NODE_B`, `NIX_STORE_NFS_SERVER`, `NIX_STORE_NFS_PATH`, `RESULT_DIR`,
and `ENVSUBST_BIN` (if `envsubst` is not on `PATH`), then run
`bash deploy/component-pipelines/k8s/vcluster/run-nix-mooncake-ceiling.sh rN`
with a new index. The script refuses the wrong API server, changed workload
plan or trace, unexpected bundle/replica topology, concurrent benchmark Jobs,
and reuse of existing output. It validates raw summary-level evidence and
checks all 12 cache-hit logs. No request-level JSONL was emitted by
`--export-level summary`, so an independent request-ID or tail-latency audit
is unavailable. The machine-readable decision is in
[benchmark_audit.json](benchmark_audit.json).

Remaining capacity work: fix or work around AIPerf 0.12.0's mmap `seek`/`read`
race in a separately versioned instrument series; then raise the offered rate
above 6,053 RPS while keeping the same single gateway and scaled downstream
components. Only that can locate the gateway's actual saturation point.
