# Fixed-reader Mooncake single-gateway capacity grid

This is a new AIPerf-instrument series. It retains the exact Nix-built
`accf6af699` serving bundle, one Envoy generic-with-callouts gateway at 20
Envoy workers plus 20 pipeline threads, eight selectors, eight preprocessors,
32 synthetic workers, and the original pre-synthesized Mooncake trace. Only
the benchmark client changed: a digest-pinned `sitecustomize.py` overlay
replaces AIPerf 0.12.0's shared-mmap `seek()`/`read()` conversation getter with
cursor-free slicing. The patch is mounted into the AIPerf Jobs only; no
serving container or upstream package is edited. Each Job proves activation
before profiling and all client logs prove a cache HIT with tokenizer and
composer skipped. All resources were deployed **inside the vCluster only**.

| Clients | Nominal offered RPS | Achieved RPS | Output tokens/s | Mean output tokens | Mean latency | Errors |
|---:|---:|---:|---:|---:|---:|---:|
| 12 | 6,053.07 | 6,043.38 | 1,034,611.08 | 171.20 | 25.63 ms | 0 |
| 18 | 9,079.60 | 8,234.63 | 1,411,468.54 | 171.41 | 71.41 ms | 0 |
| 24 | 12,106.13 | 8,252.72 | 1,413,907.67 | 171.33 | 100.41 ms | 0 |

All Jobs completed every client Pod (12/12, 18/18, and 24/24), with exactly
half the clients on each of two CPU nodes. Every raw export reports AIPerf
0.12.0, fixed-schedule mode, the same 22,699-row trace per client, 128
concurrency per client, one record processor, no errors, and no cancellation.
The source trace SHA256 is
`28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550`.
The [frozen plan](benchmark_plan.json) and [summary audit](benchmark_audit.json)
record the full identities and limitations. Raw AIPerf JSON/CSV/console
exports, execution/Pod ledgers, and cache-hit TSVs are retained for all three
points.

The 18-to-24-client achieved rate increased by only **0.22%**, despite 33%
more nominal arrivals. That is a plateau of the *measured two-node client plus
serving setup*, not proof of an Envoy gateway ceiling. In a 15-second window
during the 24-client measured phase, the gateway cgroup CPU counter increased
from 3,463,611,474 to 3,646,628,034 microseconds: about 12.20 cores, with
zero cgroup CPU-throttling periods. The vCluster Metrics API was unavailable,
so no synchronized selector, preprocessor, worker, or client CPU profile was
collected. A subsequent read-only inventory found other-namespace AIPerf and
worker pods on the two client nodes. Their occupancy was not frozen or
sampled during the measurements. Therefore client contention and other
downstream limits remain live hypotheses; do not attribute the plateau to
gateway CPU or promote it as a hard ceiling.

The 12-client bridge is numerically close to the unpatched clean 6,043.92 RPS
point, but changing the AIPerf reader created a new series. The old point is
context, not a same-series gain/loss reference. AIPerf's `--export-level
summary` did not emit request-level JSONL, so request-ID completeness,
tail-latency recomputation, and production SLO certification are unavailable.
The mocker does not validate real GPU throughput or response-content parity.

To reproduce, set `VCLUSTER_KUBECONFIG`, `VCLUSTER_EXPECTED_SERVER`,
`VCLUSTER_NAMESPACE`, `AIPERF_NODE_A`, `AIPERF_NODE_B`,
`NIX_STORE_NFS_SERVER`, `NIX_STORE_NFS_PATH`, `RESULT_DIR`, and `ENVSUBST_BIN`
if necessary, then run
`bash deploy/component-pipelines/k8s/vcluster/run-nix-mooncake-ceiling.sh fixed {12|18|24} rN`
from the repo root with a fresh `rN`. The launcher checks the exact plan,
trace, serving bundle, patch, vCluster API address, topology, and exclusive
Job state; it refuses to overwrite raw evidence.

The next informative test is an independently recorded client-placement
control: add client-node capacity without changing the serving topology or
trace, and collect synchronized cgroup CPU counters for both the clients and
gateway. A higher clean throughput would disprove 8.25k RPS as the gateway
ceiling; an unchanged result would require deeper per-stage profiling before
calling a hard gateway limit.
