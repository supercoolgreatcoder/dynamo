# Mooncake client-placement control: three load-generator nodes

This vCluster-only run kept the same Nix-built `accf6af699` serving bundle,
single Envoy generic-with-callouts gateway (20 Envoy workers and 20 pipeline
threads), eight selectors, eight preprocessors, 32 synthetic workers, and
pre-synthesized 22,699-row Mooncake trace per AIPerf client. It changed the
24-client load-generator placement from two CPU nodes to three (eight clients
per node). AIPerf 0.12.0 used the same digest-pinned cursor-free mmap reader
overlay as the preceding fixed-reader series, and all 24 clients proved an
mmap-cache HIT without tokenizer or composer work.

| Placement | Clients | Nominal offered RPS | Sum of client-reported RPS | Successful requests | Errors |
|---|---:|---:|---:|---:|---:|
| Prior two-node run (context) | 24 | 12,106.13 | 8,252.72 | 375,387 | 0 |
| This three-node run | 24 | 12,106.13 | 8,951.78 | 407,033 | 0 |

The new Job completed 24/24 Pods, exactly eight on each planned node, with no
exported errors or cancelled clients. The **sum of client-reported rates** is
above the previous 8.25k RPS plateau, but this does **not** establish a higher
synchronized gateway throughput or disprove that plateau as a hard ceiling.
The client measured phases started between 02:14:15.742 and 02:14:19.756 UTC,
a four-second spread, so summing per-client rates measured over different
windows can exaggerate a shared gateway's single-window throughput. The
two-node number is from a different series and is contextual only: the 8.47%
numerical difference is not a controlled placement-effect estimate.
The 45-second fixed schedule nominally contained 544,776 arrivals across the
24 clients, but only 407,033 completed successfully. Every client reported
replay-scheduler degradation (per-client p99 scheduling lag 9.38–12.25 s).
This run therefore did not deliver the intended schedule faithfully. Its
fixed-schedule benchmark audit is **invalid for a gateway-capacity claim**; it
does not establish the gateway's maximum sustainable throughput or a latency
SLO.

Read-only client-node occupancy snapshots at 02:11:58 and 02:15:20 UTC each
showed 15 running Pods in another namespace across the three nodes. Those
neighbors were neither moved nor profiled. The Envoy cgroup CPU sample in
this attempt missed the 45-second measured phase, so it is not evidence of
gateway CPU saturation. The vCluster Metrics API was unavailable. Raw AIPerf
exports are summary-level only, without request-level JSONL for ID or tail
latency re-audit. Synthetic workers do not prove GPU-worker performance.

The [frozen plan](benchmark_plan.json), [audit](benchmark_audit.json),
24 raw JSON/CSV/console exports, execution/placement ledger, occupancy
snapshots, cache-hit proof, and aggregate summary are retained here. The
trace SHA256 is
`28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550`;
the plan SHA256 is
`58ad8caa1175ff8458e023f3786393129c8c2fe7eb7ba7099b064ae86d2dca53`.

To reproduce with a fresh trial name, set `VCLUSTER_KUBECONFIG`,
`VCLUSTER_EXPECTED_SERVER`, `VCLUSTER_NAMESPACE`, `AIPERF_NODE_A`,
`AIPERF_NODE_B`, `AIPERF_NODE_C`, `NIX_STORE_NFS_SERVER`,
`NIX_STORE_NFS_PATH`, `RESULT_DIR`, and `ENVSUBST_BIN`, then run
`bash deploy/component-pipelines/k8s/vcluster/run-nix-mooncake-ceiling.sh fixed3 24 rN`.
The runner validates the plan, trace, Nix bundle, overlay, vCluster API,
serving topology, node placement, and absence of another active benchmark
before creating a uniquely named Job. It refuses to overwrite raw evidence.
