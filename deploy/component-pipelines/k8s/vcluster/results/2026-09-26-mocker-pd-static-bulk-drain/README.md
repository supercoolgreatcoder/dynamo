# Static bulk-drain dispatch: queue clears, throughput does not improve

This same-binary A/B tested the opt-in
`DYN_PREPROCESS_BATCH_BULK_DRAIN=1` static-facade scheduler at Dynamo commit
`e03c6cdca7`. After the normal linger it takes up to 1,024 ready items from
the bounded queue and sends concurrent preprocessing RPCs in chunks of at
most 32. The off arm follows the prior one-chunk-per-collection path. No
Dynamo core crate, facade binary, worker, or AGW host patch changed. The Nix
output is `component-pipeline-agentgateway-static-bulk-drain` at
`/nix/store/4q3hi4jd1xvkq8ys498599a83m3zg1yh-agentgateway-component-pipeline-0.0.0-b14ca87d0a`.

The [frozen plan](benchmark_plan.json) SHA-256 is
`e252785f7a13c5498a0fb010ed7d6ca33fc71b05a1247d1cdbd27e3981d77607`.
The arms were interleaved off/on/off/on/off/on with a fresh rollout of the
one-replica gateway before each run. Both used 16 gateway threads, four
gRPC channels per endpoint, 4/4/4/16 preprocessor/selector/prefill/decode
replicas, six pinned AIPerf 0.12.0 clients, the same 45-second ISL4000
measurement, and frozen raw-payload SHA-256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
The streamed synthetic P/D handoff smoke passed in both modes. All six
audits are valid: zero request errors, zero cancellations, exact binary and
flag identity from saved gateway Deployments, and all six client exports.
Every resource was deployed only within
`https://gateway-poc.mkhadkevich-dev:443/dynamo-components-v2`; the gateway
was scaled to zero afterward.

| Order | Mode | Trial | Globally normalized successful RPS |
| ---: | --- | --- | ---: |
| 1 | Off | `r60` | 6,434.76 |
| 2 | On | `r61` | 6,335.41 |
| 3 | Off | `r62` | 6,426.95 |
| 4 | On | `r63` | 6,190.67 |
| 5 | Off | `r64` | 6,210.59 |
| 6 | On | `r65` | 6,321.17 |

The off median is **6,426.95 RPS** and the on median is **6,321.17 RPS**
(−1.6%). Paired on-minus-off deltas are −99.35, −236.28, and +110.58 RPS.
Thus the direction varies by pair, but there is no evidence that bulk drain
closes the roughly 6.4k-versus-9.5k static/generic ISL4000 gap.

The low-frequency cumulative gateway snapshots show that the option worked:
on runs drained roughly 42–44 items per collection versus 20–21 off and
left about 0.02–0.03 queued items per collection versus 23–24 off. The on
path split those collections into about 23 items per RPC, with 12.7k RPCs
instead of 13.8k–14.8k off. Yet preprocessing RPC wall time stayed roughly
35–40 ms per call in both modes. Collection count and queue depth are
therefore not sufficient explanations for the remaining throughput gap.
The `batches` counter means collection passes in this experimental path;
`rpc_completed` is the RPC count. These are cumulative wall-time counters,
not a CPU profile or a statistically precise variance model. The workers
are synthetic Dynamo `AsyncEngine` fixtures, not GPU throughput evidence.

To reproduce, build the isolated Nix output, stage its closure through
`stage-nix-closure.sh`, deploy using `run-nix-mocker-pd-static.sh` with
`PD_AGW_BINARY` set to that output and `PD_PREPROCESS_BATCH_BULK_DRAIN=0`
or `1`, smoke test, and run `run-nix-mocker-pd-trial.sh isl4000 rN
static-bulk-off` or `static-bulk-on`. The runner and auditor verify the
vCluster API, plan and dataset hashes, saved gateway Deployment flag,
binary, client exports, and error counts. Raw AIPerf JSON/CSV, execution
records, occupancy, gateway snapshots and logs, and audits are retained
alongside this summary.
