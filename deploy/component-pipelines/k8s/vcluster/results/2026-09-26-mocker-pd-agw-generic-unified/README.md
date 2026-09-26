# Unified Dynamo revision, metadata-correct AGW generic P/D

The Nix bundle `ip5h1yq7nz5gdbg9yja58b0fcng537gj` builds its facade,
gateway, and disaggregated graph from Dynamo `cb970285af`. The generic graph
forwards the complete preprocessor metadata needed by Dynamo worker-side
postprocessing, including the optional image-token count. All components
rolled out Ready and the streamed P/D handoff smoke test passed inside the
vCluster before measurement.

Run r12 used the same frozen ISL4000 raw-payload dataset, six AIPerf 0.12.0
clients, two pinned client nodes, concurrency 128 per client, and 45-second
duration as the prior generic refresh. The audit passed: 436,402 successful
requests, zero errors/cancellations, and 9,370.98 globally normalized
successful requests/s. Mean AIPerf request latency averaged across clients
was 45.64 ms.

For context, the earlier generic refresh with an older graph reported
9,372.84 normalized requests/s. The corrected-graph mixed-pin diagnostic
(new gateway, old facade) reported 6,889.87. These are single unpaired runs;
the near-identical throughput of the unified corrected graph and earlier
generic refresh supports parity, but does not by itself attribute the
mixed-pin loss to one component or quantify run-to-run variance. Raw AIPerf
exports remain local and on the vCluster benchmark store. The frozen plan,
execution record, normalized summary, and validation audit are retained here.
