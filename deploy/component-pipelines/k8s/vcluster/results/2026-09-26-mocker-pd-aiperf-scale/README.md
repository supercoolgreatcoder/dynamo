<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW generic ISL4000 load-generator and worker-thread scaling

These are single-replica, vCluster-only mock-worker diagnostics, not a
production throughput guarantee. They use the same Nix AGW binary
(`/nix/store/ha9684gksqyiilhjd43y9ccyd65rxwi0-agentgateway-component-pipeline-0.0.0-b14ca87d0a/bin/agentgateway`),
frozen raw ISL4000 payload (SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`),
45-second AIPerf 0.12.0 runs, 128 concurrency per client, and four
preprocessor, four selector, four prefill, and 16 decode mock-worker replicas.
Each AIPerf node carries three clients. The gateway is pinned to one
32-core node; its `workerThreads` setting is the only gateway change in
`r87`. All window-valid runs had zero AIPerf errors or cancellations.
However, the third AIPerf node (`...-79r2b`) also hosted four of our
decode mockers and four unrelated KV workers. The nine-client runs are
therefore **not isolated client-scaling comparisons** with the six-client
runs, and must not be used as a clean gateway-ceiling estimate.

| Job | Clients/nodes | Gateway workers | Requests | Exported RPS | Start skew | Audit |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| r82 | 6 / 2 | 16 | 438,896 | 9,729.20 | 2 s | window-valid |
| r84 | 9 / 3 | 16 | 468,800 | 10,397.18 | 3 s | window-valid; shared node |
| r85 | 6 / 2 | 16 | 447,638 | 9,916.35 | 2 s | window-valid |
| r86 | 9 / 3 | 16 | 456,667 | 10,130.22 | 1 s | window-valid; shared node |
| r87 | 9 / 3 | 32 | 469,022 | 10,402.32 | 1 s | window-valid; shared node |

The two six-client controls have a 9,822.78 RPS midpoint; the two
nine-client/16-worker runs have a 10,263.70 RPS midpoint (+4.49%).
Nine-client effective concurrency was about 108–122 per client, versus
about 45–68 in the earlier six-client correlated generic run. This is
consistent with the nine-client run applying more load, but node-C
co-location prevents attributing the small throughput difference to the
gateway. The one 32-worker result is within the observed nine-client range
and does not establish a throughput gain in this shared-node topology.

A further window-valid nine-client run, `r88`, moved only node C to
`...-rd9cc`, which had no Pods from this mock pipeline. It completed
447,747 requests at **9,930.30 RPS**, with zero errors/cancellations and
1-second start skew: essentially the same as the 9,916.35 RPS six-client
return control. Node C still hosted unrelated KV workers, so this is not
perfect CPU isolation. It does show that removing direct AIPerf/decode
co-location did not expose a large hidden increase in this gateway setup.
Its raw exports and Job/Pod snapshots are retained alongside the earlier
runs.

Read-only cgroup `cpu.stat` samples from the gateway Pod, both entirely
inside the exported measurement windows, are retained in
[`gateway-cpu-r86-r87.tsv`](gateway-cpu-r86-r87.tsv). The 16-worker gateway
used 478,214,544 CPU µs over 31 s, or approximately 15.43 cores. The
32-worker gateway used 553,547,030 CPU µs over 23 s, or approximately
24.07 cores. Both reported zero cgroup throttling. Extra worker threads
therefore increased CPU use substantially without a comparably large
RPS improvement; this single pair cannot distinguish contention in the
gateway from a saturated downstream component or shared-node client
interference. An isolated load-generator topology is required next.

`r83` is deliberately **excluded**. Its twelve Pods targeted a fourth node
that was `Ready` but tainted and unschedulable. Only three Pods initially
scheduled; later waves began after earlier Pods completed. AIPerf measurement
starts ranged from 23:16:05 to 23:19:21 UTC. Summing their per-client
rates would produce a misleading 43,092 RPS, since those rates were not
simultaneous. The raw exports, final Job/Pod snapshots, and the earlier
Pending-Pod snapshots (`*-invalid.json`) are retained to make this invalid
case auditable. No ceiling inference uses it.

The corrected harness (`run-nix-mocker-trial.sh` at Dynamo commit
`96470371e2`) keeps the six-client baseline unchanged. Scaled runs require
schedulable CPU nodes, wait until every Pod is Ready, release a shared NFS
barrier, and reject exported measurement windows more than five seconds
apart. Use `PD_AIPERF_CLIENTS=9` with the original A/B nodes plus node C
`cluster-0967a26d-pool-1f83edbe-mj5s4-79r2b`; the original nodes are
`...-dlq67` and `...-gnsf4`. Set the same vCluster, NFS, tokenizer, and
result-directory variables as the six-client recipe, then run
`run-nix-mocker-trial.sh pd-agw-generic isl4000 rNEW`. The guarded generic
rollout script at commit `e1773cc097` accepts `PD_WORKER_THREADS=16` or
`32`, updates only the vCluster gateway ConfigMap, and restarts the same
binary. The full AIPerf JSON/CSV/console exports and Job/Pod/Deployment
snapshots are in this folder.

After the series, the generic gateway ConfigMap was restored to 16 workers
and all four gateway Deployments were scaled to zero. No deployment was made
outside the vCluster.
