# Matched AGW P/D gateway and preprocessor CPU samples

These read-only cgroup v2 `cpu.stat` snapshots were taken from existing
vCluster Pods during the synchronized AIPerf measurement windows of three
valid six-client ISL4000 runs. Every run used the frozen raw-payload dataset,
the same four preprocessor, four selector, four prefill, and 16 decode mock
worker replicas, one AGW gateway with 16 worker threads and four gRPC
channels per endpoint, and the pinned Dynamo `cb970285af` Nix bundle.
No GPU capacity inference is possible from these synthetic-worker runs.

| Arm/job | Audited normalized RPS | Gateway sample epoch seconds | Gateway `usage_usec` start → end | Approx. gateway cores |
| --- | ---: | --- | --- | ---: |
| Static `r28` | 6,472.90 | 1790425895 → 1790425920 | 41,168,112 → 170,542,637 | 5.17 |
| Generic `r29` | 9,513.85 | 1790426140 → 1790426166 | 153,042,721 → 538,143,158 | 14.81 |
| Static `r30` | 6,419.39 | 1790426336 → 1790426361 | 43,532,542 → 171,785,262 | 5.13 |

The gateway `cpu.stat` snapshots reported `nr_throttled=0` and
`throttled_usec=0` in all three arms. The generic run consumed nearly
three times as much gateway CPU over its sample interval while delivering
about 1.5 times the static throughput. Therefore the static throughput
gap is not explained by exhausting all 16 worker threads or by a cgroup
CPU quota. This does **not** rule out one hot serialized task, lock
contention, transport flow control, or waits on downstream RPCs.

For the near-time generic `r29` and static `r30` runs, four preprocessor
Pod `usage_usec` counters were sampled sequentially; the first and last
timestamps were 1790426142 → 1790426167 and 1790426338 → 1790426363,
respectively. The counter order in both rows is Pod suffix `6zgm6`,
`f6pbr`, `ggnhp`, `zfr4m` of the
`dynamo-pd-preprocessor-6b5c556bc4-` ReplicaSet; each counter increased
monotonically across its sampled interval:

| Arm | Preprocessor start counters | End counters | Aggregate approximate cores |
| --- | --- | --- | ---: |
| Generic `r29` | 742066342, 523630406, 622182926, 589059037 | 776126779, 523651119, 622207429, 621810725 | 2.67 |
| Static `r30` | 790767655, 530581756, 625832157, 632152526 | 799419948, 547532343, 634625146, 632170777 | 1.38 |

The preprocessor samples do not show all replicas saturated in either
arm. Per-Pod load balance differs, and cgroup CPU time alone cannot
attribute whether the lower static rate is caused by queueing in its
preprocessor batcher or at another gRPC stage. The next useful check is
batch-size/queue-wait telemetry in the static scheduler, compared with
the generic leader-follower batcher, not another blind linger adjustment.

Snapshots were obtained with `kubectl --kubeconfig
/tmp/dynamo-components-vcluster.kubeconfig -n dynamo-components-v2 exec
POD -- cat /sys/fs/cgroup/cpu.stat` and the `usage_usec` field from the
same path for each preprocessor. AIPerf plans, execution records, raw
exports, summaries, and audits are stored in the corresponding
`static-channels4-threads16/` and `agw-generic-unified/` directories.
