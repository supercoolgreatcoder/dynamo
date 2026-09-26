<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static gRPC channel-readiness diagnostic (r81)

This is one diagnostic ISL4000 run, not a new parity claim. It uses the same
frozen input (SHA256 `3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`),
six AIPerf clients, 45-second measurement, four preprocessor/selector/prefill
replicas, and 16 decode replicas as the earlier [correlated comparison](../2026-09-26-mocker-pd-rpc-timing/README.md).
Only AGW static ran. Its isolated Nix build uses Dynamo commit `e9edbf83b3`
and envs commit `a3537c2`; the sampled selector and prefill facade build is
unchanged from r77 (`4b10d580de`). The new probe delegates the *actual*
generated-client `poll_ready` call to the tonic channel, without an extra poll.
Its event is emitted only for sampled request IDs.

| Run | RPS | Requests | Errors | Cancelled |
| --- | ---: | ---: | ---: | --- |
| r81 AGW static, readiness probe | 6,365.16 | 286,920 | 0 | false |
| r77 AGW static, earlier correlated control | 6,429.88 | 289,838 | 0 | false |
| r78 AGW generic, earlier correlated comparison | 9,406.79 | 424,489 | 0 | false |

The analyzer joined 1,146 IDs across the gateway, selector, and prefill logs.
On those requests, prefill response headers took 28,893 µs on average
(p95 38,388 µs), while tonic channel-buffer readiness took **0.2 µs** on
average (p95 1 µs; all polls succeeded). Only 5.1 µs elapsed from the
gateway's prefill-RPC start to readiness, including request setup and the poll.
The remaining 28,887 µs elapsed after readiness and before response headers.
The selector RPC independently took 28,725 µs on average (p95 38,047 µs);
its facade handler took 380 µs. Prefill's facade handler took 113 µs.

Thus Tower/tonic channel-buffer `poll_ready` contention does **not** explain
the static gateway's ~3k RPS gap to the generic gateway. The timing does not
separate HTTP/2 request send, network/server scheduling, response send, and
gateway response wake-up, so it cannot yet establish which of those dominates.
The cross-Pod clock-derived segments in the analyzer are only suggestive;
the same-process readiness and header intervals above do not rely on clock
synchronization.

Recompute the correlation from the captured logs:

```bash
python3 deploy/component-pipelines/k8s/vcluster/analyze-correlated-rpc.py \
  deploy/component-pipelines/k8s/vcluster/results/2026-09-26-mocker-pd-static-buffer-ready \
  nixpds-isl4000-pd-agw-static-r81
```

The folder retains the AIPerf exports, dataset hash, Job and Deployment JSON,
and gateway/facade logs. After capture, the vCluster was returned to the
baseline binary and topology: all gateways scaled to zero; preprocessor,
selector, prefill, and decode at 4/4/4/16, with the non-instrumented facade
binary restored to selector and prefill.
