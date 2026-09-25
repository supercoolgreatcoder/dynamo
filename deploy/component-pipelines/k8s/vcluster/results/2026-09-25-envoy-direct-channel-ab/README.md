<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Envoy-direct channel-count probe: invalid intervention

This isolated, sequential A/B ran entirely inside the `dynamo-components-v2`
vCluster, using the same Nix-built Envoy direct gateway, aggregate graph, four
preprocessors, one selector, and 16 synthetic benchmark workers as the
[`2026-09-24` matrix](../2026-09-24-nix-build-mocker-parity/README.md).
The six AIPerf 0.12.0 clients reused its frozen ISL4000 raw-payload dataset,
45-second concurrency-128 policy, two-node 3/3 placement, and 90-second start
barrier. Only `DYN_GRPC_CHANNELS_PER_ENDPOINT` changed on the gateway. The
gateway was one replica for each trial and all other gateway arms were zero.
**This intervention was ineffective:** the deployed dynamic-module config
explicitly set `"grpc_conns":4`, and module initialization calls
`GrpcTransport::with_connections(&descriptor, cfg.grpc_conns)`. The environment
variable is read only by `GrpcTransport::new`, which this host does not use.
Consequently both trials used four actual channels per endpoint.

| Job | Intended environment setting | Actual gRPC channels per endpoint | Aggregate RPS | Successful requests | Errors | Effective concurrency | Weighted mean latency |
|---|---:|---:|---:|---:|---:|---:|---:|
| `nixv2-isl4000-envoy-generic-r4` | 4 | 4 | 5,879.23 | 265,143 | 0 | 742.79 | 126.31 ms |
| `nixv2-isl4000-envoy-generic-r5` | 32 | 4 | 5,677.72 | 256,039 | 0 | 745.04 | 131.18 ms |

Each value is calculated from the six retained AIPerf JSON summary exports
under [`raw_aiperf/`](raw_aiperf/). The 3.4% difference is run-to-run variation
at the **same actual channel count**; these data say nothing about whether
more channels close the roughly 5.8k versus 9.3k RPS Envoy-direct/callout
ISL4000 gap. Afterward, the vCluster Deployment environment was restored to
four and scaled to zero. A valid test must patch the module config's
`grpc_conns` field and verify the module's startup log.

The gateway's `GrpcTransport` makes separate lazy tonic channels per endpoint.
The module config, not the Deployment environment, controls that count in
Envoy direct mode. This report is retained to make the failed intervention
auditable and prevent the erroneous negative conclusion from being reused.
