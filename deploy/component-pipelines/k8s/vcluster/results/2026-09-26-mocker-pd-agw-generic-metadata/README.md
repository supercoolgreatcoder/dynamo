# Metadata-correct AGW generic P/D diagnostic

The generic graph in Dynamo `cb970285af` forwards the preprocessor's prompt
token count, reasoning and structural-tag flags, optional image-token count,
and image/video/audio counts to the worker-side Dynamo postprocessor. The
gateway is the Nix-built `dynamo-component-pipelines-cb970285af` output. This
diagnostic used the previously validated `995c04e74f` facade output because
the Nix bundle's old independent facade pin lacked `--benchmark-mode`; the
first rollout crash-looped and was recovered before measurement.

Run r1 was stopped before a Job was created: the benchmark invocation lacked
the required Nix NFS environment, so its dataset and occupancy preflight
files are not a performance result. Run r11 passed the six-client AIPerf
audit: 320,680 successful requests, zero errors or cancellations, and
6,889.87 globally normalized successful requests/s. Mean AIPerf request
latency averaged across clients was 90.84 ms. The corrected graph had passed
the streamed P/D handoff smoke test before this run.

The earlier generic refresh on the same frozen ISL4000 fixture reported
9,372.84 normalized requests/s and 38.19 ms mean AIPerf request latency.
This is an unpaired single-run comparison, not a causal attribution or a
promotion-grade performance claim. A controlled same-binary graph A/B is
needed to separate metadata-forwarding cost from run-to-run and deployment
effects. Raw AIPerf exports remain local and on the vCluster benchmark store;
the frozen plan, execution record, normalized summary, and validation audit
are retained here.
