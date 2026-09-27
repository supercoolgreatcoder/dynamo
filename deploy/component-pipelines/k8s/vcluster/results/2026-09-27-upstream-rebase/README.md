<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Dynamo upstream rebase audit, September 27, 2026

The benchmarked reference branch `feat/dynamo-component-pipelines-repro`
at `744d16bf0178df135f34ab20fe135fe88b362ef2` remains untouched.
This new branch rebases its 138 feature commits from shared base
`da27aa78dcadbb2239273cd9b4c0a0167e14b507` onto upstream Dynamo
`main` at `81a9871abd08f368eaf6a743650915898e80ce8b`. The rebase
completed without a textual conflict. Upstream main had advanced 52
commits; clean patch application alone did not prove API compatibility.

The first `cargo test --locked -p dynamo-component-facades` run compiled
but failed both selector integration tests: the new Dynamo KV router
requires a host-supplied default worker-selection policy factory. The
fix imports `dynamo-custom-policy-builtin` as a workspace dependency and
supplies `dynamo_custom_policy_builtin::default_factory()` at the
standalone selector's construction boundary, matching Dynamo's EPP
initialization. No policy implementation was copied into the facade.
Cargo updated only that one dependency entry in `Cargo.lock`.

With `protoc` and `LIBCLANG_PATH` supplied by Nix, these tests pass on
rebased main:

- `cargo test --locked -p dynamo-generic-pipeline`: 92 unit and 2 graph tests.
- `cargo test --locked -p dynamo-component-facades`: 14 unit/integration tests.
- `cargo test --locked -p dynamo-static-pipeline -p dynamo-pipeline-grpc`:
  2 static and 7 gRPC transport tests.

This is a source and CPU test gate only. The rebased Nix bundle has not
yet been built or deployed, real vLLM/SGLang correctness has not been
rechecked against this upstream revision, and the short/ISL4000/Mooncake
matrix has not been rerun. Do not use earlier benchmark numbers as evidence
of performance parity for this branch.
