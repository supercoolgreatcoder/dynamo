# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""X9 process-correctness: the lease patch + self-test must run in the process
that actually allocates KV (review-v2).

The launcher-side self-test certified the wrong process: sglang's spawned
scheduler re-imports model_loader but not the launcher bootstrap, and vLLM
builds BlockPool in the spawned EngineCore, not the worker that ran the
self-test. These tests assert the fixes:

- sglang model_loader's module-level block installs leases + self-tests in the
  child (AST contract — the child imports exactly this module);
- vLLM's EngineCore hook fails LOUD when leases are on but it can't patch,
  instead of the old silent ``return False`` the worker self-test couldn't see.
"""

import ast
from pathlib import Path

import pytest

pytest.importorskip("gms_rust_ring")

_INTEGRATIONS = (
    Path(__file__).resolve().parents[2] / "gpu_memory_service" / "integrations"
)


def _module_level_calls(path):
    """Names of functions/attrs invoked at module top level in `path`."""
    tree = ast.parse(path.read_text())
    calls = set()
    for node in tree.body:  # module level only
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            fn = node.value.func
            if isinstance(fn, ast.Attribute):
                calls.add(fn.attr)
            elif isinstance(fn, ast.Name):
                calls.add(fn.id)
    return calls


def test_sglang_model_loader_installs_leases_and_selftests_in_child():
    """The spawned scheduler imports model_loader — it must install + verify here."""
    calls = _module_level_calls(_INTEGRATIONS / "sglang" / "model_loader.py")
    # install_kv_leases.install() -> attr 'install'; gms_verify_integration(...) -> name.
    assert "install" in calls, "child-side block must call install_kv_leases.install()"
    assert (
        "gms_verify_integration" in calls
    ), "child-side block must self-test in the allocating process"


def test_vllm_engine_core_hook_raises_when_leases_on_but_unhookable(monkeypatch):
    """With leases enabled and vLLM absent, the hook must raise, not return False.

    In the ring test env vllm.v1.engine.core is not importable, standing in for
    upstream symbol drift. The old code logged debug + returned False, leaving the
    spawned EngineCore unhooked while the worker self-test still passed.
    """
    from gpu_memory_service.integrations.vllm import install_kv_leases as ikl

    # Ensure a clean, leases-enabled state.
    monkeypatch.setattr(ikl, "_engine_core_hook_patched", False, raising=False)
    monkeypatch.setenv("GMS_KV_LEASES", "1")
    monkeypatch.delenv("GMS_VLLM_KV_LEASES", raising=False)

    with pytest.raises(RuntimeError, match="EngineCoreProc is not importable"):
        ikl.install_engine_core_hook()


def test_vllm_engine_core_hook_noop_when_leases_disabled(monkeypatch):
    """Leases off -> the hook is a no-op (returns False, never raises)."""
    from gpu_memory_service.integrations.vllm import install_kv_leases as ikl

    monkeypatch.setattr(ikl, "_engine_core_hook_patched", False, raising=False)
    monkeypatch.delenv("GMS_KV_LEASES", raising=False)
    monkeypatch.delenv("GMS_VLLM_KV_LEASES", raising=False)

    assert ikl.install_engine_core_hook() is False


def test_trtllm_warns_on_leases_without_mpi_worker_setup():
    """The trtllm gap is a loud WARNING, not a silent comment (source contract)."""
    src = (_INTEGRATIONS / "trtllm" / "__init__.py").read_text()
    assert "logger.warning" in src
    assert "GMS_TRTLLM_MPI_WORKER_SETUP" in src
