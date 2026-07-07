# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""vLLM failover ordering contracts (F3/F4, review-v2).

worker_factory imports vLLM at module top, so these are AST/source contracts on
the failover control flow — cheap, import-free, and pinned to the exact
regressions: (F3) rank-liveness must be armed on every path that acquires the
active lock, not only the static-primary path; (F4) on rank loss the leader must
unregister from discovery BEFORE releasing the failover flock.
"""

import ast
from pathlib import Path

import pytest

_WF = (
    Path(__file__).resolve().parents[1] / "worker_factory.py"
)

pytestmark = pytest.mark.pre_merge


def _func(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in worker_factory.py")


def _tree():
    return ast.parse(_WF.read_text())


def test_rank_liveness_armed_on_every_lock_acquiring_path():
    """F3: the promoted shadow must arm rank-liveness, not just the static primary.

    _maybe_wait_for_failover_lock has four activation paths that end up holding
    the active lock (static-primary, pre-init-already-acquired, private-bootstrap
    shadow, legacy shadow). Each must call _maybe_start_rank_liveness_monitor.
    """
    fn = _func(_tree(), "_maybe_wait_for_failover_lock")
    starts = [
        n
        for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "_maybe_start_rank_liveness_monitor"
    ]
    assert len(starts) >= 4, (
        f"expected rank-liveness armed on all 4 lock-acquiring paths, found {len(starts)}"
    )


def test_on_rank_lost_unregisters_before_releasing_the_flock():
    """F4: discovery unregister must precede flock release on rank loss."""
    fn = _func(_tree(), "_unregister_then_release")

    unregister_lines = [
        n.lineno
        for n in ast.walk(fn)
        if (isinstance(n, ast.Attribute) and n.attr == "unregister_endpoint_instance")
        or (isinstance(n, ast.Constant) and n.value == "unregister_endpoint_instance")
    ]
    release_lines = [
        n.lineno
        for n in ast.walk(fn)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "release_attached_gms_failover_lock"
    ]
    assert unregister_lines, "unregister_endpoint_instance not referenced"
    assert release_lines, "release_attached_gms_failover_lock not called"
    assert min(unregister_lines) < min(release_lines), (
        "flock must be released AFTER discovery unregister (F4)"
    )
