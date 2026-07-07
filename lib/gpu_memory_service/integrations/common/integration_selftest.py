# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Startup patch-contract self-test for GMS engine integrations (X9, redesign 5).

The lease installers monkeypatch engine internals and, on any import/symbol
drift, historically caught the failure with ``logger.debug; return False`` while
the engine kept running with shared KV enabled — i.e. two engines writing the
same physical KV pool with no lease arbitration. That is a silent correctness
hole exactly when leases matter most.

This module makes the contract explicit and fatal: an installer records that it
successfully patched via :func:`mark_installed`; at startup the integration
owner calls :func:`gms_verify_integration`, which raises (aborting startup) if
leases + shared KV are enabled for the engine but the ``_gms_*`` patch never
took. Cost is a dict lookup at startup only — zero steady-state impact.
"""

from __future__ import annotations

import logging

from gpu_memory_service.integrations.common.kv_lease_client import kv_leases_enabled

logger = logging.getLogger(__name__)

# (engine, component) pairs whose monkeypatch verifiably took effect this process.
_INSTALLED: set[tuple[str, str]] = set()


def _key(engine: str, component: str) -> tuple[str, str]:
    return (engine.lower().replace("-", "_"), component)


def mark_installed(engine: str, component: str = "kv_leases") -> None:
    """Record that ``component``'s monkeypatch for ``engine`` took effect.

    Installers call this only on their success path (after the patch is applied
    and the ``_patched`` marker is set), so presence in the registry is a real
    patch-contract signal, not merely "install() was invoked".
    """
    _INSTALLED.add(_key(engine, component))


def is_installed(engine: str, component: str = "kv_leases") -> bool:
    return _key(engine, component) in _INSTALLED


def reset_for_test() -> None:
    """Clear the registry (unit tests only)."""
    _INSTALLED.clear()


def gms_verify_integration(engine: str, *, component: str = "kv_leases") -> None:
    """Fail startup if leases/shared-KV are on but the patch didn't take (X9).

    Call once after invoking the engine's lease installer. If KV leases are
    enabled for ``engine`` (which, in this train, is coupled to shared-KV mode),
    the monkeypatch is a hard requirement: running without it means two writers
    on one pool. If leases are disabled this is a no-op.
    """
    if not kv_leases_enabled(engine):
        return
    if is_installed(engine, component):
        logger.info(
            "[GMS selftest] %s %s integration verified (patch applied)",
            engine,
            component,
        )
        return
    raise RuntimeError(
        f"GMS integration self-test failed: KV leases are enabled for {engine!r} "
        f"but the {component} monkeypatch did not take effect (upstream symbol "
        f"drift or an install() error). Shared KV without lease arbitration means "
        f"two writers on one pool — refusing to start. Set GMS_{engine.upper()}"
        f"_KV_LEASES=0 (and disable shared KV) to run without leases."
    )
