# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Persistent KV identity helpers for the vLLM GMS integration."""

from __future__ import annotations

from gpu_memory_service.integrations.common.utils import (
    env_enabled_by_default,
    get_gms_persistent_kv_engine_id,
)


def truthy_env(name: str, *, default: bool = False) -> bool:
    return env_enabled_by_default(name, default=default)


def shared_kv_enabled() -> bool:
    return truthy_env(
        "GMS_VLLM_SHARED_KV",
        default=(
            truthy_env("DYN_VLLM_GMS_SHADOW_MODE", default=False)
            or truthy_env("DYN_GMS_FAILOVER_SHADOW_MODE", default=False)
        ),
    )


def private_bootstrap_kv_enabled() -> bool:
    """Deferred for the vLLM-first MVP: always False.

    Private-bootstrap KV let a shadow pre-warm CUDA graphs against throwaway
    scratch backing and hot-swap the physical pages to the shared pool at
    promotion. It is a pre-warmed-shadow OPTIMIZATION on top of the default
    lock-before-init failover path (a shadow simply waits on the failover lock,
    then warms up after promotion), not a correctness requirement. It is deferred
    to a follow-up; the MVP ships lock-before-init only.
    """
    return False


def private_bootstrap_scratch_warmup_enabled() -> bool:
    """Deferred with private-bootstrap KV (see private_bootstrap_kv_enabled)."""
    return False


def stable_engine_id(device: int) -> str:
    return get_gms_persistent_kv_engine_id("vllm", device, "GMS_VLLM_VMM_IPC_ENGINE_ID")


def allocation_engine_id(device: int) -> str:
    return stable_engine_id(device)


def allocation_shared() -> bool:
    return shared_kv_enabled()


def promotion_engine_id(device: int) -> str:
    return stable_engine_id(device)


def use_existing_shared_geometry() -> bool:
    return shared_kv_enabled()


def release_private_bootstrap_kv_pool(manager, engine_id: str, *, logger=None) -> int:
    """Release stale private bootstrap KV allocations after promotion.

    Private bootstrap namespaces are only used to let a shadow initialize
    without writing into the active shared KV pool. Once the worker remaps to
    the stable shared namespace, keeping the private ``kv_pool#*`` allocations
    resident wastes HBM and can prevent a replacement Bulwark pair from
    bootstrapping.
    """
    try:
        allocations = list(manager.list_persistent(engine_id=engine_id))
    except Exception:  # noqa: BLE001
        if logger is not None:
            logger.warning(
                "[GMS] Failed to list private vLLM KV bootstrap allocations for %s",
                engine_id,
                exc_info=True,
            )
        return 0

    released = 0
    for allocation in allocations:
        tag = getattr(allocation, "tag", "")
        if not tag.startswith("kv_pool"):
            continue
        try:
            if manager.release_persistent(engine_id, tag):
                released += 1
        except Exception:  # noqa: BLE001
            if logger is not None:
                logger.warning(
                    "[GMS] Failed to release private vLLM KV bootstrap allocation %s/%s",
                    engine_id,
                    tag,
                    exc_info=True,
                )

    if released and logger is not None:
        logger.info(
            "[GMS] Released %d private vLLM KV bootstrap allocations for %s",
            released,
            engine_id,
        )
    return released
