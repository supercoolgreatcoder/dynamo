# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""GPU Memory Service integration for TensorRT-LLM.

The supported TRT-LLM path is KVCacheManagerV2. GMS owns model weights and
coordinates V2 KV slot leases from Python; the legacy V1 connector hook that
required a patched TRT-LLM wheel is intentionally not part of this runtime path.
"""

from __future__ import annotations

import logging
from typing import Any

from gpu_memory_service.integrations.common import patch_empty_cache
from gpu_memory_service.integrations.common.utils import (
    get_gms_lock_mode as _resolve_lock_mode,
)

logger = logging.getLogger(__name__)

__all__ = [
    "setup_gms",
    "get_gms_lock_mode",
]


def get_gms_lock_mode():
    from gpu_memory_service.integrations.trtllm.model_loader import (
        get_gms_lock_mode as _get_gms_lock_mode,
    )

    return _get_gms_lock_mode()


def setup_gms(
    model_loader_extra_config: dict[str, Any] | None = None,
    *,
    _patch_mpi_workers: bool = True,
) -> None:
    """Set up GMS integration for TensorRT-LLM. Call once before creating the engine.

    For TP>1 the engine spawns one MPI worker per rank (MpiPoolSession); those
    workers are fresh processes that do not import this module. ``_patch_mpi_workers``
    (internal) installs a hook so each spawned worker also runs ``setup_gms`` —
    the worker initializer passes ``_patch_mpi_workers=False`` to avoid re-patching.
    """
    extra = model_loader_extra_config or {}
    lock_mode = _resolve_lock_mode(extra)

    from gpu_memory_service.integrations.trtllm.model_loader import (
        patch_model_loader,
        set_gms_enabled,
        set_gms_lock_mode,
    )

    set_gms_enabled(True)
    set_gms_lock_mode(lock_mode)

    patch_empty_cache()
    from gpu_memory_service.integrations.trtllm.remote_code_cache import (
        patch_remote_code_cache,
    )

    patch_remote_code_cache()
    patch_model_loader()

    from gpu_memory_service.integrations.trtllm import install_kv_leases_v2

    install_kv_leases_v2.install()

    # TP>1 spawns rank workers via MpiPoolSession; those fresh processes must also
    # run setup_gms or weights/KV bypass GMS. Opt-in (still being hardened: running
    # the GMS patches inside the spawned ranks currently triggers a CUDA illegal
    # access during V2 KV executor init — see GMS_TRTLLM_MPI_WORKER_SETUP notes).
    import os as _os

    if _patch_mpi_workers and _os.environ.get(
        "GMS_TRTLLM_MPI_WORKER_SETUP", ""
    ).strip().lower() not in ("", "0", "false", "no", "off"):
        _install_mpi_worker_gms(extra)

    logger.info("[GMS] TensorRT-LLM integration enabled (mode=%s)", lock_mode)


def _gms_worker_initializer(model_loader_extra_config: dict[str, Any] | None) -> None:
    """Run in each MPI-spawned TRT-LLM rank worker before it builds the engine."""
    # Re-apply the GMS patches in the worker process; do not re-patch the pool
    # (the worker does not spawn sub-workers).
    setup_gms(model_loader_extra_config, _patch_mpi_workers=False)


def _install_mpi_worker_gms(model_loader_extra_config: dict[str, Any] | None) -> None:
    """Make TP>1 MPI workers run the GMS integration.

    TRT-LLM's ``MpiPoolSession._start_mpi_pool`` creates the worker pool with a
    filtered env (only ``TRTLLM*``/``TLLM*``/``CUDA_*``) and no initializer, so the
    spawned rank workers — which actually own the GPUs and load weights/allocate KV —
    never run ``setup_gms``; their weights+KV then bypass the GMS pool. Wrap pool
    creation to (a) also propagate ``GMS*`` env and (b) run ``setup_gms`` in each
    worker via an initializer.
    """
    try:
        from tensorrt_llm.llmapi import mpi_session as _mpi
    except Exception:  # pragma: no cover - TRT-LLM not importable
        return
    cls = getattr(_mpi, "MpiPoolSession", None)
    if cls is None or getattr(cls, "_gms_worker_init_patched", False):
        return

    import os as _os
    import sys as _sys

    from mpi4py.futures import MPIPoolExecutor

    def _start_mpi_pool(self) -> None:
        assert not self.mpi_pool, "MPI session already started"
        env = {
            k: v
            for k, v in _os.environ.items()
            if k.startswith(("TRTLLM", "TLLM", "GMS"))
            or k in ("CUDA_HOME", "CUDA_PATH")
        }
        self.mpi_pool = MPIPoolExecutor(
            max_workers=self.n_workers,
            path=_sys.path,
            env=env,
            initializer=_gms_worker_initializer,
            initargs=(model_loader_extra_config or {},),
        )

    cls._start_mpi_pool = _start_mpi_pool
    cls._gms_worker_init_patched = True
    logger.info(
        "[GMS] patched MpiPoolSession: propagate GMS env + init workers (TP>1)"
    )
