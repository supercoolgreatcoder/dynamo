# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Batch-scoped, leader-arbitrated KV leases (H-B, redesign 2).

Today each TP rank acquires KV leases independently per slot. That is N×M
shared-memory ops per step and — worse — the rank-local free lists can diverge
(SGLang's _deprioritize_pages permanently reorders one rank's list), so two
ranks pick *different* block-ids for the same logical batch → divergent batches
→ collective hang (H-B). The TRT warmup-bypass only papers over warmup.

The fix makes divergence impossible by construction: the leader (TP0) acquires
the whole step's blocks once and broadcasts the exact block-ids over the CPU
channel that already carries rank liveness; every follower claims *those same
ids* (strict-preferred acquire) instead of choosing its own. All ranks end with
identical lease bookkeeping. This is also strictly fewer lease ops per step
(1 acquire + a small broadcast vs N×M), so it does not add latency — it removes
it — and the per-step block-id list is tiny, so it is cheap to epoch-stamp.

This module is the transport-agnostic mechanism. Wiring it into each engine's
allocator hot path (and deleting the TRT warmup-bypass) is the per-engine step;
the broadcast itself rides the existing rank channel.
"""

from __future__ import annotations

import logging
import struct
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gpu_memory_service.integrations.common.kv_lease_client import (
        KVLease,
        KVLeaseClient,
    )

logger = logging.getLogger(__name__)

_BATCH_MAGIC = 0x424C4541  # 'BLEA'
_BATCH_HEADER = struct.Struct("<IQI")  # magic | epoch | count


class BatchLeaseError(RuntimeError):
    """A follower could not claim the exact blocks the leader broadcast."""


def serialize_batch(epoch: int, block_ids: list[int]) -> bytes:
    """Pack (epoch, block_ids) for the CPU broadcast channel.

    The epoch is carried so a follower can reject a batch from a stale leader
    incarnation (composes with the failover-epoch fencing, redesign 1).
    """
    body = struct.Struct(f"<{len(block_ids)}I").pack(*[int(b) for b in block_ids])
    return _BATCH_HEADER.pack(_BATCH_MAGIC, int(epoch), len(block_ids)) + body


def deserialize_batch(data: bytes) -> tuple[int, list[int]]:
    """Inverse of serialize_batch. Returns (epoch, block_ids)."""
    if len(data) < _BATCH_HEADER.size:
        raise BatchLeaseError("batch broadcast too short")
    magic, epoch, count = _BATCH_HEADER.unpack_from(data, 0)
    if magic != _BATCH_MAGIC:
        raise BatchLeaseError(f"bad batch magic {magic:#x}")
    end = _BATCH_HEADER.size + count * 4
    if len(data) < end:
        raise BatchLeaseError("batch broadcast truncated")
    block_ids = list(struct.Struct(f"<{count}I").unpack_from(data, _BATCH_HEADER.size))
    return int(epoch), block_ids


class BatchLeaseCoordinator:
    """Leader/follower helpers over a shared-memory KV lease client."""

    def __init__(self, client: "KVLeaseClient") -> None:
        self._client = client

    def _epoch(self) -> int:
        return int(getattr(self._client, "fence_epoch", 0))

    # ---- leader (TP0) ----
    def leader_acquire(
        self,
        count: int,
        *,
        preferred_blocks: list[int] | None = None,
    ) -> tuple[bytes, list["KVLease"]]:
        """Acquire the whole step's blocks once; return (broadcast, leases).

        The broadcast bytes are handed to the existing CPU channel; the leases
        are the leader's own to use. One acquire per step, not N×M.
        """
        leases = self._client.acquire(
            count, preferred_blocks=preferred_blocks, allow_partial=False
        )
        block_ids = [int(l.block_id) for l in leases]
        return serialize_batch(self._epoch(), block_ids), leases

    # ---- follower (TP>0) ----
    def follower_apply(self, broadcast: bytes) -> list["KVLease"]:
        """Claim exactly the leader's block-ids (strict-preferred acquire).

        Rejects a batch stamped with an epoch newer than ours (stale-leader or
        we were fenced); claiming the same ids guarantees identical per-rank
        bookkeeping, so no two ranks can pick different blocks (H-B).
        """
        epoch, block_ids = deserialize_batch(broadcast)
        my_epoch = self._epoch()
        if epoch != my_epoch:
            raise BatchLeaseError(
                f"batch epoch {epoch} != follower epoch {my_epoch} (stale leader "
                f"or fenced follower)"
            )
        if not block_ids:
            return []
        leases = self._client.acquire(
            len(block_ids),
            preferred_blocks=block_ids,
            strict_preferred=True,
            allow_partial=False,
        )
        got = {int(l.block_id) for l in leases}
        want = set(block_ids)
        if got != want:
            raise BatchLeaseError(
                f"follower could not claim leader blocks exactly: "
                f"missing={sorted(want - got)} extra={sorted(got - want)}"
            )
        return leases
