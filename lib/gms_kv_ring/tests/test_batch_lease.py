# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Batch leader-arbitrated leases (H-B, redesign 2)."""

import os

import pytest

pytest.importorskip("gms_rust_ring")

from gpu_memory_service.integrations.common.batch_lease import (  # noqa: E402
    BatchLeaseCoordinator,
    BatchLeaseError,
    deserialize_batch,
    serialize_batch,
)
from gpu_memory_service.integrations.common.kv_lease_client import (  # noqa: E402
    SharedMemoryKVLeaseClient,
)


def _client(dirpath, name, owner):
    # Distinct rings per "rank" (rank-local lease files), same block universe.
    shm = os.path.join(dirpath, f"{name}.shm")
    return SharedMemoryKVLeaseClient(
        shm, namespace="ns", owner_id=owner, total_blocks=64
    )


def test_serialize_roundtrip():
    data = serialize_batch(3, [1, 4, 9, 16])
    assert deserialize_batch(data) == (3, [1, 4, 9, 16])
    assert deserialize_batch(serialize_batch(0, [])) == (0, [])


def test_follower_claims_exactly_leader_blocks(tmp_path):
    leader = BatchLeaseCoordinator(_client(str(tmp_path), "rank0", "r0"))
    follower = BatchLeaseCoordinator(_client(str(tmp_path), "rank1", "r1"))

    broadcast, leader_leases = leader.leader_acquire(5)
    leader_ids = sorted(int(l.block_id) for l in leader_leases)
    assert len(leader_ids) == 5

    follower_leases = follower.follower_apply(broadcast)
    follower_ids = sorted(int(l.block_id) for l in follower_leases)

    # Identical block-ids on both ranks -> no divergence possible (H-B).
    assert follower_ids == leader_ids


def test_follower_rejects_stale_epoch(tmp_path):
    leader_client = _client(str(tmp_path), "rank0", "r0")
    follower_client = _client(str(tmp_path), "rank1", "r1")
    leader = BatchLeaseCoordinator(leader_client)
    follower = BatchLeaseCoordinator(follower_client)

    # Leader promotes (epoch 1) and broadcasts a batch stamped epoch 1.
    leader_client.promote_fence_epoch()
    broadcast, _ = leader.leader_acquire(3)

    # Follower still at epoch 0 must reject the newer-epoch batch.
    with pytest.raises(BatchLeaseError, match="epoch"):
        follower.follower_apply(broadcast)


def test_one_acquire_per_step_is_the_only_leader_op(tmp_path):
    """Perf property: the leader does a single acquire for the whole batch."""
    client = _client(str(tmp_path), "rank0", "r0")

    calls = []
    real_acquire = client.acquire

    def counting_acquire(*a, **k):
        calls.append(1)
        return real_acquire(*a, **k)

    client.acquire = counting_acquire  # type: ignore[method-assign]
    coord = BatchLeaseCoordinator(client)
    coord.leader_acquire(8)
    assert sum(calls) == 1  # not 8 per-slot acquires
