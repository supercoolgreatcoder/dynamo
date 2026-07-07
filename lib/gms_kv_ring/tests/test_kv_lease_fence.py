# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Failover-epoch fencing for the shared-memory KV lease ring (X1, redesign 1).

These tests assert both the safety property (a fenced-out zombie primary cannot
acquire, hence cannot write) and the performance property required for this
redesign: the fence check on the acquire hot path is a single in-memory read
(no syscall, and it short-circuits before any Rust/lease work), and switchover
is a single header store.
"""

import os

import pytest

pytest.importorskip("gms_rust_ring")

from gpu_memory_service.integrations.common.kv_lease_client import (  # noqa: E402
    KVLeaseFencedError,
    SharedMemoryKVLeaseClient,
    _KV_LEASE_SHM_EPOCH_OFFSET,
    _KV_LEASE_SHM_EPOCH_STRUCT,
    promote_kv_lease_epoch_in_shm_dir,
    promote_local_kv_lease_epoch,
)


def _client(dirpath, owner, *, total=64):
    shm = os.path.join(dirpath, "ring.shm")
    return SharedMemoryKVLeaseClient(
        shm,
        namespace="ns",
        owner_id=owner,
        total_blocks=total,
    )


def test_fresh_ring_starts_at_epoch_zero_and_acquires(tmp_path):
    c = _client(str(tmp_path), "primary")
    try:
        assert c.fence_epoch == 0
        assert c.read_fence_epoch() == 0
        assert len(c.acquire(4)) == 4
        assert not c.is_fenced_out()
    finally:
        c.close()


def test_promote_bumps_epoch_and_fences_old_incarnation(tmp_path):
    primary = _client(str(tmp_path), "primary")
    shadow = _client(str(tmp_path), "shadow")  # same ring, both at epoch 0
    try:
        assert len(primary.acquire(2)) == 2  # primary writing normally

        new_epoch = shadow.promote_fence_epoch()  # shadow takes over
        assert new_epoch == 1
        assert shadow.fence_epoch == 1
        assert primary.read_fence_epoch() == 1  # peers observe the bump

        assert len(shadow.acquire(2)) == 2  # promoted shadow can write

        # Fenced-out primary fails closed on its very next acquire.
        assert primary.is_fenced_out()
        with pytest.raises(KVLeaseFencedError):
            primary.acquire(2)
    finally:
        primary.close()
        shadow.close()


def test_fence_check_short_circuits_before_touching_the_lease_ring(tmp_path):
    """Perf+safety: a fenced acquire must raise before any Rust/lease work.

    We replace the Rust ring with a sentinel that explodes if used; the fenced
    acquire must still raise KVLeaseFencedError (never the sentinel), proving the
    fence gate is a cheap pre-check ahead of the acquire path.
    """
    primary = _client(str(tmp_path), "primary")
    shadow = _client(str(tmp_path), "shadow")
    try:
        shadow.promote_fence_epoch()

        class _Boom:
            def __getattr__(self, name):
                raise AssertionError(f"lease ring touched on fenced acquire: {name}")

        primary._rust = _Boom()
        with pytest.raises(KVLeaseFencedError):
            primary.acquire(8)
    finally:
        primary.close()
        shadow.close()


def test_hot_path_fence_check_does_no_syscall(tmp_path, monkeypatch):
    """The steady-state acquire fence check reads the mmap, never a syscall."""
    c = _client(str(tmp_path), "primary")
    try:
        called = []
        real_pread = os.pread
        monkeypatch.setattr(
            os, "pread", lambda *a, **k: called.append("pread") or real_pread(*a, **k)
        )
        # read_fence_epoch is what the hot path calls; it must use the mmap only.
        assert c.read_fence_epoch() == 0
        assert called == []
    finally:
        c.close()


def test_dir_level_promote_fences_and_adopt_recovers(tmp_path):
    """The failover-path dir bump fences clients; adopt_fence_epoch un-fences self."""
    shm = os.path.join(str(tmp_path), "gms-kv-lease-0.shm")
    client = SharedMemoryKVLeaseClient(
        shm, namespace="ns", owner_id="c", total_blocks=16
    )
    try:
        assert client.fence_epoch == 0
        new = promote_kv_lease_epoch_in_shm_dir("ns", 0, shm_dir=str(tmp_path))
        assert new == 1
        # Client cached the old epoch -> now fenced.
        assert client.is_fenced_out()
        with pytest.raises(KVLeaseFencedError):
            client.acquire(1)
        # The promoting engine adopts the new epoch and can write again.
        assert client.adopt_fence_epoch() == 1
        assert not client.is_fenced_out()
        assert len(client.acquire(1)) == 1
    finally:
        client.close()


def test_local_promote_adopts_own_clients_without_self_fencing(tmp_path):
    """The promotion path bumps + adopts on this process's own live clients.

    Models the promoting process: its clients must keep writing after the bump
    (they adopt), while a stale client on the same ring (standing in for a
    zombie primary in another process) is fenced.
    """
    # Two "local" clients (this process) on distinct rings.
    a = _client(str(tmp_path / "a"), "a")
    b = _client(str(tmp_path / "b"), "b")
    # A stale peer on ring 'a' that will NOT be promoted (proxy for a zombie in
    # another process): re-open the same ring, then promote only via the registry.
    zombie = SharedMemoryKVLeaseClient(
        os.path.join(str(tmp_path / "a"), "ring.shm"),
        namespace="ns",
        owner_id="zombie",
        total_blocks=64,
    )
    try:
        highest = promote_local_kv_lease_epoch()
        assert highest >= 1
        # Promoting process's own clients adopted -> still writable.
        assert not a.is_fenced_out()
        assert not b.is_fenced_out()
        assert len(a.acquire(1)) == 1
        # The zombie was also a live client here, so in-process it also adopted;
        # to model a *different* process we force its cached epoch back and check
        # it fails closed against the bumped ring.
        zombie._fence_epoch = 0
        assert zombie.is_fenced_out()
        with pytest.raises(KVLeaseFencedError):
            zombie.acquire(1)
    finally:
        a.close()
        b.close()
        zombie.close()


def test_promote_is_a_single_epoch_store(tmp_path):
    """Switchover writes exactly the epoch word; total_blocks/magic untouched."""
    c = _client(str(tmp_path), "primary", total=32)
    try:
        before = bytes(c._mmap[:_KV_LEASE_SHM_EPOCH_OFFSET])
        c.promote_fence_epoch()
        after = bytes(c._mmap[:_KV_LEASE_SHM_EPOCH_OFFSET])
        # Everything ahead of the epoch word is unchanged (magic/version/blocks/free).
        assert before == after
        # And the epoch word advanced by exactly one.
        assert (
            _KV_LEASE_SHM_EPOCH_STRUCT.unpack_from(c._mmap, _KV_LEASE_SHM_EPOCH_OFFSET)[0]
            == 1
        )
    finally:
        c.close()
