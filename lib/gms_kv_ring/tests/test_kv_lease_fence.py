# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Failover-fence tests for the shared-memory KV lease ring (X1, cohort model).

The fence is `(epoch u32 @56, cohort_hash u32 @60)` in the lease-ring header —
the genuinely-free tail bytes, NOT the old byte-24 alias onto the Rust ring's
live `L_ACTIVE_MUTATIONS` counter. A promoting cohort stamps `(epoch+1, its
cohort_hash)`; clients of that same cohort self-adopt on next acquire (covers
the promoted engine's own scheduler subprocesses), a foreign cohort fails
closed. These tests assert:

- safety: a foreign-cohort zombie primary cannot acquire after a bump;
- self-adoption: a same-cohort client keeps writing (no wrong-process self-fence);
- the byte-24 regression: mutations and foreign-lease reclaim never touch the fence;
- layout: the Python fence offset lands in unclaimed header space (no drift);
- perf: the hot-path fence check is one aligned mmap read, no syscall;
- switchover writes exactly the 8-byte fence word.
"""

import os
import re
import struct
import threading
from pathlib import Path

import pytest

pytest.importorskip("gms_rust_ring")

from gpu_memory_service.integrations.common.kv_lease_client import (  # noqa: E402
    _KV_LEASE_SHM_FENCE_OFFSET,
    _KV_LEASE_SHM_FENCE_STRUCT,
    _KV_LEASE_SHM_HEADER_SIZE,
    KVLeaseFencedError,
    SharedMemoryKVLeaseClient,
    _cohort_hash,
    _owner_id_from_env,
    promote_kv_lease_epoch_in_shm_dir,
    reclaim_foreign_kv_leases_in_shm_dir,
)

_L_ACTIVE_MUTATIONS = 24  # rust_ring/src/lib.rs L_ACTIVE_MUTATIONS (the old alias trap)


def _ring_path(dirpath, idx=0):
    # Name matches the dir-level glob gms-kv-lease-*.shm used by promote/reclaim.
    return os.path.join(str(dirpath), f"gms-kv-lease-{idx}.shm")


def _client(dirpath, owner, *, cohort=None, total=64, idx=0):
    c = SharedMemoryKVLeaseClient(
        _ring_path(dirpath, idx),
        namespace="ns",
        owner_id=owner,
        total_blocks=total,
    )
    if cohort is not None:
        # Model a process whose inherited cohort env differs (self-identify).
        c._cohort_hash = _cohort_hash(cohort)
    return c


# --------------------------------------------------------------------------
# Layout guard — the whole byte-24-drift bug class in one assertion.
# --------------------------------------------------------------------------


def test_fence_offset_lands_in_unclaimed_header_space():
    """Parse the Rust L_* constants; assert the fence word doesn't alias a field."""
    src = (
        Path(__file__).resolve().parents[2]
        / "gpu_memory_service"
        / "rust_ring"
        / "src"
        / "lib.rs"
    ).read_text()

    def const(name):
        m = re.search(rf"const {name}:\s*usize\s*=\s*(\d+)", src)
        assert m, f"{name} not found in rust_ring/src/lib.rs"
        return int(m.group(1))

    header_size = const("LEASE_HEADER_SIZE")
    # The last field the Rust ring claims: reserved_owner_hash (u64) ends at +8.
    last_field_end = const("L_RESERVED_OWNER_HASH") + 8
    fence_end = _KV_LEASE_SHM_FENCE_OFFSET + _KV_LEASE_SHM_FENCE_STRUCT.size

    assert (
        _KV_LEASE_SHM_FENCE_OFFSET >= last_field_end
    ), "fence overlaps a live Rust header field — the byte-24 drift bug"
    assert _KV_LEASE_SHM_FENCE_OFFSET != _L_ACTIVE_MUTATIONS
    assert fence_end <= header_size == _KV_LEASE_SHM_HEADER_SIZE


# --------------------------------------------------------------------------
# Core fence behaviour.
# --------------------------------------------------------------------------


def test_fresh_ring_starts_unfenced_and_acquires(tmp_path):
    c = _client(tmp_path, "primary", cohort="cohort-a")
    try:
        assert c.fence_epoch == 0
        assert c.read_fence() == (0, 0)
        assert len(c.acquire(4)) == 4
        assert not c.is_fenced_out()
    finally:
        c.close()


def test_foreign_cohort_bump_fences_old_incarnation(tmp_path):
    primary = _client(tmp_path, "primary", cohort="cohort-old")
    try:
        assert len(primary.acquire(2)) == 2  # primary writing normally

        # The shadow (a DIFFERENT cohort) promotes via the dir-level bump.
        new_epoch = promote_kv_lease_epoch_in_shm_dir(
            "ns", 0, cohort_id="cohort-new", shm_dir=str(tmp_path)
        )
        assert new_epoch == 1
        epoch, cohort = primary.read_fence()
        assert epoch == 1 and cohort == _cohort_hash("cohort-new")

        # Fenced-out primary (foreign cohort) fails closed on its next acquire.
        assert primary.is_fenced_out()
        with pytest.raises(KVLeaseFencedError):
            primary.acquire(2)
    finally:
        primary.close()


def test_same_cohort_client_self_adopts_without_signalling(tmp_path):
    """A promoted engine's own subprocess keeps writing (fixes A1 self-fence).

    The client caches epoch 0 at attach; the handler process bumps to epoch 1
    stamped with the SAME cohort. The client — a different pid, cohort inherited
    via env — must adopt on its next acquire with zero cross-process signalling.
    """
    client = _client(tmp_path, "engine-core", cohort="cohort-mine")
    try:
        assert client.fence_epoch == 0
        promote_kv_lease_epoch_in_shm_dir(
            "ns", 0, cohort_id="cohort-mine", shm_dir=str(tmp_path)
        )
        # Higher ring epoch, but OUR cohort → adopt, not fence.
        assert not client.is_fenced_out()
        assert len(client.acquire(1)) == 1  # self-adopts inside acquire
        assert client.fence_epoch == 1  # cache refreshed
    finally:
        client.close()


def test_cross_cohort_fenced_but_same_cohort_survives_same_bump(tmp_path):
    """One bump: our-cohort client survives, foreign-cohort zombie is fenced."""
    mine = _client(tmp_path, "mine", cohort="cohort-new")
    zombie = _client(tmp_path, "zombie", cohort="cohort-old")
    try:
        promote_kv_lease_epoch_in_shm_dir(
            "ns", 0, cohort_id="cohort-new", shm_dir=str(tmp_path)
        )
        assert not mine.is_fenced_out()
        assert len(mine.acquire(1)) == 1
        assert zombie.is_fenced_out()
        with pytest.raises(KVLeaseFencedError):
            zombie.acquire(1)
    finally:
        mine.close()
        zombie.close()


# --------------------------------------------------------------------------
# Byte-24 regression: the fence must be immune to mutations AND to reclaim.
# --------------------------------------------------------------------------


def test_inflight_mutation_counter_does_not_spuriously_fence(tmp_path):
    """Writing L_ACTIVE_MUTATIONS (byte 24) must not look like a fence bump."""
    a = _client(tmp_path, "a", cohort="cohort-a")
    b = _client(tmp_path, "b", cohort="cohort-a")
    try:
        # Simulate a concurrent in-flight mutation bumping byte 24 sky-high.
        struct.pack_into("<Q", a._mmap, _L_ACTIVE_MUTATIONS, 999999)
        assert a.read_fence() == (0, 0)  # fence word untouched
        assert not b.is_fenced_out()
        assert len(b.acquire(2)) == 2  # no spurious KVLeaseFencedError
    finally:
        a.close()
        b.close()


def test_fence_survives_foreign_lease_reclaim(tmp_path):
    """bump-then-reclaim must NOT un-fence the zombie (the old byte-24 bug)."""
    # Zombie primary holds some leases under its own owner id.
    zombie = _client(tmp_path, "zombie-owner", cohort="cohort-old")
    zombie.acquire(4)
    epoch = promote_kv_lease_epoch_in_shm_dir(
        "ns", 0, cohort_id="cohort-new", shm_dir=str(tmp_path)
    )
    assert epoch == 1

    # The promoted shadow reclaims the zombie's foreign leases post-fence.
    reclaim_foreign_kv_leases_in_shm_dir(
        "ns", 0, owner_id="shadow-owner", shm_dir=str(tmp_path)
    )

    # Fence word must still read (epoch=1, cohort=new) — reclaim's recovery guard
    # resets byte 24, not the fence, so the zombie (cached epoch 0 from before the
    # bump) stays fenced. Use the zombie itself: a client opened AFTER the bump
    # would legitimately adopt epoch 1.
    try:
        e, c = zombie.read_fence()
        assert e == 1 and c == _cohort_hash("cohort-new")
        assert zombie.is_fenced_out()
        with pytest.raises(KVLeaseFencedError):
            zombie.acquire(1)
    finally:
        zombie.close()


# --------------------------------------------------------------------------
# Concurrency + perf + owner-id.
# --------------------------------------------------------------------------


def test_concurrent_dir_bumps_do_not_lose_an_update(tmp_path):
    """The per-ring flock RMW makes concurrent promotions serialize (fixes A3)."""
    _client(tmp_path, "seed", cohort="cohort-a").close()  # create the ring file
    n = 12
    barrier = threading.Barrier(n)

    def bump():
        barrier.wait()
        promote_kv_lease_epoch_in_shm_dir(
            "ns", 0, cohort_id="cohort-a", shm_dir=str(tmp_path)
        )

    threads = [threading.Thread(target=bump) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    checker = _client(tmp_path, "check", cohort="cohort-a")
    try:
        assert checker.read_fence()[0] == n  # no lost increments
    finally:
        checker.close()


def test_hot_path_fence_check_does_no_syscall(tmp_path, monkeypatch):
    """The steady-state acquire fence check reads the mmap, never a syscall."""
    c = _client(tmp_path, "primary", cohort="cohort-a")
    try:
        called = []
        real_pread = os.pread
        monkeypatch.setattr(
            os, "pread", lambda *a, **k: called.append("pread") or real_pread(*a, **k)
        )
        assert c.read_fence() == (0, 0)
        assert called == []
    finally:
        c.close()


def test_fence_check_short_circuits_before_touching_the_lease_ring(tmp_path):
    """A fenced acquire must raise before any Rust/lease work (perf + safety)."""
    primary = _client(tmp_path, "primary", cohort="cohort-old")
    try:
        promote_kv_lease_epoch_in_shm_dir(
            "ns", 0, cohort_id="cohort-new", shm_dir=str(tmp_path)
        )

        class _Boom:
            def __getattr__(self, name):
                raise AssertionError(f"lease ring touched on fenced acquire: {name}")

        primary._rust = _Boom()
        with pytest.raises(KVLeaseFencedError):
            primary.acquire(8)
    finally:
        primary.close()


def test_promote_writes_only_the_fence_word(tmp_path):
    """Switchover writes exactly the 8-byte fence; the rest of the header is untouched."""
    c = _client(tmp_path, "primary", cohort="cohort-a", total=32)
    try:
        # Snapshot the SAME client's mmap (MAP_SHARED) so the only writer between
        # snapshots is the promotion — a second client would perturb the header
        # via its own reservation sync at attach.
        before = bytes(c._mmap[:_KV_LEASE_SHM_FENCE_OFFSET])
        promote_kv_lease_epoch_in_shm_dir(
            "ns", 0, cohort_id="cohort-a", shm_dir=str(tmp_path)
        )
        after = bytes(c._mmap[:_KV_LEASE_SHM_FENCE_OFFSET])
        assert before == after  # magic/version/blocks/free/reserved unchanged
        epoch, cohort = c.read_fence()
        assert epoch == 1 and cohort == _cohort_hash("cohort-a")
    finally:
        c.close()


def test_default_owner_id_is_cohort_scoped_not_pid_scoped(tmp_path, monkeypatch):
    """F7: default owner-id keys on the cohort so the handler doesn't self-reclaim."""
    monkeypatch.delenv("GMS_KV_LEASE_OWNER_ID", raising=False)
    monkeypatch.delenv("GMS_SGLANG_KV_LEASE_OWNER_ID", raising=False)
    monkeypatch.delenv("GMS_COHORT_ID", raising=False)
    monkeypatch.setenv("ENGINE_ID", "engine-7")

    owner = _owner_id_from_env("sglang", 3)
    assert "engine-7" in owner  # cohort-scoped
    assert str(os.getpid()) not in owner  # NOT pid-scoped

    # No cohort at all → falls back to pid so distinct processes stay distinct.
    monkeypatch.delenv("ENGINE_ID", raising=False)
    fallback = _owner_id_from_env("sglang", 3)
    assert str(os.getpid()) in fallback
