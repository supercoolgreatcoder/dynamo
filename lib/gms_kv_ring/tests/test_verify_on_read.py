# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify-on-read KV inheritance (redesign 3)."""

from gpu_memory_service.integrations.common.verify_on_read import (
    ReadVerdict,
    SealedBlockVerifier,
)

H1 = b"\x01" * 32
H2 = b"\x02" * 32


def test_unsealed_block_is_always_dropped():
    v = SealedBlockVerifier()
    assert v.verify_on_read(5) == ReadVerdict.DROP  # never sealed


def test_sealed_block_trusted_without_failover():
    v = SealedBlockVerifier()
    v.record_seal(5, H1)
    # No failover -> no re-verification, no hashing on the read path.
    assert v.verify_on_read(5) == ReadVerdict.TRUSTED


def test_failover_forces_one_verification_then_trusts():
    v = SealedBlockVerifier()
    v.record_seal(5, H1)
    assert v.on_failover() == 1

    # First post-failover read must present a matching live hash.
    assert v.verify_on_read(5, actual_hash=H1) == ReadVerdict.VERIFIED
    # Subsequent reads are trusted (verified once) — no re-hash required.
    assert v.verify_on_read(5) == ReadVerdict.TRUSTED


def test_failover_mismatch_drops_and_forgets():
    v = SealedBlockVerifier()
    v.record_seal(5, H1)
    v.on_failover()
    # Torn block: live hash differs from the sealed hash -> drop + recompute.
    assert v.verify_on_read(5, actual_hash=H2) == ReadVerdict.DROP
    # It is forgotten, so a later read is also a drop (engine recomputed it).
    assert v.verify_on_read(5, actual_hash=H1) == ReadVerdict.DROP


def test_failover_without_hash_drops():
    v = SealedBlockVerifier()
    v.record_seal(5, H1)
    v.on_failover()
    # Unverifiable (no live hash available) -> conservative drop.
    assert v.verify_on_read(5, actual_hash=None) == ReadVerdict.DROP


def test_drop_unsealed_tail():
    v = SealedBlockVerifier()
    v.record_seal(1, H1)
    v.record_seal(2, H1)
    # 3 and 4 were mid-write (never sealed) -> the tail to drop.
    assert v.drop_unsealed_tail([1, 2, 3, 4]) == [3, 4]


def test_only_sealed_blocks_reverify_on_failover():
    v = SealedBlockVerifier()
    v.record_seal(1, H1)
    # Block 2 sealed after failover snapshot should not be pending.
    assert v.on_failover() == 1
    v.record_seal(2, H2)
    assert v.verify_on_read(2) == ReadVerdict.TRUSTED  # sealed post-failover, trusted
    assert v.verify_on_read(1, actual_hash=H1) == ReadVerdict.VERIFIED
