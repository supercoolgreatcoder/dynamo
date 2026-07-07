# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Verify-on-read KV inheritance (redesign 3).

The train tried to promise crash-instant KV coherence between a primary and its
shadow. That is a correctness cliff: the exact block being written at the crash
instant can be torn, and no amount of fencing makes an already-torn block good.

Instead, treat inherited KV as an *untrusted cache*:

* at *seal* time (a block becomes immutable) record its content hash — this is
  off the hot path and happens once per block;
* the *unsealed tail* (blocks that were mid-write at the crash) is dropped
  unconditionally — never inherited;
* on the *first* post-failover prefix-match of a sealed block, verify the live
  bytes against the recorded hash; on mismatch drop the block so the engine
  recomputes it. After a block verifies once it is trusted and never re-hashed.

This converts a silent-corruption cliff into bounded recompute on exactly the
torn blocks. Performance: hashing is at seal (once) plus one verification per
inherited block on its first post-failover read; steady-state reads are an O(1)
membership check and add nothing. Composes with the failover epoch (redesign 1)
and the (generation, daemon_epoch) descriptor stamping (redesign 4).

This is the transport/engine-agnostic bookkeeping; the caller supplies the live
content hash (e.g. via the daemon's existing crc32_at_ptr) and performs the
actual drop/recompute using the verdict returned here.
"""

from __future__ import annotations

import logging
import threading
from enum import Enum

logger = logging.getLogger(__name__)


class ReadVerdict(Enum):
    TRUSTED = "trusted"  # already verified this incarnation (or never inherited) — use as-is
    VERIFIED = "verified"  # just verified against the sealed hash — use, now trusted
    DROP = "drop"  # hash mismatch (torn/stale) or unsealed — drop + recompute


class SealedBlockVerifier:
    """Tracks sealed-block hashes and one-shot post-failover verification."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # block_id -> content hash recorded at seal time.
        self._sealed: dict[int, bytes] = {}
        # block_ids that must be re-verified once before they are trusted again
        # (populated on failover for every currently-sealed block).
        self._needs_verify: set[int] = set()

    def record_seal(self, block_id: int, content_hash: bytes) -> None:
        """Record a block's hash when it becomes immutable (sealed)."""
        with self._lock:
            self._sealed[int(block_id)] = bytes(content_hash)

    def forget(self, block_id: int) -> None:
        """Drop all state for a block (e.g. it was evicted/freed)."""
        bid = int(block_id)
        with self._lock:
            self._sealed.pop(bid, None)
            self._needs_verify.discard(bid)

    def is_sealed(self, block_id: int) -> bool:
        with self._lock:
            return int(block_id) in self._sealed

    def on_failover(self) -> int:
        """Mark every currently-sealed block as needing one re-verification.

        Call when this engine inherits KV from a crashed peer. Returns the count
        of blocks that will be verified on their next read.
        """
        with self._lock:
            self._needs_verify = set(self._sealed)
            return len(self._needs_verify)

    def drop_unsealed_tail(self, candidate_block_ids: list[int]) -> list[int]:
        """Return the subset of candidates that are NOT sealed (drop these).

        The unsealed tail was mid-write at the crash and is never inheritable.
        """
        with self._lock:
            return [int(b) for b in candidate_block_ids if int(b) not in self._sealed]

    def verify_on_read(self, block_id: int, actual_hash: bytes | None = None) -> ReadVerdict:
        """Decide whether an inherited block may be read as-is.

        * unsealed -> DROP (never inherit a mid-write block);
        * sealed and not pending re-verification -> TRUSTED (O(1), no hashing);
        * sealed and pending -> compare actual_hash to the sealed hash:
          match -> VERIFIED (and now trusted), mismatch/absent -> DROP.
        """
        bid = int(block_id)
        with self._lock:
            sealed_hash = self._sealed.get(bid)
            if sealed_hash is None:
                return ReadVerdict.DROP
            if bid not in self._needs_verify:
                return ReadVerdict.TRUSTED
            # Pending verification: require a matching live hash exactly once.
            if actual_hash is not None and bytes(actual_hash) == sealed_hash:
                self._needs_verify.discard(bid)
                return ReadVerdict.VERIFIED
            # Torn or unverifiable -> drop it and stop trusting the recorded hash.
            self._needs_verify.discard(bid)
            self._sealed.pop(bid, None)
            logger.warning(
                "[GMS verify-on-read] dropping inherited block %d "
                "(hash mismatch or unverifiable); engine will recompute",
                bid,
            )
            return ReadVerdict.DROP
