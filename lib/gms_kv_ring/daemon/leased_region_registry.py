# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Leased remote-descriptor registry for host-tier RDMA regions (X6/X7, redesign 4).

The daemon used to build a host placement descriptor like this::

    with host_tier.pin(...) as slot:
        transport.register_buffer(slot.host_ptr, size)
        descriptor = {"remote_ptr": slot.host_ptr, ...}
    return descriptor            # <-- pin already released here!

so the raw pointer embedded in the descriptor outlived its pin: eviction / LRU /
`cudaFreeHost` could reclaim the buffer while a remote peer was still RDMA-reading
it (use-after-free), and the NIXL registration was never torn down (permanent
leak). This registry closes both:

* it *holds* the pin lease (keeps ``pins > 0``) for the descriptor's whole
  lifetime, so the buffer cannot be freed while a peer may still read it;
* every region carries an opaque ``(region_id, generation, daemon_epoch)`` triple
  so a consumer can detect a stale/wrong-incarnation region (composes with the
  epoch-fencing and verify-on-read work);
* regions are released explicitly (on transfer ack) or swept on TTL expiry, and
  release both drops the pin and deregisters the NIXL memory.

Performance: registration and the opportunistic TTL sweep run only on the
descriptor-build / ack paths, never on the remote read hot path, and the sweep is
O(expired). The embedded pointer is retained in the descriptor so the read path
needs no extra round trip; it is simply now guaranteed valid for the TTL.
"""

from __future__ import annotations

import itertools
import logging
import threading
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class _LeasedRegion:
    region_id: int
    lease: object  # a host-tier _SlotLease (or any object with .release())
    ptr: int
    size: int
    generation: int
    daemon_epoch: int
    expiry_monotonic: float


class LeasedRegionRegistry:
    """Holds host-tier pins + NIXL registrations for outstanding descriptors."""

    def __init__(
        self,
        *,
        register: Optional[Callable[[int, int], None]] = None,
        deregister: Optional[Callable[[int, int], None]] = None,
        ttl_s: float = 60.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        # Registration is owned by the registry when callbacks are supplied. This
        # makes its refcount transition atomic with register/deregister and avoids
        # a duplicate publish racing the final teardown of the same pointer.
        self._register = register
        self._deregister = deregister
        self._ttl_s = float(ttl_s)
        self._clock = clock or __import__("time").monotonic
        self._lock = threading.Lock()
        self._regions: dict[int, _LeasedRegion] = {}
        self._ids = itertools.count(1)
        # Refcount NIXL registrations by (ptr, size). Two regions can advertise the
        # same host buffer (a duplicate publish); tearing one down must NOT
        # deregister the memory the other still needs (a UAF-class bug). Only the
        # last region for a given (ptr, size) triggers the actual deregister.
        self._regcount: dict[tuple[int, int], int] = {}

    def register(
        self,
        lease: object,
        ptr: int,
        size: int,
        *,
        generation: int,
        daemon_epoch: int,
        ttl_s: float | None = None,
    ) -> int:
        """Take ownership of a pin + registration; return an opaque region_id.

        The caller must NOT release the lease itself — the registry now owns it
        and releases it on release()/expiry.
        """
        region_id = next(self._ids)
        expiry = self._clock() + (self._ttl_s if ttl_s is None else float(ttl_s))
        key = (int(ptr), int(size))
        with self._lock:
            if self._regcount.get(key, 0) == 0 and self._register is not None:
                # Keep this callback under the registry lock. Final deregistration
                # uses the same lock, so a new region cannot observe a registration
                # just before the previous incarnation tears it down.
                self._register(*key)
            self._regions[region_id] = _LeasedRegion(
                region_id=region_id,
                lease=lease,
                ptr=key[0],
                size=key[1],
                generation=int(generation),
                daemon_epoch=int(daemon_epoch),
                expiry_monotonic=expiry,
            )
            self._regcount[key] = self._regcount.get(key, 0) + 1
        return region_id

    def release(self, region_id: int) -> bool:
        """Release one region (on transfer ack): drop the pin + deregister."""
        with self._lock:
            region = self._regions.pop(int(region_id), None)
        if region is None:
            return False
        return self._teardown(region)

    def sweep(self) -> int:
        """Release every region past its TTL. Returns the count released."""
        now = self._clock()
        expired: list[_LeasedRegion] = []
        with self._lock:
            for region_id in [
                rid for rid, r in self._regions.items() if r.expiry_monotonic <= now
            ]:
                expired.append(self._regions.pop(region_id))
        released = sum(self._teardown(region) for region in expired)
        if released:
            logger.debug(
                "[LeasedRegionRegistry] swept %d expired host regions", released
            )
        return released

    def close(self) -> int:
        """Release all regions (daemon shutdown)."""
        with self._lock:
            regions = list(self._regions.values())
            self._regions.clear()
        return sum(self._teardown(region) for region in regions)

    def __len__(self) -> int:
        with self._lock:
            return len(self._regions)

    def _teardown(self, region: _LeasedRegion) -> bool:
        # Deregister first (stop remote access), then drop the pin (allow free).
        # The register and deregister callbacks run under the same lock as the
        # refcount transition, closing the last-release/new-publish race.
        key = (region.ptr, region.size)
        with self._lock:
            remaining = self._regcount.get(key, 1) - 1
            if remaining <= 0:
                if self._deregister is not None:
                    try:
                        self._deregister(*key)
                    except Exception:  # noqa: BLE001
                        # Fail safe: keep both the region and its pin alive. A
                        # later sweep/release can retry deregistration; freeing a
                        # still-registered pointer would permit remote UAF.
                        self._regions[region.region_id] = region
                        self._regcount[key] = 1
                        logger.warning(
                            "[LeasedRegionRegistry] deregister failed for "
                            "ptr=%#x size=%d; retaining pin",
                            region.ptr,
                            region.size,
                            exc_info=True,
                        )
                        return False
                self._regcount.pop(key, None)
            else:
                self._regcount[key] = remaining

        release = (
            region.lease
            if callable(region.lease)
            else getattr(region.lease, "release", None)
        )
        if callable(release):
            try:
                release()
            except Exception:  # noqa: BLE001
                logger.debug(
                    "[LeasedRegionRegistry] pin release failed for region %d",
                    region.region_id,
                    exc_info=True,
                )
        return True
