# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Leased remote-descriptor registry (X6/X7, redesign 4)."""

from gms_kv_ring.daemon.leased_region_registry import LeasedRegionRegistry


class _FakeLease:
    """Stand-in for a host-tier _SlotLease: tracks release (pin drop)."""

    def __init__(self):
        self.released = 0

    def release(self):
        self.released += 1


def _registry(clock_box, dereg_log):
    return LeasedRegionRegistry(
        deregister=lambda ptr, size: dereg_log.append((ptr, size)),
        ttl_s=10.0,
        clock=lambda: clock_box[0],
    )


def test_register_holds_pin_until_release():
    clock = [0.0]
    dereg = []
    reg = _registry(clock, dereg)
    lease = _FakeLease()

    rid = reg.register(lease, 0x1000, 4096, generation=7, daemon_epoch=42)
    assert len(reg) == 1
    # Pin is HELD — not released just because the descriptor was built.
    assert lease.released == 0
    assert dereg == []

    assert reg.release(rid) is True
    # Release drops the pin AND deregisters the NIXL region.
    assert lease.released == 1
    assert dereg == [(0x1000, 4096)]
    assert len(reg) == 0
    # Double release is a no-op.
    assert reg.release(rid) is False


def test_ttl_sweep_releases_expired_only():
    clock = [0.0]
    dereg = []
    reg = _registry(clock, dereg)
    a, b = _FakeLease(), _FakeLease()

    reg.register(a, 0x1, 16, generation=1, daemon_epoch=1, ttl_s=5.0)
    reg.register(b, 0x2, 16, generation=1, daemon_epoch=1, ttl_s=20.0)

    clock[0] = 6.0  # a expired, b not
    assert reg.sweep() == 1
    assert a.released == 1 and b.released == 0
    assert len(reg) == 1

    clock[0] = 21.0
    assert reg.sweep() == 1
    assert b.released == 1
    assert len(reg) == 0


def test_deregister_ordered_before_pin_drop():
    """Region must stop being remotely readable before the buffer can be freed."""
    clock = [0.0]
    order = []
    reg = LeasedRegionRegistry(
        deregister=lambda ptr, size: order.append("dereg"),
        ttl_s=1.0,
        clock=lambda: clock[0],
    )

    class _OrderLease:
        def release(self):
            order.append("unpin")

    rid = reg.register(_OrderLease(), 0x10, 8, generation=0, daemon_epoch=0)
    reg.release(rid)
    assert order == ["dereg", "unpin"]


def test_close_releases_all():
    clock = [0.0]
    dereg = []
    reg = _registry(clock, dereg)
    leases = [_FakeLease() for _ in range(3)]
    for i, lease in enumerate(leases):
        reg.register(lease, i, 8, generation=0, daemon_epoch=0)
    assert reg.close() == 3
    assert all(l.released == 1 for l in leases)
    assert len(reg) == 0
