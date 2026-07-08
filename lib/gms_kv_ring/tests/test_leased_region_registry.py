# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Leased remote-descriptor registry (X6/X7, redesign 4)."""

from types import SimpleNamespace

import pytest
from gms_kv_ring.daemon.leased_region_registry import LeasedRegionRegistry
from gms_kv_ring.daemon.server import Daemon


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


def test_duplicate_ptr_size_deregisters_only_on_last_release():
    """H1 refcount: two regions on the same (ptr,size) must not cross-deregister.

    A duplicate publish registers the same host buffer twice. Tearing the first
    region down must drop its own pin but NOT deregister the NIXL memory the
    second region still advertises — else the survivor points at freed memory.
    """
    clock = [0.0]
    dereg = []
    reg = _registry(clock, dereg)
    first, second = _FakeLease(), _FakeLease()

    r1 = reg.register(first, 0xABC, 512, generation=1, daemon_epoch=1)
    r2 = reg.register(second, 0xABC, 512, generation=2, daemon_epoch=1)

    # First release: pin dropped, but the shared buffer stays registered.
    assert reg.release(r1) is True
    assert first.released == 1
    assert dereg == []  # NOT deregistered — second region still uses it

    # Last release: now the buffer is actually deregistered.
    assert reg.release(r2) is True
    assert second.released == 1
    assert dereg == [(0xABC, 512)]


def test_refcount_survives_ttl_sweep_of_one_duplicate():
    """A swept duplicate must not deregister a buffer a live region still holds."""
    clock = [0.0]
    dereg = []
    reg = _registry(clock, dereg)
    a, b = _FakeLease(), _FakeLease()

    reg.register(a, 0x5, 64, generation=1, daemon_epoch=1, ttl_s=5.0)
    reg.register(b, 0x5, 64, generation=1, daemon_epoch=1, ttl_s=50.0)

    clock[0] = 6.0  # only 'a' expired
    assert reg.sweep() == 1
    assert a.released == 1
    assert dereg == []  # 'b' still advertises (0x5, 64)

    clock[0] = 51.0
    assert reg.sweep() == 1
    assert b.released == 1
    assert dereg == [(0x5, 64)]


def test_close_releases_all():
    clock = [0.0]
    dereg = []
    reg = _registry(clock, dereg)
    leases = [_FakeLease() for _ in range(3)]
    for i, lease in enumerate(leases):
        reg.register(lease, i, 8, generation=0, daemon_epoch=0)
    assert reg.close() == 3
    assert all(lease.released == 1 for lease in leases)
    assert len(reg) == 0


def test_registration_is_refcounted_and_serialized_with_teardown():
    events = []
    reg = LeasedRegionRegistry(
        register=lambda ptr, size: events.append(("register", ptr, size)),
        deregister=lambda ptr, size: events.append(("deregister", ptr, size)),
    )
    first, second = _FakeLease(), _FakeLease()
    r1 = reg.register(first, 0xD00, 128, generation=1, daemon_epoch=1)
    r2 = reg.register(second, 0xD00, 128, generation=1, daemon_epoch=1)
    assert events == [("register", 0xD00, 128)]
    assert reg.release(r1) is True
    assert events == [("register", 0xD00, 128)]
    assert reg.release(r2) is True
    assert events[-1] == ("deregister", 0xD00, 128)
    assert reg._regcount == {}


def test_registration_failure_does_not_take_lease_ownership():
    lease = _FakeLease()

    def fail_register(_ptr, _size):
        raise RuntimeError("registration failed")

    reg = LeasedRegionRegistry(register=fail_register)
    with pytest.raises(RuntimeError, match="registration failed"):
        reg.register(lease, 0xBAD, 16, generation=1, daemon_epoch=1)
    assert lease.released == 0
    assert len(reg) == 0
    assert reg._regcount == {}


def test_deregister_failure_retains_pin_and_retries_safely():
    fail = [True]
    attempts = []

    def deregister(ptr, size):
        attempts.append((ptr, size))
        if fail[0]:
            raise RuntimeError("busy")

    reg = LeasedRegionRegistry(deregister=deregister)
    lease = _FakeLease()
    rid = reg.register(lease, 0xFEED, 32, generation=1, daemon_epoch=1)
    assert reg.release(rid) is False
    assert lease.released == 0
    assert len(reg) == 1

    fail[0] = False
    assert reg.release(rid) is True
    assert lease.released == 1
    assert attempts == [(0xFEED, 32), (0xFEED, 32)]


def test_staging_placement_holds_consume_pin_until_region_release():
    events = []

    class FakeAgent:
        def get_agent_metadata(self):
            return b"metadata"

    class FakeTransport:
        _agent = FakeAgent()

        def agent_name(self):
            return "agent"

        def listen_port(self):
            return 1234

        def register_buffer(self, ptr, size):
            events.append(("register", ptr, size))

        def deregister_buffer(self, ptr, size):
            events.append(("deregister", ptr, size))

    class FakeStaging:
        def __init__(self):
            self.pins = 0

        def begin_consume(self, content_hash, generation):
            assert content_hash == b"h"
            assert generation == 7
            self.pins += 1
            return object()

        def consume_pointer(self, _handle):
            return (0xCAFE, 64, 0)

        def end_consume(self, _handle):
            self.pins -= 1

    daemon = object.__new__(Daemon)
    daemon.transport = FakeTransport()
    daemon.staging_tier = FakeStaging()
    daemon.epoch = 9
    hit = SimpleNamespace(content_hash=b"h", generation=7)

    metadata = daemon._staging_hit_placement_metadata(hit)
    region = metadata["gms_descriptor"]["ranges"][0]
    assert daemon.staging_tier.pins == 1
    assert events == [("register", 0xCAFE, 64)]
    assert region["daemon_epoch"] == 9

    assert daemon._leased_region_registry.release(region["region_id"]) is True
    assert daemon.staging_tier.pins == 0
    assert events[-1] == ("deregister", 0xCAFE, 64)
