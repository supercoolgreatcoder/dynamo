# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import threading
import time
import uuid

import pytest

from dynamo.common import rank_liveness as rl

pytestmark = [
    pytest.mark.pre_merge,
    pytest.mark.unit,
    pytest.mark.none,
    pytest.mark.gpu_0,
]


def _endpoint() -> str:
    return f"inproc://gms-rank-liveness-{uuid.uuid4().hex}"


def _wait(event: threading.Event, timeout: float = 1.0) -> None:
    assert event.wait(timeout), "liveness callback did not fire"


def test_endpoint_overrides_support_replica_scoping(monkeypatch):
    monkeypatch.setenv(
        "DYN_GMS_RANK_LIVENESS_BIND_ADDR",
        "tcp://127.0.0.1:31001",
    )
    monkeypatch.setenv(
        "DYN_GMS_RANK_LIVENESS_CONNECT_ADDR",
        "tcp://{leader_host}:31001",
    )

    assert rl.leader_bind_addr() == "tcp://127.0.0.1:31001"
    assert rl.leader_connect_addr("replica-a") == "tcp://replica-a:31001"


def test_fire_is_exactly_once_even_if_callback_raises():
    calls: list[tuple[int, str]] = []

    def callback(rank: int, reason: str) -> None:
        calls.append((rank, reason))
        raise RuntimeError("expected test error")

    monitor = rl.RankLivenessMonitor(callback)

    assert monitor._fire(1, "first") is True
    assert monitor._fire(2, "second") is False
    assert calls == [(1, "first")]


def test_no_heartbeats_ever_disables_without_firing():
    # H-C: an expected rank that never registers, with ZERO heartbeats from any
    # rank, is treated as a misconfigured/blocked channel — not a dead rank — so
    # the monitor disables itself rather than suiciding a healthy primary.
    endpoint = _endpoint()
    fired = threading.Event()
    monitor = rl.RankLivenessMonitor(
        lambda _rank, _reason: fired.set(),
        bind_addr=endpoint,
        timeout_ms_override=100,
        expected_ranks={1},
        startup_grace_ms_override=40,
    )

    monitor.start()
    try:
        time.sleep(0.2)  # well past the 40ms startup grace
        assert not fired.is_set()
    finally:
        monitor.stop()


def test_startup_timeout_fires_for_absent_rank_after_channel_proven():
    # H-C: once at least one expected rank has registered (channel proven), a
    # different expected rank that never registers is genuinely absent -> fire.
    endpoint = _endpoint()
    fired = threading.Event()
    calls: list[tuple[int, str]] = []
    monitor = rl.RankLivenessMonitor(
        lambda rank, reason: (calls.append((rank, reason)), fired.set()),
        bind_addr=endpoint,
        timeout_ms_override=100,
        expected_ranks={1, 2},
        startup_grace_ms_override=150,
    )
    client = rl.RankLivenessClient(
        "unused",
        1,
        interval_ms=20,
        connect_addr=endpoint,
    )

    monitor.start()
    time.sleep(0.02)
    client.start()  # rank 1 registers; rank 2 never does
    try:
        _wait(fired, timeout=2.0)
        assert calls == [(2, "startup-timeout")]
    finally:
        client.stop()
        monitor.stop()


def test_legacy_monitor_does_not_require_unseen_ranks():
    fired = threading.Event()
    monitor = rl.RankLivenessMonitor(
        lambda _rank, _reason: fired.set(),
        bind_addr=_endpoint(),
        timeout_ms_override=40,
    )

    monitor.start()
    try:
        time.sleep(0.12)
        assert not fired.is_set()
    finally:
        monitor.stop()


def test_registered_rank_silence_fires_liveness_timeout():
    endpoint = _endpoint()
    fired = threading.Event()
    calls: list[tuple[int, str]] = []
    monitor = rl.RankLivenessMonitor(
        lambda rank, reason: (calls.append((rank, reason)), fired.set()),
        bind_addr=endpoint,
        timeout_ms_override=80,
        expected_ranks={1},
        startup_grace_ms_override=500,
    )
    client = rl.RankLivenessClient(
        "unused",
        1,
        interval_ms=20,
        connect_addr=endpoint,
    )

    monitor.start()
    time.sleep(0.03)
    client.start()
    try:
        time.sleep(0.12)
        client.stop()
        _wait(fired)
        assert calls == [(1, "liveness-timeout")]
    finally:
        client.stop()
        monitor.stop()


def test_unexpected_multipart_identity_does_not_fire():
    # An unexpected rank identity is ignored, so it never enters last_seen; with
    # no heartbeat from any *expected* rank the channel is unproven, and H-C
    # means the monitor disables without firing (rather than suiciding).
    import zmq

    endpoint = _endpoint()
    fired = threading.Event()
    monitor = rl.RankLivenessMonitor(
        lambda _rank, _reason: fired.set(),
        bind_addr=endpoint,
        timeout_ms_override=100,
        expected_ranks={1},
        startup_grace_ms_override=80,
    )
    socket = zmq.Context.instance().socket(zmq.DEALER)
    socket.setsockopt(zmq.IDENTITY, b"rank-9")
    socket.setsockopt(zmq.LINGER, 0)

    monitor.start()
    time.sleep(0.03)
    socket.connect(endpoint)
    try:
        socket.send(b"hb")
        time.sleep(0.25)  # past the 80ms startup grace
        assert not fired.is_set()
    finally:
        socket.close(0)
        monitor.stop()


def _free_tcp_endpoint() -> str:
    import socket as _socket

    s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"tcp://127.0.0.1:{port}"


def test_socket_disconnect_fires_instantly_not_on_timeout():
    """Instant crash propagation (ROUTER_NOTIFY): a dropped rank>0 socket fires
    on_rank_lost in ~ms via the disconnect event, well before the heartbeat-timeout
    would. The long timeout override proves the fire came from the disconnect, not
    the silence poll."""
    import zmq

    endpoint = _free_tcp_endpoint()
    fired = threading.Event()
    calls: list[tuple[int, str]] = []
    monitor = rl.RankLivenessMonitor(
        lambda rank, reason: (calls.append((rank, reason)), fired.set()),
        bind_addr=endpoint,
        timeout_ms_override=4000,  # long: only a socket disconnect can fire fast
        expected_ranks={1},
        startup_grace_ms_override=4000,
    )
    sock = zmq.Context.instance().socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, b"rank-1")
    sock.setsockopt(zmq.LINGER, 0)

    monitor.start()
    time.sleep(0.05)
    sock.connect(endpoint)
    sock.send(b"hb")  # register -> enters last_seen
    time.sleep(0.25)
    t0 = time.monotonic()
    sock.close(0)  # drop the worker socket -> ROUTER NOTIFY_DISCONNECT
    try:
        assert fired.wait(2.0), "socket disconnect did not fire on_rank_lost"
        dt = time.monotonic() - t0
        assert calls and calls[0][1] == "socket-disconnect", calls
        assert (
            dt < 1.5
        ), f"fired in {dt:.2f}s; expected ~instant, far under the 4s timeout"
    finally:
        monitor.stop()


def test_disconnect_before_any_heartbeat_does_not_fire():
    """A socket that connects then drops WITHOUT ever heartbeating (startup churn)
    must not trip a spurious failover — only ranks seen alive are armed."""
    import zmq

    endpoint = _free_tcp_endpoint()
    fired = threading.Event()
    monitor = rl.RankLivenessMonitor(
        lambda _r, _reason: fired.set(),
        bind_addr=endpoint,
        timeout_ms_override=4000,
        expected_ranks={1},
        startup_grace_ms_override=4000,
    )
    sock = zmq.Context.instance().socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, b"rank-1")
    sock.setsockopt(zmq.LINGER, 0)

    monitor.start()
    time.sleep(0.05)
    sock.connect(endpoint)
    time.sleep(0.1)
    sock.close(0)  # never sent a heartbeat -> not in last_seen -> must not fire
    try:
        assert not fired.wait(0.8), "spurious fire on disconnect of a never-seen rank"
    finally:
        monitor.stop()
