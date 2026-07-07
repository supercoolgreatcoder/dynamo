# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""PlacementEvent publisher — Phase 4 of cross-node design.

GMS publishes its placements (`Shared`-tier owner, content hash,
tier) so dynamo's kv-router can make cross-node routing decisions.
See `docs/CROSS_NODE_DESIGN.md` §3 (placement event types) and
`PEGAFLOW_COMPARISON.md` §3 (why this is dynamo's indexer not a
Pegaflow-style MetaServer).

`LoggingPlacementPublisher` (default) writes events to the Python logger.
Production deployments that route across nodes wire a real publisher
(components-side `DynamoGmsPlacementPublisher`) that pushes to dynamo's
ZMQ/NATS channel. The logging implementation is the standalone-GMS-without-
dynamo path — placements aren't seen across nodes but local cache still works.

Wire format (this implementation):

  Stored event:
    {
      "type": "stored",
      "daemon_id": "gms-host-X-gpu-Y",
      "daemon_epoch": <int>,
      "content_hash": "<hex-encoded 32 bytes>",
      "tier": "host_pinned" | "disk" | "external",
      "bytes_size": <int>,
      "ts": <unix seconds float>
    }

  Removed event:
    Same shape; "type": "removed". Indicates the daemon no longer
    has this hash (engine release, LRU eviction, scrub drop).

The publisher does NOT block the hot path. Send failures are logged
and the event is dropped — the indexer will reconverge from
subsequent events. This is consistent with how dynamo's existing
publisher handles backpressure.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Optional, Protocol

logger = logging.getLogger(__name__)


# ---- Event types ------------------------------------------------------------


class PlacementPublisher(Protocol):
    """Protocol implemented by concrete publishers.

    `publish_stored` and `publish_removed` MUST NOT block the caller
    for longer than tens of microseconds in the steady state.
    Concrete publishers should buffer + dispatch on a background
    thread if their underlying transport (ZMQ, NATS) is sync."""

    def publish_stored(
        self,
        content_hash: bytes,
        tier: str,
        bytes_size: int,
        metadata: Optional[dict] = None,
    ) -> None:
        ...

    def publish_removed(
        self,
        content_hash: bytes,
        tier: str,
    ) -> None:
        ...

    def close(self) -> None:
        ...


# ---- Logging implementation ------------------------------------------------


class LoggingPlacementPublisher:
    """No-op publisher that writes events to the Python logger.

    Used by:
      - Standalone GMS without dynamo (no indexer to push to).
      - Test environments (assert log content instead of running a
        broker).

    Production deployments inside dynamo should swap in a real
    publisher (ZMQ or PyO3-backed) at daemon construction."""

    def __init__(
        self,
        daemon_id: str,
        daemon_epoch: int,
        *,
        log_level: int = logging.DEBUG,
    ) -> None:
        self._daemon_id = daemon_id
        self._daemon_epoch = int(daemon_epoch)
        self._log_level = log_level
        self._closed = False
        self._stats = {"stored": 0, "removed": 0, "dropped": 0}
        self._lock = threading.Lock()

    def _event(
        self,
        kind: str,
        content_hash: bytes,
        tier: str,
        bytes_size: int = 0,
        metadata: Optional[dict] = None,
    ) -> dict:
        ev = {
            "type": kind,
            "daemon_id": self._daemon_id,
            "daemon_epoch": self._daemon_epoch,
            "content_hash": content_hash.hex(),
            "tier": tier,
            "bytes_size": int(bytes_size),
            "ts": time.time(),
        }
        if metadata:
            ev["metadata"] = metadata
        return ev

    def publish_stored(
        self,
        content_hash: bytes,
        tier: str,
        bytes_size: int,
        metadata: Optional[dict] = None,
    ) -> None:
        if self._closed:
            with self._lock:
                self._stats["dropped"] += 1
            return
        ev = self._event("stored", content_hash, tier, bytes_size, metadata)
        logger.log(self._log_level, "[PlacementPublisher] %s", json.dumps(ev))
        with self._lock:
            self._stats["stored"] += 1

    def publish_removed(
        self,
        content_hash: bytes,
        tier: str,
    ) -> None:
        if self._closed:
            with self._lock:
                self._stats["dropped"] += 1
            return
        ev = self._event("removed", content_hash, tier, 0)
        logger.log(self._log_level, "[PlacementPublisher] %s", json.dumps(ev))
        with self._lock:
            self._stats["removed"] += 1

    def close(self) -> None:
        self._closed = True

    def stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    def set_daemon_epoch(self, epoch: int) -> None:
        """Called when the daemon epoch bumps (restart). Subsequent
        events carry the new epoch — the indexer's existing
        epoch-based invalidation logic drops old placements."""
        self._daemon_epoch = int(epoch)


__all__ = [
    "PlacementPublisher",
    "LoggingPlacementPublisher",
]
