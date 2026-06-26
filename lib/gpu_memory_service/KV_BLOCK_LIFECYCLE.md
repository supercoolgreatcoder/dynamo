# GMS KV block lifecycle

How GMS-managed KV ownership works under Bulwark primary/shadow operation, and
why it is race-free. Weights and KV are managed by orthogonal mechanisms; this
document covers KV. (Weights use the GMS server FSM `EMPTY → RW → COMMITTED → RO`,
the correct model for write-once-per-generation weights.)

## The one invariant

**No KV segment is ever RW-writable by two engines at once.** Primary and shadow
coexist on the *same* GMS-owned KV pool; per segment (a chunk of blocks) exactly
one engine holds the RW lock. A shadow pre-activates on crash detection and may go
live before the primary fully dies, but it only writes segments whose RW lock it
holds (ones the primary does not), and acquires the *remaining* segments only once
the primary fully dies and releases its locks.

## Ownership model

KV ownership is tracked in two places, serialized by one cross-engine lock:

1. **Persistent allocation** (`server/persistent_allocations.py`, granted via the
   `RW_PERSISTENT` lock mode in `server/session.py`). The KV pool is a persistent
   CUDA VMM allocation that outlives any single engine process, so KV survives a
   crash. Claims (exclusive / shared, keyed by `(engine_id, tag)`) are projected
   into the daemon runtime-state + event history
   (`server/gms.py::_sync_persistent_layout_events` / `get_runtime_state`) so the
   pool's state is observable to failover orchestration.
2. **Per-segment lease** (`rust_ring/src/lib.rs`): the actual single-writer lock. A
   `/dev/shm` table of 16-byte records `{state, generation, owner_hash}` with states
   `FREE / LEASED / SEALED / RESERVED`. `state` is the lock; the `FREE→LEASED` CAS
   is what enforces single-writer-per-segment. `RESERVED` holds starvation headroom
   so a shadow is never locked out (`transition_reclaim.py`, `install_kv_leases`).

These are serialized by the **failover flock**
(`components/src/dynamo/common/gms_failover.py`, `/shared/failover.lock`): only the
flock holder reclaims foreign segments. On the holder's death the next engine
acquires the flock and reclaims orphaned segments.

## Race analysis (single-writer-per-segment)

States `FREE → LEASED → SEALED → FREE` (+ `RESERVED` for headroom); fields
`{state, generation, owner_hash}`, all atomics. The failover-relevant cases:

1. **Two engines acquire the same block** — `acquire` is a CAS `FREE→LEASED`; only
   one CAS wins, the loser tries another block. *Single writer per block.*
2. **Stale release after re-acquire** — `release`/`seal` validate `generation`
   (bumped on each acquire); a stale release with an old generation is a no-op.
3. **Shadow pre-activation overlap (the key failover case)** — the shadow goes live
   before the primary fully dies. It only writes blocks it acquired itself
   (`FREE→LEASED`, blocks the primary doesn't hold); the primary keeps its own
   `LEASED` blocks. No block is `LEASED` by two engines (CAS guarantees it).
4. **Shadow takes the primary's blocks** — only via `reclaim_foreign`
   (`LEASED/SEALED→FREE` for `owner != self`), which is **flock-gated**: it runs
   only after the shadow holds the failover flock the primary released *on death*,
   with an optional post-lock fence (`DYN_GMS_FAILOVER_POST_LOCK_FENCE_MS`). A dead
   process issues no writes, so reclaiming its blocks is race-free. `reclaim_foreign`
   also re-reads `owner_hash` after the `state→FREE` CAS and aborts if it changed,
   so the primitive is safe even if mis-sequenced.
5. **reclaim vs. a concurrent acquire** — they act on disjoint source states
   (`reclaim`: `LEASED→FREE`; `acquire`: `FREE→LEASED`) and serialize through the
   per-block state atomic; a reclaimed block becomes `FREE` and is only then
   acquirable. No torn ownership.

The invariant — **no KV segment is ever `LEASED` by two engines simultaneously** —
is held by (a) the per-block state CAS, (b) `generation` (stale-op guard),
(c) `owner_hash` (reclaim targets only foreign blocks), and (d) the failover flock
(reclaim only after the prior owner is dead). It is covered by the cross-process
single-writer stress and the reclaim-foreign tests in
`tests/test_kv_lease_shm_client.py`.
