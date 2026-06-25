# GMS KV block lifecycle — complexity analysis & simplification plan

This documents *why* the GMS-v2 / bulwark KV block lifecycle is complex, what is
actually necessary, and a staged plan to simplify it while preserving the single
correctness invariant. Companion to the failover reproduction (sglang green;
vLLM/TRT-LLM mechanism-verified).

## The one invariant everything must preserve

**No KV segment is ever RW-writable by two engines at once.** Primary and shadow
coexist on the *same* GMS-owned KV pool; per segment (a chunk of blocks) exactly
one engine holds the RW lock. A shadow pre-activates on crash detection and may
go live before the primary fully dies, but it only writes segments whose RW lock
it holds (ones the primary does not), and acquires the *remaining* segments only
once the primary fully dies and releases its locks.

## Why it looks complicated: KV ownership is tracked in five places

1. **GMS server FSM** (`server/fsm.py`): `EMPTY → RW → COMMITTED → RO`. This is
   the *weights* publish/import lock. Correct and necessary; weights are
   write-once-per-generation.
2. **`RW_PERSISTENT` lock type** (`server/session.py:152`): a 4th lock mode that
   *bypasses* the FSM entirely (returns the grant immediately, no transition).
   This is how KV survives a crash without holding the single-writer weights lock.
3. **`PersistentAllocationManager`** (`server/persistent_allocations.py`): a
   separate claim table (exclusive / shared claims keyed by `(engine_id, tag)`)
   for the persistent KV pool. Invisible to the FSM — which is exactly why the
   kv_cache daemon looked `EMPTY` until the runtime-state **projection** was added
   (`server/gms.py::_sync_persistent_layout_events` / `get_runtime_state`).
4. **Per-block KV leases** (`rust_ring/src/lib.rs`): the real per-segment RW lock.
   A `/dev/shm` table of 32-byte records `{state, generation, lease_epoch,
   owner_hash}` with states `FREE/LEASED/SEALED/RESERVED`. `state` is the lock;
   `FREE→LEASED` CAS is the single-writer enforcement.
5. **`server/kv_leases.py`** (`KVLeaseManager`): an RPC-based KV-lease table,
   parallel to #4.

Plus the **failover flock** (`components/src/dynamo/common/gms_failover.py`,
`/shared/failover.lock`): the cross-engine serializer — only the flock holder is
the active writer; on death the next engine acquires it and reclaims orphaned
segments. **This flock is the real source of truth for "who may write," which
makes much of the per-block epoch/generation machinery defensive-only.**

## What is necessary vs. removable

| Mechanism | Verdict | Rationale |
|---|---|---|
| Weights FSM (#1) | **Keep** | Correct model for write-once weights. |
| `RW_PERSISTENT` (#2) | **Keep** | The crash-survival primitive; KV must outlive the writer. |
| `PersistentAllocationManager` (#3) | **Keep, now observable** | The persistent claim table is real; the new runtime-state projection makes it visible without a parallel FSM. |
| Per-block lease `state` (#4) | **Keep** | The actual single-writer-per-segment lock (atomic CAS). |
| Per-block `generation` | **Keep** | Guards stale releases (release validates generation). |
| Per-block `lease_epoch` | **Remove (vestigial)** | Stored on acquire, returned to Python, **never validated anywhere**. `generation` already covers stale-release. Drops the `L_NEXT_EPOCH` global counter + 8 bytes/record. |
| `RESERVED` state + reservation file | **Keep** | Live: `transition_reclaim.py` + `install_kv_leases` use reserved headroom to prevent shadow starvation. |
| RPC lease table `server/kv_leases.py` (#5) | **Remove (engine-dead)** | No integration or component imports `InitKVLeaseNamespace*` / `AcquireKVLeaseBlocks` — engines use the `/dev/shm` rust_ring path only. The RPC table duplicates #4. |
| Dead ring fork `lib/gms_kv_ring/rust_ring` | **Remove (dead)** | Superseded by `lib/gpu_memory_service/rust_ring` (the superset with the `kv_lease_*` fns). The fork lacks the lease functions and is built by nothing. |

## The simple target model

- **Weights** → GMS FSM (`RW→COMMITTED→RO`). Unchanged.
- **KV** → persistent allocation (survives crashes) + per-segment lease `state`
  (single-writer CAS) + the failover flock (cross-engine serialization).
- Delete: `lease_epoch`, the RPC lease table, the dead ring fork.
- Keep `RESERVED`/reservation (starvation control) and `generation`
  (stale-release guard).

That collapses "five ownership trackers" to **two that matter** (persistent claim
+ per-segment lease) gated by **one serializer** (the flock), with weights on
their own orthogonal FSM.

## Race coverage for the invariant

The single-writer-per-segment invariant is enforced at two layers, which is
sufficient and non-redundant:

1. **Steady state** — `rust_ring` `state` CAS `FREE→LEASED` (`lib.rs:178`). Two
   engines cannot both win the CAS on the same block. Sound on its own.
2. **Failover handoff** — the flock. The shadow only reclaims foreign segments
   (`kv_lease_reclaim_foreign`) *after* it holds the flock the dead primary
   released, plus an optional post-lock fence
   (`DYN_GMS_FAILOVER_POST_LOCK_FENCE_MS`). Because the primary *process* is gone
   before the flock frees, no in-flight write can target a reclaimed segment.

**Defensive hardening (recommended, low-risk):** in `kv_lease_reclaim_foreign`,
re-read `owner_hash` after the `state→FREE` CAS and abort the reclaim of that
block if it changed — closes the theoretical window where reclaim is invoked
before the primary is fully fenced. (The flock already prevents this; the
re-check makes the primitive safe even if a caller mis-sequences it.)

## Staged execution (each stage independently testable against the green sglang failover)

1. **S1 — delete dead code** (no behavior change): remove `lib/gms_kv_ring/rust_ring`;
   delete `server/kv_leases.py` + its RPC message types once a grep confirms no
   server dispatch path references them at runtime.
2. **S2 — drop `lease_epoch`**: remove the field from the rust_ring record, the
   `L_NEXT_EPOCH` counter, and the Python `KVLease.lease_epoch` plumbing (wire
   format + Python in lock-step). Re-run the sglang failover.
3. **S3 — reclaim_foreign owner re-check**: add the post-CAS `owner_hash`
   re-validation. Re-run the sglang failover + the lease cross-process stress
   tests (`test_kv_lease_shm_client.py`).

Stages are ordered lowest-risk first; S1 is pure deletion, S2/S3 touch the hot
path and must keep the sglang shadow-failover e2e green
(`scripts/repro-bulwark-failover.sh sglang`).
