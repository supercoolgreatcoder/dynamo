// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cross-request batching, executed.
//!
//! `batch::fold`/`split` already carried the semantics; this is the scheduler that was
//! missing, and its absence was worth 45-72% of throughput against a gateway that batches.
//!
//! **Leader-follower, deliberately.** The obvious design gives the batcher its own task and
//! a `'static` transport, which forces every caller to hand over an `Arc<dyn Transport>` and
//! changes the public API. Instead the first caller into an empty slot becomes the leader:
//! it waits out the linger, takes the accumulated batch, and issues the call **with its own
//! borrowed transport**, then wakes the followers. Nothing is spawned, no transport is
//! cloned, and a batch cannot outlive the request that created it.
//!
//! Failure is shared, not swallowed: if the folded call fails, every follower gets the same
//! error. A batcher that silently dropped followers would strand them on a reply that never
//! comes, which is the failure mode this project has already paid for twice.

use crate::batch::{BatchError, fold, split_owned};
use crate::openapi::BatchFold;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot};

/// Requests waiting to be folded into one call.
#[derive(Default)]
struct Pending {
    reqs: Vec<Value>,
    txs: Vec<oneshot::Sender<Result<Value, String>>>,
}

/// One slot per (step, endpoint). Keyed by endpoint as well as step because the same step
/// can address different callees per request, and folding across endpoints would send a
/// request to a component that was never chosen for it.
#[derive(Default)]
pub struct Batchers {
    slots: Mutex<HashMap<String, Arc<Mutex<Option<Pending>>>>>,
    /// Caps concurrent folded calls per key, so batch size can tune itself. See
    /// `BatchPolicy::max_in_flight`.
    gates: Mutex<HashMap<String, Arc<Semaphore>>>,
}

/// What a caller must do after joining a batch.
pub enum Join {
    /// Wait for the leader to fulfil this request.
    Follower(oneshot::Receiver<Result<Value, String>>),
    /// Run the batch: linger, take, fold, call, split, reply.
    Leader(Leader),
}

pub struct Leader {
    slot: Arc<Mutex<Option<Pending>>>,
    my_rx: oneshot::Receiver<Result<Value, String>>,
}

impl Batchers {
    /// Waits for a slot among the folded calls allowed in flight for `key`.
    ///
    /// The leader holds this across its call. A leader that arrives while one is in flight
    /// blocks here -- and, crucially, it has ALREADY claimed leadership, so requests arriving
    /// meanwhile join its batch rather than starting a third. That is what makes batch size
    /// track load without a configured wait.
    pub async fn gate(&self, key: &str, permits: usize) -> OwnedSemaphorePermit {
        let sem = {
            let mut gates = self.gates.lock().await;
            gates
                .entry(key.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(permits.max(1))))
                .clone()
        };
        sem.acquire_owned()
            .await
            .expect("batch gate semaphore is never closed")
    }

    /// Adds `body` to the batch for `key` and says whether this caller leads it.
    pub async fn join(&self, key: &str, body: Value) -> Join {
        let slot = {
            let mut slots = self.slots.lock().await;
            slots.entry(key.to_string()).or_default().clone()
        };
        let (tx, rx) = oneshot::channel();
        let mut guard = slot.lock().await;
        match guard.as_mut() {
            Some(p) => {
                p.reqs.push(body);
                p.txs.push(tx);
                Join::Follower(rx)
            }
            None => {
                *guard = Some(Pending {
                    reqs: vec![body],
                    txs: vec![tx],
                });
                drop(guard);
                Join::Leader(Leader { slot, my_rx: rx })
            }
        }
    }
}

impl Leader {
    /// Waits out the linger, then takes EVERYTHING accumulated. Taking the slot (rather than
    /// draining it) means requests arriving during the call start a fresh batch instead of
    /// joining one already in flight.
    ///
    /// Everything, and not `max_size` of it, because a leader owns what it collects. The
    /// earlier version capped here and parked the overflow back in the slot -- where it had
    /// no leader, since the next arrival saw a non-empty slot and became a follower too.
    /// Nothing promotes a parked follower, and under closed-loop load no new request arrives
    /// until one completes, so the arm wedged: every request returning after exactly 60,000
    /// ms, all released at the same instant.
    ///
    /// `max_size` is still honoured, by the caller, which splits what it collects into calls
    /// of at most that many. That is the only split that cannot strand anyone.
    pub async fn collect(
        &self,
        linger_us: u64,
    ) -> (Vec<Value>, Vec<oneshot::Sender<Result<Value, String>>>) {
        if linger_us > 0 {
            tokio::time::sleep(std::time::Duration::from_micros(linger_us)).await;
        } else {
            // Give already-runnable peers a chance to join. Without this a linger of 0
            // batches exactly one request and the whole mechanism is inert.
            tokio::task::yield_now().await;
        }
        let mut guard = self.slot.lock().await;
        let Some(p) = guard.take() else {
            return (Vec::new(), Vec::new());
        };
        (p.reqs, p.txs)
    }

    /// The leader's own slot in the batch, awaited after it has distributed the results.
    pub fn into_receiver(self) -> oneshot::Receiver<Result<Value, String>> {
        self.my_rx
    }
}

/// Distributes one folded response back to everyone in the batch.
/// Takes the response by value: splitting it moves the parts out rather than copying them,
/// which is what keeps a large batch from stalling every member behind the leader.
pub fn distribute(
    txs: Vec<oneshot::Sender<Result<Value, String>>>,
    outcome: Result<Value, String>,
    spec: &BatchFold,
) {
    let n = txs.len();
    match outcome {
        Ok(resp) => match split_owned(resp, spec, n) {
            Ok(parts) => {
                for (tx, part) in txs.into_iter().zip(parts) {
                    let _ = tx.send(Ok(part));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for tx in txs {
                    let _ = tx.send(Err(msg.clone()));
                }
            }
        },
        Err(msg) => {
            for tx in txs {
                let _ = tx.send(Err(msg.clone()));
            }
        }
    }
}

/// Folds a batch, surfacing a disagreement rather than silently applying the first
/// request's values to all of them.
pub fn fold_checked_owned(reqs: Vec<Value>, spec: &BatchFold) -> Result<Value, BatchError> {
    // Checked while borrowed, then folded by value: the disagreement check has to see every
    // request, and the fold must not copy any of them.
    let diffs = crate::batch::disagreements(&reqs, spec);
    if !diffs.is_empty() {
        return Err(BatchError::MissingRequestField {
            index: 0,
            field: format!("requests disagree on {diffs:?}; they cannot be folded"),
        });
    }
    crate::batch::fold_owned(reqs, spec)
}

pub fn fold_checked(reqs: &[Value], spec: &BatchFold) -> Result<Value, BatchError> {
    let diffs = crate::batch::disagreements(reqs, spec);
    if !diffs.is_empty() {
        return Err(BatchError::MissingRequestField {
            index: 0,
            field: format!("requests disagree on {diffs:?}; they cannot be folded"),
        });
    }
    fold(reqs, spec)
}
