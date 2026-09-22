// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Folding N calls into one, and splitting the reply back apart.
//!
//! The *semantics* of batching live here, driven entirely by the operation's `x-batch`
//! declaration: which field becomes a list, what that list is called, and where the
//! positional results come back. That is the part that must be generic, and it is pure and
//! synchronous, so it is testable without a runtime.
//!
//! The *scheduling* -- when to close a batch, how long to linger, how many to admit -- is
//! left to the host, which already owns a runtime and the request queue. Keeping it out
//! means this crate needs no async runtime dependency and no timer.
//!
//! On `lingerUs`: the default is 0 everywhere in this project, and the reason is measured.
//! A linger raises batch size a lot, which helps fixed-cost work and can be severely
//! counterproductive for variable-cost work (see `../detok-sidecar/README.md`: +10% on
//! fixed-length datasets, -75% on a variable-length trace).

use crate::openapi::BatchFold;
use serde_json::Value;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum BatchError {
    #[error("batched response has no `{0}` array")]
    MissingResults(String),
    #[error("batched response returned {got} results for {want} requests")]
    CountMismatch { want: usize, got: usize },
    #[error("request {index} has no `{field}` field to fold")]
    MissingRequestField { index: usize, field: String },
}

/// Folds N single-call bodies into one batched body.
///
/// The folded field becomes a list in request order. Every other field is taken from the
/// first request and must agree across the batch -- a caller batching requests that differ
/// in, say, block size would otherwise silently get the first one's value for all of them,
/// so disagreement is reported rather than absorbed.
/// Folds without copying, consuming the requests.
///
/// The mirror of `split_owned`, which was added for the RESPONSE side while the request side
/// kept cloning. The leader owns the requests it collected and nothing reads them afterwards,
/// so lifting a field out by value is free where cloning it is not: at ISL 4000 the lifted
/// field is the whole rendered prompt.
pub fn fold_owned(requests: Vec<Value>, spec: &BatchFold) -> Result<Value, BatchError> {
    let mut folded = serde_json::Map::new();
    let mut list = Vec::with_capacity(requests.len());

    match spec.request_field.as_deref() {
        Some(field) => {
            for (i, r) in requests.into_iter().enumerate() {
                let Value::Object(mut obj) = r else {
                    return Err(BatchError::MissingRequestField {
                        index: i,
                        field: field.to_string(),
                    });
                };
                let v = obj
                    .remove(field)
                    .ok_or_else(|| BatchError::MissingRequestField {
                        index: i,
                        field: field.to_string(),
                    })?;
                list.push(v);
                // Everything else comes from the first request; `disagreements` has already
                // established that the rest agree, so later copies would be identical.
                if i == 0 {
                    for (k, v) in obj {
                        folded.insert(k, v);
                    }
                }
            }
        }
        None => {
            for (i, r) in requests.into_iter().enumerate() {
                let Value::Object(obj) = r else {
                    return Err(BatchError::MissingRequestField {
                        index: i,
                        field: "<whole request>".to_string(),
                    });
                };
                let mut item = serde_json::Map::new();
                for (k, v) in obj {
                    let name = spec.item_fields.get(&k).cloned().unwrap_or(k);
                    item.insert(name, v);
                }
                list.push(Value::Object(item));
            }
        }
    }
    folded.insert(spec.into_field.clone(), Value::Array(list));
    Ok(Value::Object(folded))
}

pub fn fold(requests: &[Value], spec: &BatchFold) -> Result<Value, BatchError> {
    let mut folded = serde_json::Map::new();
    let mut list = Vec::with_capacity(requests.len());

    match spec.request_field.as_deref() {
        // Lift one field into a list; everything else is taken from the first request.
        Some(field) => {
            for (i, r) in requests.iter().enumerate() {
                let obj = r
                    .as_object()
                    .ok_or_else(|| BatchError::MissingRequestField {
                        index: i,
                        field: field.to_string(),
                    })?;
                let v = obj
                    .get(field)
                    .ok_or_else(|| BatchError::MissingRequestField {
                        index: i,
                        field: field.to_string(),
                    })?;
                list.push(v.clone());
                if i == 0 {
                    for (k, v) in obj {
                        if k != field {
                            folded.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }
        // Each whole request becomes an item, renamed per `item_fields`.
        None => {
            for (i, r) in requests.iter().enumerate() {
                let obj = r
                    .as_object()
                    .ok_or_else(|| BatchError::MissingRequestField {
                        index: i,
                        field: "<whole request>".to_string(),
                    })?;
                let mut item = serde_json::Map::new();
                for (k, v) in obj {
                    let name = spec
                        .item_fields
                        .get(k)
                        .cloned()
                        .unwrap_or_else(|| k.clone());
                    item.insert(name, v.clone());
                }
                list.push(Value::Object(item));
            }
        }
    }
    folded.insert(spec.into_field.clone(), Value::Array(list));
    Ok(Value::Object(folded))
}

/// Returns the fields that differ across a batch, so a host can refuse to fold requests
/// that are not equivalent instead of silently applying the first one's values.
pub fn disagreements(requests: &[Value], spec: &BatchFold) -> Vec<String> {
    // Whole-request folds carry every field per item, so nothing is shared and nothing can
    // disagree.
    let Some(field) = spec.request_field.as_deref() else {
        return Vec::new();
    };
    let Some(first) = requests.first().and_then(|r| r.as_object()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (k, v) in first {
        if k == field {
            continue;
        }
        let same = requests
            .iter()
            .all(|r| r.as_object().and_then(|o| o.get(k)) == Some(v));
        if !same {
            out.push(k.clone());
        }
    }
    out.sort();
    out
}

/// Splits a batched response into per-request responses, positionally.
///
/// A count mismatch is an error, never a truncation: silently returning fewer results than
/// requests would strand callers waiting on a reply that never comes, and this project has
/// already spent a debugging round on one silent shortfall.
/// Splits a response the caller owns, MOVING each part out instead of copying it.
///
/// This is the batch leader's path, and at scale it is the whole ball game: one task splits
/// for the entire batch while every member waits. Cloning the results array at maxSize 128
/// and ISL 4000 means deep-copying 128 x 4,000 token ids -- measured at 7.1 ms per batch
/// (foldbench), which is the same order as the p50 gap it was producing. The leader owns the
/// response and nothing else reads it, so the parts can simply be taken.
pub fn split_owned(
    mut response: Value,
    spec: &BatchFold,
    want: usize,
) -> Result<Vec<Value>, BatchError> {
    let taken = response
        .as_object_mut()
        .and_then(|o| o.remove(&spec.response_field));
    let Some(Value::Array(results)) = taken else {
        return Err(BatchError::MissingResults(spec.response_field.clone()));
    };
    if results.len() != want {
        return Err(BatchError::CountMismatch {
            want,
            got: results.len(),
        });
    }
    Ok(results)
}

/// Borrowing form, for callers that do not own the response. Prefer `split_owned`.
pub fn split(response: &Value, spec: &BatchFold, want: usize) -> Result<Vec<Value>, BatchError> {
    let results = response
        .get(&spec.response_field)
        .and_then(|v| v.as_array())
        .ok_or_else(|| BatchError::MissingResults(spec.response_field.clone()))?;
    if results.len() != want {
        return Err(BatchError::CountMismatch {
            want,
            got: results.len(),
        });
    }
    Ok(results.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> BatchFold {
        BatchFold {
            request_field: Some("text".into()),
            item_fields: Default::default(),
            into_field: "texts".into(),
            response_field: "results".into(),
            operation_id: Some("encodeBatch".into()),
        }
    }

    #[test]
    fn folds_the_declared_field_into_a_list_preserving_order() {
        let reqs = vec![
            json!({"text": "a", "blockSize": 64}),
            json!({"text": "b", "blockSize": 64}),
        ];
        assert_eq!(
            fold(&reqs, &spec()).unwrap(),
            json!({"texts": ["a", "b"], "blockSize": 64})
        );
    }

    #[test]
    fn split_owned_moves_the_parts_out() {
        let resp = json!({"results": [{"tokenIds": [1]}, {"tokenIds": [2]}]});
        let out = split_owned(resp, &spec(), 2).unwrap();
        assert_eq!(out[0], json!({"tokenIds": [1]}));
        assert_eq!(out[1], json!({"tokenIds": [2]}));
    }

    #[test]
    fn split_owned_agrees_with_the_borrowing_form() {
        // The move exists for speed, not for different behaviour.
        let resp = json!({"results": [{"a": 1}, {"a": 2}, {"a": 3}]});
        assert_eq!(
            split(&resp, &spec(), 3).unwrap(),
            split_owned(resp, &spec(), 3).unwrap()
        );
    }

    #[test]
    fn split_owned_reports_a_missing_results_field() {
        let resp = json!({"other": []});
        assert_eq!(
            split_owned(resp, &spec(), 1),
            Err(BatchError::MissingResults("results".into()))
        );
    }

    #[test]
    fn split_owned_reports_a_results_field_that_is_not_an_array() {
        // Taking the field before checking its type must not turn a wrong type into a
        // missing field silently -- both are the same error here, and both are an error.
        let resp = json!({"results": 7});
        assert!(split_owned(resp, &spec(), 1).is_err());
    }

    #[test]
    fn split_owned_refuses_a_count_mismatch_rather_than_truncating() {
        // A short list must not quietly strand the members with no part.
        let resp = json!({"results": [{"a": 1}]});
        assert_eq!(
            split_owned(resp, &spec(), 2),
            Err(BatchError::CountMismatch { want: 2, got: 1 })
        );
    }

    #[test]
    fn splits_results_positionally() {
        let resp = json!({"results": [{"tokenIds": [1]}, {"tokenIds": [2]}]});
        let out = split(&resp, &spec(), 2).unwrap();
        assert_eq!(out[1], json!({"tokenIds": [2]}));
    }

    #[test]
    fn a_short_result_list_is_an_error_not_a_truncation() {
        let resp = json!({"results": [{"tokenIds": [1]}]});
        assert_eq!(
            split(&resp, &spec(), 2).unwrap_err(),
            BatchError::CountMismatch { want: 2, got: 1 }
        );
    }

    #[test]
    fn a_missing_results_array_is_reported() {
        assert_eq!(
            split(&json!({"other": []}), &spec(), 1).unwrap_err(),
            BatchError::MissingResults("results".into())
        );
    }

    #[test]
    fn disagreeing_non_folded_fields_are_reported_rather_than_absorbed() {
        // Batching these would silently apply blockSize 64 to the second request -- the
        // same class of defect as a block size configured twice and drifting.
        let reqs = vec![
            json!({"text": "a", "blockSize": 64}),
            json!({"text": "b", "blockSize": 16}),
        ];
        assert_eq!(disagreements(&reqs, &spec()), vec!["blockSize".to_string()]);
        assert!(disagreements(&reqs[..1], &spec()).is_empty());
    }

    #[test]
    fn a_request_missing_the_folded_field_is_an_error() {
        let reqs = vec![json!({"blockSize": 64})];
        assert!(matches!(
            fold(&reqs, &spec()).unwrap_err(),
            BatchError::MissingRequestField { .. }
        ));
    }
}

#[cfg(test)]
mod owned_parity_tests {
    use super::*;

    fn spec() -> BatchFold {
        BatchFold {
            request_field: Some("text".into()),
            item_fields: Default::default(),
            into_field: "texts".into(),
            response_field: "results".into(),
            operation_id: None,
        }
    }

    /// Two implementations of one rule is how they drift. The cheap one is checked against
    /// the original rather than trusted.
    #[test]
    fn fold_owned_agrees_with_fold_on_a_lifted_field() {
        let reqs = vec![
            serde_json::json!({"text": "alpha", "block_size": 512}),
            serde_json::json!({"text": "beta", "block_size": 512}),
        ];
        assert_eq!(
            fold_owned(reqs.clone(), &spec()).unwrap(),
            fold(&reqs, &spec()).unwrap()
        );
    }

    /// A request that is not an object is an error, by value as by reference.
    #[test]
    fn fold_owned_rejects_a_non_object_request() {
        let reqs = vec![serde_json::json!("not an object")];
        assert!(fold_owned(reqs, &spec()).is_err());
    }
}

#[cfg(test)]
mod whole_request_tests {
    use super::*;
    use serde_json::json;

    /// The real selector's batch API takes whole requests as items AND renames their fields
    /// (`block_hashes` -> `bh`). A fold that could only lift a single field could not batch
    /// it at all, which is why `requestField` is optional and `itemFields` exists.
    fn selector_spec() -> BatchFold {
        BatchFold {
            request_field: None,
            item_fields: [
                ("block_hashes".to_string(), "bh".to_string()),
                ("sequence_hashes".to_string(), "sh".to_string()),
                ("isl_tokens".to_string(), "isl".to_string()),
            ]
            .into_iter()
            .collect(),
            into_field: "items".into(),
            response_field: "results".into(),
            operation_id: Some("selectBatch".into()),
        }
    }

    #[test]
    fn folds_whole_requests_into_items_with_renamed_fields() {
        let reqs = vec![
            json!({"block_hashes": [1], "sequence_hashes": [2], "isl_tokens": 10}),
            json!({"block_hashes": [3], "sequence_hashes": [4], "isl_tokens": 20}),
        ];
        assert_eq!(
            fold(&reqs, &selector_spec()).unwrap(),
            json!({"items": [
                {"bh": [1], "sh": [2], "isl": 10},
                {"bh": [3], "sh": [4], "isl": 20}
            ]})
        );
    }

    #[test]
    fn whole_request_folds_have_nothing_that_can_disagree() {
        // Every field travels per item, so unlike a single-field fold there is no shared
        // value to silently take from the first request.
        let reqs = vec![
            json!({"block_hashes": [1], "isl_tokens": 10}),
            json!({"block_hashes": [3], "isl_tokens": 999}),
        ];
        assert!(disagreements(&reqs, &selector_spec()).is_empty());
    }

    /// The owned fold must produce exactly what the borrowing one does, for the
    /// whole-request shape with renames.
    #[test]
    fn fold_owned_agrees_with_fold_on_whole_requests() {
        let reqs = vec![
            serde_json::json!({"block_hashes": [1, 2], "isl_tokens": 7}),
            serde_json::json!({"block_hashes": [3], "isl_tokens": 9}),
        ];
        assert_eq!(
            fold_owned(reqs.clone(), &selector_spec()).unwrap(),
            fold(&reqs, &selector_spec()).unwrap(),
        );
    }
}
