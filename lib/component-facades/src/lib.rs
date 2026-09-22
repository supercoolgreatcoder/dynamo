// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod postprocess;
pub mod preprocess;
pub mod selector;

pub mod proto {
    tonic::include_proto!("dynamo.components.v1");
}

use proto::ItemError;

fn item_error(kind: &str, message: impl Into<String>, retryable: bool) -> ItemError {
    ItemError {
        kind: kind.to_string(),
        message: message.into(),
        retryable,
    }
}

fn deadline_expired(deadline_unix_ms: i64) -> bool {
    if deadline_unix_ms <= 0 {
        return false;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    now >= deadline_unix_ms as u128
}
