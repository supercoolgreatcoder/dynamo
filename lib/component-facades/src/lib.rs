// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

pub mod chat_worker;
pub mod postprocess;
pub mod preprocess;
pub mod selector;
pub mod worker;

pub mod proto {
    tonic::include_proto!("dynamo.components.v1");

    /// Canonical descriptor consumed by descriptor-driven gateway variants.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("components_descriptor");
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

fn encode_token_ids_le(token_ids: &[u32]) -> Vec<u8> {
    let mut packed = Vec::with_capacity(token_ids.len().saturating_mul(size_of::<u32>()));
    for token_id in token_ids {
        packed.extend_from_slice(&token_id.to_le_bytes());
    }
    packed
}

fn decode_token_ids_le(packed: &[u8]) -> Result<Vec<u32>, &'static str> {
    if !packed.len().is_multiple_of(size_of::<u32>()) {
        return Err("packed token IDs must contain complete little-endian u32 values");
    }
    Ok(packed
        .chunks_exact(size_of::<u32>())
        .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
        .collect())
}

fn token_ids_from_wire(token_ids: Vec<u32>, packed: &[u8]) -> Result<Vec<u32>, &'static str> {
    if packed.is_empty() {
        Ok(token_ids)
    } else if token_ids.is_empty() {
        decode_token_ids_le(packed)
    } else {
        Err("token IDs must use either repeated or packed representation, not both")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_token_ids_round_trip_and_reject_ambiguous_input() {
        let token_ids = vec![0, 1, u16::MAX as u32 + 1, u32::MAX];
        let packed = encode_token_ids_le(&token_ids);
        assert_eq!(decode_token_ids_le(&packed), Ok(token_ids.clone()));
        assert!(decode_token_ids_le(&packed[..packed.len() - 1]).is_err());
        assert!(token_ids_from_wire(token_ids, &packed).is_err());
    }
}
