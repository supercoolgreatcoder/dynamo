// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let proto_dir = root.join("proto");
    let proto = proto_dir.join("dynamo/components/v1/components.proto");
    let descriptor_path = PathBuf::from(env::var("OUT_DIR")?).join("components_descriptor.bin");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(&[proto.as_path()], &[proto_dir.as_path()])?;
    println!("cargo:rerun-if-changed={}", proto.display());
    Ok(())
}
