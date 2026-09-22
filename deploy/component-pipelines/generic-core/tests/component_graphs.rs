// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, path::Path};

use dynamo_generic_pipeline::{Document, Pipeline, Prepared, referenced_specs};

#[test]
fn aggregate_graph_validates_against_canonical_facade_specs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("generic-core has a parent");
    let graph_path = root.join("graphs/aggregate.yaml");
    let graph_dir = graph_path.parent().unwrap();
    let text = std::fs::read_to_string(&graph_path).unwrap();
    let pipeline = Pipeline::from_yaml(&text).unwrap();
    let specs = referenced_specs(&pipeline)
        .into_iter()
        .map(|reference| {
            let path = graph_dir.join(&reference);
            let text = std::fs::read_to_string(&path).unwrap();
            let document = Document::from_yaml(&text)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
            (reference, document)
        })
        .collect::<BTreeMap<_, _>>();
    let prepared = Prepared::new(pipeline, &specs).unwrap();
    let ids = prepared
        .pipeline()
        .steps
        .iter()
        .map(|step| step.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, ["prepare", "select", "generate"]);
}
