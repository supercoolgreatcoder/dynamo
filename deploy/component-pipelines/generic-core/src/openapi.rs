// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! The subset of OpenAPI the core needs to bind a step to an operation, plus validation.
//!
//! The core does not implement OpenAPI; it consumes the parts that define a call:
//! `operationId -> (method, path, parameters, body fields, response fields)` and two
//! extensions that express orchestration-relevant properties of an operation:
//!
//! * `x-batch` -- invocations may be folded together. Declared by the operation's owner,
//!   not assumed by the caller: batching an operation whose result depends on the previous
//!   call is silently wrong. The object form additionally says *how* to fold, which is what
//!   makes a generic batching executor possible at all; the bare `true` form marks an
//!   operation batchable without saying how, so the core can validate but not execute it.
//! * `x-streaming: true` -- the response is a stream of items rather than one value.
//!
//! Validating bindings against the document at load time is the point. Every contract
//! failure in this project so far was a contract that existed only in someone's head: a
//! block size configured on two sides, a tokenizer assumed identical, a proto changed on
//! one end of a link. A binding to a field the callee does not define should not be
//! discoverable in production.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Document {
    #[serde(default)]
    pub paths: BTreeMap<String, PathItem>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PathItem {
    #[serde(default)]
    pub get: Option<Operation>,
    #[serde(default)]
    pub post: Option<Operation>,
    #[serde(default)]
    pub put: Option<Operation>,
    #[serde(default)]
    pub patch: Option<Operation>,
    #[serde(default)]
    pub delete: Option<Operation>,
    /// Parameters shared by every operation on this path.
    #[serde(default)]
    pub parameters: Vec<Parameter>,
}

impl PathItem {
    fn operations(&self) -> impl Iterator<Item = (&'static str, &Operation)> {
        [
            ("GET", &self.get),
            ("POST", &self.post),
            ("PUT", &self.put),
            ("PATCH", &self.patch),
            ("DELETE", &self.delete),
        ]
        .into_iter()
        .filter_map(|(m, o)| o.as_ref().map(|o| (m, o)))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    pub operation_id: String,
    #[serde(default)]
    pub parameters: Vec<Parameter>,
    #[serde(default)]
    pub request_body: Option<Body>,
    #[serde(default)]
    pub responses: BTreeMap<String, Body>,
    #[serde(default, rename = "x-batch")]
    pub batch: Option<BatchSupport>,
    #[serde(default, rename = "x-streaming")]
    pub streaming: bool,
    /// Every other `x-` extension, passed through to the transport untouched.
    ///
    /// This is how a transport binds an operation to a concrete protocol without the core
    /// learning that protocol. `x-grpc: {service, method}` is read by the gRPC transport;
    /// the core never looks inside. Adding a transport would otherwise mean adding a field
    /// here, which is the same coupling `x-batch` was introduced to avoid.
    #[serde(flatten)]
    pub extensions: BTreeMap<String, serde_json::Value>,
}

/// Where a bound input is carried on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ParamIn {
    Path,
    Query,
    Header,
    Cookie,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Parameter {
    pub name: String,
    #[serde(rename = "in")]
    pub location: ParamIn,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub schema: Schema,
}

/// `x-batch: true` or `x-batch: {requestField, intoField, responseField}`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BatchSupport {
    /// Batchable, but the fold is unspecified -- validatable, not executable.
    Flag(bool),
    Fold(BatchFold),
}

/// How N invocations fold into one call and how the response splits back apart.
///
/// Without this a generic executor cannot batch at all: it would have to know that
/// `encode{text}` becomes a call carrying `texts`, and that results come back positionally.
/// Naming it on the operation is what moves that knowledge out of the orchestrator's code.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchFold {
    /// Field on the single-call request that becomes a list.
    ///
    /// Omit it when the batched form carries WHOLE requests as items rather than lifting
    /// one field -- the two shapes both occur in practice and a fold that only handled the
    /// first could not batch a real selector API.
    #[serde(default)]
    pub request_field: Option<String>,
    /// Renames applied to each whole request when it becomes an item. Needed because a
    /// batched API is not obliged to reuse the single API's field names, and one here does
    /// not (`block_hashes` becomes `bh`).
    #[serde(default)]
    pub item_fields: BTreeMap<String, String>,
    /// Name that list takes on the batched request.
    pub into_field: String,
    /// List on the batched response, positionally matching the inputs.
    pub response_field: String,
    /// Operation to call when batching; defaults to the same operation.
    #[serde(default)]
    pub operation_id: Option<String>,
}

impl BatchSupport {
    pub fn is_batchable(&self) -> bool {
        match self {
            BatchSupport::Flag(b) => *b,
            BatchSupport::Fold(_) => true,
        }
    }
    pub fn fold(&self) -> Option<&BatchFold> {
        match self {
            BatchSupport::Fold(f) => Some(f),
            BatchSupport::Flag(_) => None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Body {
    #[serde(default)]
    pub content: BTreeMap<String, MediaType>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MediaType {
    #[serde(default)]
    pub schema: Schema,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Schema {
    #[serde(default, rename = "type")]
    pub ty: Option<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, Schema>,
    #[serde(default)]
    pub required: Vec<String>,
}

/// An operation plus how to reach it.
#[derive(Debug, Clone)]
pub struct ResolvedOp {
    pub method: &'static str,
    pub path: String,
    pub op: Operation,
    /// Path-level parameters merged with the operation's own.
    pub parameters: Vec<Parameter>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum SpecError {
    #[error("operation `{0}` not found in the document")]
    NoSuchOperation(String),
    #[error(
        "step `{step}` binds input `{field}`, which operation `{op}` defines neither as a parameter nor a body field"
    )]
    UnknownInput {
        step: String,
        op: String,
        field: String,
    },
    #[error("step `{step}` captures output `{field}`, which operation `{op}` does not return")]
    UnknownOutput {
        step: String,
        op: String,
        field: String,
    },
    #[error("step `{step}` leaves required input `{field}` of operation `{op}` unbound")]
    MissingRequired {
        step: String,
        op: String,
        field: String,
    },
    #[error("step `{step}` sets `batch`, but operation `{op}` does not declare `x-batch`")]
    NotBatchable { step: String, op: String },
    #[error("operation `{op}` has path template variable `{var}` with no matching path parameter")]
    UnboundPathVariable { op: String, var: String },
}

impl Document {
    pub fn from_yaml(s: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(s)
    }

    pub fn operation(&self, id: &str) -> Result<ResolvedOp, SpecError> {
        for (path, item) in &self.paths {
            for (method, op) in item.operations() {
                if op.operation_id == id {
                    let mut parameters = item.parameters.clone();
                    parameters.extend(op.parameters.iter().cloned());
                    let resolved = ResolvedOp {
                        method,
                        path: path.clone(),
                        op: op.clone(),
                        parameters,
                    };
                    resolved.check_path_template()?;
                    return Ok(resolved);
                }
            }
        }
        Err(SpecError::NoSuchOperation(id.into()))
    }
}

impl ResolvedOp {
    fn request_schema(&self) -> Option<&Schema> {
        self.op
            .request_body
            .as_ref()?
            .content
            .values()
            .next()
            .map(|m| &m.schema)
    }

    pub fn response_schema(&self) -> Option<&Schema> {
        let r = self
            .op
            .responses
            .get("200")
            .or_else(|| self.op.responses.values().next())?;
        r.content.values().next().map(|m| &m.schema)
    }

    pub fn parameter(&self, name: &str) -> Option<&Parameter> {
        self.parameters.iter().find(|p| p.name == name)
    }

    /// Every `{var}` in the path template must have a declared path parameter, or the
    /// engine would emit a URL containing a literal brace.
    fn check_path_template(&self) -> Result<(), SpecError> {
        for var in path_template_vars(&self.path) {
            let ok = self
                .parameters
                .iter()
                .any(|p| p.location == ParamIn::Path && p.name == var);
            if !ok {
                return Err(SpecError::UnboundPathVariable {
                    op: self.op.operation_id.clone(),
                    var,
                });
            }
        }
        Ok(())
    }

    /// Checks a step's bindings against this operation.
    pub fn validate(
        &self,
        step: &str,
        input: &BTreeMap<String, crate::config::Expr>,
        output: &BTreeMap<String, crate::config::Expr>,
        batched: bool,
    ) -> Result<(), SpecError> {
        let op = &self.op.operation_id;
        if batched && !self.op.batch.as_ref().is_some_and(|b| b.is_batchable()) {
            return Err(SpecError::NotBatchable {
                step: step.into(),
                op: op.clone(),
            });
        }

        let body = self.request_schema();
        for field in input.keys() {
            let is_param = self.parameter(field).is_some();
            let is_body = body.is_some_and(|s| s.properties.contains_key(field));
            if !is_param && !is_body {
                return Err(SpecError::UnknownInput {
                    step: step.into(),
                    op: op.clone(),
                    field: field.clone(),
                });
            }
        }
        if let Some(schema) = body {
            for field in &schema.required {
                if !input.contains_key(field) {
                    return Err(SpecError::MissingRequired {
                        step: step.into(),
                        op: op.clone(),
                        field: field.clone(),
                    });
                }
            }
        }
        for p in self.parameters.iter().filter(|p| p.required) {
            if !input.contains_key(&p.name) {
                return Err(SpecError::MissingRequired {
                    step: step.into(),
                    op: op.clone(),
                    field: p.name.clone(),
                });
            }
        }

        if let Some(schema) = self.response_schema() {
            for expr in output.values() {
                if let Some(field) = expr.as_path().and_then(response_head) {
                    if !schema.properties.contains_key(&field) {
                        return Err(SpecError::UnknownOutput {
                            step: step.into(),
                            op: op.clone(),
                            field,
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn path_template_vars(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = path;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open..].find('}') else {
            break;
        };
        out.push(rest[open + 1..open + close].to_string());
        rest = &rest[open + close + 1..];
    }
    out
}

/// `$.response.token_ids[0]` -> `token_ids`
fn response_head(path: &str) -> Option<String> {
    let rest = path.strip_prefix("$.response.")?;
    let head = rest.split('.').next()?;
    Some(head.split('[').next()?.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Expr;
    use serde_json::json;

    const SPEC: &str = r#"
paths:
  /encode:
    post:
      operationId: encode
      x-batch: {requestField: text, intoField: texts, responseField: results}
      requestBody:
        content:
          application/json:
            schema:
              type: object
              required: [text]
              properties:
                text: {type: string}
                blockSize: {type: integer}
      responses:
        "200":
          content:
            application/json:
              schema:
                type: object
                properties:
                  tokenIds: {type: array}
                  blockHashes: {type: array}
  /reserve:
    post:
      operationId: reserve
      requestBody:
        content:
          application/json:
            schema: {type: object, properties: {id: {type: string}}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, properties: {ok: {type: boolean}}}}}
  /items/{itemId}:
    parameters:
      - {name: itemId, in: path, required: true, schema: {type: string}}
    get:
      operationId: getItem
      parameters:
        - {name: verbose, in: query, schema: {type: boolean}}
        - {name: x-tenant, in: header, required: true, schema: {type: string}}
      responses:
        "200":
          content: {application/json: {schema: {type: object, properties: {name: {type: string}}}}}
  /bad/{missing}:
    get:
      operationId: badTemplate
      responses:
        "200":
          content: {application/json: {schema: {type: object, properties: {x: {type: string}}}}}
"#;

    fn doc() -> Document {
        Document::from_yaml(SPEC).unwrap()
    }

    fn m(pairs: &[(&str, &str)]) -> BTreeMap<String, Expr> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Expr::Path(v.to_string())))
            .collect()
    }

    #[test]
    fn resolves_operation_method_and_path() {
        let op = doc().operation("encode").unwrap();
        assert_eq!((op.method, op.path.as_str()), ("POST", "/encode"));
    }

    #[test]
    fn resolves_non_post_methods() {
        assert_eq!(doc().operation("getItem").unwrap().method, "GET");
    }

    #[test]
    fn merges_path_level_and_operation_level_parameters() {
        let op = doc().operation("getItem").unwrap();
        assert_eq!(op.parameter("itemId").unwrap().location, ParamIn::Path);
        assert_eq!(op.parameter("verbose").unwrap().location, ParamIn::Query);
        assert_eq!(op.parameter("x-tenant").unwrap().location, ParamIn::Header);
    }

    #[test]
    fn accepts_parameter_bindings_with_no_request_body() {
        let op = doc().operation("getItem").unwrap();
        op.validate(
            "step_a",
            &m(&[
                ("itemId", "$.vars.id"),
                ("x-tenant", "$.request.headers.tenant"),
            ]),
            &m(&[("name", "$.response.name")]),
            false,
        )
        .unwrap();
    }

    #[test]
    fn rejects_missing_required_parameter() {
        let op = doc().operation("getItem").unwrap();
        let err = op
            .validate(
                "step_a",
                &m(&[("itemId", "$.vars.id")]),
                &Default::default(),
                false,
            )
            .unwrap_err();
        assert!(matches!(err, SpecError::MissingRequired { ref field, .. } if field == "x-tenant"));
    }

    #[test]
    fn rejects_a_path_template_with_no_declared_parameter() {
        // Otherwise the engine emits a URL containing a literal `{missing}`.
        assert!(matches!(
            doc().operation("badTemplate"),
            Err(SpecError::UnboundPathVariable { .. })
        ));
    }

    #[test]
    fn unknown_operation_is_an_error() {
        assert_eq!(
            doc().operation("nope").unwrap_err(),
            SpecError::NoSuchOperation("nope".into())
        );
    }

    #[test]
    fn accepts_a_valid_binding() {
        let op = doc().operation("encode").unwrap();
        op.validate(
            "step_a",
            &m(&[("text", "$.request.body.messages[-1].content")]),
            &m(&[("tokens", "$.response.tokenIds")]),
            true,
        )
        .unwrap();
    }

    #[test]
    fn batch_fold_describes_how_to_fold_and_split() {
        let op = doc().operation("encode").unwrap();
        let fold = op.op.batch.as_ref().unwrap().fold().unwrap();
        assert_eq!(
            (fold.request_field.as_deref(), fold.into_field.as_str()),
            (Some("text"), "texts")
        );
        assert_eq!(fold.response_field, "results");
    }

    #[test]
    fn rejects_input_the_callee_does_not_define() {
        let op = doc().operation("encode").unwrap();
        let err = op
            .validate(
                "step_a",
                &m(&[("txt", "$.request.body")]),
                &Default::default(),
                false,
            )
            .unwrap_err();
        assert!(matches!(err, SpecError::UnknownInput { .. }));
    }

    #[test]
    fn rejects_missing_required_input() {
        let op = doc().operation("encode").unwrap();
        let err = op
            .validate(
                "step_a",
                &m(&[("blockSize", "$.vars.bs")]),
                &Default::default(),
                false,
            )
            .unwrap_err();
        assert!(matches!(err, SpecError::MissingRequired { .. }));
    }

    #[test]
    fn rejects_output_the_callee_does_not_return() {
        let op = doc().operation("encode").unwrap();
        let err = op
            .validate(
                "step_a",
                &m(&[("text", "$.request.body")]),
                &m(&[("x", "$.response.sequenceHashes")]),
                false,
            )
            .unwrap_err();
        assert!(matches!(err, SpecError::UnknownOutput { .. }));
    }

    #[test]
    fn rejects_batching_an_operation_that_did_not_opt_in() {
        let op = doc().operation("reserve").unwrap();
        let err = op
            .validate(
                "step_b",
                &m(&[("id", "$.vars.id")]),
                &Default::default(),
                true,
            )
            .unwrap_err();
        assert!(matches!(err, SpecError::NotBatchable { .. }));
    }

    #[test]
    fn literal_outputs_are_not_schema_checked() {
        let op = doc().operation("encode").unwrap();
        let mut out = BTreeMap::new();
        out.insert("k".to_string(), Expr::Literal(json!(1)));
        op.validate("step_a", &m(&[("text", "$.request.body")]), &out, false)
            .unwrap();
    }
}
