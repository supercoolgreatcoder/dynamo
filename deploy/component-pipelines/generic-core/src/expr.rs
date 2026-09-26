// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Path evaluation over the execution scope.
//!
//! Supports `$.root.field`, `$.root.field[0]`, and `[-1]` for the last element. That is
//! the whole language, on purpose: an orchestrator that can compute is an orchestrator
//! that can be slow in ways config review cannot catch.

use crate::config::Expr;
use serde_json::Value;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ExprError {
    #[error("path {0} must start with `$.`")]
    NotAPath(String),
    #[error("unknown scope root `{0}` in {1}; expected request, response, vars or item")]
    UnknownRoot(String, String),
    #[error("path {0} did not resolve")]
    Unresolved(String),
    #[error("malformed index in path {0}")]
    BadIndex(String),
    #[error("length of {0}, which is not an array, string or object")]
    NotCountable(String),
}

/// What a step can read while it executes.
#[derive(Debug, Default, Clone)]
pub struct Scope {
    /// The inbound request, shaped by the pipeline's external OpenAPI operation.
    pub request: Value,
    /// The most recent call's response, available to that step's `output` bindings.
    pub response: Value,
    /// Named values captured by earlier steps.
    pub vars: Value,
    /// The streamed item currently being projected, addressable as `$.item`.
    pub item: Value,
}

impl Scope {
    pub fn new(request: Value) -> Self {
        Self {
            request,
            response: Value::Null,
            vars: Value::Object(Default::default()),
            item: Value::Null,
        }
    }

    pub fn set_var(&mut self, name: &str, v: Value) {
        if !self.vars.is_object() {
            self.vars = Value::Object(Default::default());
        }
        self.vars
            .as_object_mut()
            .expect("just ensured object")
            .insert(name.to_string(), v);
    }

    /// A string beginning `$.` is a path; any other string is a literal. Config reads the
    /// way it looks -- `tenantId: acme` is the value "acme", not a lookup that fails.
    pub fn eval(&self, e: &Expr) -> Result<Value, ExprError> {
        match e {
            Expr::Literal(v) => Ok(v.clone()),
            Expr::Path(p) if p.starts_with("$.") => self.eval_path(p),
            Expr::Path(s) => Ok(Value::String(s.clone())),
            Expr::Optional { optional } => match self.eval(optional) {
                Err(ExprError::Unresolved(_)) => Ok(Value::Null),
                other => other,
            },
            Expr::Length { length } => {
                fn count(v: &Value) -> Result<usize, ExprError> {
                    match v {
                        Value::Array(a) => Ok(a.len()),
                        Value::String(s) => Ok(s.chars().count()),
                        // A value carrying `__len` reports its own length. A transport may
                        // keep a large list outside the scope and leave this marker behind,
                        // so the orchestrator can count what it is passing through without
                        // the list ever being materialised as JSON. Checked BEFORE the object
                        // arm, which would otherwise return the field count.
                        Value::Object(o) => match o.get("__len").and_then(Value::as_u64) {
                            Some(n) => Ok(n as usize),
                            None => Ok(o.len()),
                        },
                        _ => Err(ExprError::NotCountable(format!("{v}"))),
                    }
                }
                // Count what the path POINTS AT rather than a copy of it. The general arm
                // below still handles a literal or a nested length; only the common case --
                // counting a path -- is borrowed, and that is the one that counts token arrays.
                if let Expr::Path(p) = length.as_ref() {
                    if p.starts_with("$.") {
                        return Ok(Value::from(count(self.eval_path_ref(p)?)?));
                    }
                }
                let v = self.eval(length)?;
                Ok(Value::from(count(&v)?))
            }
        }
    }

    /// Removes and returns a one-level subtree, e.g. `$.response.token_ids`.
    ///
    /// The output binding of a non-responding step is the last reader of that field, so
    /// cloning it is pure waste -- and at ISL 4000 the field is a 4,000-element array. Falls
    /// back to `eval_path` for anything deeper than one level, where ownership is not
    /// obviously transferable.
    pub fn take_path(&mut self, path: &str) -> Result<Value, ExprError> {
        let Some(rest) = path.strip_prefix("$.") else {
            return Err(ExprError::NotAPath(path.into()));
        };
        let mut parts = rest.splitn(2, '.');
        let root = parts.next().unwrap_or_default();
        let field = parts.next().unwrap_or("");
        // One level only, and no indexing: `$.response.x`, not `$.response.x.y` or `x[0]`.
        if field.is_empty() || field.contains('.') || field.contains('[') {
            return self.eval_path(path);
        }
        let base = match root {
            "response" => &mut self.response,
            "vars" => &mut self.vars,
            _ => return self.eval_path(path),
        };
        match base.as_object_mut().and_then(|o| o.remove(field)) {
            Some(v) => Ok(v),
            None => Err(ExprError::Unresolved(path.into())),
        }
    }

    /// Borrows the value a path names, without copying it.
    ///
    /// `eval_path` clones, which is right when the caller keeps the value. `{length: ...}`
    /// does not keep it -- it produces a count -- and cloning a 4,000-element array to measure
    /// it was 4.7% of the gateway's CPU.
    pub fn eval_path_ref(&self, path: &str) -> Result<&Value, ExprError> {
        let rest = path
            .strip_prefix("$.")
            .ok_or_else(|| ExprError::NotAPath(path.into()))?;
        let mut parts = rest.splitn(2, '.');
        let root = parts.next().unwrap_or_default();
        let tail = parts.next().unwrap_or("");
        let base = match root {
            "request" => &self.request,
            "response" => &self.response,
            "vars" => &self.vars,
            "item" => &self.item,
            other => return Err(ExprError::UnknownRoot(other.into(), path.into())),
        };
        if tail.is_empty() {
            return Ok(base);
        }
        resolve_ref(base, tail, path)
    }

    pub fn eval_path(&self, path: &str) -> Result<Value, ExprError> {
        let rest = path
            .strip_prefix("$.")
            .ok_or_else(|| ExprError::NotAPath(path.into()))?;
        let mut parts = rest.splitn(2, '.');
        let root = parts.next().unwrap_or_default();
        let tail = parts.next().unwrap_or("");
        let base = match root {
            "request" => &self.request,
            "response" => &self.response,
            "vars" => &self.vars,
            "item" => &self.item,
            other => return Err(ExprError::UnknownRoot(other.into(), path.into())),
        };
        if tail.is_empty() {
            return Ok(base.clone());
        }
        resolve(base, tail, path)
    }
}

fn resolve(base: &Value, tail: &str, full: &str) -> Result<Value, ExprError> {
    resolve_ref(base, tail, full).cloned()
}

/// The same walk, borrowing rather than cloning the value it lands on.
///
/// Split out because `{length: ...}` was measured CLONING what it counts. At ISL 4000 the
/// selector's `isl_tokens` binding counts a 4,000-element token array, and a CPU profile of
/// the running gateway put `Vec<serde_json::Value>::clone` at 4.7% of all samples -- plus its
/// share of the malloc/free/drop traffic around it -- to produce a single integer.
fn resolve_ref<'a>(base: &'a Value, tail: &str, full: &str) -> Result<&'a Value, ExprError> {
    let mut cur = base;
    for seg in tail.split('.') {
        let (name, indices) = split_indices(seg, full)?;
        if !name.is_empty() {
            cur = cur
                .get(name)
                .ok_or_else(|| ExprError::Unresolved(full.into()))?;
        }
        for idx in indices {
            let arr = cur
                .as_array()
                .ok_or_else(|| ExprError::Unresolved(full.into()))?;
            let i = if idx < 0 {
                arr.len()
                    .checked_sub((-idx) as usize)
                    .ok_or_else(|| ExprError::Unresolved(full.into()))?
            } else {
                idx as usize
            };
            cur = arr
                .get(i)
                .ok_or_else(|| ExprError::Unresolved(full.into()))?;
        }
    }
    Ok(cur)
}

/// `messages[-1]` -> ("messages", [-1]); `a[0][2]` -> ("a", [0, 2]).
fn split_indices<'a>(seg: &'a str, full: &str) -> Result<(&'a str, Vec<i64>), ExprError> {
    // BORROWS the name rather than copying it.
    //
    // This allocated a String per segment per evaluation, including for the overwhelmingly
    // common unindexed case. Path evaluation runs on the streaming path -- the projection
    // reads `$.item.text` twice for every chunk -- so at ~8,000 rps x 50 chunks it was over a
    // million allocations a second to name a field. `Value::get` takes `&str`, so nothing
    // downstream wanted the copy.
    let Some(open) = seg.find('[') else {
        return Ok((seg, Vec::new()));
    };
    let name = &seg[..open];
    let mut indices = Vec::new();
    let mut rest = &seg[open..];
    while !rest.is_empty() {
        if !rest.starts_with('[') {
            return Err(ExprError::BadIndex(full.into()));
        }
        let close = rest
            .find(']')
            .ok_or_else(|| ExprError::BadIndex(full.into()))?;
        let n: i64 = rest[1..close]
            .parse()
            .map_err(|_| ExprError::BadIndex(full.into()))?;
        indices.push(n);
        rest = &rest[close + 1..];
    }
    Ok((name, indices))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scope() -> Scope {
        let mut s = Scope::new(json!({
            "body": {"messages": [{"content": "hi"}, {"content": "last one"}], "n": 3}
        }));
        s.response = json!({"token_ids": [1, 2, 3], "nested": {"a": [[9, 8]]}});
        s.set_var("target", json!("http://w7:8080"));
        s
    }

    #[test]
    fn reads_last_array_element() {
        let s = scope();
        assert_eq!(
            s.eval_path("$.request.body.messages[-1].content").unwrap(),
            json!("last one")
        );
    }

    #[test]
    fn reads_response_and_vars() {
        let s = scope();
        assert_eq!(
            s.eval_path("$.response.token_ids").unwrap(),
            json!([1, 2, 3])
        );
        assert_eq!(
            s.eval_path("$.vars.target").unwrap(),
            json!("http://w7:8080")
        );
    }

    #[test]
    fn chained_indices() {
        let s = scope();
        assert_eq!(s.eval_path("$.response.nested.a[0][1]").unwrap(), json!(8));
    }

    #[test]
    fn literals_pass_through() {
        let s = scope();
        assert_eq!(s.eval(&Expr::Literal(json!(64))).unwrap(), json!(64));
    }

    #[test]
    fn unknown_root_is_an_error_not_a_null() {
        let s = scope();
        assert!(matches!(
            s.eval_path("$.bogus.x"),
            Err(ExprError::UnknownRoot(_, _))
        ));
    }

    #[test]
    fn missing_field_is_an_error_not_a_null() {
        // A silent null here would surface as an empty body at the callee, which is the
        // failure mode that cost this project a full debugging round on the detok path.
        let s = scope();
        assert!(matches!(
            s.eval_path("$.request.body.nope"),
            Err(ExprError::Unresolved(_))
        ));
    }

    #[test]
    fn optional_field_is_null_only_when_absent() {
        let s = scope();
        let absent: Expr =
            serde_yaml::from_str(r#"{optional: "$.response.image_tokens"}"#).unwrap();
        assert_eq!(s.eval(&absent).unwrap(), Value::Null);

        let mut with_value = scope();
        with_value.response["image_tokens"] = json!(512);
        assert_eq!(with_value.eval(&absent).unwrap(), json!(512));

        let malformed: Expr =
            serde_yaml::from_str(r#"{optional: "$.response.nested.a[bad]"}"#).unwrap();
        assert!(matches!(s.eval(&malformed), Err(ExprError::BadIndex(_))));
    }
}

#[cfg(test)]
mod length_tests {
    use super::*;
    use serde_json::json;

    fn scope() -> Scope {
        let mut s = Scope::new(json!({"body": {"q": "abc"}}));
        s.set_var("tokenIds", json!([1, 2, 3, 4, 5]));
        s
    }

    #[test]
    fn counts_array_elements() {
        // The case that forced this to exist: the selector wants a token count and rejects
        // the request without one.
        let e: Expr = serde_yaml::from_str(r#"{length: "$.vars.tokenIds"}"#).unwrap();
        assert_eq!(scope().eval(&e).unwrap(), json!(5));
    }

    #[test]
    fn counts_string_characters() {
        let e: Expr = serde_yaml::from_str(r#"{length: "$.request.body.q"}"#).unwrap();
        assert_eq!(scope().eval(&e).unwrap(), json!(3));
    }

    #[test]
    fn a_number_has_no_length_and_says_so() {
        let e: Expr = serde_yaml::from_str(r#"{length: 7}"#).unwrap();
        assert!(matches!(scope().eval(&e), Err(ExprError::NotCountable(_))));
    }

    #[test]
    fn a_plain_object_literal_is_still_a_literal() {
        // `length` is matched before Literal in the untagged enum, so an ordinary object
        // must not be mistaken for it.
        let e: Expr = serde_yaml::from_str(r#"{a: 1}"#).unwrap();
        assert_eq!(scope().eval(&e).unwrap(), json!({"a": 1}));
    }
}
