use crate::data::Record;
use crate::operator::expr::Expr;
use crate::operator::{EvalError, UnaryPreAggFunction};
use crate::{data, operator};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Parse {
    regex: regex::Regex,
    fields: Vec<String>,
    input_column: Option<Expr>,
    options: ParseOptions,
}

impl Parse {
    pub fn new(
        pattern: regex::Regex,
        fields: Vec<String>,
        input_column: Option<Expr>,
        options: ParseOptions,
    ) -> Self {
        Parse {
            regex: pattern,
            fields,
            input_column,
            options,
        }
    }

    fn matches(&self, rec: &Record) -> Result<Option<Vec<data::Value>>, EvalError> {
        let inp = operator::get_input(rec, &self.input_column)?;
        match self.regex.captures_iter(inp.trim()).next() {
            None => Ok(None),
            Some(capture) => {
                let mut values: Vec<data::Value> = Vec::with_capacity(self.fields.len());
                for i in 0..self.fields.len() {
                    // the first capture is the entire string
                    values.push(data::Value::from_string(&capture[i + 1]));
                }
                Ok(Some(values))
            }
        }
    }
}

impl UnaryPreAggFunction for Parse {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let matches = self.matches(&rec)?;
        match (matches, self.options.drop_nonmatching) {
            (None, true) => Ok(None),
            (None, false) => {
                let new_fields: Vec<_> = self
                    .fields
                    .iter()
                    .filter(|f| !rec.data.contains_key(*f))
                    .collect();

                let mut rec = rec;
                for field in new_fields {
                    rec = rec.put(field, data::Value::None);
                }
                Ok(Some(rec))
            }
            (Some(matches), _) => {
                let mut rec = rec;
                for (field, value) in self.fields.iter().zip(matches.into_iter()) {
                    rec = rec.put(field, value);
                }
                Ok(Some(rec))
            }
        }
    }
}

#[derive(Clone)]
pub struct ParseOptions {
    pub drop_nonmatching: bool,
}

#[derive(Clone)]
pub struct ParseJson {
    input_column: Option<Expr>,
}

impl ParseJson {
    pub fn new(input_column: Option<Expr>) -> ParseJson {
        ParseJson { input_column }
    }
}

impl UnaryPreAggFunction for ParseJson {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        fn json_to_value(v: JsonValue) -> data::Value {
            match v {
                JsonValue::Number(num) => {
                    if num.is_i64() {
                        data::Value::Int(num.as_i64().unwrap())
                    } else {
                        data::Value::from_float(num.as_f64().unwrap())
                    }
                }
                JsonValue::String(s) => data::Value::Str(s),
                JsonValue::Null => data::Value::None,
                JsonValue::Bool(b) => data::Value::Bool(b),
                JsonValue::Object(map) => data::Value::Obj(
                    map.into_iter()
                        .map(|(k, v)| (k, json_to_value(v)))
                        .collect::<HashMap<String, data::Value>>()
                        .into(),
                ),
                JsonValue::Array(vec) => data::Value::Array(
                    vec.into_iter()
                        .map(json_to_value)
                        .collect::<Vec<data::Value>>(),
                ),
            }
        }
        let json: JsonValue = {
            let inp = operator::get_input(&rec, &self.input_column)?;
            serde_json::from_str(&inp).map_err(|_| EvalError::ExpectedJson {
                found: inp.trim_end().to_string(),
            })?
        };
        let res = match json {
            JsonValue::Object(map) => {
                let mut rec = rec;
                rec.data.reserve(map.len());
                for (k, v) in map {
                    rec.put_mut(k, json_to_value(v));
                }
                rec
            }
            // TODO: we'll implicitly drop non-object root values. Maybe we should produce an EvalError here
            _other => rec,
        };
        Ok(Some(res))
    }
}

#[derive(Clone)]
pub struct ParseLogfmt {
    input_column: Option<Expr>,
}

impl ParseLogfmt {
    pub fn new(input_column: Option<Expr>) -> ParseLogfmt {
        ParseLogfmt { input_column }
    }
}

impl UnaryPreAggFunction for ParseLogfmt {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let pairs = {
            let inp = operator::get_input(&rec, &self.input_column)?;
            // Record includes the trailing newline, while logfmt considers that part of the
            // message if present. Trim any trailing whitespace.
            logfmt::parse(inp.trim_end())
        };
        let res = {
            pairs.into_iter().fold(rec, |record, pair| match pair.val {
                None => record.put(&pair.key, data::Value::None),
                Some(val) => record.put(&pair.key, data::Value::from_string(val)),
            })
        };
        Ok(Some(res))
    }
}
