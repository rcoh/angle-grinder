use crate::data::Record;
use crate::operator::{EvalError, Expr, UnaryPreAggFunction};
use std::collections::HashSet;
use std::iter::FromIterator;

#[derive(Clone)]
pub struct FieldExpressionDef {
    value: Expr,
    name: String,
}

impl FieldExpressionDef {
    pub fn new(value: Expr, name: String) -> FieldExpressionDef {
        FieldExpressionDef { value, name }
    }
}

impl UnaryPreAggFunction for FieldExpressionDef {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let res = self.value.eval_value(&rec.data)?.into_owned();

        Ok(Some(rec.put(&self.name, res)))
    }
}

impl UnaryPreAggFunction for Fields {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let mut rec = rec;
        match self.mode {
            FieldMode::Only => {
                rec.data.retain(|k, _| self.columns.contains(k));
            }
            FieldMode::Except => {
                rec.data.retain(|k, _| !self.columns.contains(k));
            }
        }
        if rec.data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rec))
        }
    }
}

#[derive(Clone)]
pub struct Fields {
    columns: HashSet<String>,
    mode: FieldMode,
}

impl Fields {
    pub fn new(columns: &[String], mode: FieldMode) -> Self {
        let columns = HashSet::from_iter(columns.iter().cloned());
        Fields { columns, mode }
    }
}

#[derive(Clone)]
pub enum FieldMode {
    Only,
    Except,
}
