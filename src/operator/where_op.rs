use crate::data::Record;
use crate::operator::{EvalError, Evaluate, UnaryPreAggFunction};

#[derive(Clone)]
pub struct Where<T> {
    expr: T,
}

impl<T> Where<T> {
    pub fn new(expr: T) -> Self {
        Where { expr }
    }
}

impl<T: Evaluate<bool>> UnaryPreAggFunction for Where<T> {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        if self.expr.eval(&rec.data)? {
            Ok(Some(rec))
        } else {
            Ok(None)
        }
    }
}
