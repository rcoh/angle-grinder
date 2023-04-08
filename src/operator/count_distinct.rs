use crate::data;
use crate::operator::expr::Expr;
use crate::operator::{AggregateFunction, Data, EvalError};
use std::collections::HashSet;

pub struct CountDistinct {
    state: HashSet<data::Value>,
    column: Expr,
}

impl CountDistinct {
    pub fn empty<T: Into<Expr>>(column: T) -> Self {
        CountDistinct {
            state: HashSet::new(),
            column: column.into(),
        }
    }
}

impl AggregateFunction for CountDistinct {
    fn process(&mut self, rec: &Data) -> Result<(), EvalError> {
        let value = self.column.eval_value(rec)?.into_owned();
        self.state.insert(value);
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::Int(self.state.len() as i64)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(CountDistinct::empty(self.column.clone()))
    }
}
