use crate::data;
use crate::operator::expr::Expr;
use crate::operator::{AggregateFunction, Data, EvalError, Evaluate};

pub struct Sum {
    total: f64,
    column: Expr,
}

impl Sum {
    pub fn empty<T: Into<Expr>>(column: T) -> Self {
        Sum {
            total: 0.0,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Sum {
    fn process(&mut self, rec: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(rec)?;
        self.total += value;
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::from_float(self.total)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Sum::empty(self.column.clone()))
    }
}
