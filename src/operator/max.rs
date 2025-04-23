use crate::data;
use crate::operator::{AggregateFunction, Data, EvalError, Evaluate, Expr};

pub struct Max {
    max: f64,
    column: Expr,
}

impl Max {
    pub fn empty<T: Into<Expr>>(column: T) -> Max {
        Max {
            max: f64::NEG_INFINITY,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Max {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        if value > self.max {
            self.max = value;
        }
        Ok(())
    }

    fn emit(&self) -> data::Value {
        if self.max.is_finite() {
            data::Value::from_float(self.max)
        } else {
            data::Value::None
        }
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Max::empty(self.column.clone()))
    }
}
