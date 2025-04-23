use crate::data;
use crate::operator::{AggregateFunction, Data, EvalError, Evaluate, Expr};

pub struct Min {
    min: f64,
    column: Expr,
}

impl Min {
    pub fn empty<T: Into<Expr>>(column: T) -> Min {
        Min {
            min: f64::INFINITY,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Min {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        if value < self.min {
            self.min = value;
        }
        Ok(())
    }

    fn emit(&self) -> data::Value {
        if self.min.is_finite() {
            data::Value::from_float(self.min)
        } else {
            data::Value::None
        }
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Min::empty(self.column.clone()))
    }
}
