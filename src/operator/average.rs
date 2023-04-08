use crate::data;
use crate::operator::{AggregateFunction, Data, EvalError, Evaluate, Expr};

pub struct Average {
    total: f64,
    count: i64,
    column: Expr,
}

impl Average {
    pub fn empty<T: Into<Expr>>(column: T) -> Average {
        Average {
            total: 0.0,
            count: 0,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Average {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        self.total += value;
        self.count += 1;
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::from_float(self.total / self.count as f64)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Average::empty(self.column.clone()))
    }
}
