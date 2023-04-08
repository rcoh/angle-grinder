use crate::data;
use crate::operator::{AggregateFunction, Data, EvalError, Evaluate, Expr};
use quantiles::ckms::CKMS;

impl Percentile {
    pub fn empty<T: Into<Expr>>(column: T, percentile: f64) -> Self {
        if percentile >= 1.0 {
            panic!("Percentiles must be < 1");
        }

        Percentile {
            ckms: CKMS::<f64>::new(0.001),
            column: column.into(),
            percentile,
        }
    }
}

pub struct Percentile {
    ckms: CKMS<f64>,
    column: Expr,
    percentile: f64,
}

impl AggregateFunction for Percentile {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        self.ckms.insert(value);
        Ok(())
    }

    fn emit(&self) -> data::Value {
        let pct_opt = self.ckms.query(self.percentile);
        pct_opt
            .map(|(_usize, pct_float)| data::Value::from_float(pct_float))
            .unwrap_or(data::Value::None)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Percentile::empty(self.column.clone(), self.percentile))
    }
}
