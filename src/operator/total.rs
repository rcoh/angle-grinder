use crate::data;
use crate::data::Record;
use crate::operator::expr::Expr;
use crate::operator::{EvalError, Evaluate, OperatorBuilder, UnaryPreAggOperator};

pub struct TotalDef {
    column: Expr,
    output_column: String,
}

impl TotalDef {
    pub fn new(column: Expr, output_column: String) -> Self {
        TotalDef {
            column,
            output_column,
        }
    }
}

impl OperatorBuilder for TotalDef {
    fn build(&self) -> Box<dyn UnaryPreAggOperator> {
        Box::new(Total::new(self.column.clone(), self.output_column.clone()))
    }
}

pub struct Total {
    column: Expr,
    total: f64,
    output_column: String,
}

impl Total {
    pub fn new<T: Into<Expr>>(column: T, output_column: String) -> Total {
        Total {
            column: column.into(),
            total: 0.0,
            output_column,
        }
    }
}

impl UnaryPreAggOperator for Total {
    fn process_mut(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        // I guess this means there are cases when you need to both emit a warning _and_ a row, TODO
        // for now, we'll just emit the row
        let val: f64 = self.column.eval(&rec.data).unwrap_or(0.0);
        self.total += val;
        let rec = rec.put(&self.output_column, data::Value::from_float(self.total));
        Ok(Some(rec))
    }
}
