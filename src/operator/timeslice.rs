use crate::data;
use crate::data::Record;
use crate::operator::{EvalError, Expr, UnaryPreAggFunction};
use chrono::DurationRound;

#[derive(Clone)]
pub struct Timeslice {
    input_column: Expr,
    duration: chrono::Duration,
    output_column: Option<String>,
}

impl Timeslice {
    pub fn new(
        input_column: Expr,
        duration: chrono::Duration,
        output_column: Option<String>,
    ) -> Self {
        Self {
            input_column,
            duration,
            output_column,
        }
    }
}

impl UnaryPreAggFunction for Timeslice {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let inp = self.input_column.eval_value(&rec.data)?;

        match inp.as_ref() {
            data::Value::DateTime(dt) => {
                let rounded =
                    dt.duration_trunc(self.duration)
                        .map_err(|e| EvalError::InvalidDuration {
                            error: format!("{:?}", e),
                        })?;
                let rec = rec.put(
                    self.output_column
                        .clone()
                        .unwrap_or_else(|| "_timeslice".to_string()),
                    data::Value::DateTime(rounded),
                );

                Ok(Some(rec))
            }
            _ => Err(EvalError::ExpectedDate {
                found: inp.to_string(),
            }),
        }
    }
}
