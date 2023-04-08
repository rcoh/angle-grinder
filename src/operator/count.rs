use crate::data;
use crate::operator::{AggregateFunction, Data, EvalError, Evaluate};

#[derive(Default)]
pub struct Count<T> {
    count: i64,
    condition: Option<T>,
}

impl<T> Count<T> {
    pub fn new(condition: Option<T>) -> Count<T> {
        Count {
            count: 0,
            condition,
        }
    }
}

impl<T: 'static + Send + Sync + Evaluate<bool>> AggregateFunction for Count<T> {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let count_record = match &self.condition {
            Some(cond) => cond.eval(data)?,
            None => true,
        };
        if count_record {
            self.count += 1;
        }
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::Int(self.count)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Count::new(self.condition.clone()))
    }
}

#[cfg(test)]
mod test {
    use super::Count;
    use crate::operator::expr::Expr;
    impl Count<Expr> {
        pub fn unconditional() -> Count<Expr> {
            Count {
                count: 0,
                condition: None,
            }
        }
    }
}
