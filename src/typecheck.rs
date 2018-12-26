use crate::data::{FALSE_VALUE, TRUE_VALUE};
use crate::operator::{AggregateOperator, Expr, TypeError, UnaryPreAggOperator, Where};

pub fn create_where(expr: Expr) -> Result<Box<UnaryPreAggOperator>, TypeError> {
    match expr {
        Expr::Comparison(binop) => Ok(Box::new(Where::new(binop))),
        Expr::Value(t) if t == TRUE_VALUE => Ok(Box::new(Where::new(true))),
        Expr::Value(t) if t == FALSE_VALUE => Ok(Box::new(Where::new(false))),
        other => Err(TypeError::ExpectedBool {
            found: format!("{:?}", other),
        }),
    }
}

pub fn create_where_adapt(expr: Expr) -> Result<Box<AggregateOperator>, TypeError> {
    match expr {
        Expr::Comparison(binop) => Ok(Where::new(binop).into()),
        Expr::Value(t) if t == TRUE_VALUE => Ok(Where::new(true).into()),
        Expr::Value(t) if t == FALSE_VALUE => Ok(Where::new(false).into()),
        other => Err(TypeError::ExpectedBool {
            found: format!("{:?}", other),
        }),
    }
}
