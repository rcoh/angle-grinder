use operator::{Expr, TypeError, UnaryPreAggOperator, AggregateOperator, Where};

pub fn create_where(expr: Expr) -> Result<Box<UnaryPreAggOperator>, TypeError> {
    match expr {
        Expr::Comparison(binop) => Ok(Box::new(Where::new(binop))),
        other => Err(TypeError::ExpectedBool {
            found: format!("{:?}", other),
        }),
    }
}

pub fn create_where_adapt(expr: Expr) -> Result<Box<AggregateOperator>, TypeError> {
    match expr {
        Expr::Comparison(binop) => Ok(Where::new(binop).into()),
        other => Err(TypeError::ExpectedBool {
            found: format!("{:?}", other),
        }),
    }
}
