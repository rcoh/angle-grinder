use operator::{Expr, TypeError, UnaryPreAggOperator, Where};

pub fn create_where(expr: Expr) -> Result<Box<UnaryPreAggOperator>, TypeError> {
    match expr {
        Expr::Comparison(binop) => Ok(Box::new(Where::new(binop))),
        other => Err(TypeError::ExpectedBool {
            found: format!("{:?}", other),
        }),
    }
}
