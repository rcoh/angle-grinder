use crate::data::Value;
use crate::errors::ErrorBuilder;
use crate::lang;
use crate::operator::{
    average, count, count_distinct, expr, fields, limit, max, min, parse, percentile, split, sum,
    timeslice, total, where_op,
};
use crate::{funcs, operator};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TypeError {
    #[error("Expected boolean expression, found {}", found)]
    ExpectedBool { found: String },

    #[error("Expected an expression")]
    ExpectedExpr,

    #[error(
        "Wrong number of patterns for parse. Pattern has {} but {} were extracted",
        pattern,
        extracted
    )]
    ParseNumPatterns { pattern: usize, extracted: usize },

    #[error("Two `from` clauses were provided")]
    DoubleFromClause,

    #[error("Limit must be a non-zero integer, found {}", limit)]
    InvalidLimit { limit: f64 },

    #[error("Unknown function {}", name)]
    UnknownFunction { name: String },

    #[error("Expected a duration for the timeslice (e.g. 1h)")]
    ExpectedDuration,
}

pub trait TypeCheck<O> {
    fn type_check<E: ErrorBuilder>(self, error_builder: &E) -> Result<O, TypeError>;
}

impl TypeCheck<expr::BoolExpr> for lang::ComparisonOp {
    fn type_check<E: ErrorBuilder>(self, _error_builder: &E) -> Result<expr::BoolExpr, TypeError> {
        match self {
            lang::ComparisonOp::Eq => Ok(expr::BoolExpr::Eq),
            lang::ComparisonOp::Neq => Ok(expr::BoolExpr::Neq),
            lang::ComparisonOp::Gt => Ok(expr::BoolExpr::Gt),
            lang::ComparisonOp::Lt => Ok(expr::BoolExpr::Lt),
            lang::ComparisonOp::Gte => Ok(expr::BoolExpr::Gte),
            lang::ComparisonOp::Lte => Ok(expr::BoolExpr::Lte),
        }
    }
}

impl TypeCheck<expr::ArithmeticExpr> for lang::ArithmeticOp {
    fn type_check<E: ErrorBuilder>(
        self,
        _error_builder: &E,
    ) -> Result<expr::ArithmeticExpr, TypeError> {
        match self {
            lang::ArithmeticOp::Add => Ok(expr::ArithmeticExpr::Add),
            lang::ArithmeticOp::Subtract => Ok(expr::ArithmeticExpr::Subtract),
            lang::ArithmeticOp::Multiply => Ok(expr::ArithmeticExpr::Multiply),
            lang::ArithmeticOp::Divide => Ok(expr::ArithmeticExpr::Divide),
        }
    }
}

impl TypeCheck<expr::LogicalExpr> for lang::LogicalOp {
    fn type_check<E: ErrorBuilder>(
        self,
        _error_builder: &E,
    ) -> Result<expr::LogicalExpr, TypeError> {
        match self {
            lang::LogicalOp::And => Ok(expr::LogicalExpr::And),
            lang::LogicalOp::Or => Ok(expr::LogicalExpr::Or),
        }
    }
}

impl TypeCheck<operator::Expr> for lang::Expr {
    fn type_check<E: ErrorBuilder>(self, error_builder: &E) -> Result<operator::Expr, TypeError> {
        match self {
            lang::Expr::Column { head, rest } => {
                let head = match head {
                    lang::DataAccessAtom::Key(s) => s,
                    lang::DataAccessAtom::Index(_) => return Err(TypeError::ExpectedExpr),
                };
                let rest = rest
                    .iter()
                    .map(|s| match s {
                        lang::DataAccessAtom::Key(s) => expr::ValueRef::Field(s.to_string()),
                        lang::DataAccessAtom::Index(i) => expr::ValueRef::IndexAt(*i),
                    })
                    .collect();

                Ok(operator::Expr::NestedColumn { head, rest })
            }
            lang::Expr::Unary { op, operand } => match op {
                lang::UnaryOp::Not => Ok(operator::Expr::BoolUnary(expr::UnaryExpr {
                    operator: expr::BoolUnaryExpr::Not,
                    operand: Box::new((*operand).type_check(error_builder)?),
                })),
            },
            lang::Expr::Binary { op, left, right } => match op {
                lang::BinaryOp::Comparison(com_op) => {
                    Ok(operator::Expr::Comparison(expr::BinaryExpr::<
                        expr::BoolExpr,
                    > {
                        left: Box::new((*left).type_check(error_builder)?),
                        right: Box::new((*right).type_check(error_builder)?),
                        operator: com_op.type_check(error_builder)?,
                    }))
                }
                lang::BinaryOp::Arithmetic(arith_op) => {
                    Ok(operator::Expr::Arithmetic(expr::BinaryExpr::<
                        expr::ArithmeticExpr,
                    > {
                        left: Box::new((*left).type_check(error_builder)?),
                        right: Box::new((*right).type_check(error_builder)?),
                        operator: arith_op.type_check(error_builder)?,
                    }))
                }
                lang::BinaryOp::Logical(logical_op) => {
                    Ok(operator::Expr::Logical(expr::BinaryExpr::<
                        expr::LogicalExpr,
                    > {
                        left: Box::new((*left).type_check(error_builder)?),
                        right: Box::new((*right).type_check(error_builder)?),
                        operator: logical_op.type_check(error_builder)?,
                    }))
                }
            },
            lang::Expr::FunctionCall { name, args } => {
                let converted_args: Result<Vec<operator::Expr>, TypeError> = args
                    .into_iter()
                    .map(|arg| arg.type_check(error_builder))
                    .collect();
                if let Some(func) = funcs::FUNC_MAP.get(name.as_str()) {
                    Ok(operator::Expr::FunctionCall {
                        func,
                        args: converted_args?,
                    })
                } else {
                    Err(TypeError::UnknownFunction { name })
                }
            }
            lang::Expr::IfOp {
                cond,
                value_if_true,
                value_if_false,
            } => Ok(operator::Expr::IfOp {
                cond: Box::new(cond.type_check(error_builder)?),
                value_if_true: Box::new(value_if_true.type_check(error_builder)?),
                value_if_false: Box::new(value_if_false.type_check(error_builder)?),
            }),
            lang::Expr::Value(value) => {
                let boxed = Box::new(value);
                let static_value: &'static mut Value = Box::leak(boxed);
                Ok(operator::Expr::Value(static_value))
            }
            lang::Expr::Error => Err(TypeError::ExpectedExpr),
        }
    }
}

const DEFAULT_LIMIT: i64 = 10;

impl TypeCheck<Box<dyn operator::OperatorBuilder + Send + Sync>>
    for lang::Positioned<lang::InlineOperator>
{
    /// Convert the operator syntax to a builder that can instantiate an operator for the
    /// pipeline.  Any semantic errors in the operator syntax should be detected here.
    fn type_check<T: ErrorBuilder>(
        self,
        error_builder: &T,
    ) -> Result<Box<dyn operator::OperatorBuilder + Send + Sync>, TypeError> {
        match self.value {
            lang::InlineOperator::Json { input_column } => Ok(Box::new(parse::ParseJson::new(
                input_column
                    .map(|e| e.type_check(error_builder))
                    .transpose()?,
            ))),
            lang::InlineOperator::Logfmt { input_column } => Ok(Box::new(parse::ParseLogfmt::new(
                input_column
                    .map(|e| e.type_check(error_builder))
                    .transpose()?,
            ))),
            lang::InlineOperator::Parse {
                pattern,
                fields,
                input_column,
                no_drop,
                no_convert,
            } => {
                let regex = pattern.to_regex();

                let input_column = match input_column {
                    (Some(from), None) | (None, Some(from)) => Some(from.value),
                    (None, None) => None,
                    (Some(l), Some(r)) => {
                        let e = TypeError::DoubleFromClause;
                        error_builder
                            .report_error_for(&e)
                            .with_code_pointer(&l, "")
                            .with_code_pointer(&r, "")
                            .with_resolution("Only one from clause is allowed")
                            .send_report();
                        return Err(e);
                    }
                };

                if (regex.captures_len() - 1) != fields.len() {
                    Err(TypeError::ParseNumPatterns {
                        pattern: regex.captures_len() - 1,
                        extracted: fields.len(),
                    })
                } else {
                    Ok(Box::new(parse::Parse::new(
                        regex,
                        fields,
                        input_column
                            .map(|e| e.type_check(error_builder))
                            .transpose()?,
                        parse::ParseOptions {
                            drop_nonmatching: !no_drop,
                            no_conversion: no_convert,
                        },
                    )))
                }
            }
            lang::InlineOperator::Fields { fields, mode } => {
                let omode = match mode {
                    lang::FieldMode::Except => fields::FieldMode::Except,
                    lang::FieldMode::Only => fields::FieldMode::Only,
                };
                Ok(Box::new(fields::Fields::new(&fields, omode)))
            }
            lang::InlineOperator::Where { expr: Some(expr) } => match expr
                .value
                .type_check(error_builder)?
            {
                operator::Expr::Value(constant) => {
                    if let Value::Bool(bool_value) = constant {
                        Ok(Box::new(where_op::Where::new(*bool_value)))
                    } else {
                        let e = TypeError::ExpectedBool {
                            found: format!("{:?}", constant),
                        };

                        error_builder
                            .report_error_for(&e)
                            .with_code_range(expr.range, "This is constant")
                            .with_resolution("Perhaps you meant to compare a field to this value?")
                            .with_resolution(format!("example: where field1 == {}", constant))
                            .send_report();

                        Err(e)
                    }
                }
                generic_expr => Ok(Box::new(where_op::Where::new(generic_expr))),
            },
            lang::InlineOperator::Where { expr: None } => {
                let e = TypeError::ExpectedExpr;

                error_builder
                    .report_error_for(&e)
                    .with_code_pointer(&self, "No condition provided for this 'where'")
                    .with_resolution(
                        "Insert an expression whose result determines whether a record should be \
                         passed downstream",
                    )
                    .with_resolution("example: where duration > 100")
                    .send_report();

                Err(e)
            }
            lang::InlineOperator::Limit { count: Some(count) } => match count.value {
                limit if limit.trunc() == 0.0 || limit.fract() != 0.0 => {
                    let e = TypeError::InvalidLimit { limit };

                    error_builder
                        .report_error_for(e.to_string())
                        .with_code_pointer(
                            &count,
                            if limit.fract() != 0.0 {
                                "Fractional limits are not allowed"
                            } else {
                                "Zero is not allowed"
                            },
                        )
                        .with_resolution("Use a positive integer to select the first N rows")
                        .with_resolution("Use a negative integer to select the last N rows")
                        .send_report();

                    Err(e)
                }
                limit => Ok(Box::new(limit::LimitDef::new(limit as i64))),
            },
            lang::InlineOperator::Limit { count: None } => {
                Ok(Box::new(limit::LimitDef::new(DEFAULT_LIMIT)))
            }
            lang::InlineOperator::Split {
                separator,
                input_column,
                output_column,
            } => Ok(Box::new(split::Split::new(
                separator,
                input_column
                    .map(|e| e.type_check(error_builder))
                    .transpose()?,
                output_column
                    .map(|e| e.type_check(error_builder))
                    .transpose()?,
            ))),
            lang::InlineOperator::Timeslice { duration: None, .. } => {
                Err(TypeError::ExpectedDuration)
            }
            lang::InlineOperator::Timeslice {
                input_column,
                duration: Some(duration),
                output_column,
            } => Ok(Box::new(timeslice::Timeslice::new(
                input_column.type_check(error_builder)?,
                duration,
                output_column,
            ))),
            lang::InlineOperator::Total {
                input_column,
                output_column,
            } => Ok(Box::new(total::TotalDef::new(
                input_column.type_check(error_builder)?,
                output_column,
            ))),
            lang::InlineOperator::FieldExpression { value, name } => Ok(Box::new(
                fields::FieldExpressionDef::new(value.type_check(error_builder)?, name),
            )),
        }
    }
}

impl TypeCheck<Box<dyn operator::AggregateFunction>> for lang::Positioned<lang::AggregateFunction> {
    fn type_check<T: ErrorBuilder>(
        self,
        error_builder: &T,
    ) -> Result<Box<dyn operator::AggregateFunction>, TypeError> {
        match self.value {
            lang::AggregateFunction::Count { condition } => {
                let expr = condition.map(|c| c.type_check(error_builder)).transpose()?;
                Ok(Box::new(count::Count::new(expr)))
            }
            lang::AggregateFunction::Min { column } => {
                Ok(Box::new(min::Min::empty(column.type_check(error_builder)?)))
            }
            lang::AggregateFunction::Average { column } => Ok(Box::new(average::Average::empty(
                column.type_check(error_builder)?,
            ))),
            lang::AggregateFunction::Max { column } => {
                Ok(Box::new(max::Max::empty(column.type_check(error_builder)?)))
            }
            lang::AggregateFunction::Sum { column } => {
                Ok(Box::new(sum::Sum::empty(column.type_check(error_builder)?)))
            }
            lang::AggregateFunction::Percentile {
                column, percentile, ..
            } => Ok(Box::new(percentile::Percentile::empty(
                column.type_check(error_builder)?,
                percentile,
            ))),
            lang::AggregateFunction::CountDistinct { column: Some(pos) } => {
                match pos.value.as_slice() {
                    [column] => Ok(Box::new(count_distinct::CountDistinct::empty(
                        column.clone().type_check(error_builder)?,
                    ))),
                    _ => {
                        error_builder
                            .report_error_for("Expecting a single expression to count")
                            .with_code_pointer(
                                &pos,
                                match pos.value.len() {
                                    0 => "No expression given",
                                    _ => "Only a single expression can be given",
                                },
                            )
                            .with_resolution("example: count_distinct(field_to_count)")
                            .send_report();

                        Err(TypeError::ExpectedExpr)
                    }
                }
            }
            lang::AggregateFunction::CountDistinct { column: None } => {
                error_builder
                    .report_error_for("Expecting an expression to count")
                    .with_code_pointer(&self, "No field argument given")
                    .with_resolution("example: count_distinct(field_to_count)")
                    .send_report();

                Err(TypeError::ExpectedExpr)
            }
            lang::AggregateFunction::Error => unreachable!(),
        }
    }
}
