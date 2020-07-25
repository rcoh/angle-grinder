use crate::data::Value;
use crate::errors::ErrorBuilder;
use crate::lang;
use crate::operator;

#[derive(Debug, Fail)]
pub enum TypeError {
    #[fail(display = "Expected boolean expression, found {}", found)]
    ExpectedBool { found: String },

    #[fail(display = "Expected an expression")]
    ExpectedExpr,

    #[fail(
        display = "Wrong number of patterns for parse. Pattern has {} but {} were extracted",
        pattern, extracted
    )]
    ParseNumPatterns { pattern: usize, extracted: usize },

    #[fail(display = "Two `from` clauses were provided")]
    DoubleFromClause,

    #[fail(display = "Limit must be a non-zero integer, found {}", limit)]
    InvalidLimit { limit: f64 },
}

pub trait TypeCheck<O> {
    fn type_check<E: ErrorBuilder>(self, error_builder: &E) -> Result<O, TypeError>;
}

impl From<lang::ComparisonOp> for operator::BoolExpr {
    fn from(op: lang::ComparisonOp) -> Self {
        match op {
            lang::ComparisonOp::Eq => operator::BoolExpr::Eq,
            lang::ComparisonOp::Neq => operator::BoolExpr::Neq,
            lang::ComparisonOp::Gt => operator::BoolExpr::Gt,
            lang::ComparisonOp::Lt => operator::BoolExpr::Lt,
            lang::ComparisonOp::Gte => operator::BoolExpr::Gte,
            lang::ComparisonOp::Lte => operator::BoolExpr::Lte,
        }
    }
}

impl From<lang::ArithmeticOp> for operator::ArithmeticExpr {
    fn from(op: lang::ArithmeticOp) -> Self {
        match op {
            lang::ArithmeticOp::Add => operator::ArithmeticExpr::Add,
            lang::ArithmeticOp::Subtract => operator::ArithmeticExpr::Subtract,
            lang::ArithmeticOp::Multiply => operator::ArithmeticExpr::Multiply,
            lang::ArithmeticOp::Divide => operator::ArithmeticExpr::Divide,
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
                        lang::DataAccessAtom::Key(s) => operator::ValueRef::Field(s.to_string()),
                        lang::DataAccessAtom::Index(i) => operator::ValueRef::IndexAt(*i),
                    })
                    .collect();

                Ok(operator::Expr::NestedColumn { head, rest })
            }
            lang::Expr::Unary { op, operand } => match op {
                lang::UnaryOp::Not => Ok(operator::Expr::BoolUnary(operator::UnaryExpr {
                    operator: operator::BoolUnaryExpr::Not,
                    operand: Box::new((*operand).type_check(error_builder)?),
                })),
            },
            lang::Expr::Binary { op, left, right } => match op {
                lang::BinaryOp::Comparison(com_op) => {
                    Ok(operator::Expr::Comparison(operator::BinaryExpr::<
                        operator::BoolExpr,
                    > {
                        left: Box::new((*left).type_check(error_builder)?),
                        right: Box::new((*right).type_check(error_builder)?),
                        operator: com_op.into(),
                    }))
                }
                lang::BinaryOp::Arithmetic(arith_op) => {
                    Ok(operator::Expr::Arithmetic(operator::BinaryExpr::<
                        operator::ArithmeticExpr,
                    > {
                        left: Box::new((*left).type_check(error_builder)?),
                        right: Box::new((*right).type_check(error_builder)?),
                        operator: arith_op.into(),
                    }))
                }
            },
            lang::Expr::Value(value) => {
                let boxed = Box::new(value);
                let static_value: &'static mut Value = Box::leak(boxed);
                Ok(operator::Expr::Value(static_value))
            }
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
            lang::InlineOperator::Json { input_column } => Ok(Box::new(operator::ParseJson::new(
                input_column
                    .map(|e| e.type_check(error_builder))
                    .transpose()?,
            ))),
            lang::InlineOperator::Logfmt { input_column } => {
                Ok(Box::new(operator::ParseLogfmt::new(
                    input_column
                        .map(|e| e.type_check(error_builder))
                        .transpose()?,
                )))
            }
            lang::InlineOperator::Parse {
                pattern,
                fields,
                input_column,
                no_drop,
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
                    Ok(Box::new(operator::Parse::new(
                        regex,
                        fields,
                        input_column
                            .map(|e| e.type_check(error_builder))
                            .transpose()?,
                        operator::ParseOptions {
                            drop_nonmatching: !no_drop,
                        },
                    )))
                }
            }
            lang::InlineOperator::Fields { fields, mode } => {
                let omode = match mode {
                    lang::FieldMode::Except => operator::FieldMode::Except,
                    lang::FieldMode::Only => operator::FieldMode::Only,
                };
                Ok(Box::new(operator::Fields::new(&fields, omode)))
            }
            lang::InlineOperator::Where { expr: Some(expr) } => match expr
                .value
                .type_check(error_builder)?
            {
                operator::Expr::Value(constant) => {
                    if let Value::Bool(bool_value) = constant {
                        Ok(Box::new(operator::Where::new(*bool_value)))
                    } else {
                        let e = TypeError::ExpectedBool {
                            found: format!("{:?}", constant),
                        };

                        error_builder
                            .report_error_for(&e)
                            .with_code_range(expr.start_pos, expr.end_pos, "This is constant")
                            .with_resolution("Perhaps you meant to compare a field to this value?")
                            .with_resolution(format!("example: where field1 == {}", constant))
                            .send_report();

                        Err(e)
                    }
                }
                generic_expr => Ok(Box::new(operator::Where::new(generic_expr))),
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
                limit => Ok(Box::new(operator::LimitDef::new(limit as i64))),
            },
            lang::InlineOperator::Limit { count: None } => {
                Ok(Box::new(operator::LimitDef::new(DEFAULT_LIMIT)))
            }
            lang::InlineOperator::Split {
                separator,
                input_column,
                output_column,
            } => Ok(Box::new(operator::Split::new(
                separator,
                input_column
                    .map(|e| e.type_check(error_builder))
                    .transpose()?,
                output_column
                    .map(|e| e.type_check(error_builder))
                    .transpose()?,
            ))),
            lang::InlineOperator::Total {
                input_column,
                output_column,
            } => Ok(Box::new(operator::TotalDef::new(
                input_column.type_check(error_builder)?,
                output_column,
            ))),
            lang::InlineOperator::FieldExpression { value, name } => Ok(Box::new(
                operator::FieldExpressionDef::new(value.type_check(error_builder)?, name),
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
            lang::AggregateFunction::Count => Ok(Box::new(operator::Count::new())),
            lang::AggregateFunction::Min { column } => Ok(Box::new(operator::Min::empty(
                column.type_check(error_builder)?,
            ))),
            lang::AggregateFunction::Average { column } => Ok(Box::new(operator::Average::empty(
                column.type_check(error_builder)?,
            ))),
            lang::AggregateFunction::Max { column } => Ok(Box::new(operator::Max::empty(
                column.type_check(error_builder)?,
            ))),
            lang::AggregateFunction::Sum { column } => Ok(Box::new(operator::Sum::empty(
                column.type_check(error_builder)?,
            ))),
            lang::AggregateFunction::Percentile {
                column, percentile, ..
            } => Ok(Box::new(operator::Percentile::empty(
                column.type_check(error_builder)?,
                percentile,
            ))),
            lang::AggregateFunction::CountDistinct { column: Some(pos) } => {
                match pos.value.as_slice() {
                    [column] => Ok(Box::new(operator::CountDistinct::empty(
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
        }
    }
}
