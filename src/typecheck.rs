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

    #[fail(display = "Limit must be a non-zero integer, found {}", limit)]
    InvalidLimit { limit: f64 },
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

impl From<lang::Expr> for operator::Expr {
    fn from(inp: lang::Expr) -> Self {
        match inp {
            lang::Expr::NestedColumn { head, rest } => operator::Expr::NestedColumn {
                head,
                rest: rest
                    .iter()
                    .map(|s| operator::ValueRef::Field(s.to_string()))
                    .collect(),
            },
            lang::Expr::Unary { op, operand } => match op {
                lang::UnaryOp::Not => operator::Expr::BoolUnary(operator::UnaryExpr {
                    operator: operator::BoolUnaryExpr::Not,
                    operand: Box::new((*operand).into()),
                }),
            },
            lang::Expr::Binary { op, left, right } => match op {
                lang::BinaryOp::Comparison(com_op) => {
                    operator::Expr::Comparison(operator::BinaryExpr::<operator::BoolExpr> {
                        left: Box::new((*left).into()),
                        right: Box::new((*right).into()),
                        operator: com_op.into(),
                    })
                }
            },
            lang::Expr::Value(value) => {
                let boxed = Box::new(value);
                let static_value: &'static mut Value = Box::leak(boxed);
                operator::Expr::Value(static_value)
            }
        }
    }
}

const DEFAULT_LIMIT: i64 = 10;

impl lang::Positioned<lang::InlineOperator> {
    /// Convert the operator syntax to a builder that can instantiate an operator for the
    /// pipeline.  Any semantic errors in the operator syntax should be detected here.
    pub fn semantic_analysis<T: ErrorBuilder>(
        self,
        error_builder: &T,
    ) -> Result<Box<operator::OperatorBuilder + Send + Sync>, TypeError> {
        match self.value {
            lang::InlineOperator::Json { input_column } => {
                Ok(Box::new(operator::ParseJson::new(input_column)))
            }
            lang::InlineOperator::Logfmt { input_column } => {
                Ok(Box::new(operator::ParseLogfmt::new(input_column)))
            }
            lang::InlineOperator::Parse {
                pattern,
                fields,
                input_column,
                no_drop,
            } => {
                let regex = pattern.to_regex();

                if (regex.captures_len() - 1) != fields.len() {
                    Err(TypeError::ParseNumPatterns {
                        pattern: regex.captures_len() - 1,
                        extracted: fields.len(),
                    })
                } else {
                    Ok(Box::new(operator::Parse::new(
                        regex,
                        fields,
                        input_column.map(|e| e.into()),
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
            lang::InlineOperator::Where { expr: Some(expr) } => match expr.value.into() {
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
            lang::InlineOperator::Total {
                input_column,
                output_column,
            } => Ok(Box::new(operator::TotalDef::new(
                input_column.into(),
                output_column,
            ))),
        }
    }
}

impl lang::Positioned<lang::AggregateFunction> {
    pub fn semantic_analysis<T: ErrorBuilder>(
        self,
        error_builder: &T,
    ) -> Result<Box<operator::AggregateFunction>, ()> {
        match self.value {
            lang::AggregateFunction::Count => Ok(Box::new(operator::Count::new())),
            lang::AggregateFunction::Min { column } => Ok(Box::new(operator::Min::empty(column))),
            lang::AggregateFunction::Average { column } => {
                Ok(Box::new(operator::Average::empty(column)))
            }
            lang::AggregateFunction::Max { column } => Ok(Box::new(operator::Max::empty(column))),
            lang::AggregateFunction::Sum { column } => Ok(Box::new(operator::Sum::empty(column))),
            lang::AggregateFunction::Percentile {
                column, percentile, ..
            } => Ok(Box::new(operator::Percentile::empty(column, percentile))),
            lang::AggregateFunction::CountDistinct { column: Some(pos) } => {
                match pos.value.as_slice() {
                    [column] => Ok(Box::new(operator::CountDistinct::empty(column.clone()))),
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

                        Err(())
                    }
                }
            }
            lang::AggregateFunction::CountDistinct { column: None } => {
                error_builder
                    .report_error_for("Expecting an expression to count")
                    .with_code_pointer(&self, "No field argument given")
                    .with_resolution("example: count_distinct(field_to_count)")
                    .send_report();

                Err(())
            }
        }
    }
}
