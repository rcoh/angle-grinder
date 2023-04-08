use crate::data::DisplayConfig;
use crate::operator::{Data, EvalError, Evaluate};
use crate::{data, funcs};
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryInto;

#[derive(Debug, Clone)]
pub enum Expr {
    // First record can only be a String, after that, you can do things like `[0]` in addition to `.foo`
    NestedColumn {
        head: String,
        rest: Vec<ValueRef>,
    },
    BoolUnary(UnaryExpr<BoolUnaryExpr>),
    Comparison(BinaryExpr<BoolExpr>),
    Arithmetic(BinaryExpr<ArithmeticExpr>),
    Logical(BinaryExpr<LogicalExpr>),
    FunctionCall {
        func: &'static funcs::FunctionContainer,
        args: Vec<Expr>,
    },
    IfOp {
        cond: Box<Expr>,
        value_if_true: Box<Expr>,
        value_if_false: Box<Expr>,
    },
    Value(&'static data::Value),
}

impl Expr {
    pub fn column(key: &str) -> Expr {
        Expr::NestedColumn {
            head: key.to_owned(),
            rest: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct UnaryExpr<T> {
    pub operator: T,
    pub operand: Box<Expr>,
}

#[derive(Debug, Clone)]
pub struct BinaryExpr<T> {
    pub operator: T,
    pub left: Box<Expr>,
    pub right: Box<Expr>,
}

impl Evaluate<bool> for BinaryExpr<BoolExpr> {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<bool, EvalError> {
        let l = self.left.eval_value(record)?;
        let r = self.right.eval_value(record)?;
        let result = match self.operator {
            BoolExpr::Eq => l == r,
            BoolExpr::Neq => l != r,
            BoolExpr::Gt => l > r,
            BoolExpr::Lt => l < r,
            BoolExpr::Gte => l >= r,
            BoolExpr::Lte => l <= r,
        };
        Ok(result)
    }
}

impl Evaluate<data::Value> for BinaryExpr<ArithmeticExpr> {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<data::Value, EvalError> {
        let l = self.left.eval_value(record)?.into_owned();
        let r = self.right.eval_value(record)?.into_owned();
        match self.operator {
            ArithmeticExpr::Add => l + r,
            ArithmeticExpr::Subtract => l - r,
            ArithmeticExpr::Multiply => l * r,
            ArithmeticExpr::Divide => l / r,
        }
    }
}

impl Evaluate<data::Value> for BinaryExpr<LogicalExpr> {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<data::Value, EvalError> {
        let l: bool = self.left.eval(record)?;
        match self.operator {
            LogicalExpr::And => {
                if l {
                    self.right.eval_value(record).map(|v| v.into_owned())
                } else {
                    Ok(data::Value::Bool(false))
                }
            }
            LogicalExpr::Or => {
                if l {
                    Ok(data::Value::Bool(true))
                } else {
                    self.right.eval_value(record).map(|v| v.into_owned())
                }
            }
        }
    }
}

impl Evaluate<bool> for UnaryExpr<BoolUnaryExpr> {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<bool, EvalError> {
        let bool_res = self.operand.eval_value(record)?;

        match bool_res.as_ref() {
            data::Value::Bool(true) => Ok(false),
            data::Value::Bool(false) => Ok(true),
            _ => Err(EvalError::ExpectedBoolean {
                found: bool_res.to_string(),
            }),
        }
    }
}

impl Evaluate<bool> for Expr {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<bool, EvalError> {
        match self.eval_value(record)?.as_ref() {
            data::Value::Bool(bool_value) => Ok(*bool_value),
            other => Err(EvalError::ExpectedBoolean {
                found: other.to_string(),
            }),
        }
    }
}

impl Evaluate<f64> for Expr {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<f64, EvalError> {
        let value = self.eval_value(record)?;
        match value.as_ref() {
            data::Value::Int(i) => Ok(*i as f64),
            data::Value::Float(f) => Ok(f.into_inner()),
            data::Value::Str(s) => data::Value::aggressively_to_num(s),
            other => Err(EvalError::ExpectedNumber {
                found: format!("{}", other),
            }),
        }
    }
}

impl Expr {
    pub(crate) fn eval_str<'a>(&self, record: &'a Data) -> Result<Cow<'a, str>, EvalError> {
        let as_value = self.eval_value(record)?;
        match as_value {
            v if v.as_ref() == &data::Value::None => Err(EvalError::UnexpectedNone {
                tpe: "String".to_string(),
            }),
            Cow::Owned(data::Value::Str(s)) => Ok(Cow::Owned(s)),
            Cow::Borrowed(data::Value::Str(s)) => Ok(Cow::Borrowed(s)),
            _ => Err(EvalError::ExpectedString {
                found: "other".to_string(),
            }),
        }
    }

    pub(crate) fn eval_value<'a>(
        &self,
        record: &'a HashMap<String, data::Value>,
    ) -> Result<Cow<'a, data::Value>, EvalError> {
        match *self {
            Expr::NestedColumn { ref head, ref rest } => {
                let mut root_record: &data::Value = record
                    .get(head)
                    .ok_or_else(|| EvalError::NoValueForKey { key: head.clone() })?;

                // TODO: probably a nice way to do this with a fold
                for value_reference in rest.iter() {
                    match (value_reference, root_record) {
                        (ValueRef::Field(ref key), data::Value::Obj(map)) => {
                            root_record = map
                                .get(key)
                                .ok_or_else(|| EvalError::NoValueForKey { key: key.clone() })?
                        }
                        (ValueRef::Field(_), other) => {
                            return Err(EvalError::ExpectedXYZ {
                                expected: "object".to_string(),
                                found: other.render(&DisplayConfig::default()),
                            });
                        }
                        (ValueRef::IndexAt(index), data::Value::Array(vec)) => {
                            let vec_len: i64 = vec.len().try_into().unwrap();
                            let real_index = if *index < 0 { *index + vec_len } else { *index };

                            if real_index < 0 || real_index >= vec_len {
                                return Err(EvalError::IndexOutOfRange { index: *index });
                            }
                            root_record = &vec[real_index as usize];
                        }
                        (ValueRef::IndexAt(_), other) => {
                            return Err(EvalError::ExpectedXYZ {
                                expected: "array".to_string(),
                                found: other.render(&DisplayConfig::default()),
                            });
                        }
                    }
                }
                Ok(Cow::Borrowed(root_record))
            }
            Expr::BoolUnary(
                ref unary_op @ UnaryExpr {
                    operator: BoolUnaryExpr::Not,
                    ..
                },
            ) => {
                let bool_res = unary_op.eval(record)?;
                Ok(Cow::Owned(data::Value::from_bool(bool_res)))
            }
            Expr::Comparison(ref binary_expr) => {
                let bool_res = binary_expr.eval(record)?;
                Ok(Cow::Owned(data::Value::from_bool(bool_res)))
            }
            Expr::Arithmetic(ref binary_expr) => binary_expr.eval(record).map(Cow::Owned),
            Expr::Logical(ref logical_expr) => logical_expr.eval(record).map(Cow::Owned),
            Expr::FunctionCall { func, ref args } => {
                let evaluated_args: Result<Vec<data::Value>, EvalError> = args
                    .iter()
                    .map(|expr| expr.eval_value(record).map(|v| v.into_owned()))
                    .collect();

                func.eval_func(&evaluated_args?).map(Cow::Owned)
            }
            Expr::IfOp {
                ref cond,
                ref value_if_true,
                ref value_if_false,
            } => {
                let evaluated_cond: bool = (*cond).eval(record)?;

                if evaluated_cond {
                    (*value_if_true).eval_value(record)
                } else {
                    (*value_if_false).eval_value(record)
                }
            }
            Expr::Value(v) => Ok(Cow::Borrowed(v)),
        }
    }
}

#[derive(Clone, Debug)]
pub enum BoolUnaryExpr {
    Not,
}

#[derive(Clone, Debug)]
pub enum BoolExpr {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

#[derive(Clone, Debug)]
pub enum ArithmeticExpr {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Clone, Debug)]
pub enum LogicalExpr {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum ValueRef {
    Field(String),
    IndexAt(i64),
}
