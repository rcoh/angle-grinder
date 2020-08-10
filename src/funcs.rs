use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::fmt::{Debug, Formatter};

use chrono::{DateTime, FixedOffset};
use itertools::Itertools;
use lazy_static::lazy_static;

use crate::data;
use crate::operator::EvalError;

/// Enum used to capture a static function that can be called by the expression language.
#[derive(Clone, Copy)]
pub enum FunctionWrapper {
    Float1(fn(f64) -> f64),
    Float2(fn(f64, f64) -> f64),
    String1(fn(&str) -> Result<data::Value, EvalError>),
    String2(fn(&str, &str) -> Result<data::Value, EvalError>),
    Generic(fn(&Vec<data::Value>) -> Result<data::Value, EvalError>),
}

/// Struct used to capture the name of the function in the expression language and a pointer
/// to the implementation.
#[derive(Clone, Copy)]
pub struct FunctionContainer {
    name: &'static str,
    func: FunctionWrapper,
}

impl Debug for FunctionContainer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

impl FunctionContainer {
    pub fn new(name: &'static str, func: FunctionWrapper) -> Self {
        FunctionContainer { name, func }
    }

    fn eval1<'v, A: TryFrom<&'v data::Value, Error = EvalError>, O: Into<data::Value>>(
        &self,
        f: fn(A) -> O,
        args: &'v Vec<data::Value>,
    ) -> Result<data::Value, EvalError> {
        match args.as_slice() {
            [arg0] => {
                let arg0_a = arg0.try_into()?;
                Ok(f(arg0_a).into())
            }
            _ => Err(EvalError::InvalidFunctionArguments {
                name: self.name,
                expected: 1,
                found: args.len(),
            }),
        }
    }

    fn eval2<'v, A: TryFrom<&'v data::Value, Error = EvalError>, O: Into<data::Value>>(
        &self,
        f: fn(A, A) -> O,
        args: &'v Vec<data::Value>,
    ) -> Result<data::Value, EvalError> {
        match args.as_slice() {
            [arg0, arg1] => {
                let arg0_a = arg0.try_into()?;
                let arg1_a = arg1.try_into()?;
                Ok(f(arg0_a, arg1_a).into())
            }
            _ => Err(EvalError::InvalidFunctionArguments {
                name: self.name,
                expected: 2,
                found: args.len(),
            }),
        }
    }

    pub fn eval_func(&self, args: &Vec<data::Value>) -> Result<data::Value, EvalError> {
        match self.func {
            FunctionWrapper::Float1(func) => self.eval1(func, args),
            FunctionWrapper::Float2(func) => self.eval2(func, args),
            FunctionWrapper::String1(func) => {
                if let [arg0] = args.as_slice() {
                    func(arg0.to_string().as_str())
                } else {
                    Err(EvalError::InvalidFunctionArguments {
                        name: self.name,
                        expected: 1,
                        found: args.len(),
                    })
                }
            }
            FunctionWrapper::String2(func) => {
                if let [arg0, arg1] = args.as_slice() {
                    func(arg0.to_string().as_str(), arg1.to_string().as_str())
                } else {
                    Err(EvalError::InvalidFunctionArguments {
                        name: self.name,
                        expected: 2,
                        found: args.len(),
                    })
                }
            }
            FunctionWrapper::Generic(func) => func(args),
        }
    }
}

// The functions below are exported to the query language
// TODO add some macro magic to extract doc attributes so the function reference
// docs can be generated automatically

fn concat(args: &Vec<data::Value>) -> Result<data::Value, EvalError> {
    Ok(data::Value::Str(
        args.into_iter().map(|arg| arg.to_string()).join(""),
    ))
}

fn contains(left: &str, right: &str) -> Result<data::Value, EvalError> {
    Ok(data::Value::from_bool(left.contains(right)))
}

fn length(s: &str) -> Result<data::Value, EvalError> {
    Ok(data::Value::Int(s.chars().count() as i64))
}

fn parse_date(date_str: &str) -> Result<data::Value, EvalError> {
    dtparse::parse(date_str)
        .map(|pair| {
            data::Value::DateTime(
                DateTime::<FixedOffset>::from_utc(pair.0, pair.1.unwrap_or(FixedOffset::west(0)))
                    .into(),
            )
        })
        .map_err(|parse_err| EvalError::FunctionFailed {
            name: "parseDate",
            msg: format!("{}", parse_err),
        })
}

fn substring(args: &Vec<data::Value>) -> Result<data::Value, EvalError> {
    match args.as_slice() {
        [arg0, arg1, arg2] => {
            let src_str = arg0.to_string();
            let start_off: usize = arg1.try_into()?;
            let end_off: usize = arg2.try_into()?;

            if end_off < start_off {
                return Err(EvalError::FunctionFailed {
                    name: "substring",
                    msg: format!(
                        "end offset ({}) is less than the start offset ({})",
                        end_off, start_off
                    ),
                });
            }

            Ok(data::Value::Str(
                src_str
                    .chars()
                    .skip(start_off)
                    .take(end_off - start_off)
                    .collect(),
            ))
        }
        [arg0, arg1] => {
            let src_str = arg0.to_string();
            let start_off: usize = arg1.try_into()?;

            Ok(data::Value::Str(src_str.chars().skip(start_off).collect()))
        }
        _ => Err(EvalError::InvalidFunctionArguments {
            name: "substring",
            expected: 2,
            found: args.len(),
        }),
    }
}

lazy_static! {
    pub static ref FUNC_MAP: HashMap<&'static str, FunctionContainer> = {
        [
            // numeric
            FunctionContainer::new("abs", FunctionWrapper::Float1(f64::abs)),
            FunctionContainer::new("acos", FunctionWrapper::Float1(f64::acos)),
            FunctionContainer::new("asin", FunctionWrapper::Float1(f64::asin)),
            FunctionContainer::new("atan", FunctionWrapper::Float1(f64::atan)),
            FunctionContainer::new("atan2", FunctionWrapper::Float2(f64::atan2)),
            FunctionContainer::new("cbrt", FunctionWrapper::Float1(f64::cbrt)),
            FunctionContainer::new("ceil", FunctionWrapper::Float1(f64::ceil)),
            FunctionContainer::new("cos", FunctionWrapper::Float1(f64::cos)),
            FunctionContainer::new("cosh", FunctionWrapper::Float1(f64::cosh)),
            FunctionContainer::new("exp", FunctionWrapper::Float1(f64::exp)),
            FunctionContainer::new("expm1", FunctionWrapper::Float1(f64::exp_m1)),
            FunctionContainer::new("floor", FunctionWrapper::Float1(f64::floor)),
            FunctionContainer::new("hypot", FunctionWrapper::Float2(f64::hypot)),
            FunctionContainer::new("log", FunctionWrapper::Float1(f64::ln)),
            FunctionContainer::new("log10", FunctionWrapper::Float1(f64::log10)),
            FunctionContainer::new("log1p", FunctionWrapper::Float1(f64::ln_1p)),
            FunctionContainer::new("round", FunctionWrapper::Float1(f64::round)),
            FunctionContainer::new("sin", FunctionWrapper::Float1(f64::sin)),
            FunctionContainer::new("sinh", FunctionWrapper::Float1(f64::sinh)),
            FunctionContainer::new("sqrt", FunctionWrapper::Float1(f64::sqrt)),
            FunctionContainer::new("tan", FunctionWrapper::Float1(f64::tan)),
            FunctionContainer::new("tanh", FunctionWrapper::Float1(f64::tanh)),
            FunctionContainer::new("toDegrees", FunctionWrapper::Float1(f64::to_degrees)),
            FunctionContainer::new("toRadians", FunctionWrapper::Float1(f64::to_radians)),
            // string
            FunctionContainer::new("concat", FunctionWrapper::Generic(concat)),
            FunctionContainer::new("contains", FunctionWrapper::String2(contains)),
            FunctionContainer::new("length", FunctionWrapper::String1(length)),
            FunctionContainer::new("parseDate", FunctionWrapper::String1(parse_date)),
            FunctionContainer::new("substring", FunctionWrapper::Generic(substring)),
        ]
        .iter()
        .map(|wrap| (wrap.name, *wrap))
        .collect()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_length() {
        assert_eq!(data::Value::Int(1), length("\u{2603}").unwrap());
    }

    #[test]
    fn does_not_contain() {
        assert_eq!(
            data::Value::from_bool(false),
            contains("abc", "def").unwrap()
        );
    }

    #[test]
    fn unicode_contains() {
        assert_eq!(
            data::Value::from_bool(true),
            contains("abc \u{2603} def", "\u{2603}").unwrap()
        );
    }

    #[test]
    fn substring_no_args() {
        assert_eq!(
            Err(EvalError::InvalidFunctionArguments {
                name: "substring",
                expected: 2,
                found: 0,
            }),
            substring(&Vec::new())
        );
    }

    #[test]
    fn substring_of_num() {
        assert_eq!(
            Ok(data::Value::Str("12".to_string())),
            substring(&vec!(
                data::Value::Int(123),
                data::Value::Int(0),
                data::Value::Int(2)
            ))
        );
    }

    #[test]
    fn substring_end_lt_start() {
        assert_eq!(
            Err(EvalError::FunctionFailed {
                name: "substring",
                msg: "end offset (0) is less than the start offset (2)".to_string()
            }),
            substring(&vec!(
                data::Value::Int(123),
                data::Value::Str("2".to_string()),
                data::Value::Int(0)
            ))
        );
    }
}
