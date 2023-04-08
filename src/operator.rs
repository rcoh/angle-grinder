use std::borrow::Cow;

use std::collections::HashMap;
use std::collections::HashSet;
use std::iter;

use thiserror::Error;

pub(crate) use expr::Expr;

use crate::data;
use crate::data::{Aggregate, Record, Row};

type Data = HashMap<String, data::Value>;

pub(crate) mod average;
pub(crate) mod count;
pub(crate) mod count_distinct;
pub(crate) mod expr;
pub(crate) mod fields;
pub(crate) mod limit;
pub(crate) mod max;
pub(crate) mod min;
// public for benchmarks
pub mod parse;
pub(crate) mod percentile;
pub(crate) mod sort;
pub(crate) mod split;
pub(crate) mod sum;
pub(crate) mod timeslice;
pub(crate) mod total;
pub(crate) mod where_op;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EvalError {
    #[error("No value for key {key:?}")]
    NoValueForKey { key: String },

    #[error("Expected {}, found {}", expected, found)]
    ExpectedXYZ { expected: String, found: String },

    #[error("Index {} out of range", index)]
    IndexOutOfRange { index: i64 },

    #[error("Found None, expected {}", tpe)]
    UnexpectedNone { tpe: String },

    #[error("Expected JSON, found {}", found)]
    ExpectedJson { found: String },

    #[error("Expected string, found {}", found)]
    ExpectedString { found: String },

    #[error("Expected number, found {}", found)]
    ExpectedNumber { found: String },

    #[error(
        "Expected date, found '{}'.  Use parseDate() to convert a value to a date",
        found
    )]
    ExpectedDate { found: String },

    #[error("Expected positive number, found {}", found)]
    ExpectedPositiveNumber { found: String },

    #[error("Expected numeric operands, found {} {} {}", left, op, right)]
    ExpectedNumericOperands {
        left: String,
        op: &'static str,
        right: String,
    },

    #[error("Expected boolean, found {}", found)]
    ExpectedBoolean { found: String },

    #[error("Unknown function {}", name)]
    UnknownFunction { name: String },

    #[error(
        "The '{}' function expects {} arguments, found {}",
        name,
        expected,
        found
    )]
    InvalidFunctionArguments {
        name: &'static str,
        expected: usize,
        found: usize,
    },

    #[error("The '{}' function failed with the error: {}", name, msg)]
    FunctionFailed { name: &'static str, msg: String },

    #[error("Duration is not valid for timeslice: {}", error)]
    InvalidDuration { error: String },
}

pub trait Evaluate<T>: Send + Sync + Clone {
    fn eval(&self, record: &Data) -> Result<T, EvalError>;
}

/// Trait for operators that are functional in nature and do not maintain state.
pub trait UnaryPreAggFunction: Send + Sync {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError>;
}

/// Get a column from the given record.
fn get_input<'a>(rec: &'a Record, col: &Option<Expr>) -> Result<Cow<'a, str>, EvalError> {
    match col {
        Some(expr) => expr.eval_str(&rec.data),
        None => Ok(Cow::Borrowed(&rec.raw)),
    }
}

/// Trait for operators that maintain state while processing records.
pub trait UnaryPreAggOperator: Send + Sync {
    fn process_mut(&mut self, rec: Record) -> Result<Option<Record>, EvalError>;
    /// Return any remaining records that may have been gathered by the operator.  This method
    /// will be called when there are no more new input records.
    fn drain(self: Box<Self>) -> Box<dyn Iterator<Item = Record>> {
        Box::new(iter::empty())
    }
}

/// Trait used to instantiate an operator from its definition.  If an operator does not maintain
/// state, the operator definition value can be cloned and returned.
pub trait OperatorBuilder: Send + Sync {
    fn build(&self) -> Box<dyn UnaryPreAggOperator>;
}

/// A trivial OperatorBuilder implementation for functional traits since they don't need to
/// maintain state.
impl<T> OperatorBuilder for T
where
    T: 'static + UnaryPreAggFunction + Clone,
{
    fn build(&self) -> Box<dyn UnaryPreAggOperator> {
        // TODO: eliminate the clone since a functional operator definition could be shared.
        Box::new((*self).clone())
    }
}

/// Adapter for pre-aggregate operators to be used on the output of aggregate operators.
pub struct PreAggAdapter {
    op_builder: Box<dyn OperatorBuilder>,
    state: Aggregate,
}

impl PreAggAdapter {
    pub fn new(op_builder: Box<dyn OperatorBuilder>) -> Self {
        PreAggAdapter {
            op_builder,
            state: Aggregate {
                columns: Vec::new(),
                data: Vec::new(),
            },
        }
    }
}

/// A trivial UnaryPreAggOperator implementation for functional operators since they don't need
/// an object to contain any state.
impl<T> UnaryPreAggOperator for T
where
    T: UnaryPreAggFunction,
{
    fn process_mut(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        self.process(rec)
    }
}

impl AggregateOperator for PreAggAdapter {
    fn emit(&self) -> Aggregate {
        self.state.clone()
    }

    fn process(&mut self, row: Row) {
        match row {
            Row::Record(_) => panic!("PreAgg adaptor should only be used after aggregates"),
            Row::Aggregate(agg) => {
                let mut op = self.op_builder.build();
                let mut processed_records: Vec<data::VMap> = agg
                    .data
                    .into_iter()
                    .map(|vmap| data::Record {
                        data: vmap,
                        raw: "".to_string(),
                    })
                    .flat_map(|rec| op.process_mut(rec).unwrap_or(None))
                    .map(|rec| rec.data)
                    .collect();
                processed_records.extend(op.drain().map(|rec| rec.data));
                let output_column_set: HashSet<String> = processed_records
                    .iter()
                    .flat_map(|vmap| vmap.keys())
                    .cloned()
                    .collect();

                let mut columns = Vec::with_capacity(output_column_set.len());
                let filtered_previous_columns = agg
                    .columns
                    .iter()
                    .filter(|col| output_column_set.contains(*col));
                columns.extend(filtered_previous_columns.cloned());
                for column in output_column_set {
                    if !columns.contains(&column) {
                        columns.push(column);
                    }
                }

                self.state = Aggregate {
                    data: processed_records,
                    columns,
                };
            }
        }
    }
}

pub trait AggregateOperator: Send + Sync {
    fn emit(&self) -> data::Aggregate;
    fn process(&mut self, row: Row);
}

pub trait AggregateFunction: Send + Sync {
    fn process(&mut self, rec: &Data) -> Result<(), EvalError>;
    fn emit(&self) -> data::Value;
    fn empty_box(&self) -> Box<dyn AggregateFunction>;
}

impl<T: Copy + Send + Sync> Evaluate<T> for T {
    fn eval(&self, _record: &HashMap<String, data::Value>) -> Result<T, EvalError> {
        Ok(*self)
    }
}

pub struct MultiGrouper {
    key_cols: Vec<Expr>,
    key_col_headers: Vec<String>,
    agg_col: Vec<(String, Box<dyn AggregateFunction>)>,
    // key-column values -> (agg_columns -> builders)
    state: HashMap<Vec<data::Value>, HashMap<String, Box<dyn AggregateFunction>>>,
}

impl MultiGrouper {
    pub fn new(
        key_cols: &[Expr],
        key_col_headers: Vec<String>,
        aggregators: Vec<(String, Box<dyn AggregateFunction>)>,
    ) -> Self {
        MultiGrouper {
            key_cols: key_cols.to_vec(),
            key_col_headers,
            agg_col: aggregators,
            state: HashMap::new(),
        }
    }
    fn process_map(&mut self, data: &Data) {
        let key_values = self.key_cols.iter().map(|expr| expr.eval_value(data));
        let key_columns: Vec<_> = key_values
            .map(|value_res| value_res.unwrap_or(Cow::Owned(data::Value::None)))
            .map(|v| v.into_owned())
            .collect();
        let agg_col = &self.agg_col;
        let row = self.state.entry(key_columns).or_insert_with(|| {
            agg_col
                .iter()
                .map(|(k, v)| (k.to_owned(), v.empty_box()))
                .collect()
        });
        for fun in row.values_mut() {
            // TODO: #25 capture erorrs here instead of ignoring
            let _ = fun.process(data);
        }
    }
}

impl AggregateOperator for MultiGrouper {
    fn emit(&self) -> Aggregate {
        let mut columns = self.key_col_headers.to_vec();
        columns.extend(self.agg_col.iter().map(|(k, ..)| k.to_string()));
        let data = self.state.iter().map(|(key_values, agg_map)| {
            let key_values = key_values.iter().cloned();
            let key_cols = self.key_col_headers.iter().map(|s| s.to_owned());
            let mut res_map = HashMap::with_capacity(key_cols.len() + agg_map.len());
            res_map.extend(itertools::zip_eq(key_cols, key_values));
            for (k, v) in agg_map {
                res_map.insert(k.to_string(), v.emit());
            }
            res_map
        });
        Aggregate {
            columns,
            data: data.collect(),
        }
    }

    fn process(&mut self, row: Row) {
        match row {
            Row::Record(rec) => {
                self.process_map(&rec.data);
            }
            Row::Aggregate(ag) => {
                self.state.clear();
                for row in ag.data {
                    self.process_map(&row);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;
    use std::cmp::Ordering;

    use crate::data::Value;
    use crate::lang;
    use crate::operator::count::Count;
    use crate::operator::count_distinct::CountDistinct;
    use crate::operator::expr::ValueRef;
    use crate::operator::fields::{FieldMode, Fields};
    use crate::operator::max::Max;
    use crate::operator::min::Min;
    use crate::operator::parse::{Parse, ParseJson, ParseLogfmt, ParseOptions};
    use crate::operator::sort::{SortDirection, Sorter};
    use crate::operator::sum::Sum;
    use crate::operator::total::TotalDef;
    use crate::operator::where_op::Where;

    use super::*;

    impl<S: Into<String>> From<S> for Expr {
        fn from(inp: S) -> Self {
            Expr::column(&inp.into())
        }
    }

    #[test]
    fn test_nested_eval() {
        let rec = Record::new(
            r#"{"k1": {"k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}}"#.to_string() + "\n",
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        let expr = Expr::NestedColumn {
            head: "k1".to_string(),
            rest: vec![ValueRef::Field("k2".to_string())],
        };
        let data = expr.eval_value(&rec.data).unwrap();
        assert_eq!(data.into_owned(), data::Value::from_float(5.5));
    }

    #[test]
    fn test_nested_eval_error() {
        let rec = Record::new(
            r#"{"k1": {"k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}}"#.to_string() + "\n",
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        let expr = Expr::NestedColumn {
            head: "k1".to_string(),
            rest: vec![ValueRef::Field("k11".to_string())],
        };
        let res = expr.eval_value(&rec.data);
        match res {
            Ok(_) => assert_eq!(false, true),
            Err(eval_error) => assert_eq!(
                eval_error,
                EvalError::NoValueForKey {
                    key: "k11".to_string()
                }
            ),
        }
    }

    #[test]
    fn json() {
        let rec = Record::new(
            r#"{"k1": 5, "k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}"#.to_string() + "\n",
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data,
            hashmap! {
                "k1".to_string() => Value::Int(5),
                "k2".to_string() => Value::from_float(5.5),
                "k3".to_string() => Value::Str("str".to_string()),
                "k4".to_string() => Value::None,
                "k5".to_string() => Value::Array(vec![Value::Int(1),Value::Int(2),Value::Int(3)])
            }
        );
    }

    #[test]
    fn nested_json() {
        let rec = Record::new(
            r#"{"k1": {"k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}}"#.to_string() + "\n",
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data,
            hashmap! {
                "k1".to_string() => Value::Obj(
                    hashmap! {
                        "k2".to_string() => Value::from_float(5.5),
                        "k3".to_string() => Value::Str("str".to_string()),
                        "k4".to_string() => Value::None,
                "k5".to_string() => Value::Array(vec![Value::Int(1),Value::Int(2),Value::Int(3)])
                    }.into()
                 )
            }
        );
    }

    #[test]
    fn logfmt() {
        let rec = Record::new(r#"k1=5 k2=5.5 k3="a str" k4="#.to_string() + "\n");
        let parser = ParseLogfmt::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data,
            hashmap! {
                "k1".to_string() => Value::Int(5),
                "k2".to_string() => Value::from_float(5.5),
                "k3".to_string() => Value::Str("a str".to_string()),
                "k4".to_string() => Value::Str("".to_string())
            }
        );
    }

    #[test]
    fn fields_only() {
        let rec = Record::new("");
        let rec = rec.put("k1", Value::Str("v1".to_string()));
        let rec = rec.put("k2", Value::Str("v2".to_string()));
        let rec = rec.put("k3", Value::Str("v3".to_string()));
        let rec = rec.put("k4", Value::Str("v4".to_string()));
        let fields = Fields::new(&["k1".to_string()], FieldMode::Only);
        let rec = fields.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data,
            hashmap! {
                "k1".to_string() => Value::Str("v1".to_string()),
            }
        );
    }

    #[test]
    fn fields_except() {
        let rec = Record::new("");
        let rec = rec.put("k1", Value::Str("v1".to_string()));
        let rec = rec.put("k2", Value::Str("v2".to_string()));
        let rec = rec.put("k3", Value::Str("v3".to_string()));
        let rec = rec.put("k4", Value::Str("v4".to_string()));
        let fields = Fields::new(&["k1".to_string()], FieldMode::Except);
        let rec = fields.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data,
            hashmap! {
                "k2".to_string() => Value::Str("v2".to_string()),
                "k3".to_string() => Value::Str("v3".to_string()),
                "k4".to_string() => Value::Str("v4".to_string()),
            }
        );
    }

    #[test]
    fn parse() {
        let rec = Record::new(
            "17:12:14.214111 IP 10.0.2.243.53938 > \"taotie.canonical.com.http\": \
             Flags [.], ack 56575, win 2375, options [nop,nop,TS val 13651369 ecr 169698010], \
             length 99",
        );
        let parser = Parse::new(
            lang::Keyword::new_wildcard("IP * > \"*\": * length *".to_string()).to_regex(),
            vec![
                "sender".to_string(),
                "recip".to_string(),
                "ignore".to_string(),
                "length".to_string(),
            ],
            None,
            ParseOptions {
                drop_nonmatching: true,
            },
        );
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data.get("sender").unwrap(),
            &Value::Str("10.0.2.243.53938".to_string())
        );
        assert_eq!(rec.data.get("length").unwrap(), &Value::Int(99));
        assert_eq!(
            rec.data.get("recip").unwrap(),
            &Value::Str("taotie.canonical.com.http".to_string())
        )
    }

    #[test]
    fn parse_drop() {
        let rec = Record::new("abcd 1234");
        let parser = Parse::new(
            lang::Keyword::new_wildcard("IP *".to_string()).to_regex(),
            vec!["ip".to_string()],
            None,
            ParseOptions {
                drop_nonmatching: true,
            },
        );
        let rec = parser.process(rec).unwrap();
        assert_eq!(rec, None);
    }

    #[test]
    fn parse_nodrop() {
        let rec = Record::new("abcd 1234");
        let parser = Parse::new(
            lang::Keyword::new_wildcard("IP *".to_string()).to_regex(),
            vec!["ip".to_string()],
            None,
            ParseOptions {
                drop_nonmatching: false,
            },
        );
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(rec.data.get("ip").unwrap(), &Value::None);
    }

    #[test]
    fn parse_all_whitespace() {
        let rec = Record::new("abcd\t1234\t4567");
        let parser = Parse::new(
            lang::Keyword::new_wildcard("abcd * *".to_string()).to_regex(),
            vec!["num_1".to_string(), "num_2".to_string()],
            None,
            ParseOptions {
                drop_nonmatching: false,
            },
        );
        let parsed = parser.process(rec).unwrap().unwrap();
        assert_eq!(parsed.data.get("num_1"), Some(&Value::Int(1234)))
    }

    #[test]
    fn parse_nodrop_preserve_existing() {
        let rec = Record::new("abcd 1234").put("ip", Value::Str("127.0.0.1".to_string()));
        let parser = Parse::new(
            lang::Keyword::new_wildcard("IP *".to_string()).to_regex(),
            vec!["ip".to_string()],
            None,
            ParseOptions {
                drop_nonmatching: false,
            },
        );
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data.get("ip").unwrap(),
            &Value::Str("127.0.0.1".to_string())
        );
    }

    #[test]
    fn parse_from_field() {
        let rec = Record::new("");
        let rec = rec.put("from_col", data::Value::Str("[k1=v1]".to_string()));
        let parser = Parse::new(
            lang::Keyword::new_wildcard("[*=*]".to_string()).to_regex(),
            vec!["key".to_string(), "value".to_string()],
            Some("from_col".into()),
            ParseOptions {
                drop_nonmatching: true,
            },
        );
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data.get("key").unwrap(),
            &data::Value::Str("k1".to_string())
        );
        assert_eq!(
            rec.data.get("value").unwrap(),
            &data::Value::Str("v1".to_string())
        );
    }

    #[test]
    fn count_no_groups() {
        let ops: Vec<(String, Box<dyn AggregateFunction>)> =
            vec![("_count".to_string(), Box::new(Count::unconditional()))];
        let mut count_agg = MultiGrouper::new(&[], vec![], ops);
        (0..10)
            .map(|n| Record::new(n.to_string()))
            .for_each(|rec| count_agg.process(Row::Record(rec)));
        let agg = count_agg.emit();
        assert_eq!(agg.columns, vec!["_count"]);
        assert_eq!(
            agg.data,
            vec![hashmap! {"_count".to_string() => data::Value::Int(10)}]
        );
        (0..10)
            .map(|n| Record::new(n.to_string()))
            .for_each(|rec| count_agg.process(Row::Record(rec)));
        assert_eq!(
            count_agg.emit().data,
            vec![hashmap! {"_count".to_string() => data::Value::Int(20)}]
        );
    }

    #[test]
    fn multi_grouper() {
        let ops: Vec<(String, Box<dyn AggregateFunction>)> = vec![
            ("_count".to_string(), Box::new(Count::unconditional())),
            ("_sum".to_string(), Box::new(Sum::empty("v1"))),
            ("_min".to_string(), Box::new(Min::empty("v1"))),
            ("_max".to_string(), Box::new(Max::empty("v1"))),
            (
                "_distinct".to_string(),
                Box::new(CountDistinct::empty("v1")),
            ),
        ];

        let mut grouper = MultiGrouper::new(&[Expr::column("k1")], vec!["k1".to_string()], ops);
        (0..10).for_each(|n| {
            let rec = Record::new(n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..10).for_each(|n| {
            let rec = Record::new(n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..25).for_each(|n| {
            let rec = Record::new(n.to_string());
            let rec = rec.put("k1", data::Value::Str("not ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..3).for_each(|n| {
            let rec = Record::new(n.to_string());
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        let agg = grouper.emit();
        let mut sorted_data = agg.data;
        let ordering = Record::ordering(vec!["_count".to_string()]);
        sorted_data.sort_by(|l, r| ordering(l, r).unwrap_or(Ordering::Less));
        sorted_data.reverse();
        assert_eq!(
            sorted_data,
            vec![
                hashmap! {
                    "k1".to_string() => data::Value::Str("not ok".to_string()),
                    "_count".to_string() => data::Value::Int(25),
                    "_distinct".to_string() => data::Value::Int(25),
                    "_sum".to_string() => data::Value::Int(300),
                    "_min".to_string() => data::Value::Int(0),
                    "_max".to_string() => data::Value::Int(24),
                },
                hashmap! {
                    "k1".to_string() => data::Value::Str("ok".to_string()),
                    "_count".to_string() => data::Value::Int(20),
                    "_distinct".to_string() => data::Value::Int(10),
                    "_sum".to_string() => data::Value::Int(90),
                    "_min".to_string() => data::Value::Int(0),
                    "_max".to_string() => data::Value::Int(9),
                },
                hashmap! {
                    "k1".to_string() => data::Value::None,
                    "_count".to_string() => data::Value::Int(3),
                    "_distinct".to_string() => data::Value::Int(3),
                    "_sum".to_string() => data::Value::Int(3),
                    "_min".to_string() => data::Value::Int(0),
                    "_max".to_string() => data::Value::Int(2),
                },
            ]
        );
    }

    #[test]
    fn count_groups() {
        let ops: Vec<(String, Box<dyn AggregateFunction>)> =
            vec![("_count".to_string(), Box::new(Count::unconditional()))];
        let mut count_agg = MultiGrouper::new(&[Expr::column("k1")], vec!["k1".to_string()], ops);
        (0..10).for_each(|n| {
            let rec = Record::new(n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            count_agg.process(Row::Record(rec));
        });
        (0..25).for_each(|n| {
            let rec = Record::new(n.to_string());
            let rec = rec.put("k1", data::Value::Str("not ok".to_string()));
            count_agg.process(Row::Record(rec));
        });
        (0..3).for_each(|n| {
            let rec = Record::new(n.to_string());
            count_agg.process(Row::Record(rec));
        });
        let agg = count_agg.emit();
        let mut sorted_data = agg.data;
        let ordering = Record::ordering(vec!["_count".to_string()]);
        sorted_data.sort_by(|l, r| ordering(l, r).unwrap_or(Ordering::Less));
        sorted_data.reverse();
        assert_eq!(
            sorted_data,
            vec![
                hashmap! {
                    "k1".to_string() => data::Value::Str("not ok".to_string()),
                    "_count".to_string() => data::Value::Int(25),
                },
                hashmap! {"k1".to_string() =>
                data::Value::Str("ok".to_string()), "_count".to_string() => data::Value::Int(10)},
                hashmap! {
                    "k1".to_string() => data::Value::None,
                    "_count".to_string() => data::Value::Int(3)
                },
            ]
        );
    }

    #[test]
    fn sort_raw() {}

    #[test]
    fn sort_aggregate() {
        let agg = Aggregate::new(
            &["kc1".to_string(), "kc2".to_string()],
            "count".to_string(),
            &[
                (
                    hashmap! {
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::Int(100),
                ),
                (
                    hashmap! {
                        "kc1".to_string() => "k300".to_string(),
                        "kc2".to_string() => "k40000".to_string()
                    },
                    Value::Int(500),
                ),
            ],
        );
        let mut sorter = Sorter::new(vec![Expr::column("count")], SortDirection::Ascending);
        sorter.process(data::Row::Aggregate(agg.clone()));
        assert_eq!(sorter.emit(), agg);

        let mut sorter = Sorter::new(vec![Expr::column("count")], SortDirection::Descending);
        sorter.process(data::Row::Aggregate(agg.clone()));

        let mut revagg = agg;
        revagg.data.reverse();
        assert_eq!(sorter.emit(), revagg);
    }

    #[test]
    fn test_agg_adapter() {
        let where_op = Where::new(true);
        let adapted = PreAggAdapter::new(Box::new(where_op));
        let mut adapted: Box<dyn AggregateOperator> = Box::new(adapted);
        let agg = Aggregate::new(
            &["kc1".to_string(), "kc2".to_string()],
            "count".to_string(),
            &[
                (
                    hashmap! {
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::Int(100),
                ),
                (
                    hashmap! {
                        "kc1".to_string() => "k300".to_string(),
                        "kc2".to_string() => "k40000".to_string()
                    },
                    Value::Int(500),
                ),
            ],
        );
        adapted.process(Row::Aggregate(agg.clone()));
        assert_eq!(adapted.emit(), agg);
    }

    #[test]
    fn test_total() {
        let mut total_op = PreAggAdapter::new(Box::new(TotalDef::new(
            Expr::column("count"),
            "_total".to_string(),
        )));
        let agg = Aggregate::new(
            &["kc1".to_string(), "kc2".to_string()],
            "count".to_string(),
            &[
                (
                    hashmap! {
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::Int(100),
                ),
                (
                    hashmap! {
                        "kc1".to_string() => "k300".to_string(),
                        "kc2".to_string() => "k40000".to_string()
                    },
                    Value::Int(500),
                ),
            ],
        );
        total_op.process(Row::Aggregate(agg.clone()));
        let result = total_op.emit().data;
        assert_eq!(result[0].get("_total").unwrap(), &Value::from_float(100.0));
        assert_eq!(result[1].get("_total").unwrap(), &Value::from_float(600.0));
        assert_eq!(result.len(), 2);
        total_op.process(Row::Aggregate(agg));
        let result = total_op.emit().data;
        assert_eq!(result[0].get("_total").unwrap(), &Value::from_float(100.0));
        assert_eq!(result[1].get("_total").unwrap(), &Value::from_float(600.0));
        assert_eq!(result.len(), 2);
    }
}
