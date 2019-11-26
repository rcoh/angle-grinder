extern crate itertools;
extern crate quantiles;
extern crate regex;
//extern crate regex_syntax;
extern crate logfmt;
extern crate serde_json;

use self::quantiles::ckms::CKMS;
use self::serde_json::Value as JsonValue;
use crate::data;
use crate::data::{Aggregate, Record, Row};
use crate::operator::itertools::Itertools;
use crate::render::RenderConfig;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::iter;
use std::iter::FromIterator;

type Data = HashMap<String, data::Value>;

mod split;

#[derive(Debug, Fail, PartialEq)]
pub enum EvalError {
    #[fail(display = "No value for key {}", key)]
    NoValueForKey { key: String },

    #[fail(display = "Expected {}, found {}", expected, found)]
    ExpectedXYZ { expected: String, found: String },

    #[fail(display = "Index {} out of range", index)]
    IndexOutOfRange { index: i64 },

    #[fail(display = "Found None, expected {}", tpe)]
    UnexpectedNone { tpe: String },

    #[fail(display = "Expected JSON, found {}", found)]
    ExpectedJson { found: String },

    #[fail(display = "Expected string, found {}", found)]
    ExpectedString { found: String },

    #[fail(display = "Expected number, found {}", found)]
    ExpectedNumber { found: String },

    #[fail(display = "Expected boolean, found {}", found)]
    ExpectedBoolean { found: String },
}

pub trait Evaluatable<T>: Send + Sync + Clone {
    fn eval(&self, record: &Data) -> Result<T, EvalError>;
}

pub trait EvaluatableBorrowed<T> {
    fn eval_borrowed<'a>(&self, record: &'a Data) -> Result<&'a T, EvalError>;
}

/// Trait for operators that are functional in nature and do not maintain state.
pub trait UnaryPreAggFunction: Send + Sync {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError>;
}

pub enum OperatorResult {
    None,
    Single(Record),
    Many(Vec<Record>),
}

impl OperatorResult {
    /// to_vec should only be used where converting the result into a vector is required -- otherwise
    /// it will can be a significant performance penalty in hot loops.
    pub fn into_vec(self) -> Vec<Record> {
        match self {
            OperatorResult::None => vec![],
            OperatorResult::Single(rec) => vec![rec],
            OperatorResult::Many(recs) => recs,
        }
    }
}

pub trait Operator: Send + Sync {
    fn process_many_mut(&mut self, rec: Record) -> Result<OperatorResult, EvalError>;
    fn drain(self: Box<Self>) -> Box<dyn Iterator<Item=Record>> {
        Box::new(iter::empty())
    }
}

impl Operator for dyn SingleRowOperator {
    fn process_many_mut(&mut self, rec: Record) -> Result<OperatorResult, EvalError> {
        let rec_opt = self.process_mut(rec)?;
        Ok(match rec_opt {
            None => OperatorResult::None,
            Some(res) => OperatorResult::Single(res),
        })
    }

    fn drain(self: Box<Self>) -> Box<Iterator<Item=Record>> {
        self.drain()
    }
}

/// Trait for operators that maintain state while processing records.
pub trait SingleRowOperator: Send + Sync {
    fn process_mut(&mut self, rec: Record) -> Result<Option<Record>, EvalError>;
    /// Return any remaining records that may have been gathered by the operator.  This method
    /// will be called when there are no more new input records.
    fn drain(self: Box<Self>) -> Box<dyn Iterator<Item=Record>> {
        Box::new(iter::empty())
    }
}

/// Get a column from the given record.
fn get_input<'a>(rec: &'a Record, col: &Option<Expr>) -> Result<&'a str, EvalError> {
    match col {
        Some(expr) => {
            let res: &String = expr.eval_borrowed(&rec.data)?;
            Ok(res)
        }
        None => Ok(&rec.raw),
    }
}

/// Trait used to instantiate an operator from its definition.  If an operator does not maintain
/// state, the operator definition value can be cloned and returned.
pub trait OperatorBuilder: Send + Sync {
    fn build(&self) -> Box<dyn Operator>;
}

/// A trivial OperatorBuilder implementation for functional traits since they don't need to
/// maintain state.
impl<T> OperatorBuilder for T
    where
        T: 'static + UnaryPreAggFunction + Clone,
{
    fn build(&self) -> Box<dyn Operator> {
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

impl<T> Operator for T
    where
        T: UnaryPreAggFunction,
{

    fn process_many_mut(&mut self, rec: Record) -> Result<OperatorResult, EvalError> {
        let res = self.process(rec)?;
        Ok(match res {
            Some(rec) => OperatorResult::Single(rec),
            None => OperatorResult::None
        })
    }
}

/// A trivial UnaryPreAggOperator implementation for functional operators since they don't need
/// an object to contain any state.
/*impl<T> SingleRowOperator for T
    where
        T: UnaryPreAggFunction,
{
    fn process_mut(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        self.process(rec)
    }
}*/


impl AggregateOperator for PreAggAdapter {
    fn emit(&self) -> Aggregate {
        self.state.clone()
    }

    fn process(&mut self, row: Row) {
        match row {
            Row::Record(_) => panic!("PreAgg adaptor should only be used after aggregates"),
            Row::Aggregate(agg) => {
                let mut op = self.op_builder.build();
                let mut processed_records: Vec<data::VMap> = {
                    let records = agg
                        .data
                        .into_iter()
                        .map(|vmap| data::Record {
                            data: vmap,
                            raw: "".to_string(),
                        })
                        .flat_map(|rec| {
                            op.process_many_mut(rec)
                                .unwrap_or(OperatorResult::None)
                                .into_vec()
                        })
                        .map(|rec| rec.data);
                    records.collect()
                };
                processed_records.extend(op.drain().map(|rec| rec.data));
                let resulting_columns: Vec<String> = {
                    processed_records
                        .iter()
                        .flat_map(|vmap| vmap.keys())
                        .unique()
                        .cloned()
                }
                    .collect();
                let output_column_set: HashSet<String> =
                    HashSet::from_iter(resulting_columns.iter().cloned());
                let input_column_set = HashSet::from_iter(agg.columns.iter().cloned());
                let new_columns: Vec<String> = output_column_set
                    .difference(&input_column_set)
                    .cloned()
                    .collect();
                let mut columns = agg.columns;
                columns.extend(new_columns);
                let columns: Vec<String> = columns
                    .into_iter()
                    .filter(|col| output_column_set.contains(col))
                    .collect();
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

#[derive(Debug, Clone)]
pub enum ValueRef {
    Field(String),
    IndexAt(i64),
}

#[derive(Debug, Clone)]
pub enum Expr {
    // First record can only be a String, after that, you can do things like `[0]` in addition to `.foo`
    NestedColumn { head: String, rest: Vec<ValueRef> },
    BoolUnary(UnaryExpr<BoolUnaryExpr>),
    Comparison(BinaryExpr<BoolExpr>),
    Value(&'static data::Value),
}

#[derive(Debug, Clone)]
pub struct UnaryExpr<T> {
    pub operator: T,
    pub operand: Box<Expr>,
}

#[derive(Clone, Debug)]
pub enum BoolUnaryExpr {
    Not,
}

#[derive(Debug, Clone)]
pub struct BinaryExpr<T> {
    pub operator: T,
    pub left: Box<Expr>,
    pub right: Box<Expr>,
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

impl<T: Copy + Send + Sync> Evaluatable<T> for T {
    fn eval(&self, _record: &HashMap<String, data::Value>) -> Result<T, EvalError> {
        Ok(*self)
    }
}

impl Evaluatable<bool> for BinaryExpr<BoolExpr> {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<bool, EvalError> {
        let l: &data::Value = self.left.eval_borrowed(record)?;
        let r: &data::Value = self.right.eval_borrowed(record)?;
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

impl Evaluatable<bool> for UnaryExpr<BoolUnaryExpr> {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<bool, EvalError> {
        let bool_res: &data::Value = self.operand.eval_borrowed(record)?;

        match bool_res {
            data::Value::Bool(true) => Ok(false),
            data::Value::Bool(false) => Ok(true),
            _ => Err(EvalError::ExpectedBoolean {
                found: bool_res.to_string(),
            }),
        }
    }
}

impl Evaluatable<bool> for Expr {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<bool, EvalError> {
        match self.eval_borrowed(record)? {
            data::Value::Bool(bool_value) => Ok(*bool_value),
            other => Err(EvalError::ExpectedBoolean {
                found: other.to_string(),
            }),
        }
    }
}

impl EvaluatableBorrowed<data::Value> for Expr {
    fn eval_borrowed<'a>(
        &self,
        record: &'a HashMap<String, data::Value>,
    ) -> Result<&'a data::Value, EvalError> {
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
                                found: other.render(&RenderConfig::default()),
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
                                found: other.render(&RenderConfig::default()),
                            });
                        }
                    }
                }
                Ok(root_record)
            }
            Expr::BoolUnary(
                ref unary_op @ UnaryExpr {
                    operator: BoolUnaryExpr::Not,
                    ..
                },
            ) => {
                let bool_res = unary_op.eval(record)?;
                Ok(data::Value::from_bool(bool_res))
            }
            Expr::Comparison(ref binary_expr) => {
                let bool_res = binary_expr.eval(record)?;
                Ok(data::Value::from_bool(bool_res))
            }
            Expr::Value(ref v) => Ok(v),
        }
    }
}

impl Evaluatable<f64> for Expr {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Result<f64, EvalError> {
        let value: &data::Value = self.eval_borrowed(record)?;
        match value {
            data::Value::Int(i) => Ok(*i as f64),
            data::Value::Float(f) => Ok(f.into_inner()),
            other => Err(EvalError::ExpectedNumber {
                found: format!("{}", other),
            }),
        }
    }
}

impl EvaluatableBorrowed<String> for Expr {
    fn eval_borrowed<'a>(&self, record: &'a Data) -> Result<&'a String, EvalError> {
        let as_value: &data::Value = self.eval_borrowed(record)?;
        match as_value {
            data::Value::None => Err(EvalError::UnexpectedNone {
                tpe: "String".to_string(),
            }),
            data::Value::Str(ref s) => Ok(s),
            _ => Err(EvalError::ExpectedString {
                found: "other".to_string(),
            }),
        }
    }
}

pub struct Count {
    count: i64,
}

impl Count {
    pub fn new() -> Count {
        Count { count: 0 }
    }
}

impl AggregateFunction for Count {
    fn process(&mut self, _rec: &Data) -> Result<(), EvalError> {
        self.count += 1;
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::Int(self.count)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Count::new())
    }
}

pub struct Sum {
    total: f64,
    column: Expr,
}

impl Sum {
    pub fn empty<T: Into<Expr>>(column: T) -> Self {
        Sum {
            total: 0.0,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Sum {
    fn process(&mut self, rec: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(rec)?;
        self.total += value;
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::from_float(self.total)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Sum::empty(self.column.clone()))
    }
}

pub struct CountDistinct {
    state: HashSet<data::Value>,
    column: Expr,
}

impl CountDistinct {
    pub fn empty<T: Into<Expr>>(column: T) -> Self {
        CountDistinct {
            state: HashSet::new(),
            column: column.into(),
        }
    }
}

impl AggregateFunction for CountDistinct {
    fn process(&mut self, rec: &Data) -> Result<(), EvalError> {
        let value: &data::Value = self.column.eval_borrowed(rec)?;
        self.state.insert(value.clone());
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::Int(self.state.len() as i64)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(CountDistinct::empty(self.column.clone()))
    }
}

pub struct Min {
    min: f64,
    column: Expr,
}

impl Min {
    pub fn empty<T: Into<Expr>>(column: T) -> Min {
        Min {
            min: std::f64::INFINITY,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Min {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        if value < self.min {
            self.min = value;
        }
        Ok(())
    }

    fn emit(&self) -> data::Value {
        if self.min.is_finite() {
            data::Value::from_float(self.min)
        } else {
            data::Value::None
        }
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Min::empty(self.column.clone()))
    }
}

pub struct Average {
    total: f64,
    count: i64,
    column: Expr,
}

impl Average {
    pub fn empty<T: Into<Expr>>(column: T) -> Average {
        Average {
            total: 0.0,
            count: 0,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Average {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        self.total += value;
        self.count += 1;
        Ok(())
    }

    fn emit(&self) -> data::Value {
        data::Value::from_float(self.total / self.count as f64)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Average::empty(self.column.clone()))
    }
}

pub struct Max {
    max: f64,
    column: Expr,
}

impl Max {
    pub fn empty<T: Into<Expr>>(column: T) -> Max {
        Max {
            max: std::f64::NEG_INFINITY,
            column: column.into(),
        }
    }
}

impl AggregateFunction for Max {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        if value > self.max {
            self.max = value;
        }
        Ok(())
    }

    fn emit(&self) -> data::Value {
        if self.max.is_finite() {
            data::Value::from_float(self.max)
        } else {
            data::Value::None
        }
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Max::empty(self.column.clone()))
    }
}

pub struct Splat {
    column: Expr
}

impl Operator for Splat {
    fn process_many_mut(&mut self, rec: Record) -> Result<OperatorResult, EvalError> {
        unimplemented!()
    }
}

pub struct Percentile {
    ckms: CKMS<f64>,
    column: Expr,
    percentile: f64,
}

impl Percentile {
    pub fn empty<T: Into<Expr>>(column: T, percentile: f64) -> Self {
        if percentile >= 1.0 {
            panic!("Percentiles must be < 1");
        }

        Percentile {
            ckms: CKMS::<f64>::new(0.001),
            column: column.into(),
            percentile,
        }
    }
}

impl AggregateFunction for Percentile {
    fn process(&mut self, data: &Data) -> Result<(), EvalError> {
        let value: f64 = self.column.eval(data)?;
        self.ckms.insert(value);
        Ok(())
    }

    fn emit(&self) -> data::Value {
        let pct_opt = self.ckms.query(self.percentile);
        pct_opt
            .map(|(_usize, pct_float)| data::Value::from_float(pct_float))
            .unwrap_or(data::Value::None)
    }

    fn empty_box(&self) -> Box<dyn AggregateFunction> {
        Box::new(Percentile::empty(self.column.clone(), self.percentile))
    }
}

#[derive(PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

pub struct Sorter {
    columns: Vec<String>,
    initial_columns: Vec<String>,
    state: Vec<Data>,
    ordering: Box<dyn Fn(&Data, &Data) -> Ordering + Send + Sync>,
    direction: SortDirection,
}

impl Sorter {
    pub fn new(columns: Vec<String>, direction: SortDirection) -> Self {
        Sorter {
            state: Vec::new(),
            columns: Vec::new(),
            direction,
            initial_columns: columns.clone(),
            ordering: Box::new(Record::ordering(columns)),
        }
    }

    fn new_columns(&self, data: &HashMap<String, data::Value>) -> Vec<String> {
        let mut new_keys: Vec<String> = data
            .keys()
            .filter(|key| !self.columns.contains(key))
            .cloned()
            .collect();
        new_keys.sort();
        new_keys
    }
}

impl AggregateOperator for Sorter {
    fn emit(&self) -> data::Aggregate {
        let mut sorted_data = self.state.to_vec();
        let order = &self.ordering;
        // potential hotspot with large numbers of columns
        let additional_columns: Vec<String> = self
            .columns
            .iter()
            .filter(|c| !self.initial_columns.contains(c))
            .cloned()
            .collect();
        // To produce a deterministic sort, we should also sort by the non-key columns

        let second_ordering = Record::ordering(additional_columns);

        if self.direction == SortDirection::Ascending {
            sorted_data.sort_by(|l, r| (order)(l, r).then(second_ordering(l, r)));
        } else {
            sorted_data.sort_by(|l, r| (order)(r, l).then(second_ordering(l, r)));
        }
        Aggregate {
            data: sorted_data,
            columns: self.columns.clone(),
        }
    }

    fn process(&mut self, row: Row) {
        let order = &self.ordering;
        match row {
            Row::Aggregate(agg) => {
                self.columns = agg.columns;
                self.state = agg.data;
            }
            Row::Record(rec) => {
                let new_cols = self.new_columns(&rec.data);
                self.state.push(rec.data);
                self.state.sort_by(|l, r| (order)(l, r));
                self.columns.extend(new_cols);
            }
        }
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
        let key_values = self.key_cols.iter().map(|expr| expr.eval_borrowed(data));
        let key_columns: Vec<data::Value> = key_values
            .map(|value_res| value_res.unwrap_or_else(|_| data::NONE))
            .cloned()
            .collect();
        let agg_col = &self.agg_col;
        let row = self.state.entry(key_columns).or_insert_with(|| {
            agg_col
                .iter()
                .map(|&(ref k, ref v)| (k.to_owned(), v.empty_box()))
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
        columns.extend(self.agg_col.iter().map(|&(ref k, ..)| k.to_string()));
        let data = self.state.iter().map(|(key_values, agg_map)| {
            let key_values = key_values.iter().cloned();
            let key_cols = self.key_col_headers.iter().map(|s| s.to_owned());
            let mut res_map: data::VMap =
                HashMap::from_iter(itertools::zip_eq(key_cols, key_values));
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

#[derive(Clone)]
pub struct ParseOptions {
    pub drop_nonmatching: bool,
}

#[derive(Clone)]
pub struct Parse {
    regex: regex::Regex,
    fields: Vec<String>,
    input_column: Option<Expr>,
    options: ParseOptions,
}

impl Parse {
    pub fn new(
        pattern: regex::Regex,
        fields: Vec<String>,
        input_column: Option<Expr>,
        options: ParseOptions,
    ) -> Self {
        Parse {
            regex: pattern,
            fields,
            input_column,
            options,
        }
    }

    fn matches(&self, rec: &Record) -> Result<Option<Vec<data::Value>>, EvalError> {
        let inp = get_input(rec, &self.input_column)?;
        let matches: Vec<regex::Captures> = self.regex.captures_iter(inp.trim()).collect();
        if matches.is_empty() {
            Ok(None)
        } else {
            let capture = &matches[0];
            let mut values: Vec<data::Value> = Vec::new();
            for i in 0..self.fields.len() {
                // the first capture is the entire string
                values.push(data::Value::from_string(&capture[i + 1]));
            }
            Ok(Some(values))
        }
    }
}

impl UnaryPreAggFunction for Parse {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let matches = self.matches(&rec)?;
        match (matches, self.options.drop_nonmatching) {
            (None, true) => Ok(None),
            (None, false) => {
                let new_fields: Vec<_> = self
                    .fields
                    .iter()
                    .filter(|f| !rec.data.contains_key(&f.to_string()))
                    .collect();

                let mut rec = rec;
                for field in new_fields {
                    rec = rec.put(field, data::Value::None);
                }
                Ok(Some(rec))
            }
            (Some(matches), _) => {
                let mut rec = rec;
                for (i, field) in self.fields.iter().enumerate() {
                    rec = rec.put(field, matches[i].clone());
                }
                Ok(Some(rec))
            }
        }
    }
}

#[derive(Clone)]
pub struct Where<T: 'static + Evaluatable<bool>> {
    expr: T,
}

impl<T: 'static + Evaluatable<bool>> Where<T> {
    pub fn new(expr: T) -> Self {
        Where { expr }
    }
}

impl<T: 'static + Evaluatable<bool>> UnaryPreAggFunction for Where<T> {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        if self.expr.eval(&rec.data)? {
            Ok(Some(rec))
        } else {
            Ok(None)
        }
    }
}

pub struct TotalDef {
    column: Expr,
    output_column: String,
}

impl TotalDef {
    pub fn new(column: Expr, output_column: String) -> Self {
        TotalDef {
            column,
            output_column,
        }
    }
}

impl OperatorBuilder for TotalDef {
    fn build(&self) -> Box<dyn Operator> {
        Box::new(Total::new(self.column.clone(), self.output_column.clone()))
    }
}

pub struct Total {
    column: Expr,
    total: f64,
    output_column: String,
}

impl Total {
    pub fn new<T: Into<Expr>>(column: T, output_column: String) -> Total {
        Total {
            column: column.into(),
            total: 0.0,
            output_column,
        }
    }
}

impl Operator for Total {
    fn process_many_mut(&mut self, rec: Record) -> Result<OperatorResult, EvalError> {
        // I guess this means there are cases when you need to both emit a warning _and_ a row, TODO
        // for now, we'll just emit the row
        let val: f64 = self.column.eval(&rec.data).unwrap_or(0.0);
        self.total += val;
        let rec = rec.put(&self.output_column, data::Value::from_float(self.total));
        Ok(OperatorResult::Single(rec))
    }
}

#[derive(Clone)]
pub struct Split {
    separator: String,
    input_column: Option<Expr>,
    output_column: Option<Expr>,
}

impl Split {
    pub fn new(separator: String, input_column: Option<Expr>, output_column: Option<Expr>) -> Self {
        Self {
            separator,
            input_column,
            output_column,
        }
    }
}

impl UnaryPreAggFunction for Split {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let inp = get_input(&rec, &self.input_column)?;
        let array = split::split_with_delimiters(&inp, &self.separator, &split::DEFAULT_DELIMITERS)
            .into_iter()
            .map(data::Value::from_string)
            .collect();
        let rec = if let Some(output_column) = &self.output_column {
            rec.put_expr(output_column, data::Value::Array(array))?
        } else {
            rec.put("_split", data::Value::Array(array))
        };
        Ok(Some(rec))
    }
}

#[derive(Clone)]
pub enum FieldMode {
    Only,
    Except,
}

#[derive(Clone)]
pub struct Fields {
    columns: HashSet<String>,
    mode: FieldMode,
}

impl Fields {
    pub fn new(columns: &[String], mode: FieldMode) -> Self {
        let columns = HashSet::from_iter(columns.iter().cloned());
        Fields { columns, mode }
    }
}

impl UnaryPreAggFunction for Fields {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let mut rec = rec;
        match self.mode {
            FieldMode::Only => {
                rec.data.retain(|k, _| self.columns.contains(k));
            }
            FieldMode::Except => {
                rec.data.retain(|k, _| !self.columns.contains(k));
            }
        }
        if rec.data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rec))
        }
    }
}

#[derive(Clone)]
pub struct ParseJson {
    input_column: Option<Expr>,
}

impl ParseJson {
    pub fn new(input_column: Option<Expr>) -> ParseJson {
        ParseJson { input_column }
    }
}

impl UnaryPreAggFunction for ParseJson {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        fn json_to_value(v: &JsonValue) -> data::Value {
            match v {
                &JsonValue::Number(ref num) => {
                    if num.is_i64() {
                        data::Value::Int(num.as_i64().unwrap())
                    } else {
                        data::Value::from_float(num.as_f64().unwrap())
                    }
                }
                &JsonValue::String(ref s) => data::Value::Str(s.to_string()),
                &JsonValue::Null => data::Value::None,
                &JsonValue::Bool(b) => data::Value::Bool(b),
                &JsonValue::Object(ref map) => data::Value::Obj(
                    map.iter()
                        .map(|(k, v)| (k.to_string(), json_to_value(v)))
                        .collect::<HashMap<String, data::Value>>()
                        .into(),
                ),
                &JsonValue::Array(ref vec) => {
                    data::Value::Array(vec.iter().map(json_to_value).collect::<Vec<data::Value>>())
                }
            }
        };
        let json: JsonValue = {
            let inp = get_input(&rec, &self.input_column)?;
            serde_json::from_str(&inp).map_err(|_| EvalError::ExpectedJson {
                found: inp.trim_end().to_string(),
            })?
        };
        let res = match json {
            JsonValue::Object(map) => map
                .iter()
                .fold(rec, |record, (k, v)| record.put(k, json_to_value(v))),
            // TODO: we'll implicitly drop non-object root values. Maybe we should produce an EvalError here
            _other => rec,
        };
        Ok(Some(res))
    }
}

#[derive(Clone)]
pub struct ParseLogfmt {
    input_column: Option<Expr>,
}

impl ParseLogfmt {
    pub fn new(input_column: Option<Expr>) -> ParseLogfmt {
        ParseLogfmt { input_column }
    }
}

impl UnaryPreAggFunction for ParseLogfmt {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let pairs = {
            let inp = get_input(&rec, &self.input_column)?;
            // Record includes the trailing newline, while logfmt considers that part of the
            // message if present. Trim any trailing whitespace.
            logfmt::parse(&inp.trim_end())
        };
        let res = {
            pairs.into_iter().fold(rec, |record, pair| match pair.val {
                None => record.put(&pair.key, data::Value::None),
                Some(val) => record.put(&pair.key, data::Value::from_string(val)),
            })
        };
        Ok(Some(res))
    }
}

/// The definition for a limit operator, which is a positive number used to specify whether
/// the first N rows should be passed through to the downstream operators.  Negative limits are
/// not supported at this time.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct LimitDef {
    limit: i64,
}

impl LimitDef {
    pub fn new(limit: i64) -> Self {
        LimitDef { limit }
    }
}

/// The state for a limit operator
pub enum Limit {
    Head {
        /// The current index into the input stream.
        index: u64,
        /// The number of rows to pass through before aborting.
        limit: u64,
    },
    Tail {
        /// A circular queue to keep track of the tail of the input stream.
        queue: VecDeque<Record>,
        /// The size of the circular buffer.
        /// XXX Might be better to use a separate type.
        limit: usize,
    },
}

impl OperatorBuilder for LimitDef {
    fn build(&self) -> Box<dyn Operator> {
        Box::new(if self.limit > 0 {
            Limit::Head {
                index: 0,
                limit: self.limit as u64,
            }
        } else {
            Limit::Tail {
                queue: VecDeque::with_capacity(-self.limit as usize),
                limit: -self.limit as usize,
            }
        })
    }
}

impl Operator for Limit {
    fn process_many_mut(&mut self, rec: Record) -> Result<OperatorResult, EvalError> {
        match self {
            Limit::Head {
                ref mut index,
                limit,
            } => {
                (*index) += 1;

                if index <= limit {
                    Ok(OperatorResult::Single(rec))
                } else {
                    Ok(OperatorResult::None)
                }
            }
            Limit::Tail {
                ref mut queue,
                limit,
            } => {
                if queue.len() == *limit {
                    queue.pop_front();
                }
                queue.push_back(rec);

                Ok(OperatorResult::None)
            }
        }
    }

    fn drain(self: Box<Self>) -> Box<dyn Iterator<Item=Record>> {
        match *self {
            Limit::Head { .. } => Box::new(iter::empty()),
            Limit::Tail { queue, .. } => Box::new(queue.into_iter()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Value;
    use crate::lang;
    use maplit::hashmap;

    impl Expr {
        fn column(key: &str) -> Expr {
            Expr::NestedColumn {
                head: key.to_owned(),
                rest: vec![],
            }
        }
    }

    impl<S: Into<String>> From<S> for Expr {
        fn from(inp: S) -> Self {
            Expr::column(&inp.into())
        }
    }

    #[test]
    fn test_nested_eval() {
        let rec = Record::new(
            &(r#"{"k1": {"k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}}"#.to_string() + "\n"),
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        let expr = Expr::NestedColumn {
            head: "k1".to_string(),
            rest: vec![ValueRef::Field("k2".to_string())],
        };
        let data: &data::Value = expr.eval_borrowed(&rec.data).unwrap();
        assert_eq!(data, &data::Value::from_float(5.5));
    }

    #[test]
    fn test_nested_eval_error() {
        let rec = Record::new(
            &(r#"{"k1": {"k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}}"#.to_string() + "\n"),
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        let expr = Expr::NestedColumn {
            head: "k1".to_string(),
            rest: vec![ValueRef::Field("k11".to_string())],
        };
        let res: Result<&data::Value, EvalError> = expr.eval_borrowed(&rec.data);
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
            &(r#"{"k1": 5, "k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}"#.to_string() + "\n"),
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
            &(r#"{"k1": {"k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}}"#.to_string() + "\n"),
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
        let rec = Record::new(&(r#"k1=5 k2=5.5 k3="a str" k4="#.to_string() + "\n"));
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
            vec![("_count".to_string(), Box::new(Count::new()))];
        let mut count_agg = MultiGrouper::new(&[], vec![], ops);
        (0..10)
            .map(|n| Record::new(&n.to_string()))
            .for_each(|rec| count_agg.process(Row::Record(rec)));
        let agg = count_agg.emit();
        assert_eq!(agg.columns, vec!["_count"]);
        assert_eq!(
            agg.data,
            vec![hashmap! {"_count".to_string() => data::Value::Int(10)}]
        );
        (0..10)
            .map(|n| Record::new(&n.to_string()))
            .for_each(|rec| count_agg.process(Row::Record(rec)));
        assert_eq!(
            count_agg.emit().data,
            vec![hashmap! {"_count".to_string() => data::Value::Int(20)}]
        );
    }

    #[test]
    fn multi_grouper() {
        let ops: Vec<(String, Box<dyn AggregateFunction>)> = vec![
            ("_count".to_string(), Box::new(Count::new())),
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
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..10).for_each(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..25).for_each(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("not ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..3).for_each(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        let agg = grouper.emit();
        let mut sorted_data = agg.data.clone();
        let ordering = Record::ordering(vec!["_count".to_string()]);
        sorted_data.sort_by(|l, r| ordering(l, r));
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
            vec![("_count".to_string(), Box::new(Count::new()))];
        let mut count_agg = MultiGrouper::new(&[Expr::column("k1")], vec!["k1".to_string()], ops);
        (0..10).for_each(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            count_agg.process(Row::Record(rec));
        });
        (0..25).for_each(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("not ok".to_string()));
            count_agg.process(Row::Record(rec));
        });
        (0..3).for_each(|n| {
            let rec = Record::new(&n.to_string());
            count_agg.process(Row::Record(rec));
        });
        let agg = count_agg.emit();
        let mut sorted_data = agg.data.clone();
        let ordering = Record::ordering(vec!["_count".to_string()]);
        sorted_data.sort_by(|l, r| ordering(l, r));
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
        let mut sorter = Sorter::new(vec!["count".to_string()], SortDirection::Ascending);
        sorter.process(data::Row::Aggregate(agg.clone()));
        assert_eq!(sorter.emit(), agg.clone());

        let mut sorter = Sorter::new(vec!["count".to_string()], SortDirection::Descending);
        sorter.process(data::Row::Aggregate(agg.clone()));

        let mut revagg = agg.clone();
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
        let _: () = adapted.process(Row::Aggregate(agg.clone()));
        assert_eq!(adapted.emit(), agg.clone());
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
        total_op.process(Row::Aggregate(agg.clone()));
        let result = total_op.emit().data;
        assert_eq!(result[0].get("_total").unwrap(), &Value::from_float(100.0));
        assert_eq!(result[1].get("_total").unwrap(), &Value::from_float(600.0));
        assert_eq!(result.len(), 2);
        //assert_eq!(, agg.clone());
    }
}
