extern crate itertools;
extern crate quantiles;
extern crate regex;
//extern crate regex_syntax;
extern crate serde_json;

use self::quantiles::ckms::CKMS;
use self::serde_json::Value as JsonValue;
use crate::data;
use crate::data::{Aggregate, Record, Row};
use crate::operator::itertools::Itertools;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::iter;
use std::iter::FromIterator;

type Data = HashMap<String, data::Value>;

#[derive(Debug, Fail)]
pub enum EvalError {
    #[fail(display = "No value for key {}", key)]
    NoValueForKey { key: String },

    #[fail(display = "Found None, expected {}", tpe)]
    UnexpectedNone { tpe: String },

    #[fail(display = "Expected JSON, found {}", found)]
    ExpectedJson { found: String },

    #[fail(display = "Expected string, found {}", found)]
    ExpectedString { found: String },
}

pub trait Evaluatable<T>: Send + Sync + Clone {
    fn eval(&self, record: &Data) -> Result<T, EvalError>;
}

pub trait EvaluatableBorrowed<T> {
    fn eval_borrowed<'a>(&self, record: &'a Data) -> Result<&'a T, EvalError>;
}

pub trait UnaryPreAggOperator: Send + Sync {
    /// Returns true if the operator is still willing to accept input records.
    fn is_open(&self) -> bool { true }
    fn process(&mut self, rec: Record) -> Result<Option<Record>, EvalError>;
    /// Return any remaining records that may have been gathered by the operator.  This method
    /// will be called when there are no more new input records.
    fn drain(self: Box<Self>) -> Box<Iterator<Item=Record>> {
        Box::new(iter::empty())
    }
    fn process_mut(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        self.process(rec)
    }
    fn get_input<'a>(&self, rec: &'a Record, col: &Option<Expr>) -> Result<&'a str, EvalError> {
        match col {
            Some(expr) => {
                let res: &String = expr.eval_borrowed(&rec.data)?;
                Ok(res)
            }
            None => Ok(&rec.raw),
        }
    }
}

/// Trait used to instantiate an operator from its definition.  If an operator does not maintain
/// state, the operator definition value can be cloned and returned.
pub trait OperatorBuilder: Send + Sync {
    fn to_preagg_operator(&self) -> Box<UnaryPreAggOperator>;
}

pub struct PreAggAdapter {
    op_supplier: Box<OperatorBuilder>,
    state: Aggregate,
}

impl PreAggAdapter {
    pub fn new(op_supplier: Box<OperatorBuilder>) -> Self {
        PreAggAdapter {
            op_supplier,
            state: Aggregate {
                columns: Vec::new(),
                data: Vec::new(),
            },
        }
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
                let mut op = self.op_supplier.to_preagg_operator();
                let columns = agg.columns;
                let processed_records: Vec<data::VMap> = {
                    let records = agg
                        .data
                        .into_iter()
                        .map(|vmap| data::Record {
                            data: vmap,
                            raw: "".to_string(),
                        })
                        .flat_map(|rec| if op.is_open() {
                            op.process(rec).unwrap_or(None)
                        } else {
                            None
                        })
                        .map(|rec| rec.data);
                    records.collect()
                };
                let new_keys: Vec<String> = {
                    processed_records
                        .iter()
                        .flat_map(|vmap| vmap.keys())
                        .filter(|col| !columns.contains(col))
                        .unique()
                        .cloned()
                }
                .collect();
                let mut columns = columns;
                columns.extend(new_keys);
                self.state = Aggregate {
                    data: processed_records,
                    columns,
                };

                //self.state = Aggregate { }
            }
        }
    }
}

pub trait AggregateOperator: Send + Sync {
    fn emit(&self) -> data::Aggregate;
    fn process(&mut self, row: Row);
}

pub trait AggregateFunction: Send + Sync {
    fn process(&mut self, rec: &Data);
    fn emit(&self) -> data::Value;
    fn empty_box(&self) -> Box<AggregateFunction>;
}

#[derive(Debug, Clone)]
pub enum Expr {
    Column(String),
    Comparison(BinaryExpr<BoolExpr>),
    Value(&'static data::Value),
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

impl EvaluatableBorrowed<data::Value> for Expr {
    fn eval_borrowed<'a>(
        &self,
        record: &'a HashMap<String, data::Value>,
    ) -> Result<&'a data::Value, EvalError> {
        match *self {
            Expr::Column(ref col) => record
                .get(col)
                .ok_or_else(|| EvalError::NoValueForKey { key: col.clone() }),
            Expr::Comparison(ref binary_expr) => {
                let bool_res = binary_expr.eval(record)?;
                Ok(data::Value::from_bool(bool_res))
            }
            Expr::Value(ref v) => Ok(v),
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
    fn process(&mut self, _rec: &Data) {
        self.count += 1;
    }

    fn emit(&self) -> data::Value {
        data::Value::Int(self.count)
    }

    fn empty_box(&self) -> Box<AggregateFunction> {
        Box::new(Count::new())
    }
}

pub struct Sum {
    total: f64,
    column: Expr,
    warnings: Vec<String>,
    is_float: bool,
}

impl Sum {
    pub fn empty<T: Into<Expr>>(column: T) -> Self {
        Sum {
            total: 0.0,
            column: column.into(),
            warnings: Vec::new(),
            is_float: false,
        }
    }
}

impl AggregateFunction for Sum {
    fn process(&mut self, rec: &Data) {
        self.column
            .eval_borrowed(rec)
            .iter()
            .for_each(|value| match value {
                &data::Value::Float(ref f) => {
                    self.is_float = true;
                    self.total += f.into_inner();
                }
                &data::Value::Int(ref i) => {
                    self.total += *i as f64;
                }
                _other => self
                    .warnings
                    .push("Got string. Can only average int or float".to_string()),
            });
    }

    fn emit(&self) -> data::Value {
        if self.is_float {
            data::Value::from_float(self.total)
        } else {
            data::Value::Int(self.total as i64)
        }
    }

    fn empty_box(&self) -> Box<AggregateFunction> {
        Box::new(Sum::empty(self.column.clone()))
    }
}

pub struct CountDistinct {
    state: HashSet<data::Value>,
    column: String,
}

impl CountDistinct {
    pub fn empty(column: &str) -> Self {
        CountDistinct {
            state: HashSet::new(),
            column: column.to_string(),
        }
    }
}

impl AggregateFunction for CountDistinct {
    fn process(&mut self, rec: &Data) {
        rec.get(&self.column).iter().cloned().for_each(|value| {
            self.state.insert(value.clone());
        });
    }

    fn emit(&self) -> data::Value {
        data::Value::Int(self.state.len() as i64)
    }

    fn empty_box(&self) -> Box<AggregateFunction> {
        Box::new(CountDistinct::empty(&self.column))
    }
}

pub struct Average {
    total: f64,
    count: i64,
    column: Expr,
    warnings: Vec<String>,
}

impl Average {
    pub fn empty<T: Into<Expr>>(column: T) -> Average {
        Average {
            total: 0.0,
            count: 0,
            column: column.into(),
            warnings: Vec::new(),
        }
    }
}

impl AggregateFunction for Average {
    fn process(&mut self, data: &Data) {
        self.column
            .eval_borrowed(data)
            .iter()
            .for_each(|value| match value {
                &data::Value::Float(ref f) => {
                    self.total += f.into_inner();
                    self.count += 1
                }
                &data::Value::Int(ref i) => {
                    self.total += *i as f64;
                    self.count += 1
                }
                _other => self
                    .warnings
                    .push("Got string. Can only average into or float".to_string()),
            });
    }

    fn emit(&self) -> data::Value {
        data::Value::from_float(self.total / self.count as f64)
    }

    fn empty_box(&self) -> Box<AggregateFunction> {
        Box::new(Average::empty(self.column.clone()))
    }
}

pub struct Percentile {
    ckms: CKMS<f64>,
    column: String,
    percentile: f64,
    warnings: Vec<String>,
}

impl Percentile {
    pub fn empty(column: String, percentile: f64) -> Self {
        if percentile >= 1.0 {
            panic!("Percentiles must be < 1");
        }

        Percentile {
            ckms: CKMS::<f64>::new(0.001),
            column,
            warnings: Vec::new(),
            percentile,
        }
    }
}

impl AggregateFunction for Percentile {
    fn process(&mut self, data: &Data) {
        data.get(&self.column)
            .iter()
            .for_each(|value| match *value {
                &data::Value::Float(ref f) => {
                    self.ckms.insert(f.into_inner());
                }
                &data::Value::Int(ref i) => self.ckms.insert(*i as f64),
                _other => self
                    .warnings
                    .push("Got string. Can only average int or float".to_string()),
            });
    }

    fn emit(&self) -> data::Value {
        let pct_opt = self.ckms.query(self.percentile);
        pct_opt
            .map(|(_usize, pct_float)| data::Value::from_float(pct_float))
            .unwrap_or(data::Value::None)
    }

    fn empty_box(&self) -> Box<AggregateFunction> {
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
    state: Vec<Data>,
    ordering: Box<Fn(&Data, &Data) -> Ordering + Send + Sync>,
    direction: SortDirection,
}

impl Sorter {
    pub fn new(columns: Vec<String>, direction: SortDirection) -> Self {
        Sorter {
            state: Vec::new(),
            columns: Vec::new(),
            direction,
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
        Aggregate {
            data: self.state.to_vec(),
            columns: self.columns.clone(),
        }
    }

    fn process(&mut self, row: Row) {
        let order = &self.ordering;
        match row {
            Row::Aggregate(agg) => {
                self.columns = agg.columns;
                self.state = agg.data;
                if self.direction == SortDirection::Ascending {
                    self.state.sort_by(|l, r| (order)(l, r));
                } else {
                    self.state.sort_by(|l, r| (order)(r, l));
                }
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
    agg_col: Vec<(String, Box<AggregateFunction>)>,
    // key-column values -> (agg_columns -> builders)
    state: HashMap<Vec<data::Value>, HashMap<String, Box<AggregateFunction>>>,
}

impl MultiGrouper {
    pub fn new(
        key_cols: &[Expr],
        key_col_headers: Vec<String>,
        aggregators: Vec<(String, Box<AggregateFunction>)>,
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
            fun.process(data);
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
pub struct Parse {
    regex: regex::Regex,
    fields: Vec<String>,
    input_column: Option<Expr>,
}

impl Parse {
    pub fn new(
        pattern: regex::Regex,
        fields: Vec<String>,
        input_column: Option<Expr>,
    ) -> Self {
        Parse {
            regex: pattern,
            fields,
            input_column: input_column,
        }
    }

    fn matches(&self, rec: &Record) -> Result<Option<Vec<data::Value>>, EvalError> {
        let inp = self.get_input(rec, &self.input_column)?;
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

impl OperatorBuilder for Parse {
    fn to_preagg_operator(&self) -> Box<UnaryPreAggOperator> {
        Box::new((*self).clone())
    }
}

impl UnaryPreAggOperator for Parse {
    fn process(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        let matches = self.matches(&rec)?;
        match matches {
            None => Ok(None),
            Some(matches) => {
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

impl<T: 'static + Evaluatable<bool>> OperatorBuilder for Where<T> {
    fn to_preagg_operator(&self) -> Box<UnaryPreAggOperator> {
        Box::new((*self).clone())
    }
}

impl<T: 'static + Evaluatable<bool>> UnaryPreAggOperator for Where<T> {
    fn process(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        if self.expr.eval(&rec.data)? {
            Ok(Some(rec))
        } else {
            Ok(None)
        }
    }
}

pub struct Total {
    column: Expr,
    total: f64,
    state: Vec<Data>,
    columns: Vec<String>,
    output_column: String,
}

impl Total {
    pub fn new<T: Into<Expr>>(column: T, output_column: String) -> Total {
        Total {
            column: column.into(),
            total: 0.0,
            state: Vec::new(),
            columns: Vec::new(),
            output_column,
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

    fn proc_rec(&mut self, mut data: Data) -> Data {
        let val: f64 = self
            .column
            .eval_borrowed(&data)
            .map(|value| match value {
                data::Value::Int(i) => *i as f64,
                data::Value::Float(f) => f.into_inner(),
                _ => 0.0,
            })
            .unwrap_or(0.0);
        self.total += val;
        data.insert(
            self.output_column.to_string(),
            data::Value::from_float(self.total),
        );
        data
    }
}

impl AggregateOperator for Total {
    fn emit(&self) -> data::Aggregate {
        Aggregate {
            data: self.state.to_vec(),
            columns: self.columns.clone(),
        }
    }

    fn process(&mut self, row: Row) {
        match row {
            Row::Aggregate(mut agg) => {
                agg.columns.push(self.output_column.to_string());
                self.columns = agg.columns;
                self.total = 0.0;
                let new_agg = agg
                    .data
                    .into_iter()
                    .map(|data| self.proc_rec(data))
                    .collect();
                self.state = new_agg;
            }
            Row::Record(rec) => {
                let processed = self.proc_rec(rec.data);
                let new_cols = self.new_columns(&processed);
                self.state.push(processed);
                self.columns.extend(new_cols);
            }
        }
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

impl OperatorBuilder for Fields {
    fn to_preagg_operator(&self) -> Box<UnaryPreAggOperator> {
        Box::new((*self).clone())
    }
}

impl UnaryPreAggOperator for Fields {
    fn process(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
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
    pub fn new(input_column: Option<String>) -> ParseJson {
        ParseJson {
            input_column: input_column.map(Expr::Column),
        }
    }
}

impl OperatorBuilder for ParseJson {
    fn to_preagg_operator(&self) -> Box<UnaryPreAggOperator> {
        Box::new((*self).clone())
    }
}

impl UnaryPreAggOperator for ParseJson {
    fn process(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        let json: JsonValue = {
            let inp = self.get_input(&rec, &self.input_column)?;
            serde_json::from_str(&inp).map_err(|_| EvalError::ExpectedJson{
                found: inp.trim_end().to_string()
            })?
        };
        let res = match json {
            JsonValue::Object(map) => map.iter().fold(Some(rec), |record_opt, (k, v)| {
                record_opt.and_then(|record| match v {
                    &JsonValue::Number(ref num) => {
                        if num.is_i64() {
                            Some(record.put(k, data::Value::Int(num.as_i64().unwrap())))
                        } else {
                            Some(record.put(k, data::Value::from_float(num.as_f64().unwrap())))
                        }
                    }
                    &JsonValue::String(ref s) => {
                        Some(record.put(k, data::Value::Str(s.to_string())))
                    }
                    &JsonValue::Null => Some(record.put(k, data::Value::None)),
                    &JsonValue::Bool(b) => Some(record.put(k, data::Value::Bool(b))),
                    _other => Some(record.put(k, data::Value::Str(_other.to_string()))),
                })
            }),
            _other => Some(rec),
        };
        Ok(res)
    }
}

/// The definition for a limit operator, which is a positive or negative number used to specify
/// whether the first number of rows should be passed through or the last number of rows,
/// respectively.
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
    }
}

impl OperatorBuilder for LimitDef {
    fn to_preagg_operator(&self) -> Box<UnaryPreAggOperator> {
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

impl UnaryPreAggOperator for Limit {
    fn is_open(&self) -> bool {
        match self {
            Limit::Head { index, limit } => index < limit,
            _ => true,
        }
    }

    fn process(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        match self {
            Limit::Head { ref mut index, .. } => {
                (*index) += 1;

                Ok(Some(rec))
            }
            Limit::Tail { ref mut queue, limit } => {
                if queue.len() == *limit {
                    queue.pop_front();
                }
                queue.push_back(rec);

                Ok(None)
            }
        }
    }

    fn drain(self: Box<Self>) -> Box<Iterator<Item=Record>> {
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
    //use crate::operator::itertools::Itertools;

    impl From<String> for Expr {
        fn from(inp: String) -> Self {
            Expr::Column(inp)
        }
    }

    #[test]
    fn json() {
        let rec = Record::new(
            &(r#"{"k1": 5, "k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}"#.to_string() + "\n"),
        );
        let mut parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            rec.data,
            hashmap! {
                "k1".to_string() => Value::Int(5),
                "k2".to_string() => Value::from_float(5.5),
                "k3".to_string() => Value::Str("str".to_string()),
                "k4".to_string() => Value::None,
                "k5".to_string() => Value::Str("[1,2,3]".to_string())
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
        let mut fields = Fields::new(&["k1".to_string()], FieldMode::Only);
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
        let mut fields = Fields::new(&["k1".to_string()], FieldMode::Except);
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
        let mut parser = Parse::new(
            lang::Keyword::new_wildcard("IP * > \"*\": * length *".to_string()).to_regex(),
            vec![
                "sender".to_string(),
                "recip".to_string(),
                "ignore".to_string(),
                "length".to_string(),
            ],
            None,
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
    fn parse_from_field() {
        let rec = Record::new("");
        let rec = rec.put("from_col", data::Value::Str("[k1=v1]".to_string()));
        let mut parser = Parse::new(
            lang::Keyword::new_wildcard("[*=*]".to_string()).to_regex(),
            vec!["key".to_string(), "value".to_string()],
            Some(Expr::Column("from_col".to_string())),
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
        let ops: Vec<(String, Box<AggregateFunction>)> =
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
        let ops: Vec<(String, Box<AggregateFunction>)> = vec![
            ("_count".to_string(), Box::new(Count::new())),
            ("_sum".to_string(), Box::new(Sum::empty("v1".to_string()))),
            (
                "_distinct".to_string(),
                Box::new(CountDistinct::empty("v1")),
            ),
        ];

        let mut grouper = MultiGrouper::new(
            &[Expr::Column("k1".to_string())],
            vec!["k1".to_string()],
            ops,
        );
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
                },
                hashmap! {
                    "k1".to_string() => data::Value::Str("ok".to_string()),
                    "_count".to_string() => data::Value::Int(20),
                    "_distinct".to_string() => data::Value::Int(10),
                    "_sum".to_string() => data::Value::Int(90)
                },
                hashmap! {
                    "k1".to_string() => data::Value::None,
                    "_count".to_string() => data::Value::Int(3),
                    "_distinct".to_string() => data::Value::Int(3),
                    "_sum".to_string() => data::Value::Int(3)
                },
            ]
        );
    }

    #[test]
    fn count_groups() {
        let ops: Vec<(String, Box<AggregateFunction>)> =
            vec![("_count".to_string(), Box::new(Count::new()))];
        let mut count_agg = MultiGrouper::new(
            &[Expr::Column("k1".to_string())],
            vec!["k1".to_string()],
            ops,
        );
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
        let mut adapted: Box<AggregateOperator> = Box::new(adapted);
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
        let mut total_op = Total::new(Expr::Column("count".to_string()), "_total".to_string());
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

    #[test]
    fn test_limit() {
        let mut limit_op = LimitDef::new(2).to_preagg_operator();
        let rec1 = Record::new("hi");
        let rec2 = Record::new("bye");
        assert_eq!(Some(rec1.clone()), limit_op.process(rec1.clone()).unwrap());
        assert_eq!(true, limit_op.is_open());
        assert_eq!(Some(rec2.clone()), limit_op.process(rec2.clone()).unwrap());
        assert_eq!(false, limit_op.is_open());

        let mut limit_op = LimitDef::new(-2).to_preagg_operator();
        let rec3 = Record::new("in a while, crocodile");
        assert_eq!(None, limit_op.process(rec1.clone()).unwrap());
        assert_eq!(true, limit_op.is_open());
        assert_eq!(None, limit_op.process(rec2.clone()).unwrap());
        assert_eq!(true, limit_op.is_open());
        assert_eq!(None, limit_op.process(rec3.clone()).unwrap());
        assert_eq!(true, limit_op.is_open());

        assert_eq!(vec![rec2.clone(), rec3.clone()], limit_op.drain()
            .collect_vec());
    }
}
