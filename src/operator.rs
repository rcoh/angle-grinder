extern crate itertools;
extern crate quantiles;
extern crate regex;
extern crate regex_syntax;
extern crate serde_json;

use self::quantiles::ckms::CKMS;
use self::serde_json::Value as JsonValue;
use data;
use data::{Aggregate, Record, Row};
use operator::itertools::Itertools;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator;

type Data = HashMap<String, data::Value>;

#[derive(Debug, Fail)]
pub enum EvalError {
    #[fail(display = "No value for key {}", key)]
    NoValueForKey { key: String },

    #[fail(display = "Found None, expected {}", tpe)]
    UnexpectedNone { tpe: String },

    #[fail(display = "Expected JSON")]
    ExpectedJson,

    #[fail(display = "Expected string, found {}", found)]
    ExpectedString { found: String },
}

#[derive(Debug, Fail)]
pub enum TypeError {
    #[fail(display = "Expected boolean expression, found {}", found)]
    ExpectedBool { found: String },

    #[fail(display = "Wrong number of patterns for parse. Pattern has {} but {} were extracted",
           pattern, extracted)]
    ParseNumPatterns { pattern: usize, extracted: usize },
}

pub trait Evaluatable<T> {
    fn eval(&self, record: &Data) -> Result<T, EvalError>;
}

pub trait EvaluatableBorrowed<T> {
    fn eval_borrowed<'a>(&self, record: &'a Data) -> Result<&'a T, EvalError>;
}

pub trait UnaryPreAggOperator: Send + Sync {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError>;
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

impl<'a, T: UnaryPreAggOperator + Send + Sync + 'a> From<T> for Box<AggregateOperator + 'a> {
    fn from(op: T) -> Self {
        Box::new(PreAggAdapter::new(op))
    }
}

pub struct PreAggAdapter<T: UnaryPreAggOperator + Send + Sync> {
    op: T,
    state: Aggregate,
}

impl<T: UnaryPreAggOperator + Send + Sync> PreAggAdapter<T> {
    pub fn new(op: T) -> Self {
        PreAggAdapter {
            op,
            state: Aggregate {
                columns: Vec::new(),
                data: Vec::new(),
            },
        }
    }
}

impl<T: UnaryPreAggOperator + Send + Sync> UnaryPreAggOperator for PreAggAdapter<T> {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        self.op.process(rec)
    }

    fn get_input<'a>(&self, rec: &'a Record, col: &Option<Expr>) -> Result<&'a str, EvalError> {
        self.op.get_input(rec, col)
    }
}

impl<T: UnaryPreAggOperator + Send + Sync> AggregateOperator for PreAggAdapter<T> {
    fn emit(&self) -> Aggregate {
        self.state.clone()
    }

    fn process(&mut self, row: Row) {
        match row {
            Row::Record(_) => panic!("PreAgg adaptor should only be used after aggregataes"),
            Row::Aggregate(agg) => {
                let columns = agg.columns;
                let processed_records: Vec<data::VMap> = {
                    let records = agg.data
                        .into_iter()
                        .map(|vmap| data::Record {
                            data: vmap,
                            raw: "".to_string(),
                        })
                        .flat_map(|rec| self.op.process(rec).unwrap_or(None))
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
                }.collect();
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

impl<T: Copy> Evaluatable<T> for T {
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
                _other => self.warnings
                    .push("Got string. Can only average into or float".to_string()),
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
                _other => self.warnings
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
                _other => self.warnings
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
        let mut new_keys: Vec<String> = data.keys()
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

pub struct Parse {
    regex: regex::Regex,
    fields: Vec<String>,
    input_column: Option<Expr>,
}

impl Parse {
    pub fn new(
        pattern: &str,
        fields: Vec<String>,
        input_column: Option<String>,
    ) -> Result<Self, TypeError> {
        let regex_str = regex_syntax::quote(pattern);
        let mut regex_str = regex_str.replace("\\*", "(.*?)");
        // If it ends with a star, we need to ensure we read until the end.
        if pattern.ends_with('*') {
            regex_str = format!("{}$", regex_str);
        }
        let pattern_matches = pattern.matches('*').count();
        if pattern_matches != fields.len() {
            Result::Err(TypeError::ParseNumPatterns {
                pattern: pattern_matches,
                extracted: fields.len(),
            })
        } else {
            Result::Ok(Parse {
                regex: regex::Regex::new(&regex_str).unwrap(),
                fields,
                input_column: input_column.map(Expr::Column),
            })
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

impl UnaryPreAggOperator for Parse {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
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

pub struct Where<T: Evaluatable<bool> + Send + Sync> {
    expr: T,
}

impl<T: Evaluatable<bool> + Send + Sync> Where<T> {
    pub fn new(expr: T) -> Self {
        Where { expr }
    }
}

impl<T: Evaluatable<bool> + Send + Sync> UnaryPreAggOperator for Where<T> {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        if self.expr.eval(&rec.data)? {
            Ok(Some(rec))
        } else {
            Ok(None)
        }
    }
}

pub enum FieldMode {
    Only,
    Except,
}

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

impl UnaryPreAggOperator for Fields {
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

impl UnaryPreAggOperator for ParseJson {
    fn process(&self, rec: Record) -> Result<Option<Record>, EvalError> {
        let json: JsonValue = {
            let inp = self.get_input(&rec, &self.input_column)?;
            serde_json::from_str(&inp).map_err(|_| EvalError::ExpectedJson)?
        };
        let res = match json {
            JsonValue::Object(map) => map.iter().fold(Some(rec), |record_opt, (k, v)| {
                record_opt.and_then(|record| match v {
                    &JsonValue::Number(ref num) => if num.is_i64() {
                        Some(record.put(k, data::Value::Int(num.as_i64().unwrap())))
                    } else {
                        Some(record.put(k, data::Value::from_float(num.as_f64().unwrap())))
                    },
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

#[cfg(test)]
mod tests {
    use super::*;
    use data::Value;
    use operator::itertools::Itertools;

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
        let parser = ParseJson::new(None);
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
            "IP * > \"*\": * length *",
            vec![
                "sender".to_string(),
                "recip".to_string(),
                "ignore".to_string(),
                "length".to_string(),
            ],
            None,
        ).unwrap();
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
        let parser = Parse::new(
            "[*=*]",
            vec!["key".to_string(), "value".to_string()],
            Some("from_col".to_string()),
        ).unwrap();
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
            .foreach(|rec| count_agg.process(Row::Record(rec)));
        let agg = count_agg.emit();
        assert_eq!(agg.columns, vec!["_count"]);
        assert_eq!(
            agg.data,
            vec![hashmap! {"_count".to_string() => data::Value::Int(10)}]
        );
        (0..10)
            .map(|n| Record::new(&n.to_string()))
            .foreach(|rec| count_agg.process(Row::Record(rec)));
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
        (0..10).foreach(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..10).foreach(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..25).foreach(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("not ok".to_string()));
            let rec = rec.put("v1", data::Value::Int(n));
            grouper.process(Row::Record(rec));
        });
        (0..3).foreach(|n| {
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
        (0..10).foreach(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            count_agg.process(Row::Record(rec));
        });
        (0..25).foreach(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("not ok".to_string()));
            count_agg.process(Row::Record(rec));
        });
        (0..3).foreach(|n| {
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
        let adapted = PreAggAdapter::new(where_op);
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
}
