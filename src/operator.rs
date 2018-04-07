extern crate itertools;
extern crate quantiles;
extern crate regex;
extern crate regex_syntax;
extern crate serde_json;

use self::quantiles::ckms::CKMS;
use self::serde_json::Value as JsonValue;
use data;
use data::{Aggregate, Record, Row};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::FromIterator;

type Data = HashMap<String, data::Value>;

pub trait Evaluatable<T> {
    fn eval(&self, record: &Data) -> Option<T>;
}

pub trait UnaryPreAggOperator {
    fn process(&self, rec: Record) -> Option<Record>;
    fn get_input<'a>(&self, input_column: &Option<String>, rec: &'a Record) -> Option<&'a str> {
        //inp_
        let inp_col = input_column.as_ref().map(|s| &**s);
        rec.get_str(inp_col)
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

#[derive(Clone)]
pub enum Expr {
    Column(String),
    Equal { left: Box<Expr>, right: Box<Expr> },
    Value(data::Value),
}

impl Evaluatable<data::Value> for Expr {
    fn eval(&self, record: &HashMap<String, data::Value>) -> Option<data::Value> {
        match *self {
            Expr::Column(ref str) => record.get(str).cloned(),
            Expr::Equal {
                ref left,
                ref right,
            } => {
                let l = (*left).eval(record);
                let r = (*right).eval(record);
                let value = data::Value::Bool(l == r);
                Some(value)
            }
            Expr::Value(ref v) => Some(v.clone()),
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
        self.column.eval(rec).iter().for_each(|value| match value {
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
        self.column.eval(data).iter().for_each(|value| match value {
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
            ordering: Record::ordering(columns),
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
    key_cols: Vec<String>,
    agg_col: Vec<(String, Box<AggregateFunction>)>,
    // key-column values -> (agg_columns -> builders)
    state: HashMap<Vec<data::Value>, HashMap<String, Box<AggregateFunction>>>,
}

impl MultiGrouper {
    pub fn new(key_cols: &[&str], aggregators: Vec<(String, Box<AggregateFunction>)>) -> Self {
        MultiGrouper {
            key_cols: key_cols.to_vec().iter().map(|s| s.to_string()).collect(),
            agg_col: aggregators,
            state: HashMap::new(),
        }
    }
    fn process_map(&mut self, data: &Data) {
        let key_values = self.key_cols.iter().map(|column| data.get(column));
        let key_columns: Vec<data::Value> = key_values
            .map(|value_opt| value_opt.cloned().unwrap_or_else(|| data::Value::None))
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
        let mut columns = self.key_cols.to_vec();
        columns.extend(self.agg_col.iter().map(|&(ref k, ..)| k.to_string()));
        let data = self.state.iter().map(|(key_values, agg_map)| {
            let key_values = key_values.iter().cloned();
            let key_cols = self.key_cols.iter().map(|s| s.to_owned());
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
    input_column: Option<String>,
}

impl Parse {
    pub fn new(
        pattern: &str,
        fields: Vec<String>,
        input_column: Option<String>,
    ) -> Result<Self, String> {
        let regex_str = regex_syntax::quote(pattern);
        let mut regex_str = regex_str.replace("\\*", "(.*?)");
        // If it ends with a star, we need to ensure we read until the end.
        if pattern.ends_with('*') {
            regex_str = format!("{}$", regex_str);
        }
        if pattern.matches('*').count() != fields.len() {
            Result::Err("Wrong number of extractions".to_string())
        } else {
            Result::Ok(Parse {
                regex: regex::Regex::new(&regex_str).unwrap(),
                fields,
                input_column,
            })
        }
    }

    fn matches(&self, rec: &Record) -> Option<Vec<data::Value>> {
        let inp_opt: Option<&str> = self.get_input(&self.input_column, rec);
        inp_opt.and_then(|inp| {
            let matches: Vec<regex::Captures> = self.regex.captures_iter(inp.trim()).collect();
            if matches.is_empty() {
                None
            } else {
                let capture = &matches[0];
                let mut values: Vec<data::Value> = Vec::new();
                for i in 0..self.fields.len() {
                    // the first capture is the entire string
                    values.push(data::Value::from_string(&capture[i + 1]));
                }
                Some(values)
            }
        })
    }
}

impl UnaryPreAggOperator for Parse {
    fn process(&self, rec: Record) -> Option<Record> {
        let matches = self.matches(&rec);
        match matches {
            None => None,
            Some(matches) => {
                let mut rec = rec;
                for (i, field) in self.fields.iter().enumerate() {
                    rec = rec.put(field, matches[i].clone());
                }
                Some(rec)
            }
        }
    }
}

pub struct Where {
    expr: Expr,
}

impl Where {
    pub fn new<T: Into<Expr>>(expr: T) -> Self {
        Where { expr: expr.into() }
    }
}

impl UnaryPreAggOperator for Where {
    fn process(&self, rec: Record) -> Option<Record> {
        let res = self.expr.eval(&rec.data);
        match res {
            Some(data::Value::Bool(true)) => Some(rec),
            _other => None,
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
    fn process(&self, rec: Record) -> Option<Record> {
        let mut rec = rec;
        match self.mode {
            FieldMode::Only => {
                rec.data.retain(|k, _| self.columns.contains(k));
            }
            FieldMode::Except => {
                rec.data.retain(|k, _| !self.columns.contains(k));
            }
        }
        Some(rec)
    }
}

pub struct ParseJson {
    input_column: Option<String>,
}

impl ParseJson {
    pub fn new(input_column: Option<String>) -> ParseJson {
        ParseJson { input_column }
    }
}

impl UnaryPreAggOperator for ParseJson {
    fn process(&self, rec: Record) -> Option<Record> {
        let json_opt = {
            let inp_str_opt = self.get_input(&self.input_column, &rec);
            inp_str_opt.map(|inp_str| serde_json::from_str(inp_str))
        };
        match json_opt {
            Some(Ok(v)) => {
                let v: JsonValue = v;
                match v {
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
                    _other => None,
                }
            }
            _e => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data::Record;
    use data::Value;
    use operator::itertools::Itertools;

    impl From<String> for Expr {
        fn from(inp: String) -> Self {
            Expr::Column(inp)
        }
    }

    #[test]
    fn test_json() {
        let rec = Record::new(
            &(r#"{"k1": 5, "k2": 5.5, "k3": "str", "k4": null, "k5": [1,2,3]}"#.to_string() + "\n"),
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap();
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
    fn test_fields_only() {
        let rec = Record::new("");
        let rec = rec.put("k1", Value::Str("v1".to_string()));
        let rec = rec.put("k2", Value::Str("v2".to_string()));
        let rec = rec.put("k3", Value::Str("v3".to_string()));
        let rec = rec.put("k4", Value::Str("v4".to_string()));
        let fields = Fields::new(&["k1".to_string()], FieldMode::Only);
        let rec = fields.process(rec).unwrap();
        assert_eq!(
            rec.data,
            hashmap! {
                "k1".to_string() => Value::Str("v1".to_string()),
            }
        );
    }

    #[test]
    fn test_fields_except() {
        let rec = Record::new("");
        let rec = rec.put("k1", Value::Str("v1".to_string()));
        let rec = rec.put("k2", Value::Str("v2".to_string()));
        let rec = rec.put("k3", Value::Str("v3".to_string()));
        let rec = rec.put("k4", Value::Str("v4".to_string()));
        let fields = Fields::new(&["k1".to_string()], FieldMode::Except);
        let rec = fields.process(rec).unwrap();
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
    fn test_parse() {
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
        let rec = parser.process(rec).unwrap();
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
    fn test_parse_from_field() {
        let rec = Record::new("");
        let rec = rec.put("from_col", data::Value::Str("[k1=v1]".to_string()));
        let parser = Parse::new(
            "[*=*]",
            vec!["key".to_string(), "value".to_string()],
            Some("from_col".to_string()),
        ).unwrap();
        let rec = parser.process(rec).unwrap();
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
    fn test_count_no_groups() {
        let ops: Vec<(String, Box<AggregateFunction>)> =
            vec![("_count".to_string(), Box::new(Count::new()))];
        let mut count_agg = MultiGrouper::new(&[], ops);
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
    fn test_multi_grouper() {
        let ops: Vec<(String, Box<AggregateFunction>)> = vec![
            ("_count".to_string(), Box::new(Count::new())),
            ("_sum".to_string(), Box::new(Sum::empty("v1".to_string()))),
            (
                "_distinct".to_string(),
                Box::new(CountDistinct::empty("v1")),
            ),
        ];

        let mut grouper = MultiGrouper::new(&["k1"], ops);
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
    fn test_count_groups() {
        let ops: Vec<(String, Box<AggregateFunction>)> =
            vec![("_count".to_string(), Box::new(Count::new()))];
        let mut count_agg = MultiGrouper::new(&["k1"], ops);
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
    fn test_sort_raw() {}

    #[test]
    fn test_sort_aggregate() {
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
}
