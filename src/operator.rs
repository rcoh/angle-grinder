extern crate itertools;
extern crate ord_subset;
extern crate serde_json;
use std::collections::HashMap;
use std::iter::FromIterator;
use self::ord_subset::OrdSubsetSliceExt;
use data::{Aggregate, Record, Row};
use data;
use self::serde_json::Value;
pub trait UnaryPreAggOperator {
    fn process(&self, rec: &Record) -> Option<Record>;
}

pub trait AggregateOperator {
    fn emit(&self) -> data::Aggregate;
    fn process(&mut self, row: &Row);
}

pub trait AccumOperator: Clone {
    fn process(&mut self, rec: &Record);
    fn emit(&self) -> data::Value;
}

#[derive(Clone)]
pub struct Count {
    count: i64,
}

impl Count {
    pub fn new() -> Count {
        Count { count: 0 }
    }
}

impl AccumOperator for Count {
    fn process(&mut self, _rec: &Record) {
        self.count += 1;
    }

    fn emit(&self) -> data::Value {
        data::Value::Int(self.count)
    }
}

#[derive(Clone)]
pub struct Average {
    total: f64,
    count: i64,
    column: String,
    warnings: Vec<String>,
}

impl Average {
    pub fn empty(column: String) -> Average {
        Average {
            total: 0.0,
            count: 0,
            column: column,
            warnings: Vec::new(),
        }
    }
}

impl AccumOperator for Average {
    fn process(&mut self, rec: &Record) {
        rec.data
            .get(&self.column)
            .iter()
            .for_each(|value| match value {
                &&data::Value::Float(ref f) => {
                    self.total += f;
                    self.count += 1
                }
                &&data::Value::Int(ref i) => {
                    self.total += *i as f64;
                    self.count += 1
                }
                _other => self.warnings
                    .push("Got string. Can only average into or float".to_string()),
            });
    }

    fn emit(&self) -> data::Value {
        data::Value::Float(self.total / self.count as f64)
    }
}

pub struct Grouper<T: AccumOperator> {
    key_cols: Vec<String>,
    agg_col: String,
    state: HashMap<Vec<String>, T>,
    empty: T,
}

impl<T: AccumOperator> Grouper<T> {
    pub fn new(key_cols: Vec<&str>, agg_col: &str, empty: T) -> Grouper<T> {
        Grouper {
            key_cols: key_cols.iter().map(|s| s.to_string()).collect(),
            agg_col: String::from(agg_col),
            state: HashMap::new(),
            empty: empty,
        }
    }
}

impl<T: AccumOperator> AggregateOperator for Grouper<T> {
    fn emit(&self) -> data::Aggregate {
        let mut data: Vec<(HashMap<String, String>, data::Value)> = self.state
            .iter()
            .map(|(ref key_cols, ref accum)| {
                let key_value =
                    itertools::zip_eq(self.key_cols.iter().cloned(), key_cols.iter().cloned());
                let res_map: HashMap<String, String> = HashMap::from_iter(key_value);
                (res_map, accum.emit())
            })
            .collect(); // todo: avoid clone here
        data.ord_subset_sort_by_key(|ref kv| kv.1.clone());
        data.reverse();
        Aggregate {
            key_columns: self.key_cols.clone(),
            agg_column: self.agg_col.clone(),
            data: data,
        }
    }

    fn process(&mut self, row: &Row) {
        match row {
            &Row::Record(ref rec) => {
                let key_values: Vec<Option<&data::Value>> = self.key_cols
                    .iter()
                    .cloned()
                    .map(|column| rec.data.get(&column))
                    .collect();
                let key_columns: Vec<String> = key_values
                    .iter()
                    .cloned()
                    .map(|value_opt| {
                        value_opt
                            .map(|value| value.to_string())
                            .unwrap_or("$None$".to_string())
                    })
                    .collect();
                let accum = self.state.entry(key_columns).or_insert(self.empty.clone());
                accum.process(rec);
            }
            &Row::Aggregate(ref _ag) => panic!("Unsupported"),
        }
    }
}

pub struct ParseJson {
        // any options here
}
impl UnaryPreAggOperator for ParseJson {
    fn process(&self, rec: &Record) -> Option<Record> {
        match serde_json::from_str(&rec.raw) {
            Ok(v) => {
                let v: Value = v;
                let rec: Record = rec.clone();
                match v {
                    Value::Object(map) => {
                        map.iter().fold(Some(rec), |record_opt, (ref k, ref v)| {
                            record_opt.and_then(|record| match v {
                                &&Value::Number(ref num) => if num.is_i64() {
                                    Some(record.put(k, data::Value::Int(num.as_i64().unwrap())))
                                } else {
                                    Some(record.put(k, data::Value::Float(num.as_f64().unwrap())))
                                },
                                &&Value::String(ref s) => {
                                    Some(record.put(k, data::Value::Str(s.to_string())))
                                }
                                _other => None,
                            })
                        })
                    }
                    _other => None,
                }
            }
            _e => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use data::Record;
    use data::Value;
    use super::*;
    use operator::itertools::Itertools;

    #[test]
    fn test_json() {
        let rec = Record::new(r#"{"k1": 5, "k2": 5.5, "k3": "str"}"#);
        let parser = ParseJson {};
        let rec = parser.process(&rec).unwrap();
        assert_eq!(
            rec.data,
            hashmap!{
                "k1".to_string() => Value::Int(5),
                "k2".to_string() => Value::Float(5.5),
                "k3".to_string() => Value::Str("str".to_string())
            }
        );
    }

    #[test]
    fn test_count_no_groups() {
        let mut count_agg = Grouper::<Count>::new(Vec::new(), "_count", Count::new());
        (0..10)
            .map(|n| Record::new(&n.to_string()))
            .foreach(|rec| count_agg.process(&Row::Record(rec)));
        let agg = count_agg.emit();
        assert_eq!(agg.key_columns.len(), 0);
        assert_eq!(agg.agg_column, "_count");
        assert_eq!(agg.data, vec![(HashMap::new(), data::Value::Int(10))]);
        (0..10)
            .map(|n| Record::new(&n.to_string()))
            .foreach(|rec| count_agg.process(&Row::Record(rec)));
        assert_eq!(
            count_agg.emit().data,
            vec![(HashMap::new(), data::Value::Int(20))]
        );
    }

    #[test]
    fn test_count_groups() {
        let mut count_agg = Grouper::<Count>::new(vec!["k1"], "_count", Count::new());
        (0..10).foreach(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("ok".to_string()));
            count_agg.process(&Row::Record(rec));
        });
        (0..25).foreach(|n| {
            let rec = Record::new(&n.to_string());
            let rec = rec.put("k1", data::Value::Str("not ok".to_string()));
            count_agg.process(&Row::Record(rec));
        });
        (0..3).foreach(|n| {
            let rec = Record::new(&n.to_string());
            count_agg.process(&Row::Record(rec));
        });
        let agg = count_agg.emit();
        assert_eq!(
            agg.data,
            vec![
                (
                    hashmap!{"k1".to_string() => "not ok".to_string()},
                    data::Value::Int(25),
                ),
                (
                    hashmap!{"k1".to_string() => "ok".to_string()},
                    data::Value::Int(10),
                ),
                (
                    hashmap!{"k1".to_string() => "$None$".to_string()},
                    data::Value::Int(3),
                ),
            ]
        );
    }
}
