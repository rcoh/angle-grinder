extern crate ord_subset;
use self::ord_subset::OrdSubset;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt;
use render;

pub enum Row {
    Aggregate(Aggregate),
    Record(Record),
}

pub struct Aggregate {
    // key columns ["v1", "v2", "v3"]
    // agg column "_count"
    pub key_columns: Vec<String>,
    pub agg_column: String,
    // [{v2: "a", "v2": "b", "v3": c}, 99]
    // [5]
    pub data: Vec<(HashMap<String, String>, Value)>,
}

#[derive(Clone)]
pub struct Record {
    pub data: HashMap<String, Value>,
    pub raw: String,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Str(String),
    // Consider big int
    Int(i64),
    Float(f64),
    None,
}

impl OrdSubset for Value {
    fn is_outside_order(&self) -> bool {
        match self {
            &Value::Float(f) => f.is_outside_order(),
            _other => false,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Value::Str(ref s) => write!(f, "{}", s),
            &Value::Int(ref s) => write!(f, "{}", s),
            &Value::Float(ref s) => write!(f, "{}", s),
            &Value::None => write!(f, "$None$"),
        }
    }
}

impl Value {
    pub fn render(&self, render_config: &render::RenderConfig) -> String {
        match self {
            &Value::Str(ref s) => format!("{}", s),
            &Value::Int(ref s) => format!("{}", s),
            &Value::None => format!("$None$"),
            &Value::Float(ref s) => format!("{:.*}", render_config.floating_points, s),
        }
    }

    pub fn from_string(s: &str) -> Value {
        let int_value = s.parse::<i64>();
        let float_value = s.parse::<f64>();
        int_value
            .map(Value::Int)
            .or(float_value.map(Value::Float))
            .unwrap_or(Value::Str(s.to_string()))
    }
}

impl Aggregate {
    pub fn new(
        key_columns: Vec<String>,
        agg_column: String,
        data: Vec<(HashMap<String, String>, Value)>,
    ) -> Aggregate {
        data.iter().for_each(|&(ref row, ref _value)| {
            if row.len() != key_columns.len() {
                panic!("Invalid number of key columns")
            }
            key_columns.iter().for_each(|key_column| {
                if !row.contains_key(key_column) {
                    panic!("New row missing key column: {}", key_column);
                }
            });
        });
        Aggregate {
            key_columns: key_columns,
            agg_column: agg_column,
            data: data,
        }
    }

    pub fn rows(&self) -> Vec<HashMap<String, Value>> {
        self.data
            .iter()
            .map(|&(ref keycols, ref value)| {
                let mut new_map: HashMap<String, Value> = keycols
                    .iter()
                    .map(|(keycol, val)| (keycol.clone(), Value::Str(val.clone())))
                    .collect();
                new_map.insert(self.agg_column.clone(), value.clone());
                new_map
            })
            .collect()
    }
}

impl Record {
    pub fn put(&self, key: &str, value: Value) -> Record {
        let mut new_map = self.data.clone();
        new_map.insert(key.to_string(), value);
        Record {
            data: new_map,
            raw: self.raw.clone(),
        }
    }

    pub fn new(raw: &str) -> Record {
        Record {
            data: HashMap::new(),
            raw: raw.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    macro_rules! veclit {
        // match a list of expressions separated by comma:
        ($($str:expr),*) => ({
            // create a Vec with this list of expressions,
            // calling String::from on each:
            vec![$(String::from($str),)*] as Vec<String>
        });
    }

    #[test]
    fn test_record_put_get() {
        let rec = Record::new("heres some data");
        let rec = rec.put("key1", Value::Int(9999));
        assert_eq!(rec.data.get("key1").unwrap(), &Value::Int(9999));
        assert!(rec.data.get("key2").is_none());
        assert_eq!(rec.raw, "heres some data");
    }

    #[test]
    fn test_agg() {
        let agg = Aggregate::new(
            veclit!("kc1", "kc2"),
            "count".to_string(),
            vec![
                (
                    hashmap!{
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::Int(100),
                ),
            ],
        );
        assert_eq!(agg.data.len(), 1);
    }

    #[test]
    #[should_panic]
    fn test_panic_on_invalid_row() {
        Aggregate::new(
            veclit!("kc1", "kc2"),
            "count".to_string(),
            vec![
                (
                    hashmap!{
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::Int(100),
                ),
            ],
        );
    }

    #[test]
    fn test_from_string() {
        assert_eq!(Value::from_string("949919"), Value::Int(949919));
        assert_eq!(Value::from_string("0.00001"), Value::Float(0.00001));
        assert_eq!(
            Value::from_string("not a number"),
            Value::Str("not a number".to_string())
        );
    }
}
