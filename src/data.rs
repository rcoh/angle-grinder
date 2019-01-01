extern crate ordered_float;

use self::ordered_float::OrderedFloat;
use crate::render;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

pub type VMap = HashMap<String, Value>;

pub enum Row {
    Aggregate(Aggregate),
    Record(Record),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Aggregate {
    pub columns: Vec<String>,
    pub data: Vec<VMap>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Record {
    pub data: VMap,
    pub raw: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Str(String),
    // Consider big int
    Int(i64),
    Float(OrderedFloat<f64>),
    Bool(bool),
    None,
}

pub static FALSE_VALUE: &'static Value = &Value::Bool(false);
pub static TRUE_VALUE: &'static Value = &Value::Bool(true);
pub static NONE: &'static Value = &Value::None;

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Ints and floats are converted to floats
            (&Value::Int(ref int_val), &Value::Float(ref float_val)) => {
                (OrderedFloat::from(*int_val as f64)).cmp(float_val)
            }
            (&Value::Float(ref float_val), &Value::Int(ref int_val)) => {
                float_val.cmp(&OrderedFloat::from(*int_val as f64))
            }

            (&Value::Float(ref l), &Value::Float(ref r)) => l.cmp(r),
            (&Value::Int(ref l), &Value::Int(ref r)) => l.cmp(r),
            (&Value::Str(ref l), &Value::Str(ref r)) => l.cmp(r),
            (&Value::Bool(l), &Value::Bool(r)) => l.cmp(&r),
            // None is less than everything
            (&Value::None, _any) => Ordering::Less,
            (_any, &Value::None) => Ordering::Greater,
            (&Value::Str(..), _any) => Ordering::Greater,
            (_any, &Value::Str(..)) => Ordering::Less,

            // Bool is in the middle
            (&Value::Bool(_), &Value::Int(_)) => Ordering::Greater,
            (&Value::Int(_), &Value::Bool(_)) => Ordering::Less,
            (&Value::Bool(_), &Value::Float(_)) => Ordering::Greater,
            (&Value::Float(_), &Value::Bool(_)) => Ordering::Less,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Value::Str(ref s) => write!(f, "{}", s),
            Value::Int(ref s) => write!(f, "{}", s),
            Value::Float(ref s) => write!(f, "{}", s),
            Value::Bool(ref s) => write!(f, "{}", s),
            Value::None => write!(f, "$None$"),
        }
    }
}

impl Value {
    pub fn render(&self, render_config: &render::RenderConfig) -> String {
        match *self {
            Value::Str(ref s) => s.to_string(),
            Value::Int(ref s) => format!("{}", s),
            Value::None => "$None$".to_string(),
            Value::Float(ref s) => format!("{:.*}", render_config.floating_points, s),
            Value::Bool(ref s) => format!("{}", s),
        }
    }

    pub fn from_bool(b: bool) -> &'static Value {
        if b {
            TRUE_VALUE
        } else {
            FALSE_VALUE
        }
    }

    pub fn from_float(f: f64) -> Value {
        let rounded = f as i64;
        if (f - f.floor()).abs() < std::f64::EPSILON {
            Value::Int(rounded)
        } else {
            Value::Float(OrderedFloat(f))
        }
    }

    pub fn from_string(s: &str) -> Value {
        let int_value = s.parse::<i64>();
        let float_value = s.parse::<f64>();
        let bool_value = s.parse::<bool>();
        int_value
            .map(Value::Int)
            .or_else(|_| float_value.map(Value::from_float))
            .or_else(|_| bool_value.map(Value::Bool))
            .unwrap_or_else(|_| Value::Str(s.to_string()))
    }
}

impl Aggregate {
    pub fn new(
        key_columns: &[String],
        agg_column: String,
        data: &[(HashMap<String, String>, Value)],
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
        let raw_data: Vec<HashMap<String, Value>> = data
            .iter()
            .map(|&(ref keycols, ref value)| {
                let mut new_map: HashMap<String, Value> = keycols
                    .iter()
                    .map(|(keycol, val)| (keycol.clone(), Value::Str(val.clone())))
                    .collect();
                new_map.insert(agg_column.clone(), value.clone());
                new_map
            })
            .collect();
        let mut columns = key_columns.to_owned();
        columns.push(agg_column);

        Aggregate {
            data: raw_data,
            columns,
        }
    }
}

impl Record {
    pub fn put(mut self, key: &str, value: Value) -> Record {
        self.data.insert(key.to_string(), value);
        self
    }

    pub fn new(raw: &str) -> Record {
        Record {
            data: HashMap::new(),
            raw: raw.to_string(),
        }
    }

    pub fn ordering<'a>(
        columns: Vec<String>,
    ) -> impl Fn(&VMap, &VMap) -> Ordering + 'a + Send + Sync {
        move |rec_l: &VMap, rec_r: &VMap| {
            for col in &columns {
                let l_val = rec_l.get(col);
                let r_val = rec_r.get(col);
                if l_val != r_val {
                    return l_val.cmp(&r_val);
                }
            }
            Ordering::Equal
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;

    #[test]
    fn record_put_get() {
        let rec = Record::new("heres some data");
        let rec = rec.put("key1", Value::Int(9999));
        assert_eq!(rec.data.get("key1").unwrap(), &Value::Int(9999));
        assert!(rec.data.get("key2").is_none());
        assert_eq!(rec.raw, "heres some data");
    }

    #[test]
    fn agg() {
        let agg = Aggregate::new(
            &["kc1".to_string(), "kc2".to_string()],
            "count".to_string(),
            &[(
                hashmap! {
                    "kc1".to_string() => "k1".to_string(),
                    "kc2".to_string() => "k2".to_string()
                },
                Value::Int(100),
            )],
        );
        assert_eq!(agg.data.len(), 1);
    }

    #[test]
    #[should_panic]
    fn panic_on_invalid_row() {
        Aggregate::new(
            &["k1".to_string(), "k2".to_string()],
            "count".to_string(),
            &[(
                hashmap! {
                    "kc2".to_string() => "k2".to_string()
                },
                Value::Int(100),
            )],
        );
    }

    #[test]
    fn from_string() {
        assert_eq!(Value::from_string("949919"), Value::Int(949919));
        assert_eq!(Value::from_string("0.00001"), Value::from_float(0.00001));
        assert_eq!(
            Value::from_string("not a number"),
            Value::Str("not a number".to_string())
        );
    }

    #[test]
    fn ordering() {
        let mut r1 = HashMap::<String, Value>::new();
        r1.insert("k1".to_string(), Value::Int(5));
        r1.insert("k3".to_string(), Value::from_float(0.1));
        r1.insert("k2".to_string(), Value::Str("abc".to_string()));
        let mut r2 = HashMap::<String, Value>::new();
        r2.insert("k1".to_string(), Value::Int(4));
        r2.insert("k2".to_string(), Value::Str("xyz".to_string()));
        r2.insert("k3".to_string(), Value::from_float(0.1));
        let ord1 = Record::ordering(vec!["k1".to_string(), "k2".to_string()]);
        assert_eq!(ord1(&r1, &r2), Ordering::Greater);
        assert_eq!(ord1(&r1, &r1), Ordering::Equal);
        assert_eq!(ord1(&r2, &r1), Ordering::Less);

        let ord2 = Record::ordering(vec!["k2".to_string(), "k1".to_string()]);
        assert_eq!(ord2(&r1, &r2), Ordering::Less);
        assert_eq!(ord2(&r1, &r1), Ordering::Equal);
        assert_eq!(ord2(&r2, &r1), Ordering::Greater);

        let ord3 = Record::ordering(vec!["k3".to_string()]);
        assert_eq!(ord3(&r1, &r2), Ordering::Equal);

        let ord4 = Record::ordering(vec!["k3".to_string(), "k1".to_string()]);
        assert_eq!(ord4(&r1, &r2), Ordering::Greater);
        assert_eq!(ord4(&r1, &r1), Ordering::Equal);
        assert_eq!(ord4(&r2, &r1), Ordering::Less);
    }
}
