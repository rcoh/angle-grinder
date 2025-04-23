use crate::operator::expr::{Expr, ValueRef};
use crate::operator::EvalError;
use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serializer;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::{Add, Div, Mul, Sub};

pub type VMap = HashMap<String, Value>;

pub enum Row {
    Aggregate(Aggregate),
    Record(Record),
}

/// Helper to allow for a custom serde Serializer for the rows in an Aggregate.
/// Serde serializers are not allowed to emit nested serializations directly, so
/// it's necessary to create an intermediary type for each row, and to provide
/// it with the columns to enable ordering.
pub(crate) struct WrappedAggregateRow<'a> {
    pub columns: &'a Vec<String>,
    pub data: &'a VMap,
}

impl serde::Serialize for WrappedAggregateRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for k in self.columns {
            map.serialize_entry(k, self.data.get(k).unwrap_or(&Value::None))?;
        }
        map.end()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Aggregate {
    pub columns: Vec<String>,
    pub data: Vec<VMap>,
}

// This custom serializer respects column order, whereas directly serializing
// the `data` map would not.  (At least not while VMap is a HashMap.  serde_json
// supports a "preserve_order" feature that uses the "indexmap" crate.  But
// since we already have the info necessary for ordering, we use that.)
impl serde::Serialize for Aggregate {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.data.len()))?;
        for row in &self.data {
            seq.serialize_element(&WrappedAggregateRow {
                columns: &self.columns,
                data: row,
            })?
        }
        seq.end()
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Record {
    pub data: VMap,
    pub raw: String,
}

impl serde::Serialize for Record {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.data.len()))?;
        for (k, v) in self.data.iter().sorted() {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Str(String),
    // Consider big int
    Int(i64),
    Float(OrderedFloat<f64>),
    Bool(bool),
    DateTime(DateTime<Utc>),
    Duration(Duration),
    Obj(im::HashMap<String, Value>),
    Array(Vec<Value>),
    None,
}

impl serde::Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Str(s) => serializer.serialize_str(s),
            Value::Int(i) => serializer.serialize_i64(*i),
            Value::Float(ofloat) => serializer.serialize_f64(ofloat.0),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::DateTime(dt) => serializer.serialize_str(dt.to_rfc3339().as_str()),
            Value::Duration(d) => serializer.serialize_str(d.to_string().as_str()),
            Value::Obj(map) => {
                let mut m = serializer.serialize_map(Some(map.len()))?;
                for (k, v) in map {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
            Value::Array(v) => serializer.collect_seq(v),
            Value::None => serializer.serialize_none(),
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Ints and floats are converted to floats
            (Value::Int(int_val), Value::Float(float_val)) => {
                (OrderedFloat::from(*int_val as f64)).cmp(float_val)
            }
            (Value::Float(float_val), Value::Int(int_val)) => {
                float_val.cmp(&OrderedFloat::from(*int_val as f64))
            }
            (Value::Float(l), Value::Float(r)) => l.cmp(r),
            (Value::Int(l), Value::Int(r)) => l.cmp(r),
            (Value::Str(l), Value::Str(r)) => l.cmp(r),
            (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
            (Value::DateTime(l), Value::DateTime(r)) => l.cmp(r),
            (Value::Duration(l), Value::Duration(r)) => l.cmp(r),
            (Value::Obj(l), Value::Obj(r)) => l.cmp(r),
            // All these remaining cases aren't directly comparable
            (unrelated_l, unrelated_r) => unrelated_l.rank().cmp(&unrelated_r.rank()),
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
            Value::DateTime(ref dt) => write!(f, "{:?}", dt),
            Value::Duration(ref d) => write!(f, "{:?}", d),
            Value::Obj(ref o) => write!(f, "{:?}", o),
            Value::Array(ref o) => write!(f, "{:?}", o),
            Value::None => write!(f, "None"),
        }
    }
}

impl Add for Value {
    type Output = Result<Value, EvalError>;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::DateTime(ldt), Value::Duration(rd)) => Ok(Value::DateTime(ldt.add(rd))),
            (Value::Duration(ld), Value::DateTime(rdt)) => Ok(Value::DateTime(rdt.add(ld))),
            (Value::Duration(ld), Value::Duration(rd)) => Ok(Value::Duration(ld.add(rd))),
            (Value::Float(lf), Value::Float(rf)) => Ok(Value::Float(lf + rf)),
            (Value::Int(li), Value::Int(ri)) => Ok(Value::Int(li + ri)),
            (left, right) => left.binary_op(&f64::add, "+", &right),
        }
    }
}

impl Sub for Value {
    type Output = Result<Value, EvalError>;

    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::DateTime(ldt), Value::Duration(rf)) => Ok(Value::DateTime(ldt.sub(rf))),
            (Value::DateTime(ldt), Value::DateTime(rdt)) => Ok(Value::Duration(ldt.sub(rdt))),
            (Value::Duration(ld), Value::Duration(rd)) => Ok(Value::Duration(ld.sub(rd))),
            (Value::Float(lf), Value::Float(rf)) => Ok(Value::Float(lf - rf)),
            (Value::Int(li), Value::Int(ri)) => Ok(Value::Int(li - ri)),
            (left, right) => left.binary_op(&f64::sub, "-", &right),
        }
    }
}

impl Mul for Value {
    type Output = Result<Value, EvalError>;

    fn mul(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Duration(ld), Value::Int(ri)) => Ok(Value::Duration(ld.mul(ri as i32))),
            (Value::Int(li), Value::Duration(rd)) => Ok(Value::Duration(rd.mul(li as i32))),
            (Value::Float(lf), Value::Float(rf)) => Ok(Value::Float(lf * rf)),
            (Value::Int(li), Value::Int(ri)) => Ok(Value::Int(li * ri)),
            (left, right) => left.binary_op(&f64::mul, "*", &right),
        }
    }
}

impl Div for Value {
    type Output = Result<Value, EvalError>;

    fn div(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Value::Duration(ld), Value::Int(ri)) => Ok(Value::Duration(ld.div(ri as i32))),
            (left, right) => left.binary_op(&f64::div, "/", &right),
        }
    }
}

#[derive(Clone)]
pub struct DisplayConfig {
    pub floating_points: usize,
}

impl Default for DisplayConfig {
    fn default() -> Self {
        DisplayConfig { floating_points: 2 }
    }
}

pub(crate) struct ValueDisplay<'a> {
    value: &'a Value,
    display_config: &'a DisplayConfig,
}

impl<'a> ValueDisplay<'a> {
    pub(crate) fn new(value: &'a Value, display_config: &'a DisplayConfig) -> Self {
        Self {
            value,
            display_config,
        }
    }

    fn nested(&self, new_value: &'a Value) -> ValueDisplay<'a> {
        ValueDisplay {
            value: new_value,
            display_config: self.display_config,
        }
    }
}

impl Display for ValueDisplay<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self.value {
            Value::Str(ref s) => write!(f, "{}", s),
            Value::Int(ref s) => write!(f, "{}", s),
            Value::None => write!(f, "None"),
            Value::Float(ref s) => write!(f, "{:.*}", self.display_config.floating_points, s),
            Value::Bool(ref s) => write!(f, "{}", s),
            Value::DateTime(ref dt) => write!(f, "{}", dt),
            Value::Duration(ref d) => {
                let mut remaining: Duration = *d;
                let weeks = remaining.num_seconds() / Duration::weeks(1).num_seconds();
                remaining -= Duration::weeks(weeks);
                let days = remaining.num_seconds() / Duration::days(1).num_seconds();
                remaining -= Duration::days(days);
                let hours = remaining.num_seconds() / Duration::hours(1).num_seconds();
                remaining -= Duration::hours(hours);
                let mins = remaining.num_seconds() / Duration::minutes(1).num_seconds();
                remaining -= Duration::minutes(mins);
                let secs = remaining.num_seconds() / Duration::seconds(1).num_seconds();
                remaining -= Duration::seconds(secs);
                let msecs = remaining.num_milliseconds();
                remaining -= Duration::milliseconds(msecs);
                let usecs = remaining.num_microseconds().unwrap_or(0);

                let pairs = &[
                    (weeks, "w"),
                    (days, "d"),
                    (hours, "h"),
                    (mins, "m"),
                    (secs, "s"),
                    (msecs, "ms"),
                    (usecs, "us"),
                ];

                for (val, sym) in pairs.iter().filter(|(val, _sym)| *val != 0) {
                    write!(f, "{}{}", val, sym)?;
                }
                Ok(())
            }
            Value::Obj(ref o) => {
                // todo: this is pretty janky...
                // These values are sorted so the output is deterministic.
                let mut items = o.iter().collect::<Vec<_>>();
                items.sort();

                let mut rendered = items
                    .iter()
                    .map(|(k, v)| format!("{}:{}", k, self.nested(v)));
                write!(f, "{{{}}}", rendered.join(", "))
            }
            Value::Array(ref o) => {
                let rendered: Vec<_> = o.iter().map(|v| format!("{}", self.nested(v))).collect();
                write!(f, "[{}]", rendered.join(", "))
            }
        }
    }
}

impl Value {
    /// Used to sort mixed values
    pub fn rank(&self) -> u8 {
        match self {
            Value::None => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::Float(_) => 2,
            Value::Str(_) => 3,
            Value::DateTime(_) => 4,
            Value::Duration(_) => 5,
            Value::Array(_) => 6,
            Value::Obj(_) => 7,
        }
    }

    pub fn render(&self, render_config: &DisplayConfig) -> String {
        ValueDisplay::new(self, render_config).to_string()
    }

    pub fn from_bool(b: bool) -> Value {
        Value::Bool(b)
    }

    pub fn from_float(f: f64) -> Value {
        let rounded = f as i64;
        if (f - f.floor()).abs() < f64::EPSILON {
            Value::Int(rounded)
        } else {
            Value::Float(OrderedFloat(f))
        }
    }

    pub fn aggressively_to_num(s: impl AsRef<str> + Into<String>) -> Result<f64, EvalError> {
        // Handle cases like
        // 1,000,000
        match Value::from_string(
            s.as_ref()
                .chars()
                .filter(|c| c.is_numeric() || c == &'.')
                .collect::<String>(),
        ) {
            Value::Float(f) => Ok(f.0),
            Value::Int(i) => Ok(i as f64),
            _other => {
                //println!("not a number...{}", s);
                Err(EvalError::ExpectedNumber { found: s.into() })
            }
        }
    }

    pub fn from_string(s: impl AsRef<str> + Into<String>) -> Value {
        let trimmed = s.as_ref().trim();
        let int_value = trimmed.parse::<i64>();
        let float_value = trimmed.parse::<f64>();
        let bool_value = trimmed.parse::<bool>();
        int_value
            .map(Value::Int)
            .or_else(|_| float_value.map(Value::from_float))
            .or_else(|_| bool_value.map(Value::Bool))
            .unwrap_or_else(|_| Value::Str(trimmed.into()))
    }

    pub fn binary_op(
        &self,
        op_fn: &dyn Fn(f64, f64) -> f64,
        op: &'static str,
        right: &Value,
    ) -> Result<Value, EvalError> {
        let left_res: Result<f64, EvalError> = self.try_into();
        let right_res: Result<f64, EvalError> = right.try_into();

        match (left_res, right_res) {
            (Ok(lf1), Ok(rf1)) => Ok(Value::from_float(op_fn(lf1, rf1))),
            _ => Err(EvalError::ExpectedNumericOperands {
                left: format!("{}", self),
                op,
                right: format!("{}", right),
            }),
        }
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::from_float(f)
    }
}

impl TryFrom<&Value> for f64 {
    type Error = EvalError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(*i as f64),
            Value::Float(f) => Ok(f.0),
            Value::Str(s) => Value::aggressively_to_num(s),
            Value::DateTime(dt) => Ok(dt.timestamp_millis() as f64),
            _ => Err(EvalError::ExpectedNumber {
                found: format!("{}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for usize {
    type Error = EvalError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int(i) if *i >= 0 => Ok(*i as usize),
            Value::Float(f) if f.0 >= 0.0 => Ok(f.0 as usize),
            Value::Str(s) => {
                let num = Value::aggressively_to_num(s)?;

                if num < 0.0 {
                    Err(EvalError::ExpectedPositiveNumber {
                        found: format!("{}", value),
                    })
                } else {
                    Ok(num as usize)
                }
            }
            _ => Err(EvalError::ExpectedPositiveNumber {
                found: format!("{}", value),
            }),
        }
    }
}

impl Aggregate {
    #[cfg(test)]
    pub fn new(
        key_columns: &[String],
        agg_column: String,
        data: &[(HashMap<String, String>, Value)],
    ) -> Aggregate {
        data.iter().for_each(|(row, _value)| {
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
            .map(|(keycols, value)| {
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
    pub fn put<T: Into<String>>(mut self, key: T, value: Value) -> Record {
        self.data.insert(key.into(), value);
        self
    }

    pub fn put_mut<T: Into<String>>(&mut self, key: T, value: Value) {
        self.data.insert(key.into(), value);
    }

    /// Places a Value in the data based on the Expr accessor.
    /// Only works with NestedColumn exprs.
    pub fn put_expr(mut self, key: &Expr, value: Value) -> Result<Record, EvalError> {
        match key {
            Expr::NestedColumn { ref head, ref rest } => {
                let mut root_record: &mut Value = if let Some(record) = self.data.get_mut(head) {
                    record
                } else {
                    return if rest.is_empty() {
                        self.data.insert(head.clone(), value);
                        Ok(self)
                    } else {
                        Err(EvalError::NoValueForKey { key: head.clone() })
                    };
                };

                let rest_len = rest.len();
                for (index, value_reference) in rest.iter().enumerate() {
                    match (value_reference, root_record) {
                        (ValueRef::Field(ref key), Value::Obj(map)) => {
                            if !map.contains_key(key) {
                                let is_last = index + 1 == rest_len;
                                return if is_last {
                                    map.insert(key.clone(), value);
                                    Ok(self)
                                } else {
                                    Err(EvalError::NoValueForKey { key: key.clone() })
                                };
                            }

                            root_record = map.get_mut(key).expect("exists");
                        }
                        (ValueRef::Field(_), other) => {
                            return Err(EvalError::ExpectedXYZ {
                                expected: "object".to_string(),
                                found: other.render(&DisplayConfig::default()),
                            });
                        }
                        (ValueRef::IndexAt(index), Value::Array(vec)) => {
                            let vec_len: i64 = vec.len().try_into().unwrap();
                            let real_index = if *index < 0 { *index + vec_len } else { *index };

                            if real_index < 0 || real_index >= vec_len {
                                return Err(EvalError::IndexOutOfRange { index: *index });
                            }
                            root_record = &mut vec[real_index as usize];
                        }
                        (ValueRef::IndexAt(_), other) => {
                            return Err(EvalError::ExpectedXYZ {
                                expected: "array".to_string(),
                                found: other.render(&DisplayConfig::default()),
                            });
                        }
                    }
                }
                *root_record = value;
            }
            // These should not happen, if so this is a programming error
            // since the data cannot be indexed by BoolUnary / Comparison / Value Exprs.
            Expr::BoolUnary(_) => {
                return Err(EvalError::ExpectedXYZ {
                    expected: "valid expr".to_string(),
                    found: "bool unary expr".to_string(),
                })
            }
            Expr::Comparison(_) => {
                return Err(EvalError::ExpectedXYZ {
                    expected: "valid expr".to_string(),
                    found: "comparison expr".to_string(),
                })
            }
            Expr::Arithmetic(_) => {
                return Err(EvalError::ExpectedXYZ {
                    expected: "valid expr".to_string(),
                    found: "arithmetic expr".to_string(),
                })
            }
            Expr::Logical(_) => {
                return Err(EvalError::ExpectedXYZ {
                    expected: "valid expr".to_string(),
                    found: "logical expr".to_string(),
                })
            }
            Expr::FunctionCall { .. } => {
                return Err(EvalError::ExpectedXYZ {
                    expected: "valid expr".to_string(),
                    found: "function call".to_string(),
                })
            }
            Expr::IfOp { .. } => {
                return Err(EvalError::ExpectedXYZ {
                    expected: "valid expr".to_string(),
                    found: "if operator".to_string(),
                })
            }
            Expr::Value(_) => {
                return Err(EvalError::ExpectedXYZ {
                    expected: "valid expr".to_string(),
                    found: "value expr".to_string(),
                })
            }
        }
        Ok(self)
    }

    pub fn new<T: Into<String>>(raw: T) -> Record {
        Record {
            data: HashMap::new(),
            raw: raw.into(),
        }
    }

    pub fn ordering<T: Into<Expr> + Send + Sync>(
        columns: Vec<T>,
    ) -> impl Fn(&VMap, &VMap) -> Result<Ordering, EvalError> + Send + Sync {
        let columns: Vec<Expr> = columns.into_iter().map(Into::into).collect();
        move |rec_l: &VMap, rec_r: &VMap| {
            for col in &columns {
                let l_val = col.eval_value(rec_l)?;
                let r_val = col.eval_value(rec_r)?;
                let cmp = l_val.cmp(&r_val);
                if cmp != Ordering::Equal {
                    return Ok(cmp);
                }
            }
            Ok(Ordering::Equal)
        }
    }

    pub fn ordering_ref<'a, T: 'a + AsRef<str> + Send + Sync>(
        columns: &'a [T],
    ) -> impl Fn(&VMap, &VMap) -> Ordering + 'a + Send + Sync {
        move |rec_l: &VMap, rec_r: &VMap| {
            for col in columns {
                let l_val = rec_l.get(col.as_ref());
                let r_val = rec_r.get(col.as_ref());
                let cmp = l_val.cmp(&r_val);
                if cmp != Ordering::Equal {
                    return cmp;
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
    use std::str::FromStr;

    #[test]
    fn render_duration() {
        let cfg = DisplayConfig { floating_points: 2 };

        assert_eq!("2w", Value::Duration(Duration::weeks(2)).render(&cfg));
        assert_eq!(
            "2w2d5h25m4s232ms",
            Value::Duration(
                Duration::weeks(2)
                    + Duration::days(2)
                    + Duration::hours(5)
                    + Duration::minutes(25)
                    + Duration::seconds(4)
                    + Duration::milliseconds(232)
            )
            .render(&cfg)
        );
    }

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
    fn serialize_vec() {
        let rec = Value::Array(vec![
            Value::Bool(false),
            Value::from_string("123.5"),
            Value::Array(vec![]),
        ]);
        assert_eq!(rec.render(&DisplayConfig::default()), "[false, 123.50, []]");
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
        assert_eq!(Value::from_string("1 "), Value::Int(1));
        assert_eq!(Value::from_string("abcd "), Value::Str("abcd".to_owned()));
    }

    #[test]
    fn value_ordering() {
        assert_eq!(
            Value::from_string("hello").cmp(&Value::Int(0)),
            Ordering::Greater
        );
    }

    #[test]
    fn test_aggresively_to_num() {
        assert_eq!(
            Value::from_string("1,000,000"),
            Value::Str("1,000,000".to_owned())
        );
        assert_eq!(Value::aggressively_to_num("1,000,000"), Ok(1_000_000_f64));
        assert_eq!(
            Value::aggressively_to_num("1,000,000.1"),
            Ok(1_000_000.1_f64)
        );
    }

    #[test]
    fn record_ordering() {
        let mut r1 = HashMap::<String, Value>::new();
        r1.insert("k1".to_string(), Value::Int(5));
        r1.insert("k3".to_string(), Value::from_float(0.1));
        r1.insert("k2".to_string(), Value::Str("abc".to_string()));
        let mut r2 = HashMap::<String, Value>::new();
        r2.insert("k1".to_string(), Value::Int(4));
        r2.insert("k2".to_string(), Value::Str("xyz".to_string()));
        r2.insert("k3".to_string(), Value::from_float(0.1));
        let ord1 = Record::ordering(vec!["k1".to_string(), "k2".to_string()]);
        assert_eq!(ord1(&r1, &r2), Ok(Ordering::Greater));
        assert_eq!(ord1(&r1, &r1), Ok(Ordering::Equal));
        assert_eq!(ord1(&r2, &r1), Ok(Ordering::Less));

        let ord2 = Record::ordering(vec!["k2".to_string(), "k1".to_string()]);
        assert_eq!(ord2(&r1, &r2), Ok(Ordering::Less));
        assert_eq!(ord2(&r1, &r1), Ok(Ordering::Equal));
        assert_eq!(ord2(&r2, &r1), Ok(Ordering::Greater));

        let ord3 = Record::ordering(vec!["k3".to_string()]);
        assert_eq!(ord3(&r1, &r2), Ok(Ordering::Equal));

        let ord4 = Record::ordering(vec!["k3".to_string(), "k1".to_string()]);
        assert_eq!(ord4(&r1, &r2), Ok(Ordering::Greater));
        assert_eq!(ord4(&r1, &r1), Ok(Ordering::Equal));
        assert_eq!(ord4(&r2, &r1), Ok(Ordering::Less));
    }

    #[test]
    fn record_ordering_matching_prefix() {
        let mut r1 = HashMap::<String, Value>::new();
        r1.insert("k1".to_string(), Value::Int(5));
        r1.insert("k2".to_string(), Value::from_float(6.0));

        let mut r2 = HashMap::<String, Value>::new();
        r2.insert("k1".to_string(), Value::from_float(5.0));
        r2.insert("k2".to_string(), Value::Int(7));

        let ord = Record::ordering(vec!["k1".to_string(), "k2".to_string()]);
        assert_eq!(ord(&r1, &r2), Ok(Ordering::Less));
    }

    #[test]
    fn arithmetic() {
        assert_eq!(
            Value::from_float(5.),
            (Value::from_float(4.) + Value::from_float(1.)).unwrap()
        );
        assert_eq!(Value::Int(5), (Value::Int(4) + Value::Int(1)).unwrap());
        assert_eq!(
            Value::from_float(5.),
            (Value::Int(4) + Value::from_float(1.0)).unwrap()
        );
        assert_eq!(
            Value::from_float(5.),
            (Value::from_float(4.) + Value::Int(1)).unwrap()
        );

        assert_eq!(
            Value::from_float(3.),
            (Value::from_float(4.) - Value::from_float(1.)).unwrap()
        );
        assert_eq!(Value::Int(3), (Value::Int(4) - Value::Int(1)).unwrap());
        assert_eq!(
            Value::from_float(3.),
            (Value::Int(4) - Value::from_float(1.0)).unwrap()
        );
        assert_eq!(
            Value::from_float(3.),
            (Value::from_float(4.) - Value::Int(1)).unwrap()
        );

        assert_eq!(
            Value::from_float(4.),
            (Value::from_float(4.) * Value::from_float(1.)).unwrap()
        );
        assert_eq!(Value::Int(4), (Value::Int(4) * Value::Int(1)).unwrap());
        assert_eq!(
            Value::from_float(4.),
            (Value::Int(4) * Value::from_float(1.0)).unwrap()
        );
        assert_eq!(
            Value::from_float(4.),
            (Value::from_float(4.) * Value::Int(1)).unwrap()
        );

        assert_eq!(
            Value::from_float(2.),
            (Value::from_float(4.) / Value::from_float(2.)).unwrap()
        );
        assert_eq!(Value::Int(2), (Value::Int(4) / Value::Int(2)).unwrap());
        assert_eq!(
            Value::from_float(2.),
            (Value::Int(4) / Value::from_float(2.0)).unwrap()
        );
        assert_eq!(
            Value::from_float(2.),
            (Value::from_float(4.) / Value::Int(2)).unwrap()
        );

        assert_eq!(
            Value::Duration(Duration::days(1)),
            (Value::DateTime(DateTime::<Utc>::from_str("2021-08-11T00:00:00Z").unwrap())
                - Value::DateTime(DateTime::<Utc>::from_str("2021-08-10T00:00:00Z").unwrap()))
            .unwrap()
        );
        assert_eq!(
            Value::Duration(Duration::days(1)),
            (Value::Duration(Duration::days(2)) - Value::Duration(Duration::days(1))).unwrap()
        );
        assert_eq!(
            Value::Duration(Duration::days(3)),
            (Value::Duration(Duration::days(2)) + Value::Duration(Duration::days(1))).unwrap()
        );
        assert_eq!(
            Value::Duration(Duration::days(4)),
            (Value::Duration(Duration::days(2)) * Value::Int(2)).unwrap()
        );
        assert_eq!(
            Value::Duration(Duration::days(1)),
            (Value::Duration(Duration::days(2)) / Value::Int(2)).unwrap()
        );
    }
}
