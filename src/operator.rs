extern crate serde_json;
use data::Record;
use data;
use self::serde_json::{Result, Value};
trait UnaryOp {
    fn process(&self, rec: &Record) -> Option<Record>;
}

struct ParseJson {
        // any options here
}
impl UnaryOp for ParseJson {
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
    use super::UnaryOp;
    use super::ParseJson;

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
}
