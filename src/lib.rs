mod data;
mod operator;

mod operator {
    extern crate serde_json;
    use data;
    use data::Record;
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
                            map.iter().fold(Some(rec), |acc, (ref k, ref v)| match v {
                                &&Value::Number(ref num) => if num.is_i64() {
                                    acc.map(|a| a.put(k, data::Value::Int(num.as_i64().unwrap())))
                                } else {
                                    acc.map(|a| a.put(k, data::Value::Float(num.as_f64().unwrap())))
                                },
                                _other => None,
                            })
                        }
                        _other => None,
                    }
                }
                _e => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    mod test_data {
        use super::super::data::Record;
        use super::super::data::Value;

        #[test]
        fn test_record_put_get() {
            let rec = Record::new("heres some data");
            let rec = rec.put("key1", Value::Int(9999));
            assert_eq!(rec.data.get("key1").unwrap(), &Value::Int(9999));
            assert!(rec.data.get("key2").is_none());
            assert_eq!(rec.raw, "heres some data");
        }

    }

    mod test_operators {
        use super::super::data::Record;
        use super::super::data::Value;

        #[test]
        fn test_json() {
            let rec = Record::new(r#"{"k1": 5, "k2": 5.5}"#);


    }

}

pub mod inputreader {
    pub fn read_input() {
        println!("hello ok!");
    }
}
