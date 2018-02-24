
use std::collections::HashMap;
#[derive(Clone)]
pub struct Record {
    pub data: HashMap<String, Value>,
    pub raw: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    // Consider big int
    Int(i64),
    Float(f64),
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
    use super::Record;
    use super::Value;

    #[test]
    fn test_record_put_get() {
        let rec = Record::new("heres some data");
        let rec = rec.put("key1", Value::Int(9999));
        assert_eq!(rec.data.get("key1").unwrap(), &Value::Int(9999));
        assert!(rec.data.get("key2").is_none());
        assert_eq!(rec.raw, "heres some data");
    }

}
