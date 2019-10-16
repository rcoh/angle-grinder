use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static! {
    pub static ref DEFAULT_CLOSURES: HashMap<&'static str, &'static str> = {
        let mut h = HashMap::new();
        h.insert("\"", "\"");
        h.insert("'", "'");
        h
    };
}

fn find_close_terminator(s: &str, term: &str) -> Option<usize> {
    let mut pos = 0;
    while pos < s.len() {
        match s[pos..].find(term) {
            Some(i) if i == 0 || &s[pos + i - 1..pos + i] != "\\" => return Some(i + pos),
            Some(other) => pos += other + 1,
            None => return None,
        }
    }
    return None;
}

pub fn split_with_separator_and_closures<'a>(
    input: &'a str,
    separator: &'a str,
    closures: &HashMap<&'static str, &'static str>,
) -> Vec<&'a str> {
    // 1. Find the next separator.
    // 2. Determine if there's a quote involved. Quotes can only follow separators.
    // 3. Read until matching quote.

    let mut index = 0;
    let mut ret: Vec<&'a str> = vec![];

    while index < input.len() {
        let terminator = closures
            .into_iter()
            .flat_map(|(k, v)| {
                if input[index..].starts_with(k) {
                    Some((k, v))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if let Some((term_start, term_end)) = terminator.first() {
            index += term_start.len();
            let end_quote = find_close_terminator(&input[index..], *term_end).map(|i| i + index);
            if let Some(end) = end_quote {
                ret.push(&input[index..end]);
                index = end + term_end.len();
                if input[index..].starts_with(separator) {
                    index += separator.len();
                } else {
                    // malformed split -- quotes need to end at a separator.
                }
            } else {
                ret.push(&input[index - term_start.len()..]);
                index = input.len();
            }
        } else {
            let next_sep = input[index..]
                .find(separator)
                .map(|i| i + index)
                .unwrap_or(input.len());
            if index != next_sep {
                ret.push(&input[index..next_sep]);
            }
            index = next_sep + separator.len();
        }
    }
    ret.into_iter().map(|s| s.trim()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_works() {
        assert_eq!(
            split_with_separator_and_closures("power hello", " ", &DEFAULT_CLOSURES),
            vec!["power", "hello"],
        );
        assert_eq!(
            split_with_separator_and_closures("morecomplicated", "ecomp", &DEFAULT_CLOSURES),
            vec!["mor", "licated"],
        );
        assert_eq!(
            split_with_separator_and_closures("owmmowmow", "ow", &DEFAULT_CLOSURES),
            vec!["mm", "m"],
        );
        assert_eq!(
            split_with_separator_and_closures(r#"Oct 09 20:22:21 web-001 influxd[188053]: 127.0.0.1 "POST /write \"escaped\" HTTP/1.0" 204"#, " ", &DEFAULT_CLOSURES),
            vec!["Oct", "09", "20:22:21", "web-001", "influxd[188053]:", "127.0.0.1", "POST /write \\\"escaped\\\" HTTP/1.0", "204"],
        );
    }

    #[test]
    fn split_with_closures_works() {
        assert_eq!(
            split_with_separator_and_closures("power hello \"good bye\"", " ", &DEFAULT_CLOSURES),
            vec!["power", "hello", "good bye"],
        );
        assert_eq!(
            split_with_separator_and_closures("more'ecomp'licated", "ecomp", &DEFAULT_CLOSURES),
            vec!["more'", "'licated"],
        );
        assert_eq!(
            split_with_separator_and_closures("ow\"mm\"ow'\"mow\"'", "ow", &DEFAULT_CLOSURES),
            vec!["mm", "\"mow\""],
        );
    }
}
