use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static! {
    pub static ref DEFAULT_DELIMITERS: HashMap<&'static str, &'static str> = {
        let mut h = HashMap::new();
        h.insert("\"", "\"");
        h.insert("'", "'");
        h
    };
}

/// Find the character before the character beginning ad idx
fn prev_char(s: &str, idx: usize) -> Option<&str> {
    if idx == 0 {
        None
    } else {
        let chr_end = idx;
        let mut char_start = chr_end - 1;
        while !s.is_char_boundary(char_start) {
            char_start -= 1;
        }
        Some(&s[char_start..chr_end])
    }
}

/// Given a slice and start and end terminators, find a closing terminator while respecting escaped values.
/// If a closing terminator is found, starting and ending terminators will be removed.
/// If no closing terminator exists, the starting terminator will not be removed.
fn find_close_delimiter<'a>(
    s: &'a str,
    term_start: &'a str,
    term_end: &'a str,
) -> (&'a str, &'a str) {
    let mut pos = term_start.len();
    while pos < s.len() {
        match s[pos..].find(term_end).map(|index| index + pos) {
            None => break,
            Some(i) if i == 0 || prev_char(s, i) != Some("\\") => {
                return (&s[term_start.len()..i], &s[i + term_end.len()..]);
            }
            Some(other) => pos = other + 1,
        }
    }
    // We end up here if we never found a close quote. In that case, don't strip the leading quote.
    (s, &s[0..0])
}

fn split_once<'a>(s: &'a str, p: &'a str) -> (&'a str, &'a str) {
    let mut split_iter = s.splitn(2, p);
    (split_iter.next().unwrap(), split_iter.next().unwrap_or(""))
}

/// split function that respects delimiters and strips whitespace
pub fn split_with_delimiters<'a>(
    input: &'a str,
    separator: &'a str,
    delimiters: &HashMap<&'static str, &'static str>,
) -> Vec<&'a str> {
    let mut wip = input;
    let mut ret: Vec<&'a str> = vec![];

    while !wip.is_empty() {
        // Look for a leading quote
        let leading_delimiter = delimiters
            .iter()
            .flat_map(|(k, v)| {
                if wip.starts_with(k) {
                    Some((k, v))
                } else {
                    None
                }
            })
            .next();

        // If we're left with a quoted string, consume it, otherwise read until the next separator
        let (token, rest) = match leading_delimiter {
            Some((term_start, term_end)) => find_close_delimiter(wip, *term_start, *term_end),
            None => split_once(wip, separator),
        };
        let token = token.trim();
        if !token.is_empty() {
            ret.push(token);
        }
        wip = rest;
    }
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_terminator() {
        assert_eq!(
            find_close_delimiter(r#""foo \" bar" cde"#, "\"", "\""),
            (r#"foo \" bar"#, " cde")
        );
        // Only strip the quote if a terminator is actually found
        assert_eq!(
            find_close_delimiter(r#""'foo "#, "\"", "\""),
            ("\"'foo ", "")
        );
    }

    #[test]
    fn test_split_once() {
        assert_eq!(split_once("abc cde", " "), ("abc", "cde"));
        assert_eq!(split_once(" cde", " "), ("", "cde"));
        assert_eq!(split_once("", " "), ("", ""));
    }

    #[test]
    fn split_works() {
        assert_eq!(
            split_with_delimiters("power hello", " ", &DEFAULT_DELIMITERS),
            vec!["power", "hello"],
        );
        assert_eq!(
            split_with_delimiters("morecomplicated", "ecomp", &DEFAULT_DELIMITERS),
            vec!["mor", "licated"],
        );
        assert_eq!(
            split_with_delimiters("owmmowmow", "ow", &DEFAULT_DELIMITERS),
            vec!["mm", "m"],
        );
        assert_eq!(
            split_with_delimiters(
                r#"Oct 09 20:22:21 web-001 influxd[188053]: 127.0.0.1 "POST /write \"escaped\" HTTP/1.0" 204"#,
                " ",
                &DEFAULT_DELIMITERS
            ),
            vec![
                "Oct",
                "09",
                "20:22:21",
                "web-001",
                "influxd[188053]:",
                "127.0.0.1",
                "POST /write \\\"escaped\\\" HTTP/1.0",
                "204"
            ],
        );
    }

    #[test]
    fn split_with_closures_works() {
        assert_eq!(
            split_with_delimiters("power hello \"good bye\"", " ", &DEFAULT_DELIMITERS),
            vec!["power", "hello", "good bye"],
        );
        assert_eq!(
            split_with_delimiters("more'ecomp'licated", "ecomp", &DEFAULT_DELIMITERS),
            vec!["more'", "'licated"],
        );
        assert_eq!(
            split_with_delimiters("ow\"mm\"ow'\"mow\"'", "ow", &DEFAULT_DELIMITERS),
            vec!["mm", "\"mow\""],
        );
    }

    // see https://github.com/rcoh/angle-grinder/issues/138
    #[test]
    fn split_with_wide_characters() {
        assert_eq!(
            split_with_delimiters(
                r#""Bourgogne-Franche-Comté" hello"#,
                " ",
                &DEFAULT_DELIMITERS
            ),
            vec!["Bourgogne-Franche-Comté", "hello"]
        );
    }
}
