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

/// Given a slice and start and end terminators, find a closing terminator while respecting escaped values.
/// If a closing terminator is found, starting and ending terminators will be removed.
/// If no closing terminator exists, the starting terminator will not be removed.
fn find_close_terminator<'a>(
    s: &'a str,
    term_start: &'a str,
    term_end: &'a str,
) -> (&'a str, &'a str) {
    let mut pos = term_start.len();
    while pos < s.len() {
        match s[pos..].find(term_end).map(|index| index + pos) {
            None => break,
            Some(i) if i == 0 || &s[i - 1..i] != "\\" => {
                return (&s[term_start.len()..i], &s[i + term_end.len()..])
            }
            Some(other) => pos = other + 1,
        }
    }
    // We end up here if we never found a close quote. In that case, don't strip the leading quote.
    return (&s, &s[0..0]);
}

fn split_once<'a>(s: &'a str, p: &'a str) -> (&'a str, &'a str) {
    let mut split_iter = s.splitn(2, p);
    (split_iter.next().unwrap(), split_iter.next().unwrap_or(""))
}

/// split function that respects delimiters
pub fn split_with_delimiters<'a>(
    input: &'a str,
    separator: &'a str,
    delimiters: &HashMap<&'static str, &'static str>,
) -> Vec<&'a str> {
    let mut wip = input;
    let mut ret: Vec<&'a str> = vec![];

    while wip.len() > 0 {
        // Strip separators
        if wip.starts_with(separator) {
            wip = &wip[separator.len()..];
            continue;
        }
        // Look for a leading quote
        let terminator = delimiters
            .into_iter()
            .flat_map(|(k, v)| {
                if wip.starts_with(k) {
                    Some((k, v))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if let Some((term_start, term_end)) = terminator.first() {
            // If we're left with a quoted string, consume it.
            let (quoted_section, rest) = find_close_terminator(wip, *term_start, *term_end);
            wip = rest;
            ret.push(quoted_section);
        } else {
            // Otherwise, read until the next separator
            let (next_section, rest) = split_once(wip, separator);
            wip = rest;
            if !next_section.is_empty() {
                ret.push(next_section)
            }
        }
    }
    ret.into_iter().map(|s| s.trim()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_terminator() {
        assert_eq!(
            find_close_terminator(r#""foo \" bar" cde"#, "\"", "\""),
            (r#"foo \" bar"#, " cde")
        );
        // Only strip the quote if a terminator is actually found
        assert_eq!(
            find_close_terminator(r#""'foo "#, "\"", "\""),
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
            split_with_delimiters(r#"Oct 09 20:22:21 web-001 influxd[188053]: 127.0.0.1 "POST /write \"escaped\" HTTP/1.0" 204"#, " ", &DEFAULT_DELIMITERS),
            vec!["Oct", "09", "20:22:21", "web-001", "influxd[188053]:", "127.0.0.1", "POST /write \\\"escaped\\\" HTTP/1.0", "204"],
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
}
