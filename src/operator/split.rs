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

/// Helper function for splitting a string by a separator, but also
/// respecting closures (like "double-quoted strings").
pub fn split_with_separator_and_closures<T>(
    input: &str,
    separator: &str,
    closures: &HashMap<&'static str, &'static str>,
    map: impl Fn(String) -> T,
) -> Vec<T> {
    let input = input.trim();

    let mut prev_index = 0;
    let mut index = 0;
    let mut tokens = vec![];

    // If any tokens are surrounded by closure start / end, then trim.
    let transform = |mut token: String| {
        for (start, end) in closures {
            if token.starts_with(start) && token.ends_with(end) {
                token = token[start.len()..token.len() - end.len()].to_string();
                break;
            }
        }

        map(token)
    };

    while index < input.len() {
        let mut found_separator = false;
        let remaining = &input[index..];
        if remaining.starts_with(separator) {
            found_separator = true;
        } else {
            let mut found = false;
            for (start, end) in closures {
                if !remaining.starts_with(start) {
                    continue;
                }

                if let Some(end_index) = remaining[start.len()..].find(end) {
                    index += start.len() + end_index + end.len();
                    found = true;
                    break;
                }
            }

            if !found {
                index += 1;
            }
        }

        if found_separator {
            if index != prev_index {
                tokens.push(transform(input[prev_index..index].to_string()));
            }
            index += separator.len();
            prev_index = index;
        }
    }

    if index != prev_index {
        tokens.push(transform(input[prev_index..index].to_string()));
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split_with_separator_and_closures_string(
        input: &str,
        separator: &str,
        closures: &HashMap<&'static str, &'static str>,
    ) -> Vec<String> {
        split_with_separator_and_closures(input, separator, closures, |s| s)
    }

    #[test]
    fn split_works() {
        assert_eq!(
            split_with_separator_and_closures_string("power hello", " ", &DEFAULT_CLOSURES),
            vec!["power", "hello"],
        );
        assert_eq!(
            split_with_separator_and_closures_string("morecomplicated", "ecomp", &DEFAULT_CLOSURES),
            vec!["mor", "licated"],
        );
        assert_eq!(
            split_with_separator_and_closures_string("owmmowmow", "ow", &DEFAULT_CLOSURES),
            vec!["mm", "m"],
        );
    }

    #[test]
    fn split_with_closures_works() {
        assert_eq!(
            split_with_separator_and_closures_string(
                "power hello \"good bye\"",
                " ",
                &DEFAULT_CLOSURES
            ),
            vec!["power", "hello", "good bye"],
        );
        assert_eq!(
            split_with_separator_and_closures_string(
                "more'ecomp'licated",
                "ecomp",
                &DEFAULT_CLOSURES
            ),
            vec!["more'ecomp'licated"],
        );
        assert_eq!(
            split_with_separator_and_closures_string(
                "ow\"mm\"ow'\"mow\"'",
                "ow",
                &DEFAULT_CLOSURES
            ),
            vec!["mm", "\"mow\""],
        );
    }
}
