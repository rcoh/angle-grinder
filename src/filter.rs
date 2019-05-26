#[derive(Debug)]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Keyword(regex::Regex),
}

impl Filter {
    pub fn matches(&self, inp: &str) -> bool {
        match self {
            Filter::Keyword(regex) => regex.is_match(inp),
            Filter::And(clauses) => clauses.iter().all(|clause| clause.matches(inp)),
            Filter::Or(clauses) => clauses.iter().any(|clause| clause.matches(inp)),
            Filter::Not(clause) => !clause.matches(inp),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lang::Keyword;

    pub fn from_str(inp: &str) -> Filter {
        Filter::Keyword(Keyword::new_exact(inp.to_owned()).to_regex())
    }

    #[test]
    fn filter_and() {
        let filt = Filter::And(vec![from_str("abc"), from_str("cde")]);
        assert_eq!(filt.matches("abc cde"), true);
        assert_eq!(filt.matches("abc"), false);
    }

    #[test]
    fn filter_or() {
        let filt = Filter::Or(vec![from_str("abc"), from_str("cde")]);
        assert_eq!(filt.matches("abc cde"), true);
        assert_eq!(filt.matches("abc"), true);
        assert_eq!(filt.matches("cde"), true);
        assert_eq!(filt.matches("def"), false);
    }

    #[test]
    fn filter_wildcard() {
        let filt = Filter::And(vec![]);
        assert_eq!(filt.matches("abc"), true);
    }

    #[test]
    fn filter_not() {
        let filt = Filter::And(vec![from_str("abc"), from_str("cde")]);
        let filt = Filter::Not(Box::new(filt));
        assert_eq!(filt.matches("abc cde"), false);
        assert_eq!(filt.matches("abc"), true);
    }

    #[test]
    fn filter_complex() {
        let filt_letters = Filter::And(vec![from_str("abc"), from_str("cde")]);
        let filt_numbers = Filter::And(vec![from_str("123"), from_str("456")]);
        let filt = Filter::Or(vec![filt_letters, filt_numbers]);
        assert_eq!(filt.matches("abc cde"), true);
        assert_eq!(filt.matches("abc"), false);
        assert_eq!(filt.matches("123 456"), true);
        assert_eq!(filt.matches("123 cde"), false);
    }
}
