use std::str;
use nom::is_alphanumeric;
use nom::IResult;

#[derive(Debug, PartialEq, Eq)]
pub enum Search {
    MatchFilter(String),
    MatchAll,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Operator {
    Inline(InlineOperator),
    Aggregate(AggregateOperator),
}

#[derive(Debug, PartialEq, Eq)]
pub enum InlineOperator {
    Json,
    Parse {
        pattern: String,
        fields: Vec<String>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Average {
        column: String,
    },
    Percentile {
        percentiles: Vec<String>,
        column: String,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub struct AggregateOperator {
    pub key_cols: Vec<String>,
    pub aggregate_function: AggregateFunction,
    pub output_column: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Query {
    pub search: Search,
    pub operators: Vec<Operator>,
}

fn is_ident(c: char) -> bool {
    return is_alphanumeric(c as u8) || c == '_';
}

fn not_escape(c: char) -> bool {
    return c != '\\' && c != '\"';
}

fn vec_str_vec_string(vec: Vec<&str>) -> Vec<String> {
    vec.iter().map(|s| s.to_string()).collect()
}

named!(ident<&str, &str>, ws!(take_while1!(is_ident)));

named!(json<&str, InlineOperator>, map!(ws!(tag!("json")), |_s|InlineOperator::Json));

named!(quoted_string <&str, &str>, delimited!(
    tag!("\""), 
    escaped!(take_while1!(not_escape), '\\', one_of!("\"n\\")),  
    tag!("\"") 
));

named!(var_list<&str, Vec<&str> >, ws!(separated_nonempty_list!(
    tag!(","), ident
)));

// parse "blah * ... *" as x, y
named!(parse<&str, InlineOperator>, ws!(do_parse!(
    tag!("parse") >>
    pattern: quoted_string >>
    tag!("as") >>
    vars: var_list >>
    ( InlineOperator::Parse{
        pattern: pattern.to_string(),
        fields: vec_str_vec_string(vars)} )
)));

named!(count<&str, AggregateFunction>, map!(tag!("count"), |_s|AggregateFunction::Count{}));

named!(average<&str, AggregateFunction>, ws!(do_parse!(
    alt!(tag!("avg") | tag!("average")) >>
    column: delimited!(tag!("("), ident ,tag!(")")) >>
    (AggregateFunction::Average{column: column.to_string()})
)));

named!(inline_operator<&str, Operator>, map!(alt!(parse | json), |op|Operator::Inline(op)));
named!(aggregate_function<&str, AggregateFunction>, alt!(count | average));

named!(operator<&str, Operator>, alt!(inline_operator | aggregate_operator));

// count by x,y
// avg(foo) by x

named!(aggregate_operator<&str, Operator>, ws!(do_parse!(
    agg_function: aggregate_function >>
    key_cols_opt: opt!(preceded!(tag!("by"), var_list)) >>
    rename_opt: opt!(preceded!(tag!("as"), ident)) >>
    (Operator::Aggregate(AggregateOperator{
        key_cols: vec_str_vec_string(key_cols_opt.unwrap_or(vec![])),
        aggregate_function: agg_function,
        output_column: rename_opt.map(|s|s.to_string())
     })))
));

//named!(agg_operator<&str, Operator>, map!())
named!(filter<&str, Search>, alt!(
    map!(quoted_string, |s|Search::MatchFilter(s.to_string())) |
    map!(tag!("*"), |_s|Search::MatchAll)
));

named!(query<&str, Query>, ws!(do_parse!(
    filter: dbg!(filter) >>
    operators: dbg!(opt!(preceded!(tag!("|"), ws!(separated_nonempty_list!(tag!("|"), operator))))) >>
    (Query{
        search: filter,
        operators: operators.unwrap_or(Vec::new())
    })
)));

pub fn parse_query(query_str: &str) -> IResult<&str, Query> {
    terminated!(query_str, query, tag!("!"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quoted_string() {
        assert_eq!(quoted_string(r#""hello""#), Ok(("", "hello")));
        assert_eq!(
            quoted_string(r#""test = [*=*] * ""#),
            Ok(("", "test = [*=*] * "))
        );
    }

    #[test]
    fn test_ident() {
        assert_eq!(ident("hello123!"), Ok(("!", "hello123")));
    }

    #[test]
    fn test_var_list() {
        assert_eq!(
            var_list("a, b, def, g_55!"),
            Ok(("!", vec!["a", "b", "def", "g_55"]))
        );
    }

    #[test]
    fn test_parses() {
        assert_eq!(
            parse(r#"parse "[key=*]" as v!"#),
            Ok((
                "!",
                InlineOperator::Parse {
                    pattern: "[key=*]".to_string(),
                    fields: vec!["v".to_string()],
                }
            ))
        );
        assert_eq!(
            parse(r#"parse "[key=*]" as v!"#),
            Ok((
                "!",
                InlineOperator::Parse {
                    pattern: "[key=*]".to_string(),
                    fields: vec!["v".to_string()],
                }
            ))
        );
    }

    #[test]
    fn test_operator() {
        assert_eq!(
            operator("  json!"),
            Ok(("!", Operator::Inline(InlineOperator::Json)))
        );
        assert_eq!(
            operator(r#" parse "[key=*]" as v !"#),
            Ok((
                "!",
                Operator::Inline(InlineOperator::Parse {
                    pattern: "[key=*]".to_string(),
                    fields: vec!["v".to_string()],
                })
            ))
        );
    }

    #[test]
    fn test_agg_operator() {
        assert_eq!(
            aggregate_operator("count by x, y as renamed!"),
            Ok((
                "!",
                Operator::Aggregate(AggregateOperator {
                    key_cols: vec_str_vec_string(vec!["x", "y"]),
                    aggregate_function: AggregateFunction::Count,
                    output_column: Some("renamed".to_string()),
                })
            ))
        );
    }

    #[test]
    fn test_query_no_operators() {
        // TODO: make | optional
        let query_str = r#" "filter"!"#;
        assert_eq!(
            parse_query(query_str),
            Ok((
                "",
                Query {
                    search: Search::MatchFilter("filter".to_string()),
                    operators: vec![],
                }
            ))
        );
    }

    #[test]
    fn test_query_operators() {
        let query_str = r#"* | json | parse "!123*" as foo | count by foo !"#;
        assert_eq!(
            parse_query(query_str),
            Ok((
                "",
                Query {
                    search: Search::MatchAll,
                    operators: vec![
                        Operator::Inline(InlineOperator::Json),
                        Operator::Inline(InlineOperator::Parse {
                            pattern: "!123*".to_string(),
                            fields: vec!["foo".to_string()],
                        }),
                        Operator::Aggregate(AggregateOperator {
                            key_cols: vec!["foo".to_string()],
                            aggregate_function: AggregateFunction::Count {},
                            output_column: None,
                        }),
                    ],
                }
            ))
        );
    }

}
/*
// parse "blah * ... *" as x, y
named!(parse<&str, InlineOperator>, do_parse!(
    tag!("parse") >> multispace >> pattern: quoted_string >> multispace >> tag!("as") >> 
))*/
