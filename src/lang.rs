use nom::IResult;
use nom::{is_alphanumeric, is_digit};
use std::str;

#[derive(Debug, PartialEq, Eq)]
pub enum Search {
    MatchFilter(String),
    MatchAll,
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Inline(InlineOperator),
    Aggregate(AggregateOperator),
    Sort(SortOperator),
}

#[derive(Debug, PartialEq, Eq)]
pub enum InlineOperator {
    Json {
        input_column: Option<String>,
    },
    Parse {
        pattern: String,
        fields: Vec<String>,
        input_column: Option<String>,
    },
    Fields {
        mode: FieldMode,
        fields: Vec<String>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum FieldMode {
    Only,
    Except,
}

#[derive(Debug, PartialEq)]
pub enum SortMode {
    Ascending,
    Descending,
}

#[derive(Debug, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum {
        column: String,
    },
    Average {
        column: String,
    },
    Percentile {
        percentile: f64,
        percentile_str: String,
        column: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct AggregateOperator {
    pub key_cols: Vec<String>,
    pub aggregate_function: AggregateFunction,
    pub output_column: String,
}

#[derive(Debug, PartialEq)]
pub struct SortOperator {
    pub sort_cols: Vec<String>,
    pub direction: SortMode,
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub search: Search,
    pub operators: Vec<Operator>,
}

fn is_ident(c: char) -> bool {
    is_alphanumeric(c as u8) || c == '_'
}

fn not_escape(c: char) -> bool {
    c != '\\' && c != '\"'
}

fn vec_str_vec_string(vec: &[&str]) -> Vec<String> {
    vec.iter().map(|s| s.to_string()).collect()
}

named!(ident<&str, &str>, ws!(take_while1!(is_ident)));

named!(json<&str, InlineOperator>, ws!(do_parse!(
    tag!("json") >>
    from_column_opt: opt!(preceded!(tag!("from"), ident)) >>
    (InlineOperator::Json { input_column: from_column_opt.map(|s|s.to_string()) })
)));

named!(quoted_string <&str, &str>, delimited!(
    tag!("\""), 
    escaped!(take_while1!(not_escape), '\\', one_of!("\"n\\")),  
    tag!("\"") 
));

named!(var_list<&str, Vec<&str> >, ws!(separated_nonempty_list!(
    tag!(","), ident
)));

// parse "blah * ... *" [from other_field] as x, y
named!(parse<&str, InlineOperator>, ws!(do_parse!(
    tag!("parse") >>
    pattern: quoted_string >>
    from_column_opt: opt!(preceded!(tag!("from"), ident)) >>
    tag!("as") >>
    vars: var_list >>
    ( InlineOperator::Parse{
        pattern: pattern.replace("\\\"", "\""),
        fields: vec_str_vec_string(&vars),
        input_column: from_column_opt.map(|s|s.to_string())
        } )
)));

named!(fields_mode<&str, FieldMode>, alt!(
    map!(
        alt!(tag!("+") | tag!("only") | tag!("include")),
        |_|FieldMode::Only
    ) |
    map!(
        alt!(tag!("-") | tag!("except") | tag!("drop")),
        |_|FieldMode::Except
    )
));

named!(fields<&str, InlineOperator>, ws!(do_parse!(
    tag!("fields") >>
    mode: opt!(fields_mode) >>
    fields: var_list >>
    (
        InlineOperator::Fields {
            mode: mode.unwrap_or(FieldMode::Only),
            fields: vec_str_vec_string(&fields)
        }
    )
)));

named!(count<&str, AggregateFunction>, map!(tag!("count"), |_s|AggregateFunction::Count{}));

named!(average<&str, AggregateFunction>, ws!(do_parse!(
    alt!(tag!("avg") | tag!("average")) >>
    column: delimited!(tag!("("), ident ,tag!(")")) >>
    (AggregateFunction::Average{column: column.to_string()})
)));

named!(sum<&str, AggregateFunction>, ws!(do_parse!(
    tag!("sum") >>
    column: delimited!(tag!("("), ident ,tag!(")")) >>
    (AggregateFunction::Sum{column: column.to_string()})
)));

fn is_digit_char(digit: char) -> bool {
    is_digit(digit as u8)
}

named!(p_nn<&str, AggregateFunction>, ws!(
    do_parse!(
        alt!(tag!("pct") | tag!("percentile") | tag!("p")) >>
        pct: take_while_m_n!(2, 2, is_digit_char) >>
        column: delimited!(tag!("("), ident ,tag!(")")) >>
        (AggregateFunction::Percentile{
            column: column.to_string(),
            percentile: (".".to_owned() + pct).parse::<f64>().unwrap(),
            percentile_str: pct.to_string()
        })
    )
));

named!(inline_operator<&str, Operator>,
   map!(alt!(parse | json | fields), Operator::Inline)
);
named!(aggregate_function<&str, AggregateFunction>, alt!(count | average | sum | p_nn));

named!(operator<&str, Operator>, alt!(inline_operator | aggregate_operator | sort));

// count by x,y
// avg(foo) by x

fn default_output(func: &AggregateFunction) -> String {
    match *func {
        AggregateFunction::Count { .. } => "_count".to_string(),
        AggregateFunction::Sum { .. } => "_sum".to_string(),
        AggregateFunction::Average { .. } => "_average".to_string(),
        AggregateFunction::Percentile {
            ref percentile_str, ..
        } => "p".to_string() + percentile_str,
    }
}

named!(aggregate_operator<&str, Operator>, ws!(do_parse!(
    agg_function: aggregate_function >>
    key_cols_opt: opt!(preceded!(tag!("by"), var_list)) >>
    rename_opt: opt!(preceded!(tag!("as"), ident)) >>
    (Operator::Aggregate(AggregateOperator{
        key_cols: vec_str_vec_string(&key_cols_opt.unwrap_or_default()),
        output_column: rename_opt
                            .map(|s|s.to_string())
                            .unwrap_or_else(|| default_output(&agg_function)),
        aggregate_function: agg_function,
     })))
));

named!(sort_mode<&str, SortMode>, alt!(
    map!(
        alt!(tag!("asc") | tag!("ascending")),
        |_|SortMode::Ascending
    ) |
    map!(
        alt!(tag!("desc") | tag!("dsc") | tag!("descending")),
        |_|SortMode::Descending
    )
));

named!(sort<&str, Operator>, ws!(do_parse!(
    tag!("sort") >>
    key_cols_opt: opt!(preceded!(opt!(tag!("by")), var_list)) >>
    dir: opt!(sort_mode) >>
    (Operator::Sort(SortOperator{
        sort_cols: vec_str_vec_string(&key_cols_opt.unwrap_or_default()),
        direction: dir.unwrap_or(SortMode::Ascending) ,
     })))
));

named!(filter<&str, Search>, alt!(
    map!(quoted_string, |s|Search::MatchFilter(s.to_string())) |
    map!(tag!("*"), |_s|Search::MatchAll)
));

named!(query<&str, Query>, ws!(do_parse!(
    filter: dbg!(filter) >>
    operators: dbg!(opt!(preceded!(tag!("|"), ws!(separated_nonempty_list!(tag!("|"), operator))))) >>
    (Query{
        search: filter,
        operators: operators.unwrap_or_default()
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
                    input_column: None,
                },
            ))
        );
        assert_eq!(
            parse(r#"parse "[key=*]" as v!"#),
            Ok((
                "!",
                InlineOperator::Parse {
                    pattern: "[key=*]".to_string(),
                    fields: vec!["v".to_string()],
                    input_column: None,
                },
            ))
        );
    }

    #[test]
    fn test_operator() {
        assert_eq!(
            operator("  json!"),
            Ok((
                "!",
                Operator::Inline(InlineOperator::Json { input_column: None })
            ))
        );
        assert_eq!(
            operator(r#" parse "[key=*]" from field as v !"#),
            Ok((
                "!",
                Operator::Inline(InlineOperator::Parse {
                    pattern: "[key=*]".to_string(),
                    fields: vec!["v".to_string()],
                    input_column: Some("field".to_string()),
                },)
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
                    key_cols: vec_str_vec_string(&["x", "y"]),
                    aggregate_function: AggregateFunction::Count,
                    output_column: "renamed".to_string(),
                },)
            ))
        );
    }

    #[test]
    fn test_percentile() {
        assert_eq!(
            aggregate_operator("p50(x)!"),
            Ok((
                "!",
                Operator::Aggregate(AggregateOperator {
                    key_cols: vec![],
                    aggregate_function: AggregateFunction::Percentile {
                        column: "x".to_string(),
                        percentile: 0.5,
                        percentile_str: "50".to_string(),
                    },
                    output_column: "p50".to_string(),
                },)
            ))
        );
    }

    #[test]
    fn test_query_no_operators() {
        let query_str = r#" "filter"!"#;
        assert_eq!(
            parse_query(query_str),
            Ok((
                "",
                Query {
                    search: Search::MatchFilter("filter".to_string()),
                    operators: vec![],
                },
            ))
        );
    }

    #[test]
    fn test_query_operators() {
        let query_str =
            r#"* | json from col | parse "!123*" as foo | count by foo | sort by foo dsc !"#;
        assert_eq!(
            parse_query(query_str),
            Ok((
                "",
                Query {
                    search: Search::MatchAll,
                    operators: vec![
                        Operator::Inline(InlineOperator::Json {
                            input_column: Some("col".to_string()),
                        }),
                        Operator::Inline(InlineOperator::Parse {
                            pattern: "!123*".to_string(),
                            fields: vec!["foo".to_string()],
                            input_column: None,
                        }),
                        Operator::Aggregate(AggregateOperator {
                            key_cols: vec!["foo".to_string()],
                            aggregate_function: AggregateFunction::Count {},
                            output_column: "_count".to_string(),
                        }),
                        Operator::Sort(SortOperator {
                            sort_cols: vec!["foo".to_string()],
                            direction: SortMode::Descending,
                        }),
                    ],
                },
            ))
        );
    }

}
