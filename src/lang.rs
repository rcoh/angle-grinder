use data;
use nom;
use nom::types::CompleteStr;
use nom::{is_alphabetic, is_alphanumeric, is_digit, digit1};
use std::str;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ComparisonOp {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BinaryOp {
    Comparison(ComparisonOp),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expr {
    Column(String),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Value(data::Value),
}

impl Expr {
    pub fn force(&self) -> String {
        match self {
            &Expr::Column(ref s) => s.clone(),
            _other => unimplemented!(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Search {
    MatchFilter(String),
    MatchAll,
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Inline(InlineOperator),
    MultiAggregate(MultiAggregateOperator),
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
        input_column: Option<Expr>,
    },
    Fields {
        mode: FieldMode,
        fields: Vec<String>,
    },
    Where {
        expr: Expr,
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
        column: Expr,
    },
    Average {
        column: Expr,
    },
    Percentile {
        percentile: f64,
        percentile_str: String,
        column: Expr,
    },
    CountDistinct {
        column: Expr,
    },
}

#[derive(Debug, PartialEq)]
pub struct MultiAggregateOperator {
    pub key_cols: Vec<Expr>,
    pub key_col_headers: Vec<String>,
    pub aggregate_functions: Vec<(String, AggregateFunction)>,
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

fn starts_ident(c: char) -> bool {
    is_alphabetic(c as u8) || c == '_'
}

fn not_escape(c: char) -> bool {
    c != '\\' && c != '\"'
}

named!(value<CompleteStr, data::Value>, ws!(
    alt!(
        map!(quoted_string, |s|data::Value::Str(s.to_string()))
        | map!(digit1, |s|data::Value::from_string(s.0))
    )
));
named!(ident<CompleteStr, String>, do_parse!(
    start: take_while1!(starts_ident) >>
    rest: take_while!(is_ident) >>
    (start.0.to_owned() + rest.0)
));

named!(e_ident<CompleteStr, Expr>,
    ws!(alt!(
      map!(ident, |col|Expr::Column(col.to_owned()))
    | map!(value, Expr::Value)
      //expr
    |  ws!(delimited!( tag_s!("("), expr, tag_s!(")") ))
)));

named!(comp_op<CompleteStr, ComparisonOp>, ws!(alt!(
    map!(tag!("=="), |_|ComparisonOp::Eq)
    | map!(tag!("<="), |_|ComparisonOp::Lte)
    | map!(tag!(">="), |_|ComparisonOp::Gte)
    | map!(tag!("!="), |_|ComparisonOp::Neq)
    | map!(tag!(">"), |_|ComparisonOp::Gt)
    | map!(tag!("<"), |_|ComparisonOp::Lt)
)));

named!(expr<CompleteStr, Expr>, ws!(alt!(
    do_parse!(
        l: e_ident >>
        comp: comp_op >>
        r: e_ident >>
        ( Expr::Binary { op: BinaryOp::Comparison(comp), left: Box::new(l), right: Box::new(r)} )
    )
    | e_ident
)));

named!(json<CompleteStr, InlineOperator>, ws!(do_parse!(
    tag!("json") >>
    from_column_opt: opt!(ws!(preceded!(tag!("from"), ident))) >>
    (InlineOperator::Json { input_column: from_column_opt.map(|s|s.to_string()) })
)));

named!(whre<CompleteStr, InlineOperator>, ws!(do_parse!(
    tag!("where") >>
    ex: expr >>
    (InlineOperator::Where { expr: ex })
)));

named!(quoted_string <CompleteStr, &str>, delimited!(
    tag!("\""), 
    map!(escaped!(take_while1!(not_escape), '\\', one_of!("\"n\\")), |ref s|s.0),
    tag!("\"") 
));

named!(var_list<CompleteStr, Vec<String> >, ws!(separated_nonempty_list!(
    tag!(","), ws!(ident)
)));

named!(sourced_expr_list<CompleteStr, Vec<(String, Expr)> >, ws!(separated_nonempty_list!(
    tag!(","), ws!(sourced_expr)
)));

named!(sourced_expr<CompleteStr, (String, Expr)>, ws!(
    do_parse!(
        ex: recognize!(expr) >>
        (
            (ex.0.trim().to_string(), expr(ex).unwrap().1)
        )
)));

// parse "blah * ... *" [from other_field] as x, y
named!(parse<CompleteStr, InlineOperator>, ws!(do_parse!(
    tag!("parse") >>
    pattern: quoted_string >>
    from_column_opt: opt!(ws!(preceded!(tag!("from"), expr))) >>
    tag!("as") >>
    vars: var_list >>
    ( InlineOperator::Parse{
        pattern: pattern.replace("\\\"", "\""),
        fields: vars,
        input_column: from_column_opt
        } )
)));

named!(fields_mode<CompleteStr, FieldMode>, alt!(
    map!(
        alt!(tag!("+") | tag!("only") | tag!("include")),
        |_|FieldMode::Only
    ) |
    map!(
        alt!(tag!("-") | tag!("except") | tag!("drop")),
        |_|FieldMode::Except
    )
));

named!(fields<CompleteStr, InlineOperator>, ws!(do_parse!(
    tag!("fields") >>
    mode: opt!(fields_mode) >>
    fields: var_list >>
    (
        InlineOperator::Fields {
            mode: mode.unwrap_or(FieldMode::Only),
            fields
        }
    )
)));

named!(count<CompleteStr, AggregateFunction>, map!(tag!("count"), |_s|AggregateFunction::Count{}));

named!(average<CompleteStr, AggregateFunction>, ws!(do_parse!(
    alt!(tag!("avg") | tag!("average")) >>
    column: delimited!(tag!("("), expr ,tag!(")")) >>
    (AggregateFunction::Average{column})
)));

named!(count_distinct<CompleteStr, AggregateFunction>, ws!(do_parse!(
    tag!("count_distinct") >>
    column: delimited!(tag!("("), expr,tag!(")")) >>
    (AggregateFunction::CountDistinct{column})
)));

named!(sum<CompleteStr, AggregateFunction>, ws!(do_parse!(
    tag!("sum") >>
    column: delimited!(tag!("("), expr,tag!(")")) >>
    (AggregateFunction::Sum{column})
)));

fn is_digit_char(digit: char) -> bool {
    is_digit(digit as u8)
}

named!(p_nn<CompleteStr, AggregateFunction>, ws!(
    do_parse!(
        alt!(tag!("pct") | tag!("percentile") | tag!("p")) >>
        pct: take_while_m_n!(2, 2, is_digit_char) >>
        column: delimited!(tag!("("), expr,tag!(")")) >>
        (AggregateFunction::Percentile{
            column,
            percentile: (".".to_owned() + pct.0).parse::<f64>().unwrap(),
            percentile_str: pct.0.to_string()
        })
    )
));

named!(inline_operator<CompleteStr, Operator>,
   map!(alt!(parse | json | fields | whre), Operator::Inline)
);
named!(aggregate_function<CompleteStr, AggregateFunction>, alt!(
    count_distinct |
    count |
    average |
    sum |
    p_nn));

named!(operator<CompleteStr, Operator>, alt!(inline_operator | sort | multi_aggregate_operator));

// count by x,y
// avg(foo) by x

fn default_output(func: &AggregateFunction) -> String {
    match *func {
        AggregateFunction::Count { .. } => "_count".to_string(),
        AggregateFunction::Sum { .. } => "_sum".to_string(),
        AggregateFunction::Average { .. } => "_average".to_string(),
        AggregateFunction::CountDistinct { .. } => "_countDistinct".to_string(),
        AggregateFunction::Percentile {
            ref percentile_str, ..
        } => "p".to_string() + percentile_str,
    }
}

named!(complete_agg_function<CompleteStr, (String, AggregateFunction)>, ws!(do_parse!(
        agg_function: aggregate_function >>
        rename_opt: opt!(ws!(preceded!(tag!("as"), ident))) >>
        (
            rename_opt.map(|s|s.to_string()).unwrap_or_else(||default_output(&agg_function)),
            agg_function
        )
    ))
);

named!(multi_aggregate_operator<CompleteStr, Operator>, ws!(do_parse!(
    agg_functions: ws!(separated_nonempty_list!(tag!(","), complete_agg_function)) >>
    key_cols_opt: opt!(preceded!(tag!("by"), sourced_expr_list)) >>
    (Operator::MultiAggregate(MultiAggregateOperator {
        key_col_headers: key_cols_opt.clone()
            .unwrap_or_default()
            .iter().cloned().map(|col|col.0).collect(),
        key_cols: key_cols_opt.clone()
            .unwrap_or_default()
            .iter().cloned().map(|col|col.1).collect(),
        aggregate_functions: agg_functions,
     })))
));

named!(sort_mode<CompleteStr, SortMode>, alt!(
    map!(
        alt!(tag!("asc") | tag!("ascending")),
        |_|SortMode::Ascending
    ) |
    map!(
        alt!(tag!("desc") | tag!("dsc") | tag!("descending")),
        |_|SortMode::Descending
    )
));

named!(sort<CompleteStr, Operator>, ws!(do_parse!(
    tag!("sort") >>
    key_cols_opt: opt!(preceded!(opt!(tag!("by")), var_list)) >>
    dir: opt!(sort_mode) >>
    (Operator::Sort(SortOperator{
        sort_cols: key_cols_opt.unwrap_or_default(),
        direction: dir.unwrap_or(SortMode::Ascending) ,
     })))
));

named!(filter<CompleteStr, Search>, alt!(
    map!(quoted_string, |s|Search::MatchFilter(s.to_string())) |
    map!(tag!("*"), |_s|Search::MatchAll)
));

named!(query<CompleteStr, Query>, ws!(do_parse!(
    filter: dbg!(filter) >>
    operators: dbg!(opt!(preceded!(tag!("|"), ws!(separated_nonempty_list!(tag!("|"), operator))))) >>
    eof!() >>
    (Query{
        search: filter,
        operators: operators.unwrap_or_default()
    })
)));

pub fn parse_query(query_str: &str) -> Result<Query, nom::Err<CompleteStr, u32>> {
    let parse_result = query(CompleteStr(query_str));
    //Err(CompleteStr("ok"))
    parse_result.map(|x| x.1)
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! expect {
        ($f:expr, $inp:expr, $res:expr) => {{
            let parse_result = $f(CompleteStr($inp));
            let actual_result = parse_result.map(|res| res.1);
            assert_eq!(actual_result, $res);
            let parse_result = $f(CompleteStr($inp));
            let _ = parse_result.map(|(leftover, _res)| {
                assert_eq!(leftover, CompleteStr(""));
            });
        }};
    }

    #[test]
    fn parse_quoted_string() {
        expect!(quoted_string, "\"hello\"", Ok("hello"));
        expect!(quoted_string, r#""test = [*=*] * ""#, Ok("test = [*=*] * "))
    }

    #[test]
    fn parse_expr() {
        expect!(
            expr,
            "a == b",
            Ok(Expr::Binary {
                op: BinaryOp::Comparison(ComparisonOp::Eq),
                left: Box::new(Expr::Column("a".to_string())),
                right: Box::new(Expr::Column("b".to_string())),
            })
        );
    }

    #[test]
    fn parse_expr_value() {
        expect!(
            expr,
            "a <= \"b\"",
            Ok(Expr::Binary {
                op: BinaryOp::Comparison(ComparisonOp::Lte),
                left: Box::new(Expr::Column("a".to_string())),
                right: Box::new(Expr::Value(data::Value::Str("b".to_string()))),
            })
        );
    }

    #[test]
    fn parse_expr_ident() {
        expect!(expr, "foo", Ok(Expr::Column("foo".to_string())));
    }

    #[test]
    fn parse_ident() {
        expect!(ident, "hello123", Ok("hello123".to_string()));
        expect!(ident, "x", Ok("x".to_string()));
        expect!(ident, "_x", Ok("_x".to_string()));
        // TODO: improve ergonomics of failure testing
        // expect!(ident,"5x", Ok("_x".to_string()));
    }

    #[test]
    fn parse_var_list() {
        expect!(
            var_list,
            "a, b, def, g_55",
            Ok(vec![
                "a".to_string(),
                "b".to_string(),
                "def".to_string(),
                "g_55".to_string(),
            ])
        );
    }

    #[test]
    fn parse_parses() {
        expect!(
            parse,
            r#"parse "[key=*]" as v"#,
            Ok(InlineOperator::Parse {
                pattern: "[key=*]".to_string(),
                fields: vec!["v".to_string()],
                input_column: None,
            },)
        );
        expect!(
            parse,
            r#"parse "[key=*]" as v"#,
            Ok(InlineOperator::Parse {
                pattern: "[key=*]".to_string(),
                fields: vec!["v".to_string()],
                input_column: None,
            },)
        );
    }

    #[test]
    fn parse_operator() {
        expect!(
            operator,
            "  json",
            Ok(Operator::Inline(InlineOperator::Json {
                input_column: None
            }))
        );
        expect!(
            operator,
            r#" parse "[key=*]" from field as v "#,
            Ok(Operator::Inline(InlineOperator::Parse {
                pattern: "[key=*]".to_string(),
                fields: vec!["v".to_string()],
                input_column: Some(Expr::Column("field".to_string())),
            },))
        );
    }

    #[test]
    fn parse_agg_operator() {
        expect!(
            multi_aggregate_operator,
            "count as renamed by x, y",
            Ok(Operator::MultiAggregate(MultiAggregateOperator {
                key_cols: vec![Expr::Column("x".to_string()), Expr::Column("y".to_string())],
                key_col_headers: vec!["x".to_string(), "y".to_string()],
                aggregate_functions: vec![("renamed".to_string(), AggregateFunction::Count)],
            },))
        );
    }

    #[test]
    fn parse_percentile() {
        expect!(
            complete_agg_function,
            "p50(x)",
            Ok((
                "p50".to_string(),
                AggregateFunction::Percentile {
                    column: Expr::Column("x".to_string()),
                    percentile: 0.5,
                    percentile_str: "50".to_string(),
                }
            ),)
        );
    }

    #[test]
    fn query_no_operators() {
        let query_str = r#" "filter" "#;
        assert_eq!(
            parse_query(query_str),
            Ok(Query {
                search: Search::MatchFilter("filter".to_string()),
                operators: vec![],
            },)
        );
    }

    #[test]
    fn query_operators() {
        let query_str =
            r#"* | json from col | parse "!123*" as foo | count by foo, foo == 123 | sort by foo dsc "#;
        assert_eq!(
            parse_query(query_str),
            Ok(Query {
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
                    Operator::MultiAggregate(MultiAggregateOperator {
                        key_col_headers: vec!["foo".to_string(), "foo == 123".to_string()],
                        key_cols: vec![
                            Expr::Column("foo".to_string()),
                            Expr::Binary {
                                op: BinaryOp::Comparison(ComparisonOp::Eq),
                                left: Box::new(Expr::Column("foo".to_string())),
                                right: Box::new(Expr::Value(data::Value::Int(123))),
                            },
                        ],
                        aggregate_functions: vec![
                            ("_count".to_string(), AggregateFunction::Count {}),
                        ],
                    }),
                    Operator::Sort(SortOperator {
                        sort_cols: vec!["foo".to_string()],
                        direction: SortMode::Descending,
                    }),
                ],
            },)
        );
    }
}
