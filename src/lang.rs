use crate::data;
use annotate_snippets::snippet::{Annotation, AnnotationType, Slice, Snippet, SourceAnnotation};
use nom;
use nom::types::CompleteStr;
use nom::*;
use nom::{digit1, double, is_alphabetic, is_alphanumeric, is_digit, multispace};
use nom_locate::LocatedSpan;
use num_traits::FromPrimitive;
use std::convert::From;
use std::str;

/// Wraps the result of the child parser in a Positioned and sets the start_pos and end_pos
/// accordingly.
macro_rules! with_pos {
  ($i:expr, $submac:ident!( $($args:tt)* )) => ({
      let start_pos: QueryPosition = $i.into();
      match $submac!($i, $($args)*) {
          Ok((i,o)) => Ok((i, Positioned {
              start_pos,
              value: o,
              end_pos: i.into(),
          })),
          Err(e) => Err(e),
      }
  });
  ($i:expr, $f:expr) => (
    with_pos!($i, call!($f));
  );
}

/// Type used to track the current fragment being parsed and its location in the original input.
pub type Span<'a> = LocatedSpan<CompleteStr<'a>>;

/// Container for the position of some syntax in the input string.  This is similar to the Span,
/// but it only contains the offset.
#[derive(Debug, PartialEq, Clone)]
pub struct QueryPosition(pub usize);

impl<'a> From<Span<'a>> for QueryPosition {
    fn from(located_span: Span<'a>) -> Self {
        QueryPosition(located_span.offset)
    }
}

/// Container for values from the query that records the location in the query string.
#[derive(Debug, PartialEq, Clone)]
pub struct Positioned<T> {
    pub start_pos: QueryPosition,
    pub end_pos: QueryPosition,
    pub value: T,
}

impl<T> Positioned<T> {
    pub fn into(&self) -> &T {
        &self.value
    }
}

/// Common syntax errors.
#[derive(PartialEq, Debug, FromPrimitive, Fail)]
pub enum SyntaxErrors {
    #[fail(display = "")]
    DelimiterStart,
    #[fail(display = "unterminated double quote string")]
    UnterminatedString,
    #[fail(display = "expecting close parentheses")]
    MissingParen,
}

impl SyntaxErrors {
    fn to_resolution(&self) -> &'static str {
        match self {
            SyntaxErrors::DelimiterStart => "",
            SyntaxErrors::UnterminatedString => "Insert a double quote to terminate this string",
            SyntaxErrors::MissingParen => "Insert a right parenthesis to close this expression",
        }
    }
}

/// Converts the ordinal from the nom error object back into a SyntaxError.
impl From<u32> for SyntaxErrors {
    fn from(ord: u32) -> Self {
        SyntaxErrors::from_u32(ord).unwrap()
    }
}

impl From<SyntaxErrors> for ErrorKind {
    fn from(e: SyntaxErrors) -> Self {
        ErrorKind::Custom(e as u32)
    }
}

/// Callback for handling error Snippets.
pub trait ErrorReporter {
    fn handle_error(&self, _snippet: Snippet) {}
}

/// Container for the query string that can be used to parse and report errors.
pub struct QueryContainer {
    query: String,
    reporter: Box<ErrorReporter>,
}

impl QueryContainer {
    pub fn new(query: String, reporter: Box<ErrorReporter>) -> QueryContainer {
        QueryContainer { query, reporter }
    }

    /// Create a SnippetBuilder for the given error
    pub fn report_error_for<E: ToString>(&self, error: E) -> SnippetBuilder {
        SnippetBuilder {
            query: self,
            data: SnippetData {
                error: error.to_string(),
                source: self.query.to_string(),
                ..Default::default()
            },
        }
    }

    /// Parse the contained query string.
    pub fn parse(&self) -> Result<Query, QueryPosition> {
        let parse_result = query(Span::new(CompleteStr(&self.query)));

        match parse_result {
            Err(nom::Err::Failure(nom::Context::List(ref list))) => {
                // Check for an error from a delimited!() parser.  The error list will contain
                // the location of the start as the last item and the location of the end as the
                // penultimate item.
                let last_chunk = list.rchunks_exact(2).next().map(|p| (&p[0], &p[1]));

                match last_chunk {
                    Some((
                        (ref end_span, ErrorKind::Custom(ref delim_error)),
                        (ref start_span, ErrorKind::Custom(SyntaxErrors::DelimiterStart)),
                    )) => {
                        self.report_error_for(delim_error)
                            .with_annotation_range((*start_span).into(), (*end_span).into(), "")
                            .with_resolution(delim_error.to_resolution())
                            .send_report();
                    }
                    _ => self.report_error_for(format!("{:?}", list)).send_report(),
                }
            }
            Err(nom::Err::Error(nom::Context::Code(span, _))) => {
                self.report_error_for("Unexpected input")
                    .with_annotation_range(span.into(), QueryPosition(self.query.len()), "")
                    .send_report();
            }
            _ => (),
        }
        // Return the parsed value or the last position of valid syntax
        parse_result.map(|x| x.1).map_err(|e| match e {
            nom::Err::Incomplete(_) => QueryPosition(0),
            nom::Err::Error(context) | nom::Err::Failure(context) => match context {
                nom::Context::Code(span, _) => span.into(),
                nom::Context::List(list) => list.first().unwrap().0.into(),
            },
        })
    }
}

/// Container for data that will be used to construct a Snippet
#[derive(Default)]
pub struct SnippetData {
    error: String,
    source: String,
    annotations: Vec<((usize, usize), String)>,
    resolution: Vec<String>,
}

pub struct SnippetBuilder<'a> {
    query: &'a QueryContainer,
    data: SnippetData,
}

impl<'a> SnippetBuilder<'a> {
    /// Adds an annotation to a portion of the query string.  The given position will be
    /// highlighted with the accompanying label.
    pub fn with_annotation<T, S: ToString>(mut self, pos: &Positioned<T>, label: S) -> Self {
        self.data
            .annotations
            .push(((pos.start_pos.0, pos.end_pos.0), label.to_string()));
        self
    }

    /// Adds an annotation to a portion of the query string.  The given position will be
    /// highlighted with the accompanying label.
    pub fn with_annotation_range<S: ToString>(
        mut self,
        start_pos: QueryPosition,
        end_pos: QueryPosition,
        label: S,
    ) -> Self {
        self.data
            .annotations
            .push(((start_pos.0, end_pos.0), label.to_string()));
        self
    }

    /// Add a message to help the user resolve the error.
    pub fn with_resolution<T: ToString>(mut self, resolution: T) -> Self {
        self.data.resolution.push(resolution.to_string());
        self
    }

    /// Build and send the Snippet to the ErrorReporter in the QueryContainer.
    pub fn send_report(mut self) {
        self.query.reporter.handle_error(Snippet {
            title: Some(Annotation {
                label: Some(self.data.error),
                id: None,
                annotation_type: AnnotationType::Error,
            }),
            slices: vec![Slice {
                source: self.data.source,
                line_start: 1,
                origin: None,
                fold: false,
                annotations: self
                    .data
                    .annotations
                    .drain(..)
                    .map(move |anno| SourceAnnotation {
                        range: anno.0,
                        label: anno.1,
                        annotation_type: AnnotationType::Error,
                    })
                    .collect(),
            }],
            footer: self
                .data
                .resolution
                .iter()
                .map(|res| Annotation {
                    label: Some(res.to_string()),
                    id: None,
                    annotation_type: AnnotationType::Help,
                })
                .collect(),
        });
    }
}

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

/// The KeywordType determines how a keyword string should be interpreted.
#[derive(Debug, PartialEq, Eq, Clone)]
enum KeywordType {
    /// The keyword string should exactly match the input.
    EXACT,
    /// The keyword string can contain wildcards.
    WILDCARD,
}

/// Represents a `keyword` search string.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Keyword(String, KeywordType);

impl Keyword {
    /// Create a Keyword that will exactly match an input string.
    pub fn new_exact(str: String) -> Keyword {
        Keyword(str, KeywordType::EXACT)
    }

    /// Create a Keyword that can contain wildcards
    pub fn new_wildcard(str: String) -> Keyword {
        Keyword(str, KeywordType::WILDCARD)
    }

    /// Test if this is an empty keyword string
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Convert this keyword to a `regex::Regex` object.
    pub fn to_regex(&self) -> regex::Regex {
        let mut regex_str = regex::escape(&self.0.replace("\\\"", "\""));

        regex_str.insert_str(0, "(?i)");
        if self.1 == KeywordType::WILDCARD {
            regex_str = regex_str.replace("\\*", "(.*?)");
            // If it ends with a star, we need to ensure we read until the end.
            if self.0.ends_with('*') {
                regex_str.push('$');
            }
        }

        regex::Regex::new(&regex_str).unwrap()
    }
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Inline(InlineOperator),
    MultiAggregate(MultiAggregateOperator),
    Sort(SortOperator),
}

#[derive(Debug, PartialEq, Clone)]
pub enum InlineOperator {
    Json {
        input_column: Option<String>,
    },
    Parse {
        pattern: Keyword,
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
    Limit {
        /// The count for the limit is pretty loosely typed at this point, the next phase will
        /// check the value to see if it's sane or provide a default if no number was given.
        count: Option<Positioned<f64>>,
    },
    Total {
        input_column: Expr,
        output_column: String,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
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
        column: Option<Positioned<Vec<Expr>>>,
    },
}

#[derive(Debug, PartialEq)]
pub struct MultiAggregateOperator {
    pub key_cols: Vec<Expr>,
    pub key_col_headers: Vec<String>,
    pub aggregate_functions: Vec<(String, Positioned<AggregateFunction>)>,
}

#[derive(Debug, PartialEq)]
pub struct SortOperator {
    pub sort_cols: Vec<String>,
    pub direction: SortMode,
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub search: Vec<Keyword>,
    pub operators: Vec<Operator>,
}

fn is_ident(c: char) -> bool {
    is_alphanumeric(c as u8) || c == '_'
}

fn starts_ident(c: char) -> bool {
    is_alphabetic(c as u8) || c == '_'
}

/// Tests if the input character can be part of a search keyword.
///
/// Based on the SumoLogic keyword syntax:
///
/// https://help.sumologic.com/05Search/Get-Started-with-Search/How-to-Build-a-Search/Keyword-Search-Expressions
fn is_keyword(c: char) -> bool {
    match c {
        '-' | '_' | ':' | '/' | '.' | '+' | '@' | '#' | '$' | '%' | '^' | '*' => true,
        alpha if is_alphanumeric(alpha as u8) => true,
        _ => false,
    }
}

fn not_escape(c: char) -> bool {
    c != '\\' && c != '\"'
}

named!(value<Span, data::Value>, ws!(
    alt!(
        map!(quoted_string, |s|data::Value::Str(s.to_string()))
        | map!(digit1, |s|data::Value::from_string(s.fragment.0))
    )
));
named!(ident<Span, String>, do_parse!(
    start: take_while1!(starts_ident) >>
    rest: take_while!(is_ident) >>
    (start.fragment.0.to_owned() + rest.fragment.0)
));

named!(e_ident<Span, Expr>,
    ws!(alt!(
      map!(ident, |col|Expr::Column(col.to_owned()))
    | map!(value, Expr::Value)
      //expr
    | ws!(add_return_error!(SyntaxErrors::DelimiterStart.into(), delimited!(
          tag!("("),
          expr,
          return_error!(SyntaxErrors::MissingParen.into(), tag!(")")))))
)));

named!(keyword<Span, String>, do_parse!(
    start: take_while1!(is_keyword) >>
    rest: take_while!(is_keyword) >>
    (start.fragment.0.to_owned() + rest.fragment.0)
));

named!(comp_op<Span, ComparisonOp>, ws!(alt!(
    map!(tag!("=="), |_|ComparisonOp::Eq)
    | map!(tag!("<="), |_|ComparisonOp::Lte)
    | map!(tag!(">="), |_|ComparisonOp::Gte)
    | map!(tag!("!="), |_|ComparisonOp::Neq)
    | map!(tag!(">"), |_|ComparisonOp::Gt)
    | map!(tag!("<"), |_|ComparisonOp::Lt)
)));

named!(expr<Span, Expr>, ws!(alt!(
    do_parse!(
        l: e_ident >>
        comp: comp_op >>
        r: e_ident >>
        ( Expr::Binary { op: BinaryOp::Comparison(comp), left: Box::new(l), right: Box::new(r)} )
    )
    | e_ident
)));

named!(json<Span, InlineOperator>, ws!(do_parse!(
    tag!("json") >>
    from_column_opt: opt!(ws!(preceded!(tag!("from"), ident))) >>
    (InlineOperator::Json { input_column: from_column_opt.map(|s|s.to_string()) })
)));

named!(whre<Span, InlineOperator>, ws!(do_parse!(
    tag!("where") >>
    ex: expr >>
    (InlineOperator::Where { expr: ex })
)));

named!(limit<Span, InlineOperator>, ws!(do_parse!(
    tag!("limit") >>
    count: opt!(with_pos!(double)) >>
    (InlineOperator::Limit{
        count
    })
)));

named!(total<Span, InlineOperator>, ws!(do_parse!(
    tag!("total") >>
    input_column: delimited!(tag!("("), expr, tag!(")")) >>
    rename_opt: opt!(ws!(preceded!(tag!("as"), ident))) >>
    (InlineOperator::Total{
        input_column,
        output_column:
            rename_opt.map(|s|s.to_string()).unwrap_or_else(||"_total".to_string()),
}))));

named!(quoted_string <Span, &str>,  add_return_error!(
    SyntaxErrors::DelimiterStart.into(), delimited!(
        tag!("\""),
        map!(escaped!(take_while1!(not_escape), '\\', anychar), |ref s|s.fragment.0),
        return_error!(SyntaxErrors::UnterminatedString.into(), tag!("\""))
)));

named!(var_list<Span, Vec<String> >, ws!(separated_nonempty_list!(
    tag!(","), ws!(ident)
)));

named!(sourced_expr_list<Span, Vec<(String, Expr)> >, ws!(separated_nonempty_list!(
    tag!(","), ws!(sourced_expr)
)));

named!(sourced_expr<Span, (String, Expr)>, ws!(
    do_parse!(
        ex: recognize!(expr) >>
        (
            (ex.fragment.0.trim().to_string(), expr(ex).unwrap().1)
        )
)));

// parse "blah * ... *" [from other_field] as x, y
named!(parse<Span, InlineOperator>, ws!(do_parse!(
    tag!("parse") >>
    pattern: quoted_string >>
    from_column_opt: opt!(ws!(preceded!(tag!("from"), expr))) >>
    tag!("as") >>
    vars: var_list >>
    ( InlineOperator::Parse{
        pattern: Keyword::new_wildcard(pattern.to_string()),
        fields: vars,
        input_column: from_column_opt
        } )
)));

named!(fields_mode<Span, FieldMode>, alt!(
    map!(
        alt!(tag!("+") | tag!("only") | tag!("include")),
        |_|FieldMode::Only
    ) |
    map!(
        alt!(tag!("-") | tag!("except") | tag!("drop")),
        |_|FieldMode::Except
    )
));

named!(fields<Span, InlineOperator>, ws!(do_parse!(
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

named!(arg_list<Span, Positioned<Vec<Expr>>>, add_return_error!(
    SyntaxErrors::DelimiterStart.into(), with_pos!(delimited!(
        tag!("("),
        ws!(separated_list!(tag!(","), ws!(expr))),
        return_error!(SyntaxErrors::MissingParen.into(), tag!(")"))))
));

named!(count<Span, Positioned<AggregateFunction>>, with_pos!(map!(tag!("count"),
    |_s|AggregateFunction::Count{}))
);

named!(average<Span, Positioned<AggregateFunction>>, ws!(with_pos!(do_parse!(
    alt!(tag!("avg") | tag!("average")) >>
    column: delimited!(tag!("("), expr ,tag!(")")) >>
    (AggregateFunction::Average{column})
))));

named!(count_distinct<Span, Positioned<AggregateFunction>>, ws!(with_pos!(do_parse!(
    tag!("count_distinct") >>
    column: opt!(arg_list) >>
    (AggregateFunction::CountDistinct{ column })
))));

named!(sum<Span, Positioned<AggregateFunction>>, ws!(with_pos!(do_parse!(
    tag!("sum") >>
    column: delimited!(tag!("("), expr,tag!(")")) >>
    (AggregateFunction::Sum{column})
))));

fn is_digit_char(digit: char) -> bool {
    is_digit(digit as u8)
}

named!(p_nn<Span, Positioned<AggregateFunction>>, ws!(
    with_pos!(do_parse!(
        alt!(tag!("pct") | tag!("percentile") | tag!("p")) >>
        pct: take_while_m_n!(2, 2, is_digit_char) >>
        column: delimited!(tag!("("), expr,tag!(")")) >>
        (AggregateFunction::Percentile{
            column,
            percentile: (".".to_owned() + pct.fragment.0).parse::<f64>().unwrap(),
            percentile_str: pct.fragment.0.to_string()
        })
    ))
));

named!(inline_operator<Span, Operator>,
   map!(alt!(parse | json | fields | whre | limit | total), Operator::Inline)
);
named!(aggregate_function<Span, Positioned<AggregateFunction>>, alt!(
    count_distinct |
    count |
    average |
    sum |
    p_nn));

named!(operator<Span, Operator>, alt!(inline_operator | sort | multi_aggregate_operator));

// count by x,y
// avg(foo) by x

fn default_output(func: &Positioned<AggregateFunction>) -> String {
    match func.into() {
        AggregateFunction::Count { .. } => "_count".to_string(),
        AggregateFunction::Sum { .. } => "_sum".to_string(),
        AggregateFunction::Average { .. } => "_average".to_string(),
        AggregateFunction::CountDistinct { .. } => "_countDistinct".to_string(),
        AggregateFunction::Percentile {
            ref percentile_str, ..
        } => "p".to_string() + percentile_str,
    }
}

named!(complete_agg_function<Span, (String, Positioned<AggregateFunction>)>, ws!(do_parse!(
        agg_function: aggregate_function >>
        rename_opt: opt!(ws!(preceded!(tag!("as"), ident))) >>
        (
            rename_opt.map(|s|s.to_string()).unwrap_or_else(||default_output(&agg_function)),
            agg_function
        )
    ))
);

named!(multi_aggregate_operator<Span, Operator>, ws!(do_parse!(
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

named!(sort_mode<Span, SortMode>, alt!(
    map!(
        alt!(tag!("asc") | tag!("ascending")),
        |_|SortMode::Ascending
    ) |
    map!(
        alt!(tag!("desc") | tag!("dsc") | tag!("descending")),
        |_|SortMode::Descending
    )
));

named!(sort<Span, Operator>, ws!(do_parse!(
    tag!("sort") >>
    key_cols_opt: opt!(preceded!(opt!(tag!("by")), var_list)) >>
    dir: opt!(sort_mode) >>
    (Operator::Sort(SortOperator{
        sort_cols: key_cols_opt.unwrap_or_default(),
        direction: dir.unwrap_or(SortMode::Ascending) ,
     })))
));

named!(filter_cond<Span, Keyword>, alt!(
    map!(quoted_string, |s| Keyword::new_exact(s.to_string())) |
    map!(keyword, |s| Keyword::new_wildcard(s.trim_matches('*').to_string()))
));

named!(filter<Span, Vec<Keyword>>, map!(
    separated_nonempty_list!(multispace, filter_cond),
    // An empty keyword would match everything, so there's no reason to
    |mut v| {
        v.retain(|k| !k.is_empty());
        v
    }
));

named!(query<Span, Query, SyntaxErrors>, fix_error!(SyntaxErrors, exact!(ws!(do_parse!(
    filter: filter >>
    operators: opt!(preceded!(tag!("|"), ws!(separated_nonempty_list!(tag!("|"), operator)))) >>
    (Query{
        search: filter,
        operators: operators.unwrap_or_default()
    }))
))));

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! expect {
        ($f:expr, $inp:expr, $res:expr) => {{
            let parse_result = $f(Span::new(CompleteStr($inp)));
            match parse_result {
                Ok((
                    LocatedSpan {
                        fragment: leftover, ..
                    },
                    actual_result,
                )) => {
                    assert_eq!(actual_result, $res);
                    assert_eq!(leftover, CompleteStr(""));
                }
                Err(e) => panic!(format!("{:?}", e)),
            }
        }};
    }

    #[test]
    fn parse_keyword_string() {
        expect!(keyword, "abc", "abc".to_string());
        expect!(keyword, "one-two-three", "one-two-three".to_string());
    }

    #[test]
    fn parse_quoted_string() {
        expect!(quoted_string, "\"hello\"", "hello");
        expect!(quoted_string, r#""test = [*=*] * ""#, "test = [*=*] * ");
    }

    #[test]
    fn parse_expr() {
        expect!(
            expr,
            "a == b",
            Expr::Binary {
                op: BinaryOp::Comparison(ComparisonOp::Eq),
                left: Box::new(Expr::Column("a".to_string())),
                right: Box::new(Expr::Column("b".to_string())),
            }
        );
    }

    #[test]
    fn parse_expr_value() {
        expect!(
            expr,
            "a <= \"b\"",
            Expr::Binary {
                op: BinaryOp::Comparison(ComparisonOp::Lte),
                left: Box::new(Expr::Column("a".to_string())),
                right: Box::new(Expr::Value(data::Value::Str("b".to_string()))),
            }
        );
    }

    #[test]
    fn parse_expr_ident() {
        expect!(expr, "foo", Expr::Column("foo".to_string()));
    }

    #[test]
    fn parse_ident() {
        expect!(ident, "hello123", "hello123".to_string());
        expect!(ident, "x", "x".to_string());
        expect!(ident, "_x", "_x".to_string());
        // TODO: improve ergonomics of failure testing
        // expect!(ident,"5x", Ok("_x".to_string()));
    }

    #[test]
    fn parse_var_list() {
        expect!(
            var_list,
            "a, b, def, g_55",
            vec![
                "a".to_string(),
                "b".to_string(),
                "def".to_string(),
                "g_55".to_string(),
            ]
        );
    }

    #[test]
    fn parse_parses() {
        expect!(
            parse,
            r#"parse "[key=*]" as v"#,
            InlineOperator::Parse {
                pattern: Keyword::new_wildcard("[key=*]".to_string()),
                fields: vec!["v".to_string()],
                input_column: None,
            }
        );
        expect!(
            parse,
            r#"parse "[key=*]" as v"#,
            InlineOperator::Parse {
                pattern: Keyword::new_wildcard("[key=*]".to_string()),
                fields: vec!["v".to_string()],
                input_column: None,
            }
        );
    }

    #[test]
    fn parse_operator() {
        expect!(
            operator,
            "  json",
            Operator::Inline(InlineOperator::Json { input_column: None })
        );
        expect!(
            operator,
            r#" parse "[key=*]" from field as v "#,
            Operator::Inline(InlineOperator::Parse {
                pattern: Keyword::new_wildcard("[key=*]".to_string()),
                fields: vec!["v".to_string()],
                input_column: Some(Expr::Column("field".to_string())),
            })
        );
    }

    #[test]
    fn parse_limit() {
        expect!(
            operator,
            " limit",
            Operator::Inline(InlineOperator::Limit { count: None })
        );
        expect!(
            operator,
            " limit 5",
            Operator::Inline(InlineOperator::Limit {
                count: Some(Positioned {
                    value: 5.0,
                    start_pos: QueryPosition(7),
                    end_pos: QueryPosition(8)
                })
            })
        );
        expect!(
            operator,
            " limit -5",
            Operator::Inline(InlineOperator::Limit {
                count: Some(Positioned {
                    value: -5.0,
                    start_pos: QueryPosition(7),
                    end_pos: QueryPosition(9)
                }),
            })
        );
        expect!(
            operator,
            " limit 1e2",
            Operator::Inline(InlineOperator::Limit {
                count: Some(Positioned {
                    value: 1e2,
                    start_pos: QueryPosition(7),
                    end_pos: QueryPosition(10)
                })
            })
        );
    }

    #[test]
    fn parse_agg_operator() {
        expect!(
            multi_aggregate_operator,
            "count as renamed by x, y",
            Operator::MultiAggregate(MultiAggregateOperator {
                key_cols: vec![Expr::Column("x".to_string()), Expr::Column("y".to_string())],
                key_col_headers: vec!["x".to_string(), "y".to_string()],
                aggregate_functions: vec![(
                    "renamed".to_string(),
                    Positioned {
                        value: AggregateFunction::Count,
                        start_pos: QueryPosition(0),
                        end_pos: QueryPosition(5)
                    }
                )],
            })
        );
    }

    #[test]
    fn parse_percentile() {
        expect!(
            complete_agg_function,
            "p50(x)",
            (
                "p50".to_string(),
                Positioned {
                    value: AggregateFunction::Percentile {
                        column: Expr::Column("x".to_string()),
                        percentile: 0.5,
                        percentile_str: "50".to_string(),
                    },
                    start_pos: QueryPosition(0),
                    end_pos: QueryPosition(6),
                }
            )
        );
    }

    #[test]
    fn query_no_operators() {
        expect!(
            query,
            " * ",
            Query {
                search: vec![],
                operators: vec![],
            }
        );
        expect!(
            query,
            " filter ",
            Query {
                search: vec![Keyword::new_wildcard("filter".to_string())],
                operators: vec![],
            }
        );
        expect!(
            query,
            " *abc* ",
            Query {
                search: vec![Keyword::new_wildcard("abc".to_string())],
                operators: vec![],
            }
        );
        expect!(
            query,
            " abc def \"*ghi*\" ",
            Query {
                search: vec![
                    Keyword::new_wildcard("abc".to_string()),
                    Keyword::new_wildcard("def".to_string()),
                    Keyword::new_exact("*ghi*".to_string()),
                ],
                operators: vec![],
            }
        );
    }

    #[test]
    fn query_operators() {
        let query_str =
            r#"* | json from col | parse "!123*" as foo | count by foo, foo == 123 | sort by foo dsc "#;
        expect!(
            query,
            query_str,
            Query {
                search: vec![],
                operators: vec![
                    Operator::Inline(InlineOperator::Json {
                        input_column: Some("col".to_string()),
                    }),
                    Operator::Inline(InlineOperator::Parse {
                        pattern: Keyword::new_wildcard("!123*".to_string()),
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
                        aggregate_functions: vec![(
                            "_count".to_string(),
                            Positioned {
                                value: AggregateFunction::Count {},
                                start_pos: QueryPosition(43),
                                end_pos: QueryPosition(48),
                            }
                        ),],
                    }),
                    Operator::Sort(SortOperator {
                        sort_cols: vec!["foo".to_string()],
                        direction: SortMode::Descending,
                    }),
                ],
            }
        );
    }
}
