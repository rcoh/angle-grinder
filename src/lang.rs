use std::fmt::Debug;
use std::ops::Range;
use std::str;

use itertools::Itertools;
use lazy_static::lazy_static;
use nom::bytes::complete::escaped;
use nom::combinator::not;
use nom::multi::{fold_many0, fold_many1};
use nom::sequence::{delimited, separated_pair};
use nom::{
    branch::alt,
    bytes::complete::{take, take_while, take_while1},
    character::complete::{anychar, digit1, multispace0, multispace1, none_of, satisfy},
    character::{is_alphabetic, is_alphanumeric},
    combinator::{eof, map, map_res, opt, peek, recognize},
    error::ParseError,
    multi::{many0, many_till, separated_list0, separated_list1},
    number::complete::double,
    sequence::{pair, tuple},
    IResult, InputIter, Parser, Slice,
};
use nom_locate::position;
use nom_locate::LocatedSpan;
use nom_supreme::error::ErrorTree;
use nom_supreme::parser_ext::ParserExt;
use nom_supreme::tag::complete::tag;

use crate::alias::{self, AliasCollection};
use crate::data;
use crate::errors::{ErrorBuilder, QueryContainer};
use crate::pipeline::CompileError;

pub const VALID_AGGREGATES: &[&str] = &[
    "count",
    "min",
    "average",
    "avg",
    "max",
    "sum",
    "count_distinct",
    "sort",
];

pub const VALID_INLINE: &[&str] = &[
    "parse",
    "limit",
    "json",
    "logfmt",
    "total",
    "fields",
    "where",
    "split",
    "timeslice",
];

lazy_static! {
    pub static ref VALID_OPERATORS: Vec<&'static str> = {
        [
            VALID_INLINE,
            VALID_AGGREGATES,
            alias::LOADED_KEYWORDS.as_slice(),
        ]
        .concat()
    };
}

pub const RESERVED_FILTER_WORDS: &[&str] = &["AND", "OR", "NOT"];

/// Type used to track the current fragment being parsed and its location in the original input.
pub type Span<'a> = LocatedSpan<&'a str, &'a QueryContainer<'a>>;

pub type LResult<I, O, E = ErrorTree<I>> = Result<(I, O), nom::Err<E>>;

pub type QueryRange = Range<usize>;

/// Container for values from the query that records the location in the query string.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Positioned<T> {
    pub range: QueryRange,
    pub value: T,
}

impl<T> Positioned<T> {
    pub fn into(&self) -> &T {
        &self.value
    }
}

/// Methods for converting a Span to different ranges over the span
trait ToRange {
    /// Return the entire Span as a Range
    fn to_range(&self) -> Range<usize>;

    /// Return a range from the current offset in the Span to the "sync point".  For the
    /// angle-grinder syntax, the sync points are the vertical bar, end-of-query, and closing
    /// braces/parens/etc...
    fn to_sync_point(&self) -> Range<usize>;

    /// Return a range from the current offset in the Span to the next whitespace
    fn to_whitespace(&self) -> Range<usize>;
}

impl ToRange for Span<'_> {
    fn to_range(&self) -> Range<usize> {
        let start = self.location_offset();
        let end = start + self.fragment().len();
        start..end
    }

    fn to_sync_point(&self) -> Range<usize> {
        let s: &str = self.fragment();
        let sync = s
            .chars()
            .find_position(|ch| matches!(ch, '|' | ')' | ']' | '}'));

        let end = sync.map(|pair| pair.0).unwrap_or(s.len());

        Range {
            start: self.location_offset(),
            end: self.location_offset() + end,
        }
    }

    fn to_whitespace(&self) -> Range<usize> {
        let s: &str = self.fragment();
        let sync = s
            .chars()
            .find_position(|ch| matches!(ch, ' ' | '\t' | '\n'));

        let end = sync.map(|pair| pair.0).unwrap_or(s.len());

        Range {
            start: self.location_offset(),
            end: self.location_offset() + end,
        }
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
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BinaryOp {
    Comparison(ComparisonOp),
    Arithmetic(ArithmeticOp),
    Logical(LogicalOp),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum UnaryOp {
    Not,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DataAccessAtom {
    Key(String),
    Index(i64),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Expr {
    Column {
        head: DataAccessAtom,
        rest: Vec<DataAccessAtom>,
    },
    Unary {
        op: UnaryOp,
        operand: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    IfOp {
        cond: Box<Expr>,
        value_if_true: Box<Expr>,
        value_if_false: Box<Expr>,
    },
    Value(data::Value),
    Error,
}

impl Expr {
    pub fn column(key: &str) -> Expr {
        Expr::Column {
            head: DataAccessAtom::Key(key.to_owned()),
            rest: vec![],
        }
    }
}

/// Debug helper
pub fn dbg_dmp<'a, F, O, E>(
    mut f: F,
    context: &'static str,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, O, E>
where
    F: FnMut(Span<'a>) -> IResult<Span<'a>, O, E>,
{
    move |i: Span<'a>| match f(i) {
        Err(e) => {
            println!("{}: Error at:\n{}", context, i);
            Err(e)
        }
        Ok((s, a)) => {
            println!("{}: Ok at:\n{}\nnext:\n{}", context, i, s.fragment());
            Ok((s, a))
        }
    }
}

/// Combinator that expects the given parser to succeed.  If the parser returns an error:
/// - the given error message is logged
/// - the input is consumed up to the sync point
/// - None is returned
fn expect<'a, F, E, T>(
    mut parser: F,
    error_msg: E,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, Option<T>>
where
    F: FnMut(Span<'a>) -> IResult<Span<'a>, T>,
    E: ToString,
{
    move |input: Span<'a>| match parser(input) {
        Ok((remaining, out)) => Ok((remaining, Some(out))),
        Err(nom::Err::Error(nom::error::Error { input, .. }))
        | Err(nom::Err::Failure(nom::error::Error { input, .. })) => {
            let r = input.to_sync_point();
            let end = r.end - input.location_offset();
            input
                .extra
                .report_error_for(error_msg.to_string())
                .with_code_range(r, "")
                .send_report();
            Ok((input.slice(end..), None))
        }
        Err(err) => Err(err),
    }
}

/// Combinator that expects the given parser to succeed.  If the parser returns an error:
/// - the given error function is called with the range
/// - the input is consumed up to the sync point
/// - None is returned
fn expect_fn<'a, F, O, EF>(
    mut parser: F,
    mut error_fn: EF,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, Option<O>>
where
    F: Parser<Span<'a>, O, nom::error::Error<Span<'a>>>,
    EF: FnMut(&QueryContainer, QueryRange),
{
    move |input: Span<'a>| match parser.parse(input) {
        Ok((remaining, out)) => Ok((remaining, Some(out))),
        Err(nom::Err::Error(nom::error::Error { input, .. }))
        | Err(nom::Err::Failure(nom::error::Error { input, .. })) => {
            let r = input.to_sync_point();
            let end = r.end - input.location_offset();
            error_fn(input.extra, r);
            let next = input.slice(end..);
            Ok((next, None))
        }
        Err(err) => Err(err),
    }
}

/// A version of the `delimited()` combinator() that calls an error-handling function when the
/// terminator parser fails.
pub fn expect_delimited<'a, O1, O2, O3, F, G, H, EF>(
    mut first: F,
    mut second: G,
    mut third: H,
    mut error_fn: EF,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, O2, nom::error::Error<Span<'a>>>
where
    F: Parser<Span<'a>, O1, nom::error::Error<Span<'a>>>,
    G: Parser<Span<'a>, O2, nom::error::Error<Span<'a>>>,
    H: Parser<Span<'a>, O3, nom::error::Error<Span<'a>>>,
    EF: FnMut(&QueryContainer, QueryRange),
{
    move |input: Span<'a>| {
        let full_r = input.to_sync_point();
        let (input, _) = first.parse(input)?;
        let (input, o2) = second.parse(input)?;

        match third.parse(input) {
            Ok((input, _)) => Ok((input, o2)),
            Err(_) => {
                let start = input.location_offset();
                let mut remaining = input;

                loop {
                    if remaining.is_empty() {
                        error_fn(
                            remaining.extra,
                            Range {
                                start: full_r.start,
                                end: remaining.location_offset(),
                            },
                        );
                        return Ok((remaining, o2));
                    }

                    remaining = remaining.slice(1..);
                    let end = remaining.location_offset();
                    let res = third.parse(remaining);

                    if let Ok((remaining, _)) = res {
                        remaining
                            .extra
                            .report_error_for("unhandled input")
                            .with_code_range(Range { start, end }, "")
                            .send_report();
                        return Ok((remaining, o2));
                    }
                }
            }
        }
    }
}

/// Combinator that expects some optional whitespace followed by the vertical bar or EOF.  If that
/// input is not found, the error message is logged.
fn expect_pipe<'a, M>(
    error_msg: M,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, Option<(Span<'a>, Span<'a>)>>
where
    M: ToString,
{
    expect(peek(multispace0.and(tag("|").or(eof))), error_msg)
}

/// The KeywordType determines how a keyword string should be interpreted.
#[derive(Debug, PartialEq, Eq, Clone)]
enum KeywordType {
    /// The keyword string should exactly match the input.
    Exact,
    /// The keyword string can contain wildcards.
    Wildcard,
    Regex,
}

/// Represents a `keyword` search string.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Keyword(String, KeywordType);

impl Keyword {
    /// Create a Keyword that will exactly match an input string.
    pub fn new_exact(str: String) -> Keyword {
        Keyword(str, KeywordType::Exact)
    }

    /// Create a Keyword that can contain wildcards
    pub fn new_wildcard(str: String) -> Keyword {
        Keyword(str, KeywordType::Wildcard)
    }

    /// Create a Keyword that can contain wildcards
    pub fn new_regex(str: String) -> Keyword {
        Keyword(str, KeywordType::Regex)
    }

    /// Test if this is an empty keyword string
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Convert this keyword to a `regex::Regex` object.
    pub fn to_regex(&self) -> regex::Regex {
        if self.1 == KeywordType::Regex {
            return regex::Regex::new(&self.0).unwrap();
        }

        let mut regex_str = regex::escape(&self.0.replace("\\\"", "\"")).replace(' ', "\\s");

        regex_str.insert_str(0, "(?i)");
        if self.1 == KeywordType::Wildcard {
            regex_str = regex_str.replace("\\*", "(.*?)");
            // If it ends with a star, we need to ensure we read until the end.
            if self.0.ends_with('*') {
                regex_str.push('$');
            }
        }

        regex::Regex::new(&regex_str).unwrap()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Search {
    And(Vec<Search>),
    Or(Vec<Search>),
    Not(Box<Search>),
    Keyword(Keyword),
}

impl Search {
    pub fn from_quoted_input(s: String) -> Option<Self> {
        if s.is_empty() {
            None
        } else {
            Some(Search::Keyword(Keyword::new_exact(s)))
        }
    }

    pub fn from_keyword_input(s: &str) -> Option<Self> {
        let trimmed = s.trim_matches('*');

        if trimmed.is_empty() {
            None
        } else {
            Some(Search::Keyword(Keyword::new_wildcard(trimmed.to_string())))
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    RenderedAlias(Vec<Operator>),
    Inline(Positioned<InlineOperator>),
    MultiAggregate(MultiAggregateOperator),
    Sort(SortOperator),
    Error,
}

#[derive(Debug, PartialEq, Clone)]
pub enum InlineOperator {
    Json {
        input_column: Option<Expr>,
    },
    Logfmt {
        input_column: Option<Expr>,
    },
    Parse {
        pattern: Keyword,
        fields: Vec<String>,
        input_column: (Option<Positioned<Expr>>, Option<Positioned<Expr>>),
        no_drop: bool,
        no_convert: bool,
    },
    Fields {
        mode: FieldMode,
        fields: Vec<String>,
    },
    Where {
        expr: Option<Positioned<Expr>>,
    },
    Limit {
        /// The count for the limit is pretty loosely typed at this point, the next phase will
        /// check the value to see if it's sane or provide a default if no number was given.
        count: Option<Positioned<f64>>,
    },
    Split {
        separator: String,
        input_column: Option<Expr>,
        output_column: Option<Expr>,
    },
    Timeslice {
        input_column: Expr,
        duration: Option<chrono::Duration>,
        output_column: Option<String>,
    },
    Total {
        input_column: Expr,
        output_column: String,
    },
    FieldExpression {
        value: Expr,
        name: String,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FieldMode {
    Only,
    Except,
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum SortMode {
    Ascending,
    Descending,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateFunction {
    Count {
        condition: Option<Expr>,
    },
    Sum {
        column: Expr,
    },
    Min {
        column: Expr,
    },
    Average {
        column: Expr,
    },
    Max {
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
    Error,
}

impl AggregateFunction {
    fn default_name(&self) -> String {
        match self {
            AggregateFunction::Count { .. } => "_count".to_string(),
            AggregateFunction::Sum { .. } => "_sum".to_string(),
            AggregateFunction::Min { .. } => "_min".to_string(),
            AggregateFunction::Average { .. } => "_average".to_string(),
            AggregateFunction::Max { .. } => "_max".to_string(),
            AggregateFunction::Percentile {
                ref percentile_str, ..
            } => format!("p{}", percentile_str),
            AggregateFunction::CountDistinct { .. } => "_countDistinct".to_string(),
            AggregateFunction::Error => "_err".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct MultiAggregateOperator {
    pub key_cols: Vec<Expr>,
    pub key_col_headers: Vec<String>,
    pub aggregate_functions: Vec<(String, Positioned<AggregateFunction>)>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SortOperator {
    pub sort_cols: Vec<Expr>,
    pub direction: SortMode,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Query {
    pub search: Search,
    pub operators: Vec<Operator>,
}

/// Parses the +/- binary operators
fn addsub_op(input: Span) -> IResult<Span, ArithmeticOp> {
    alt((
        tag("+").map(|_| ArithmeticOp::Add),
        tag("-").map(|_| ArithmeticOp::Subtract),
    ))(input)
}

/// Parses an argument list for a function
fn arg_list(input: Span) -> IResult<Span, Vec<Expr>> {
    expect_delimited(
        tag("(").and(multispace0),
        separated_list0(tag(","), delimited(multispace0, opt_expr, multispace0)),
        tag(")"),
        |qc, r| {
            qc.report_error_for("unterminated function call")
                .with_code_range(r, "unterminated function call")
                .with_resolution("Insert a right parenthesis to terminate this call")
                .send_report()
        },
    )
    .parse(input)
}

/// Parses a, potentially optional, argument list for a function that has a single parameter
fn single_arg(description: &'static str) -> impl Clone + Fn(Span) -> IResult<Span, Expr> {
    move |input: Span| {
        expect_delimited(
            tag("(").and(multispace0),
            expect_fn(opt_expr, |qc, r| {
                qc.report_error_for("this operator takes 1 argument, but 0 were supplied")
                    .with_code_range(r, "- supplied 0 arguments")
                    .with_resolution(format!("the argument should supply {}", description))
                    .send_report();
            })
            .map(|e| e.unwrap_or(Expr::Error)),
            tag(")"),
            |qc, r| {
                qc.report_error_for("unterminated function call")
                    .with_code_range(r, "unterminated function call")
                    .with_resolution("Insert a right parenthesis to terminate this call")
                    .send_report()
            },
        )
        .parse(input)
    }
}

/// A version of the single_arg parser that logs an error if no argument list was provided
fn req_single_arg(description: &'static str) -> impl Clone + Fn(Span) -> IResult<Span, Expr> {
    move |input: Span| {
        expect_fn(single_arg(description), |qc, r| {
            qc.report_error_for(format!(
                "expecting a parenthesized argument that supplies {}",
                description
            ))
            .with_code_range(r, "")
            .send_report()
        })
        .map(|e| e.unwrap_or(Expr::Error))
        .parse(input)
    }
}

/// Combinator that checks for an optional keyword that should be followed by an expression
fn kw_expr(
    keyword: &'static str,
    description: &'static str,
) -> impl Fn(Span) -> IResult<Span, Option<Expr>> {
    move |input: Span| {
        let res = opt(tag(keyword).preceded_by(multispace1)).parse(input)?;

        match res {
            (input, None) => Ok((input, None)),
            (input, Some(keyword_span)) => {
                expect_fn(opt_expr.preceded_by(multispace1), |qc, _r| {
                    qc.report_error_for(format!(
                        "expecting an expression that supplies {}",
                        description
                    ))
                    .with_code_range(
                        keyword_span.to_range(),
                        "should be followed by an expression",
                    )
                    .send_report();
                })
                .map(|e| e.map(Some).unwrap_or(Some(Expr::Error)))
                .parse(input)
            }
        }
    }
}

fn fcall(input: Span) -> IResult<Span, Expr> {
    ident
        .and(arg_list)
        .map(|(name, args)| Expr::FunctionCall { name, args })
        .parse(input)
}

fn if_op(input: Span) -> IResult<Span, Expr> {
    tag("if")
        .precedes(with_pos(arg_list))
        .map(|Positioned { range, value: args }| match args.as_slice() {
            [cond, value_if_true, value_if_false] => Expr::IfOp {
                cond: Box::new(cond.clone()),
                value_if_true: Box::new(value_if_true.clone()),
                value_if_false: Box::new(value_if_false.clone()),
            },
            _ => {
                input
                    .extra
                    .report_error_for(
                        "the 'if' operator expects exactly 3 arguments, the condition, \
                        value-if-true, and value-if-false",
                    )
                    .with_code_range(range, format!("supplied {} arguments", args.len()))
                    .send_report();
                Expr::Error
            }
        })
        .parse(input)
}

fn filter_atom(input: Span) -> IResult<Span, Option<Search>> {
    let keyword = take_while1(is_keyword).map(|i: Span| Search::from_keyword_input(i.fragment()));

    alt((quoted_string.map(Search::from_quoted_input), keyword))(input)
}

fn filter_not(input: Span) -> IResult<Span, Option<Search>> {
    tag("NOT")
        .precedes(multispace1)
        .precedes(low_filter)
        .map(|optk| optk.map(|k| Search::Not(Box::new(k))))
        .parse(input)
}

fn sourced_expr(input: Span) -> IResult<Span, (String, Expr)> {
    recognize(expr)
        .map(|ex| (ex.fragment().trim().to_string(), expr(ex).ok().unwrap().1))
        .parse(input)
}

fn sourced_expr_list(input: Span) -> IResult<Span, Vec<(String, Expr)>> {
    separated_list1(tag(",").delimited_by(multispace0), sourced_expr)(input)
}

fn sort_mode(input: Span) -> IResult<Span, SortMode> {
    alt((
        alt((tag("asc"), tag("ascending"))).map(|_| SortMode::Ascending),
        alt((tag("desc"), tag("dsc"), tag("descending"))).map(|_| SortMode::Descending),
    ))(input)
}

fn sort(input: Span) -> IResult<Span, Operator> {
    tuple((
        tag("sort").precedes(
            opt(tag("by")
                .delimited_by(multispace1)
                .precedes(sourced_expr_list))
            .map(|opt_cols| opt_cols.unwrap_or_default()),
        ),
        opt(sort_mode.preceded_by(multispace1))
            .map(|opt_mode| opt_mode.unwrap_or(SortMode::Ascending)),
    ))
    .map(|(mut sort_cols, direction)| {
        Operator::Sort(SortOperator {
            sort_cols: sort_cols.drain(..).map(|(_name, ex)| ex).collect(),
            direction,
        })
    })
    .parse(input)
}

fn filter_explicit_and(input: Span) -> IResult<Span, Option<Search>> {
    separated_pair(low_filter, tag("AND").delimited_by(multispace1), low_filter)
        .map(|p| match p {
            (Some(l), Some(r)) => Some(Search::And(vec![l, r])),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        })
        .parse(input)
}

fn filter_explicit_or(input: Span) -> IResult<Span, Option<Search>> {
    separated_pair(mid_filter, tag("OR").delimited_by(multispace1), mid_filter)
        .map(|p| match p {
            (Some(l), Some(r)) => Some(Search::Or(vec![l, r])),
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (None, None) => None,
        })
        .parse(input)
}

fn low_filter(input: Span) -> IResult<Span, Option<Search>> {
    alt((
        filter_not,
        filter_atom,
        expect_delimited(tag("("), high_filter, tag(")"), |qc, r| {
            qc.report_error_for("unterminated parenthesized filter")
                .with_code_range(r, "unterminated parenthesized filter")
                .with_resolution("Insert a right parenthesis to terminate this filter")
                .send_report()
        }),
    ))(input)
}

fn mid_filter(input: Span) -> IResult<Span, Option<Search>> {
    alt((filter_explicit_and, low_filter))(input)
}

fn high_filter(input: Span) -> IResult<Span, Option<Search>> {
    alt((filter_explicit_or, mid_filter))(input)
}

fn end_of_query(input: Span) -> IResult<Span, Span> {
    peek(multispace0.precedes(alt((peek(tag("|")), eof))))(input)
}

fn parse_search(input: Span) -> IResult<Span, Search> {
    many_till(high_filter.delimited_by(multispace0), end_of_query)
        .map(|(s, _)| Search::And(s.into_iter().flatten().collect()))
        .parse(input)
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

/// Parses a single number with a time suffix
fn duration_fragment(input: Span) -> IResult<Span, chrono::Duration> {
    let (input, amount) = i64_parse(input)?;

    alt((
        tag("ns").map(move |_| chrono::Duration::nanoseconds(amount)),
        tag("us").map(move |_| chrono::Duration::microseconds(amount)),
        tag("ms").map(move |_| chrono::Duration::milliseconds(amount)),
        tag("s").map(move |_| chrono::Duration::seconds(amount)),
        tag("m").map(move |_| chrono::Duration::minutes(amount)),
        tag("h").map(move |_| chrono::Duration::hours(amount)),
        tag("d").map(move |_| chrono::Duration::days(amount)),
        tag("w").map(move |_| chrono::Duration::weeks(amount)),
    ))
    .parse(input)
}

/// Parses a duration that can be made up of multiple integer/time-suffix values
fn duration(input: Span) -> IResult<Span, chrono::Duration> {
    fold_many1(duration_fragment, chrono::Duration::zero, |left, right| {
        left + right
    })(input)
}

fn dot_property(input: Span) -> IResult<Span, DataAccessAtom> {
    tag(".")
        .precedes(ident)
        .map(DataAccessAtom::Key)
        .parse(input)
}

fn i64_parse(input: Span) -> IResult<Span, i64> {
    map_res(recognize(opt(tag("-")).precedes(digit1)), |s: Span| {
        s.fragment().parse::<i64>()
    })
    .parse(input)
}

fn index_access(input: Span) -> IResult<Span, DataAccessAtom> {
    delimited(tag("["), i64_parse, tag("]"))
        .map(DataAccessAtom::Index)
        .parse(input)
}

fn column_ref(input: Span) -> IResult<Span, Expr> {
    tuple((ident, many0(alt((dot_property, index_access)))))
        .map(|(head, rest)| Expr::Column {
            head: DataAccessAtom::Key(head),
            rest,
        })
        .parse(input)
}

fn ident(input: Span) -> IResult<Span, String> {
    alt((bare_ident, escaped_ident))(input)
}

fn bare_ident(input: Span) -> IResult<Span, String> {
    recognize(pair(satisfy(starts_ident), take_while(is_ident)))
        .map(|span: Span| span.fragment().to_string())
        .parse(input)
}

fn escaped_ident(input: Span) -> IResult<Span, String> {
    expect_delimited(tag("["), quoted_string, tag("]"), |qc, r| {
        qc.report_error_for("unterminated identifier")
            .with_code_range(r, "")
            .with_resolution("Insert a closing square bracket")
            .send_report()
    })
    .parse(input)
}

/// Parses the basic unit of an expression
fn atomic(input: Span) -> IResult<Span, Expr> {
    let num = digit1.map(|s: Span| data::Value::from_string(*s.fragment()));
    let bool_lit = alt((
        tag("true").map(|_| data::Value::Bool(true)),
        tag("false").map(|_| data::Value::Bool(false)),
    ));
    let null = tag("null").map(|_| data::Value::None);
    let quoted_string_value = quoted_string.map(data::Value::Str);
    let duration_value = duration.map(data::Value::Duration);
    let value = alt((quoted_string_value, duration_value, num, bool_lit, null)).map(Expr::Value);
    let parens = expect_delimited(tag("("), expr, tag(")"), |qc, r| {
        qc.report_error_for("unterminated parenthesized expression")
            .with_code_range(r, "unterminated parenthesized expression")
            .with_resolution("Insert a right parenthesis to terminate this expression")
            .send_report()
    });

    alt((if_op, fcall, value, column_ref, parens)).parse(input)
}

/// Parses an atomic expression with an optional unary prefix
fn unary(input: Span) -> IResult<Span, Expr> {
    let (input, opt_op) = opt(unary_op)(input)?;

    match opt_op {
        None => atomic(input),
        Some(op) => expect_fn(atomic, |qc, r| {
            qc.report_error_for("expecting expression for unary operator")
                .with_code_range(r, "-")
                .send_report()
        })
        .map(|operand| Expr::Unary {
            op: op.clone(),
            operand: Box::new(operand.unwrap_or(Expr::Error)),
        })
        .parse(input),
    }
}

/// Parses the comparison operators
fn comp_op(input: Span) -> IResult<Span, ComparisonOp> {
    alt((
        tag("==").map(|_| ComparisonOp::Eq),
        tag("!=").map(|_| ComparisonOp::Neq),
        tag("<>").map(|_| ComparisonOp::Neq),
        tag(">=").map(|_| ComparisonOp::Gte),
        tag("<=").map(|_| ComparisonOp::Lte),
        tag(">").map(|_| ComparisonOp::Gt),
        tag("<").map(|_| ComparisonOp::Lt),
    ))(input)
}

/// Parses the unary operators
fn unary_op(input: Span) -> IResult<Span, UnaryOp> {
    tag("!").map(|_| UnaryOp::Not).parse(input)
}

fn muldiv_op(input: Span) -> IResult<Span, ArithmeticOp> {
    alt((
        tag("*").map(|_| ArithmeticOp::Multiply),
        tag("/").map(|_| ArithmeticOp::Divide),
    ))(input)
}

/// Parses a chain of multiple/divide expressions
fn term(input: Span) -> IResult<Span, Expr> {
    let (input, init) = unary(input)?;
    let qc = &input.extra;

    let retval = fold_many0(
        pair(with_pos(muldiv_op).delimited_by(multispace0), opt(unary)).map(
            |(pos_op, opt_right)| {
                if let Some(right) = opt_right {
                    (pos_op.value, right)
                } else {
                    qc.report_error_for("expecting an operand for binary operator")
                        .with_code_range(pos_op.range, "dangling binary operator")
                        .with_resolution("Add the operand or delete the operator")
                        .send_report();
                    (pos_op.value, Expr::Error)
                }
            },
        ),
        || init.clone(),
        |left, (op, right)| Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::Arithmetic(op),
            right: Box::new(right),
        },
    )(input);

    retval
}

/// Parses a chain of plus/minus expressions
fn arith_expr(input: Span) -> IResult<Span, Expr> {
    let (input, init) = term(input)?;
    let init = move || init.clone();

    fold_many0(
        pair(
            addsub_op.delimited_by(multispace0),
            expect_fn(term, |qc, r| {
                qc.report_error_for("expecting an operand for binary operator")
                    .with_code_range(r, "- dangling binary operator")
                    .with_resolution("Add the operand or delete the operator")
                    .send_report()
            })
            .map(|e| e.unwrap_or(Expr::Error)),
        ),
        init,
        |left, (op, right)| Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::Arithmetic(op),
            right: Box::new(right),
        },
    )(input)
}

fn cmp_expr(input: Span) -> IResult<Span, Expr> {
    let cmp = map(
        arith_expr.and(opt(pair(
            delimited(multispace0, comp_op, multispace0),
            expect(
                arith_expr,
                "expecting the right-hand-side of the comparison",
            ),
        ))),
        |(left, opt_right)| match opt_right {
            None => left,
            Some((op, right)) => Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::Comparison(op),
                right: Box::new(right.unwrap_or(Expr::Error)),
            },
        },
    );

    cmp.preceded_by(multispace0).parse(input)
}

fn logical_and(input: Span) -> IResult<Span, Expr> {
    let (input, init) = cmp_expr(input)?;
    let qc = &input.extra;

    let retval = fold_many0(
        alt((
            pair(
                multispace1.precedes(with_pos(tag("and"))),
                opt(multispace1.precedes(cmp_expr)),
            ),
            pair(with_pos(tag("&&").delimited_by(multispace0)), opt(cmp_expr)),
        ))
        .map(|(pos_and, opt_right)| {
            if let Some(right) = opt_right {
                right
            } else {
                qc.report_error_for("expecting an operand for 'and' operator")
                    .with_code_range(pos_and.range, "- dangling 'and'")
                    .with_resolution("Add the operand or delete the 'and'")
                    .send_report();

                Expr::Error
            }
        }),
        || init.clone(),
        |left, right| Expr::Binary {
            op: BinaryOp::Logical(LogicalOp::And),
            left: Box::new(left),
            right: Box::new(right),
        },
    )
    .parse(input);

    retval
}

fn logical_or(input: Span) -> IResult<Span, Expr> {
    let (input, init) = logical_and(input)?;
    let qc = &input.extra;

    let retval = fold_many0(
        alt((
            pair(
                multispace1.precedes(with_pos(tag("or"))),
                opt(multispace1.precedes(logical_and)),
            ),
            pair(
                with_pos(tag("||").delimited_by(multispace0)),
                opt(logical_and),
            ),
        ))
        .map(|(pos_or, opt_right)| {
            if let Some(right) = opt_right {
                right
            } else {
                qc.report_error_for("expecting an operand for 'or' operator")
                    .with_code_range(pos_or.range, "- dangling 'or'")
                    .with_resolution("Add the operand or delete the 'or'")
                    .send_report();

                Expr::Error
            }
        }),
        || init.clone(),
        |left, right| Expr::Binary {
            op: BinaryOp::Logical(LogicalOp::Or),
            left: Box::new(left),
            right: Box::new(right),
        },
    )
    .parse(input);

    retval
}

/// Parses an expression, optionally, including a comparison operation
fn opt_expr(input: Span) -> IResult<Span, Expr> {
    logical_or.preceded_by(multispace0).parse(input)
}

/// Parses a required expression
fn expr(input: Span) -> IResult<Span, Expr> {
    Ok(opt_expr(input).unwrap_or_else(|_| {
        let mut r = input.to_sync_point();

        if r.is_empty() && r.start > 0 {
            r.start -= 1;
        }
        input
            .extra
            .report_error_for("expecting an expression")
            .with_code_range(r.clone(), "")
            .send_report();

        let end = r.end - input.location_offset();
        (input.slice(end..), Expr::Error)
    }))
}

fn req_ident(input: Span) -> IResult<Span, String> {
    expect_fn(ident, |qc, r| {
        qc.report_error_for("expecting a name for the field")
            .with_code_range(r, "")
            .with_resolution("Give the value a name")
            .send_report();
    })
    .map(|opt_id| opt_id.unwrap_or_default())
    .parse(input)
}

fn escaped_chars(input: Span) -> IResult<Span, Span> {
    alt((
        tag("\\"),
        tag("r"),
        tag("n"),
        tag("t"),
        tag("0"),
        tag("'"),
        tag("\""),
        // Accept other characters and pass them through
        // so the user doesn't have to do extra escaping
        take(1usize),
        // TODO handle unicode
    ))
    .parse(input)
}

fn escaped_str_transformer(input: Span) -> String {
    let mut in_escape = false;

    input
        .fragment()
        .iter_elements()
        .fold(String::new(), |mut acc, ch| {
            let was_in_escape = in_escape;
            match ch {
                '\\' if !in_escape => {
                    in_escape = true;
                }
                '\\' if in_escape => acc.push('\\'),
                't' if in_escape => acc.push('\t'),
                'r' if in_escape => acc.push('\r'),
                'n' if in_escape => acc.push('\n'),
                '0' if in_escape => acc.push('\0'),
                '\'' if in_escape => acc.push('\''),
                '"' if in_escape => acc.push('"'),
                unhandled_escape if in_escape => {
                    // note: unknown escapes are passed through so that escape sequences in
                    // regexes "just work"
                    acc.push('\\');
                    acc.push(unhandled_escape)
                }
                other => acc.push(other),
            };
            if was_in_escape {
                in_escape = false;
            }

            acc
        })
}

fn quoted_string(input: Span) -> IResult<Span, String> {
    let sq_esc = escaped(none_of("\\\'"), '\\', escaped_chars);
    let sq_esc_or_empty = alt((sq_esc, tag("")));
    let single_quoted = expect_delimited(tag("'"), sq_esc_or_empty, tag("'"), |qc, r| {
        qc.report_error_for("unterminated single quoted string")
            .with_code_range(r, "")
            .with_resolution("Insert a single quote (') to terminate this string")
            .send_report()
    });

    let dq_esc = escaped(none_of("\\\""), '\\', escaped_chars);
    let dq_esc_or_empty = alt((dq_esc, tag("")));
    let double_quoted = expect_delimited(tag("\""), dq_esc_or_empty, tag("\""), |qc, r| {
        qc.report_error_for("unterminated double quoted string")
            .with_code_range(r, "")
            .with_resolution("Insert a double quote (\") to terminate this string")
            .send_report()
    });

    alt((single_quoted, double_quoted))
        .map(escaped_str_transformer)
        .parse(input)
}

fn req_quoted_string(input: Span) -> IResult<Span, String> {
    let upto_ws = input.to_whitespace();

    match quoted_string(input) {
        Ok(found) => Ok(found),
        Err(_) => {
            input
                .extra
                .report_error_for("expecting a quoted string")
                .with_code_range(upto_ws.clone(), "")
                .with_resolution("Enclose the text in a single or double-quoted string")
                .send_report();

            Ok((input.slice(upto_ws.len()..), "".to_string()))
        }
    }
}

fn var_list(input: Span) -> IResult<Span, Vec<String>> {
    separated_list1(tag(","), ident.preceded_by(multispace0))(input)
}

fn parse(input: Span) -> IResult<Span, Positioned<InlineOperator>> {
    with_pos(
        tuple((
            tag("parse").precedes(multispace1),
            opt(tag("regex").precedes(multispace1)),
            with_pos(req_quoted_string),
            opt(multispace1.precedes(with_pos(pair(tag("from"), multispace1).precedes(expr)))),
            opt(with_pos(tag("as").preceded_by(multispace1).precedes(var_list))),
            opt(multispace1.precedes(with_pos(pair(tag("from"), multispace1).precedes(expr)))),
            opt(tag("nodrop").preceded_by(multispace1)).map(|nd| nd.is_some()),
            opt(tag("noconvert").preceded_by(multispace1)).map(|nd| nd.is_some()),
        ))
        .map(|(_p, is_regex, s, from_col_before, user_fields_opt, from_col_after, no_drop, no_convert)| {
            let (pattern, fields) = if is_regex.is_some() {
                let named_fields: Vec<String> = match regex::Regex::new(&s.value) {
                    Err(regex_err) => {
                        input
                            .extra
                            .report_error_for("invalid regular expression")
                            .with_code_range(s.range, format!("{}", regex_err))
                            .send_report();
                        Vec::new()
                    }
                    Ok(re) => {
                        let named_caps: Vec<String> = re
                            .capture_names()
                            .flatten()
                            .map_into()
                            .collect();
                        let unnamed_capture_count = re.captures_len() - 1 - named_caps.len();

                        if unnamed_capture_count > 0 {
                            input
                                .extra
                                .report_error_for("regular expression for the parse operator cannot have unnamed captures")
                                .with_code_range(s.range, format!(
                                    "contains {} unnamed capture(s)", unnamed_capture_count))
                                .with_resolution("name the capture groups by adding -- ?P<capname> -- right after the left parenthesis")
                                .send_report();
                            Vec::new()
                        } else if let Some(user_fields) = user_fields_opt {
                            input
                                .extra
                                .report_error_for("the parse regex operator does not support an 'as' clause")
                                .with_code_range(user_fields.range, "remove this")
                                .with_resolution("remove the 'as' clause, the named capture groups in the regular expression determine the output column")
                                .send_report();
                            Vec::new()
                        } else {
                            named_caps
                        }
                    }
                };

                (Keyword::new_regex(s.value), named_fields)
            } else {
                (
                    Keyword::new_wildcard(s.value),
                    user_fields_opt.map(|user_fields| user_fields.value).unwrap_or_default(),
                )
            };

            InlineOperator::Parse {
                pattern,
                fields,
                input_column: (from_col_before, from_col_after),
                no_drop,
                no_convert
            }
        }),
    )
    .terminated(expect_pipe(
        "unrecognized option, only the 'from' and 'nodrop' options are available",
    ))
    .parse(input)
}

fn fields_mode(input: Span) -> IResult<Span, FieldMode> {
    alt((
        alt((tag("+"), tag("only"), tag("include"))).map(|_| FieldMode::Only),
        alt((tag("-"), tag("except"), tag("drop"))).map(|_| FieldMode::Except),
    ))(input)
}

fn fields(input: Span) -> IResult<Span, Positioned<InlineOperator>> {
    with_pos(
        tuple((
            tag("fields")
                .precedes(multispace1)
                .precedes(opt(fields_mode).map(|m| m.unwrap_or(FieldMode::Only))),
            var_list,
        ))
        .map(|(mode, fields)| InlineOperator::Fields { mode, fields }),
    )
    .parse(input)
}

fn pct(input: Span) -> IResult<Span, Positioned<AggregateFunction>> {
    with_pos(
        alt((tag("pct"), tag("percentile"), tag("p")))
            .precedes(with_pos(digit1))
            .and(req_single_arg("the value to compute the percentile of"))
            .map(|(pct_pos, column)| match pct_pos.value.parse::<f64>() {
                Ok(pct) if pct > 0.0 && pct < 100.0 => AggregateFunction::Percentile {
                    column,
                    percentile: pct / 100.0,
                    percentile_str: pct.to_string(),
                },
                _ => {
                    input
                        .extra
                        .report_error_for(format!("invalid percentile number: {}", pct_pos.value))
                        .with_code_range(pct_pos.range, "expecting a number between (0, 100)")
                        .send_report();

                    AggregateFunction::Error
                }
            }),
    )
    .parse(input)
}

pub fn with_pos<'a, O, E: ParseError<Span<'a>>, F>(
    mut f: F,
) -> impl FnMut(Span<'a>) -> IResult<Span<'a>, Positioned<O>, E>
where
    F: Parser<Span<'a>, O, E>,
{
    move |input: Span<'a>| {
        let (input, start) = position(input)?;
        match f.parse(input) {
            Ok((i, o)) => {
                let (i, end) = position(i)?;
                Ok((
                    i,
                    Positioned {
                        range: Range {
                            start: start.location_offset(),
                            end: end.location_offset(),
                        },
                        value: o,
                    },
                ))
            }
            Err(e) => Err(e),
        }
    }
}

fn oper_0_args(name: &'static str) -> impl Clone + Fn(Span) -> IResult<Span, Span> {
    move |input: Span| {
        tag(name)
            .and(expect_fn(not(tag("(")), |qc, r| {
                qc.report_error_for(format!(
                    "{} does not take any parenthesized arguments",
                    name
                ))
                .with_code_range(r, "")
                .send_report()
            }))
            .precedes(alt((peek(multispace1), end_of_query)))
            .parse(input)
    }
}

/// Parser that consumes characters up to the end of the query
fn skip_to_end_of_query(input: Span) -> IResult<Span, Operator> {
    if input.extra.get_error_count() > 0 {
        many_till(anychar, end_of_query)
            .map(|_| Operator::Error)
            .parse(input)
    } else {
        satisfy(|_| false).map(|_| Operator::Error).parse(input)
    }
}

struct ValidOperators<'a> {
    aliases: &'a AliasCollection<'a>,
}

impl<'a> ValidOperators<'a> {
    fn valid_aggs(&self) -> impl Iterator<Item = &'a str> {
        VALID_AGGREGATES.iter().copied()
    }

    fn valid_operators(&self) -> impl Iterator<Item = &'a str> {
        self.valid_aggs()
            .chain(VALID_INLINE.iter().copied())
            .chain(self.aliases.valid_aliases())
    }
}

fn parse_operators<'a>(
    input: Span<'a>,
    aliases: &AliasCollection,
) -> IResult<Span<'a>, Vec<Operator>> {
    let json = with_pos(
        oper_0_args("json")
            .precedes(kw_expr("from", "a JSON-encoded string"))
            .terminated(expect_pipe(
                "unrecognized option, only the 'from' option is available",
            ))
            .map(|input_column| InlineOperator::Json { input_column }),
    );
    let limit = with_pos(
        oper_0_args("limit")
            .precedes(opt(with_pos(double).preceded_by(multispace1)))
            .terminated(expect_pipe(
                "unrecognized option, only the numeric limit can be specified",
            ))
            .map(|count| InlineOperator::Limit { count }),
    );
    let logfmt = with_pos(
        oper_0_args("logfmt")
            .precedes(kw_expr("from", "a logfmt-serialized string"))
            .terminated(expect_pipe(
                "unrecognized option, only the 'from' option is available",
            ))
            .map(|input_column| InlineOperator::Logfmt { input_column }),
    );
    let split = with_pos(
        tag("split")
            .precedes(tuple((
                opt(single_arg("the string to split")),
                opt(tag("on")
                    .delimited_by(multispace1)
                    .precedes(req_quoted_string)),
                opt(tag("as").delimited_by(multispace1).precedes(expr)),
            )))
            .terminated(expect_pipe(
                "unrecognized option, only the 'on' and 'as' options are available",
            ))
            .map(|(e, o, a)| InlineOperator::Split {
                separator: o.unwrap_or_else(|| ",".to_string()),
                input_column: e.clone(),
                output_column: a.or(e),
            }),
    );
    let timeslice = with_pos(
        tuple((
            tag("timeslice").precedes(req_single_arg("the date-time value for the log message")),
            opt(duration.preceded_by(multispace1)),
            opt(tag("as").delimited_by(multispace1).precedes(ident)),
        ))
        .terminated(expect_pipe(
            "unrecognized option, only the 'as' option is available",
        ))
        .map(
            |(input_column, duration, output_column)| InlineOperator::Timeslice {
                input_column,
                duration,
                output_column,
            },
        ),
    );
    let total = with_pos(
        tag("total")
            .precedes(req_single_arg("the value to sum"))
            .and(
                opt(tag("as").delimited_by(multispace1).precedes(req_ident))
                    .map(|i| i.unwrap_or_else(|| "_total".to_string())),
            )
            .terminated(expect_pipe(
                "unrecognized option, only the 'as' option is available",
            ))
            .map(|(input_column, output_column)| InlineOperator::Total {
                input_column,
                output_column,
            }),
    );
    let wher = with_pos(
        tag("where")
            .precedes(opt(delimited(multispace1, with_pos(expr), multispace0)))
            .terminated(expect_pipe(
                "unrecognized option, only the condition can be specified",
            ))
            .map(|expr| InlineOperator::Where { expr }),
    );

    let count = with_pos(
        tag("count")
            .precedes(opt(single_arg("the value to count")))
            .map(|condition| AggregateFunction::Count { condition }),
    );
    let count_distinct = with_pos(
        tag("count_distinct")
            .precedes(opt(with_pos(arg_list)))
            .map(|column| AggregateFunction::CountDistinct { column }),
    );
    let min = with_pos(
        tag("min")
            .precedes(req_single_arg("the numeric value to find the minimum of"))
            .map(|column| AggregateFunction::Min { column }),
    );
    let max = with_pos(
        tag("max")
            .precedes(req_single_arg("the numeric value to find the maximum of"))
            .map(|column| AggregateFunction::Max { column }),
    );
    let sum = with_pos(
        tag("sum")
            .precedes(req_single_arg("the numeric value to find the sum of"))
            .map(|column| AggregateFunction::Sum { column }),
    );
    let avg = with_pos(
        tag("avg")
            .or(tag("average"))
            .precedes(req_single_arg("the numeric value to find the average of"))
            .map(|column| AggregateFunction::Average { column }),
    );

    let agg_opers = alt((count_distinct, count, min, max, pct, sum, avg))
        .and(opt(tag("as").delimited_by(multispace1).precedes(req_ident)))
        .map(|(f, n)| (n.unwrap_or_else(|| f.value.default_name()), f));

    let multi_agg_opers = tuple((
        separated_list1(tag(","), agg_opers.delimited_by(multispace0)),
        opt(tag("by")
            .terminated(multispace1)
            .precedes(sourced_expr_list))
        .terminated(end_of_query)
        .map(|opt_cols| opt_cols.unwrap_or_default()),
    ))
    .map(|(aggregate_functions, cols)| {
        let (key_col_headers, key_cols) = cols.iter().cloned().unzip();
        Operator::MultiAggregate(MultiAggregateOperator {
            key_cols,
            key_col_headers,
            aggregate_functions,
        })
    });

    let inline_opers = alt((
        parse, json, logfmt, fields, limit, split, timeslice, total, wher,
    ))
    .map(Operator::Inline);

    let field_expr = with_pos(
        expr.and(tag("as").delimited_by(multispace1).precedes(req_ident))
            .map(|(value, name)| InlineOperator::FieldExpression { value, name }),
    )
    .map(Operator::Inline);

    let alias = recognize(ident).map_res(|span| {
        aliases
            .get_alias(span.fragment())
            .ok_or(())
            .map(|pipe| Operator::RenderedAlias(pipe.render()))
    });

    let unknown_op = recognize(ident).terminated(opt(arg_list));

    let did_you_mean = separated_list1(tag(","), unknown_op.delimited_by(multispace0))
        .terminated(
            opt(tag("by")
                .terminated(multispace1)
                .precedes(sourced_expr_list))
            .terminated(many_till(anychar, end_of_query)),
        )
        .map(|unknown_ids| {
            let is_agg = unknown_ids.len() > 1;
            for i in unknown_ids {
                if is_agg {
                    if VALID_AGGREGATES.contains(&i) {
                        continue;
                    }
                } else if VALID_OPERATORS.contains(&i) {
                    continue;
                }
                let valid_operators = ValidOperators { aliases };

                let m = if is_agg {
                    crate::errors::did_you_mean(&i, valid_operators.valid_aggs())
                } else {
                    crate::errors::did_you_mean(&i, valid_operators.valid_operators())
                };

                let mut builder = input
                    .extra
                    .report_error_for(if is_agg {
                        "Not an aggregate operator"
                    } else {
                        "Expected an operator"
                    })
                    .with_code_range(i.to_range(), "")
                    .with_resolution(if is_agg {
                        format!("{} is not a valid aggregate operator", i)
                    } else {
                        format!("{} is not a valid operator", i)
                    });

                if m.is_some() {
                    builder = builder.with_resolution(format!("Did you mean \"{}\"?", m.unwrap()));
                } else if is_agg && VALID_INLINE.contains(&i) {
                    builder = builder.with_resolution(format!("{} is an inline operator, but only aggregate operators (count, average, etc.) are valid here", i));
                }
                builder.send_report();
            }

            Operator::Error
        });

    let garbage = expect(alt((tag("|"), eof)), "unrecognized syntax").map(|_| Operator::Error);

    let opers = alt((
        inline_opers,
        multi_agg_opers,
        sort,
        field_expr,
        alias,
        skip_to_end_of_query,
        did_you_mean,
        garbage,
    ));

    separated_list1(tag("|"), opers.delimited_by(multispace0)).parse(input)
}

pub fn pipeline_template(input: &QueryContainer) -> Result<Vec<Operator>, CompileError> {
    let span = Span::new_extra(input.query.as_str(), input);
    let (_input, operators) =
        parse_operators(span, &input.aliases).map_err(|_| CompileError::Parse)?;

    Ok(operators)
}

pub fn query(container: &QueryContainer) -> Result<Query, CompileError> {
    let span = Span::new_extra(container.query.as_str(), container);
    let (input, search) = parse_search(span).map_err(|_| CompileError::Parse)?;
    let (input, operators) =
        opt(tag("|").precedes(|span| parse_operators(span, &container.aliases)))
            .map(|ops| ops.unwrap_or_default())
            .parse(input)
            .map_err(|_| CompileError::Parse)?;

    if input.extra.get_error_count() > 0 {
        Err(CompileError::Parse)
    } else {
        Ok(Query { search, operators })
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::ErrorReporter;
    use annotate_snippets::snippet::Snippet;
    use expect_test::{expect, Expect};

    use super::*;
    use std::sync::{Arc, Mutex};

    fn check(actual: (Query, &Vec<String>), expect: Expect) {
        let actual_errors = actual.1.join("\n");
        let actual_combined = format!("{:#?}\n{}", actual.0, actual_errors);
        expect.assert_eq(&actual_combined);
    }

    fn check_query(query_in: &str, expect: Expect) {
        let errors = Arc::new(Mutex::new(vec![]));
        let parsed = {
            let qc = QueryContainer::new_with_aliases(
                query_in.to_string(),
                Box::new(VecErrorReporter::new(errors.clone())),
                AliasCollection::default(),
            );
            let r = query(&qc);

            r.unwrap_or_else(|_| Query {
                search: Search::And(vec![]),
                operators: vec![],
            })
        };

        {
            let errvec = errors.lock().unwrap();

            check((parsed, errvec.as_ref()), expect);
        }
    }

    struct VecErrorReporter {
        errors: Arc<Mutex<Vec<String>>>,
    }

    impl ErrorReporter for VecErrorReporter {
        fn handle_error(&self, snippet: Snippet) {
            let dl = annotate_snippets::display_list::DisplayList::from(snippet);

            self.errors.lock().unwrap().push(format!("{}", dl));
        }
    }

    impl VecErrorReporter {
        fn new(errors: Arc<Mutex<Vec<String>>>) -> Self {
            VecErrorReporter { errors }
        }
    }

    #[test]
    fn empty_query_is_query() {
        check_query(
            "*",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
            "#]],
        );

        check_query(
            " * ",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
            "#]],
        );
    }

    #[test]
    fn parse_ident() {
        check_query(
            "* | 1 as hello123",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..17,
                            value: FieldExpression {
                                value: Value(
                                    Int(
                                        1,
                                    ),
                                ),
                                name: "hello123",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | 1 as x",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..10,
                            value: FieldExpression {
                                value: Value(
                                    Int(
                                        1,
                                    ),
                                ),
                                name: "x",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | 1 as _x",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..11,
                            value: FieldExpression {
                                value: Value(
                                    Int(
                                        1,
                                    ),
                                ),
                                name: "_x",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | 1 as 5x",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: expecting a name for the field
                  |
                1 | * | 1 as 5x
                  |          ^^
                  |
                  = help: Give the value a name"#]],
        );
    }

    #[test]
    fn parse_parses() {
        check_query(
            r#"* | parse regex "(?P<abc>def)""#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..30,
                                value: Parse {
                                    pattern: Keyword(
                                        "(?P<abc>def)",
                                        Regex,
                                    ),
                                    fields: [
                                        "abc",
                                    ],
                                    input_column: (
                                        None,
                                        None,
                                    ),
                                    no_drop: false,
                                    no_convert: false,
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            r#"* | parse regex "(?<abc>def""#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: invalid regular expression
                  |
                1 | * | parse regex "(?<abc>def"
                  |                 ^^^^^^^^^^^^ regex parse error:
                    (?<abc>def
                    ^
                error: unclosed group
                  |"#]],
        );
        check_query(
            r#"* | parse regex "(?P<abc>def)" as v"#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: the parse regex operator does not support an 'as' clause
                  |
                1 | * | parse regex "(?P<abc>def)" as v
                  |                               ^^^^^ remove this
                  |
                  = help: remove the 'as' clause, the named capture groups in the regular expression determine the output column"#]],
        );
        check_query(
            r#"* | parse regex "([0-9])""#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: regular expression for the parse operator cannot have unnamed captures
                  |
                1 | * | parse regex "([0-9])"
                  |                 ^^^^^^^^^ contains 1 unnamed capture(s)
                  |
                  = help: name the capture groups by adding -- ?P<capname> -- right after the left parenthesis"#]],
        );
        check_query(
            r#"* | parse "[key=*]" as v"#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..24,
                                value: Parse {
                                    pattern: Keyword(
                                        "[key=*]",
                                        Wildcard,
                                    ),
                                    fields: [
                                        "v",
                                    ],
                                    input_column: (
                                        None,
                                        None,
                                    ),
                                    no_drop: false,
                                    no_convert: false,
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            r#"* | parse "[key=*]" as v nodrop"#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..31,
                                value: Parse {
                                    pattern: Keyword(
                                        "[key=*]",
                                        Wildcard,
                                    ),
                                    fields: [
                                        "v",
                                    ],
                                    input_column: (
                                        None,
                                        None,
                                    ),
                                    no_drop: true,
                                    no_convert: false,
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            r#"* | parse "[key=*][val=*]" as k,v nodrop"#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..40,
                                value: Parse {
                                    pattern: Keyword(
                                        "[key=*][val=*]",
                                        Wildcard,
                                    ),
                                    fields: [
                                        "k",
                                        "v",
                                    ],
                                    input_column: (
                                        None,
                                        None,
                                    ),
                                    no_drop: true,
                                    no_convert: false,
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            r#"* | parse "[key=*]" from field as v"#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..35,
                                value: Parse {
                                    pattern: Keyword(
                                        "[key=*]",
                                        Wildcard,
                                    ),
                                    fields: [
                                        "v",
                                    ],
                                    input_column: (
                                        Some(
                                            Positioned {
                                                range: 20..30,
                                                value: Column {
                                                    head: Key(
                                                        "field",
                                                    ),
                                                    rest: [],
                                                },
                                            },
                                        ),
                                        None,
                                    ),
                                    no_drop: false,
                                    no_convert: false,
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            r#"* | parse "[key=*]" as v from field"#,
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..35,
                                value: Parse {
                                    pattern: Keyword(
                                        "[key=*]",
                                        Wildcard,
                                    ),
                                    fields: [
                                        "v",
                                    ],
                                    input_column: (
                                        None,
                                        Some(
                                            Positioned {
                                                range: 25..35,
                                                value: Column {
                                                    head: Key(
                                                        "field",
                                                    ),
                                                    rest: [],
                                                },
                                            },
                                        ),
                                    ),
                                    no_drop: false,
                                    no_convert: false,
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            "* | parse 'x=* y=*' as x, y extra",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: unrecognized option, only the 'from' and 'nodrop' options are available
                  |
                1 | * | parse 'x=* y=*' as x, y extra
                  |                             ^^^^^
                  |"#]],
        );
    }

    #[test]
    fn parse_keyword_string() {
        check_query(
            "abc",
            expect![[r#"
                Query {
                    search: And(
                        [
                            Keyword(
                                Keyword(
                                    "abc",
                                    Wildcard,
                                ),
                            ),
                        ],
                    ),
                    operators: [],
                }
            "#]],
        );
        check_query(
            "one-two-three",
            expect![[r#"
                Query {
                    search: And(
                        [
                            Keyword(
                                Keyword(
                                    "one-two-three",
                                    Wildcard,
                                ),
                            ),
                        ],
                    ),
                    operators: [],
                }
            "#]],
        );
    }

    #[test]
    fn parse_expr() {
        check_query(
            "* | k1&&k2&&k3 and k4 as value",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..30,
                            value: FieldExpression {
                                value: Binary {
                                    op: Logical(
                                        And,
                                    ),
                                    left: Binary {
                                        op: Logical(
                                            And,
                                        ),
                                        left: Binary {
                                            op: Logical(
                                                And,
                                            ),
                                            left: Column {
                                                head: Key(
                                                    "k1",
                                                ),
                                                rest: [],
                                            },
                                            right: Column {
                                                head: Key(
                                                    "k2",
                                                ),
                                                rest: [],
                                            },
                                        },
                                        right: Column {
                                            head: Key(
                                                "k3",
                                            ),
                                            rest: [],
                                        },
                                    },
                                    right: Column {
                                        head: Key(
                                            "k4",
                                        ),
                                        rest: [],
                                    },
                                },
                                name: "value",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | k1||k2||k3 or k4 as value",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..29,
                            value: FieldExpression {
                                value: Binary {
                                    op: Logical(
                                        Or,
                                    ),
                                    left: Binary {
                                        op: Logical(
                                            Or,
                                        ),
                                        left: Binary {
                                            op: Logical(
                                                Or,
                                            ),
                                            left: Column {
                                                head: Key(
                                                    "k1",
                                                ),
                                                rest: [],
                                            },
                                            right: Column {
                                                head: Key(
                                                    "k2",
                                                ),
                                                rest: [],
                                            },
                                        },
                                        right: Column {
                                            head: Key(
                                                "k3",
                                            ),
                                            rest: [],
                                        },
                                    },
                                    right: Column {
                                        head: Key(
                                            "k4",
                                        ),
                                        rest: [],
                                    },
                                },
                                name: "value",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | k1 + 0 > 2 or k3 > 4 and k5 < 6 as value",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..44,
                                value: FieldExpression {
                                    value: Binary {
                                        op: Logical(
                                            Or,
                                        ),
                                        left: Binary {
                                            op: Comparison(
                                                Gt,
                                            ),
                                            left: Binary {
                                                op: Arithmetic(
                                                    Add,
                                                ),
                                                left: Column {
                                                    head: Key(
                                                        "k1",
                                                    ),
                                                    rest: [],
                                                },
                                                right: Value(
                                                    Int(
                                                        0,
                                                    ),
                                                ),
                                            },
                                            right: Value(
                                                Int(
                                                    2,
                                                ),
                                            ),
                                        },
                                        right: Binary {
                                            op: Logical(
                                                And,
                                            ),
                                            left: Binary {
                                                op: Comparison(
                                                    Gt,
                                                ),
                                                left: Column {
                                                    head: Key(
                                                        "k3",
                                                    ),
                                                    rest: [],
                                                },
                                                right: Value(
                                                    Int(
                                                        4,
                                                    ),
                                                ),
                                            },
                                            right: Binary {
                                                op: Comparison(
                                                    Lt,
                                                ),
                                                left: Column {
                                                    head: Key(
                                                        "k5",
                                                    ),
                                                    rest: [],
                                                },
                                                right: Value(
                                                    Int(
                                                        6,
                                                    ),
                                                ),
                                            },
                                        },
                                    },
                                    name: "value",
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            "* | lowercase(and) as value",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..27,
                            value: FieldExpression {
                                value: FunctionCall {
                                    name: "lowercase",
                                    args: [
                                        Column {
                                            head: Key(
                                                "and",
                                            ),
                                            rest: [],
                                        },
                                    ],
                                },
                                name: "value",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | if(abc and, abc, def) as value",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [],
            }
            error: expecting an operand for 'and' operator
              |
            1 | * | if(abc and, abc, def) as value
              |            ^^^ - dangling 'and'
              |
              = help: Add the operand or delete the 'and'"#]],
        );
        check_query(
            "* | if() as i",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: the 'if' operator expects exactly 3 arguments, the condition, value-if-true, and value-if-false
                  |
                1 | * | if() as i
                  |       ^^ supplied 0 arguments
                  |"#]],
        );
        check_query(
            "* | if(a, b, c, d) as i",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: the 'if' operator expects exactly 3 arguments, the condition, value-if-true, and value-if-false
                  |
                1 | * | if(a, b, c, d) as i
                  |       ^^^^^^^^^^^^ supplied 4 arguments
                  |"#]],
        );
        check_query(
            "* | now() - 1w2d as yesterday",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..29,
                                value: FieldExpression {
                                    value: Binary {
                                        op: Arithmetic(
                                            Subtract,
                                        ),
                                        left: FunctionCall {
                                            name: "now",
                                            args: [],
                                        },
                                        right: Value(
                                            Duration(
                                                TimeDelta {
                                                    secs: 777600,
                                                    nanos: 0,
                                                },
                                            ),
                                        ),
                                    },
                                    name: "yesterday",
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            "* | where foo *",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: expecting an operand for binary operator
                  |
                1 | * | where foo *
                  |               ^ dangling binary operator
                  |
                  = help: Add the operand or delete the operator"#]],
        );
        check_query(
            "* | null as n",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..13,
                            value: FieldExpression {
                                value: Value(
                                    None,
                                ),
                                name: "n",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
    }

    #[test]
    fn parse_func_call() {
        check_query(
            "* | parseDate(abc) as foo",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..25,
                                value: FieldExpression {
                                    value: FunctionCall {
                                        name: "parseDate",
                                        args: [
                                            Column {
                                                head: Key(
                                                    "abc",
                                                ),
                                                rest: [],
                                            },
                                        ],
                                    },
                                    name: "foo",
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            "* | parseDate(abc 123) as foo",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [],
            }
            error: unhandled input
              |
            1 | * | parseDate(abc 123) as foo
              |                   ^^^
              |"#]],
        );
    }

    #[test]
    fn parse_expr_precedence() {
        check_query(
            "* | 2 * 3 + 4 * 5 < 4 / 2 + 1 as val",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..36,
                                value: FieldExpression {
                                    value: Binary {
                                        op: Comparison(
                                            Lt,
                                        ),
                                        left: Binary {
                                            op: Arithmetic(
                                                Add,
                                            ),
                                            left: Binary {
                                                op: Arithmetic(
                                                    Multiply,
                                                ),
                                                left: Value(
                                                    Int(
                                                        2,
                                                    ),
                                                ),
                                                right: Value(
                                                    Int(
                                                        3,
                                                    ),
                                                ),
                                            },
                                            right: Binary {
                                                op: Arithmetic(
                                                    Multiply,
                                                ),
                                                left: Value(
                                                    Int(
                                                        4,
                                                    ),
                                                ),
                                                right: Value(
                                                    Int(
                                                        5,
                                                    ),
                                                ),
                                            },
                                        },
                                        right: Binary {
                                            op: Arithmetic(
                                                Add,
                                            ),
                                            left: Binary {
                                                op: Arithmetic(
                                                    Divide,
                                                ),
                                                left: Value(
                                                    Int(
                                                        4,
                                                    ),
                                                ),
                                                right: Value(
                                                    Int(
                                                        2,
                                                    ),
                                                ),
                                            },
                                            right: Value(
                                                Int(
                                                    1,
                                                ),
                                            ),
                                        },
                                    },
                                    name: "val",
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
    }

    #[test]
    fn complex_filters() {
        check_query(
            " abc def \"*ghi*\" ",
            expect![[r#"
                Query {
                    search: And(
                        [
                            Keyword(
                                Keyword(
                                    "abc",
                                    Wildcard,
                                ),
                            ),
                            Keyword(
                                Keyword(
                                    "def",
                                    Wildcard,
                                ),
                            ),
                            Keyword(
                                Keyword(
                                    "*ghi*",
                                    Exact,
                                ),
                            ),
                        ],
                    ),
                    operators: [],
                }
            "#]],
        );
        check_query(
            "(abc AND def) OR xyz",
            expect![[r#"
                Query {
                    search: And(
                        [
                            Or(
                                [
                                    And(
                                        [
                                            Keyword(
                                                Keyword(
                                                    "abc",
                                                    Wildcard,
                                                ),
                                            ),
                                            Keyword(
                                                Keyword(
                                                    "def",
                                                    Wildcard,
                                                ),
                                            ),
                                        ],
                                    ),
                                    Keyword(
                                        Keyword(
                                            "xyz",
                                            Wildcard,
                                        ),
                                    ),
                                ],
                            ),
                        ],
                    ),
                    operators: [],
                }
            "#]],
        );
    }

    #[test]
    fn parse_limit() {
        check_query(
            "* | limit",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..9,
                            value: Limit {
                                count: None,
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | limit 5",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..11,
                            value: Limit {
                                count: Some(
                                    Positioned {
                                        range: 10..11,
                                        value: 5.0,
                                    },
                                ),
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | limit -5",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..12,
                            value: Limit {
                                count: Some(
                                    Positioned {
                                        range: 10..12,
                                        value: -5.0,
                                    },
                                ),
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | limit 1e2",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..13,
                            value: Limit {
                                count: Some(
                                    Positioned {
                                        range: 10..13,
                                        value: 100.0,
                                    },
                                ),
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
        check_query(
            "* | limit foo",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [],
            }
            error: unrecognized option, only the numeric limit can be specified
              |
            1 | * | limit foo
              |           ^^^
              |"#]],
        );
    }

    #[test]
    fn parse_agg_operator() {
        check_query(
            "* | p200(abc)",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [],
            }
            error: invalid percentile number: 200
              |
            1 | * | p200(abc)
              |      ^^^ expecting a number between (0, 100)
              |"#]],
        );
        check_query(
            "* | count()",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: this operator takes 1 argument, but 0 were supplied
                  |
                1 | * | count()
                  |            - supplied 0 arguments
                  |
                  = help: the argument should supply the value to count"#]],
        );
        check_query(
            "* | json | count, count_distinct(message) by level",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..8,
                                value: Json {
                                    input_column: None,
                                },
                            },
                        ),
                        MultiAggregate(
                            MultiAggregateOperator {
                                key_cols: [
                                    Column {
                                        head: Key(
                                            "level",
                                        ),
                                        rest: [],
                                    },
                                ],
                                key_col_headers: [
                                    "level",
                                ],
                                aggregate_functions: [
                                    (
                                        "_count",
                                        Positioned {
                                            range: 11..16,
                                            value: Count {
                                                condition: None,
                                            },
                                        },
                                    ),
                                    (
                                        "_countDistinct",
                                        Positioned {
                                            range: 18..41,
                                            value: CountDistinct {
                                                column: Some(
                                                    Positioned {
                                                        range: 32..41,
                                                        value: [
                                                            Column {
                                                                head: Key(
                                                                    "message",
                                                                ),
                                                                rest: [],
                                                            },
                                                        ],
                                                    },
                                                ),
                                            },
                                        },
                                    ),
                                ],
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            "* | count as renamed by x, y",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    MultiAggregate(
                        MultiAggregateOperator {
                            key_cols: [
                                Column {
                                    head: Key(
                                        "x",
                                    ),
                                    rest: [],
                                },
                                Column {
                                    head: Key(
                                        "y",
                                    ),
                                    rest: [],
                                },
                            ],
                            key_col_headers: [
                                "x",
                                "y",
                            ],
                            aggregate_functions: [
                                (
                                    "renamed",
                                    Positioned {
                                        range: 4..9,
                                        value: Count {
                                            condition: None,
                                        },
                                    },
                                ),
                            ],
                        },
                    ),
                ],
            }
        "#]],
        );
    }

    #[test]
    fn or_filter() {
        check_query(
            "abc OR (def OR xyz)",
            expect![[r#"
                Query {
                    search: And(
                        [
                            Or(
                                [
                                    Keyword(
                                        Keyword(
                                            "abc",
                                            Wildcard,
                                        ),
                                    ),
                                    Or(
                                        [
                                            Keyword(
                                                Keyword(
                                                    "def",
                                                    Wildcard,
                                                ),
                                            ),
                                            Keyword(
                                                Keyword(
                                                    "xyz",
                                                    Wildcard,
                                                ),
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                        ],
                    ),
                    operators: [],
                }
            "#]],
        );
    }

    #[test]
    fn not_filter() {
        check_query(
            "NOT abc",
            expect![[r#"
                Query {
                    search: And(
                        [
                            Not(
                                Keyword(
                                    Keyword(
                                        "abc",
                                        Wildcard,
                                    ),
                                ),
                            ),
                        ],
                    ),
                    operators: [],
                }
            "#]],
        );
        check_query(
            "(error OR warn) AND NOT hide",
            expect![[r#"
                Query {
                    search: And(
                        [
                            And(
                                [
                                    Or(
                                        [
                                            Keyword(
                                                Keyword(
                                                    "error",
                                                    Wildcard,
                                                ),
                                            ),
                                            Keyword(
                                                Keyword(
                                                    "warn",
                                                    Wildcard,
                                                ),
                                            ),
                                        ],
                                    ),
                                    Not(
                                        Keyword(
                                            Keyword(
                                                "hide",
                                                Wildcard,
                                            ),
                                        ),
                                    ),
                                ],
                            ),
                        ],
                    ),
                    operators: [],
                }
            "#]],
        );
    }

    #[test]
    fn quoted_strings() {
        check_query(
            "* | 'abc\\tdef' as value",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [
                        Inline(
                            Positioned {
                                range: 4..23,
                                value: FieldExpression {
                                    value: Value(
                                        Str(
                                            "abc\tdef",
                                        ),
                                    ),
                                    name: "value",
                                },
                            },
                        ),
                    ],
                }
            "#]],
        );
        check_query(
            "* | 'abc\\d+def' as v",
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..20,
                            value: FieldExpression {
                                value: Value(
                                    Str(
                                        "abc\\d+def",
                                    ),
                                ),
                                name: "v",
                            },
                        },
                    ),
                ],
            }
        "#]],
        );
    }

    #[test]
    fn unterminated() {
        check_query(
            "* | (abc",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: unterminated parenthesized expression
                  |
                1 | * | (abc
                  |     ^^^^ unterminated parenthesized expression
                  |
                  = help: Insert a right parenthesis to terminate this expression"#]],
        );

        check_query(
            "* | parseDate(abc) | bar",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: Expected an operator
                  |
                1 | * | parseDate(abc) | bar
                  |     ^^^^^^^^^
                  |
                  = help: parseDate is not a valid operator"#]],
        );
        check_query(
            "* | where 'abc",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: unterminated single quoted string
                  |
                1 | * | where 'abc
                  |           ^^^^
                  |
                  = help: Insert a single quote (') to terminate this string"#]],
        );
    }

    #[test]
    fn split_operator() {
        check_query(
            "* | split(abc) on , as foo",
            expect![[r#"
                Query {
                    search: And(
                        [],
                    ),
                    operators: [],
                }
                error: expecting a quoted string
                  |
                1 | * | split(abc) on , as foo
                  |                   ^
                  |
                  = help: Enclose the text in a single or double-quoted string"#]],
        );
    }

    #[test]
    fn logfmt_operator() {
        check_query(
            r#"* | logfmt from col | sort by foo dsc "#,
            expect![[r#"
            Query {
                search: And(
                    [],
                ),
                operators: [
                    Inline(
                        Positioned {
                            range: 4..19,
                            value: Logfmt {
                                input_column: Some(
                                    Column {
                                        head: Key(
                                            "col",
                                        ),
                                        rest: [],
                                    },
                                ),
                            },
                        },
                    ),
                    Sort(
                        SortOperator {
                            sort_cols: [
                                Column {
                                    head: Key(
                                        "foo",
                                    ),
                                    rest: [],
                                },
                            ],
                            direction: Descending,
                        },
                    ),
                ],
            }
        "#]],
        );
    }
}
