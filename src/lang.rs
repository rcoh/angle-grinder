use crate::alias::{self, AliasConfig};
use crate::data;
use crate::errors::SyntaxErrors;
use lazy_static::lazy_static;
use nom;
use nom::types::CompleteStr;
use nom::*;
use nom::{digit1, double, is_alphabetic, is_alphanumeric, is_digit, multispace};
use nom_locate::LocatedSpan;
use std::convert::From;
use std::str;

/// Wraps the result of the child parser in a Positioned and sets the start_pos and end_pos
/// accordingly.
macro_rules! with_pos {
  ($i:expr, $submac:ident!( $($args:tt)* )) => ({
      // XXX The ws!() combinator does not mix well with custom combinators since it does some
      // rewriting, but only for things it knows about.  So, we put the ws!() combinator inside
      // calls to with_pos!() and have with_pos!() eat up any initial space with space0().
      match space0($i) {
          Err(e) => Err(e),
          Ok((i1, _o)) => {
              let start_pos: QueryPosition = i1.into();
              match $submac!(i1, $($args)*) {
                  Ok((i, o)) => Ok((i, Positioned {
                      start_pos,
                      value: o,
                      end_pos: i.into(),
                  })),
                  Err(e) => Err(e),
              }
          }
      }
  });
  ($i:expr, $f:expr) => (
    with_pos!($i, call!($f));
  );
}

/// Dynamic version of `alt` that takes a slice of strings
fn alternative<'a, T>(input: T, alternatives: &[&'a str]) -> IResult<T, T>
where
    T: InputTake,
    T: Compare<&'a str>,
    T: InputLength,
    T: AtEof,
    T: Clone,
{
    let mut last_err = None;
    for alternative in alternatives {
        let inp = input.clone();
        match tag!(inp, alternative) {
            done @ Ok(..) => return done,
            err @ Err(..) => last_err = Some(err), // continue
        }
    }
    last_err.unwrap()
}

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
    "parse", "limit", "json", "logfmt", "total", "fields", "where", "split",
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
pub enum BinaryOp {
    Comparison(ComparisonOp),
    Arithmetic(ArithmeticOp),
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
    Value(data::Value),
}

impl Expr {
    pub fn column(key: &str) -> Expr {
        Expr::Column {
            head: DataAccessAtom::Key(key.to_owned()),
            rest: vec![],
        }
    }
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
        let mut regex_str = regex::escape(&self.0.replace("\\\"", "\"")).replace(" ", "\\s");

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
pub enum Search {
    And(Vec<Search>),
    Or(Vec<Search>),
    Not(Box<Search>),
    Keyword(Keyword),
}

impl Search {
    fn no_op(&self) -> bool {
        match self {
            Search::Keyword(Keyword(k, _)) => k.is_empty(),
            _ => false,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    RenderedAlias(Positioned<String>),
    Inline(Positioned<InlineOperator>),
    MultiAggregate(MultiAggregateOperator),
    Sort(SortOperator),
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
}

#[derive(Debug, PartialEq)]
pub struct MultiAggregateOperator {
    pub key_cols: Vec<Expr>,
    pub key_col_headers: Vec<String>,
    pub aggregate_functions: Vec<(String, Positioned<AggregateFunction>)>,
}

#[derive(Debug, PartialEq)]
pub struct SortOperator {
    pub sort_cols: Vec<Expr>,
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

fn not_escape_sq(c: char) -> bool {
    c != '\\' && c != '\''
}

fn not_escape_dq(c: char) -> bool {
    c != '\\' && c != '\"'
}
named!(value<Span, data::Value>, ws!(
    alt!(
        map!(quoted_string, |s|data::Value::Str(s.to_string()))
        | map!(digit1, |s|data::Value::from_string(s.fragment.0))
    )
));

named!(dot_property<Span, DataAccessAtom>, do_parse!(
    field: preceded!(tag!("."), ident) >>
    (DataAccessAtom::Key(field))
));

named!(i64_parser<Span, i64>,
    map_res!(
        recognize!(tuple!(opt!(char!('-')), digit)),
        |s: Span|s.fragment.0.parse::<i64>())
);

named!(index_access<Span, DataAccessAtom>, do_parse!(
    index: delimited!(tag!("["), i64_parser, tag!("]"))  >>
    (DataAccessAtom::Index(index))
));

named!(column_ref<Span, Expr>, do_parse!(
    head: ident >>
    rest: many0!(alt!(dot_property | index_access)) >>
    (Expr::Column { head: DataAccessAtom::Key(head), rest: rest })
));

named!(ident<Span, String>, do_parse!(
    start: take_while1!(starts_ident) >>
    rest: take_while!(is_ident) >>
    (start.fragment.0.to_owned() + rest.fragment.0)
));

named!(arguments<Span, Vec<Expr>>, add_return_error!(SyntaxErrors::StartOfError.into(), delimited!(
   tag!("("),
   separated_list!(tag!(","), expr),
   return_error!(SyntaxErrors::MissingParen.into(), tag!(")"))
)));

named!(e_ident<Span, Expr>,
    ws!(alt_complete!(
      map!(pair!(ident, arguments), |(name, args)| Expr::FunctionCall { name, args })
    | column_ref
    | map!(value, Expr::Value)
    | ws!(add_return_error!(SyntaxErrors::StartOfError.into(), delimited!(
          tag!("("),
          expr,
          return_error!(SyntaxErrors::MissingParen.into(), tag!(")")))))
)));

named!(unary<Span, Expr>, ws!(do_parse!(
   opt_op: opt!(unary_op) >>
   atomic: e_ident >>
   (if let Some(op) = opt_op {
       Expr::Unary {
          op: op,
          operand: Box::new(atomic)
       }
    } else {
       atomic
    })
)));

named!(raw_keyword<Span, Span>,
   take_while1!(is_keyword)
);

named!(keyword<Span, String>, map!(
    verify!(raw_keyword, |k: Span|!RESERVED_FILTER_WORDS.contains(&k.fragment.0)),
    |span|span.fragment.0.to_owned()

));

named!(comp_op<Span, ComparisonOp>, ws!(alt_complete!(
    map!(tag!("=="), |_|ComparisonOp::Eq)
    | map!(tag!("<="), |_|ComparisonOp::Lte)
    | map!(tag!(">="), |_|ComparisonOp::Gte)
    | map!(tag!("!="), |_|ComparisonOp::Neq)
    | map!(tag!(">"), |_|ComparisonOp::Gt)
    | map!(tag!("<"), |_|ComparisonOp::Lt)
)));

named!(unary_op<Span, UnaryOp>, ws!(alt_complete!(
    map!(tag!("!"), |_|UnaryOp::Not)
)));

named!(term<Span, Expr>, do_parse!(
   init: unary >>
   res: fold_many0!(
      add_return_error!(SyntaxErrors::StartOfError.into(), pair!(ws!(alt!(
         map!(tag!("*"), |_|ArithmeticOp::Multiply)
         | map!(tag!("/"), |_|ArithmeticOp::Divide)
      )), return_error!(SyntaxErrors::MissingOperand.into(), unary))),
      init,
      |left, (op, right)| {
          Expr::Binary {
             left: Box::new(left),
             op: BinaryOp::Arithmetic(op),
             right: Box::new(right)
          }
      }
   ) >>
   (res)
));

named!(arith_expr<Span, Expr>, ws!(do_parse!(
   init: term >>
   res: fold_many0!(
      add_return_error!(SyntaxErrors::StartOfError.into(), pair!(ws!(alt!(
         map!(tag!("+"), |_|ArithmeticOp::Add)
         | map!(tag!("-"), |_|ArithmeticOp::Subtract)
      )), return_error!(SyntaxErrors::MissingOperand.into(), term))),
      init,
      |left, (op, right)| {
         Expr::Binary {
             left: Box::new(left),
             op: BinaryOp::Arithmetic(op),
             right: Box::new(right)
         }
      }
   ) >>
   (res)
)));

named!(expr<Span, Expr>, ws!(do_parse!(
   init: arith_expr >>
   res: opt!(pair!(comp_op, arith_expr)) >>
   (if let Some((op, right)) = res {
       Expr::Binary {
          left: Box::new(init),
          op: BinaryOp::Comparison(op),
          right: Box::new(right)
       }
    } else {
       init
    })
)));

named!(req_ident<Span, String>, return_error!(SyntaxErrors::MissingName.into(), ident));

named!(field_expr<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
   not!(alt_complete!(
       tag!("count") |
       tag!("count_frequent") |
       tag!("total")
   )) >>
   value: expr >>
   tag!("as") >>
   name: req_ident >>
   (InlineOperator::FieldExpression {
       value,
       name,
    })
))));

named!(json<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("json") >>
    from_column_opt: opt!(ws!(preceded!(tag!("from"), expr))) >>
    (InlineOperator::Json { input_column: from_column_opt })
))));

named!(logfmt<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("logfmt") >>
    from_column_opt: opt!(ws!(preceded!(tag!("from"), expr))) >>
    (InlineOperator::Logfmt { input_column: from_column_opt })
))));

named!(whre<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("where") >>
    ex: opt!(with_pos!(expr)) >>
    (InlineOperator::Where { expr: ex })
))));

named!(limit<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("limit") >>
    count: opt!(with_pos!(double)) >>
    (InlineOperator::Limit{
        count
    })
))));

named!(split<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("split") >>
    from_column_opt: opt!(delimited!(
        tag!("("),
        expr,
        tag!(")")
    )) >>
    separator_opt: opt!(ws!(preceded!(tag!("on"), quoted_string))) >>
    // TODO: make variant of expr that only accepts Expr::Column instead of all types
    rename_opt: opt!(ws!(preceded!(tag!("as"), expr))) >>
    (InlineOperator::Split {
        separator: separator_opt.map(|s| s.to_string()).unwrap_or_else(|| ",".to_string()),
        input_column: from_column_opt.clone(),
        // If from column specified, but output column not specified
        // output should be the from column.
        output_column: rename_opt.or(from_column_opt),
    })
))));

named!(total<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("total") >>
    input_column: delimited!(tag!("("), expr, tag!(")")) >>
    rename_opt: opt!(ws!(preceded!(tag!("as"), ident))) >>
    (InlineOperator::Total{
        input_column,
        output_column:
            rename_opt.unwrap_or_else(||"_total".to_string()),
})))));

named!(double_quoted_string <Span, &str>, add_return_error!(
    SyntaxErrors::StartOfError.into(), delimited!(
        tag!("\""),
        map!(escaped!(take_while1!(not_escape_dq), '\\', anychar), |ref s|s.fragment.0),
        return_error!(SyntaxErrors::UnterminatedDoubleQuotedString.into(), tag!("\""))
)));

named!(single_quoted_string <Span, &str>, add_return_error!(
    SyntaxErrors::StartOfError.into(), delimited!(
        tag!("'"),
        map!(escaped!(take_while1!(not_escape_sq), '\\', anychar), |ref s|s.fragment.0),
        return_error!(SyntaxErrors::UnterminatedSingleQuotedString.into(), tag!("'"))
)));

named!(quoted_string<Span, &str>, alt_complete!(double_quoted_string | single_quoted_string));

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

named_args!(did_you_mean<'a>(choices: &[&'a str], err: SyntaxErrors)<Span<'a>, Span<'a>>,
        preceded!(space0,
            return_error!(SyntaxErrors::StartOfError.into(),
                alt_complete!(
                    // Either we find a valid operator name
                    // To prevent a prefix from partially matching, require `not!(alpha1)` after the tag is consumed
                    terminated!(alt_complete!(pct_fn | call!(alternative, choices)), not!(alpha1)) |

                    // Or we return an error after consuming a word
                    // If we exhaust all other possibilities, consume a word and return an error. We won't
                    // find `tag!("a")` after an identifier, so that's a guaranteed to fail and produce `not an operator`
                    terminated!(take_while!(is_ident), return_error!(err.clone().into(), tag!("a")))))
        )
);

named!(did_you_mean_operator<Span, Span>,
    call!(did_you_mean, &VALID_OPERATORS, SyntaxErrors::NotAnOperator)
);

named!(did_you_mean_aggregate<Span, Span>,
    call!(did_you_mean, &VALID_AGGREGATES, SyntaxErrors::NotAnAggregateOperator)
);

// parse "blah * ... *" [from other_field] as x, y
named!(parse<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("parse") >>
    pattern: quoted_string >>
    from_column_opt: opt!(with_pos!(ws!(preceded!(tag!("from"), expr)))) >>
    tag!("as") >>
    vars: var_list >>
    from_column_opt_2: opt!(with_pos!(ws!(preceded!(tag!("from"), expr)))) >>
    no_drop_opt: opt!(ws!(tag!("nodrop"))) >>
    ( InlineOperator::Parse{
        pattern: Keyword::new_wildcard(pattern.to_string()),
        fields: vars,
        input_column: (from_column_opt, from_column_opt_2),
        no_drop: no_drop_opt.is_some()
        } )
))));

named!(fields_mode<Span, FieldMode>, alt_complete!(
    map!(
        alt_complete!(tag!("+") | tag!("only") | tag!("include")),
        |_|FieldMode::Only
    ) |
    map!(
        alt_complete!(tag!("-") | tag!("except") | tag!("drop")),
        |_|FieldMode::Except
    )
));

named!(fields<Span, Positioned<InlineOperator>>, with_pos!(ws!(do_parse!(
    tag!("fields") >>
    mode: opt!(fields_mode) >>
    fields: var_list >>
    (
        InlineOperator::Fields {
            mode: mode.unwrap_or(FieldMode::Only),
            fields
        }
    )
))));

named!(arg_list<Span, Positioned<Vec<Expr>>>, add_return_error!(
    SyntaxErrors::StartOfError.into(), with_pos!(delimited!(
        tag!("("),
        ws!(separated_list!(tag!(","), ws!(expr))),
        return_error!(SyntaxErrors::MissingParen.into(), tag!(")"))))
));

named!(count<Span, Positioned<AggregateFunction>>, with_pos!(map!(tag!("count"),
    |_s|AggregateFunction::Count{}))
);

named!(min<Span, Positioned<AggregateFunction>>, with_pos!(ws!(do_parse!(
    tag!("min") >>
    column: delimited!(tag!("("), expr,tag!(")")) >>
    (AggregateFunction::Min{column})
))));

named!(average<Span, Positioned<AggregateFunction>>, with_pos!(ws!(do_parse!(
    alt_complete!(tag!("avg") | tag!("average")) >>
    column: delimited!(tag!("("), expr ,tag!(")")) >>
    (AggregateFunction::Average{column})
))));

named!(max<Span, Positioned<AggregateFunction>>, with_pos!(ws!(do_parse!(
    tag!("max") >>
    column: delimited!(tag!("("), expr,tag!(")")) >>
    (AggregateFunction::Max{column})
))));

named!(count_distinct<Span, Positioned<AggregateFunction>>, with_pos!(ws!(do_parse!(
    tag!("count_distinct") >>
    column: opt!(arg_list) >>
    (AggregateFunction::CountDistinct{ column })
))));

named!(sum<Span, Positioned<AggregateFunction>>, with_pos!(ws!(do_parse!(
    tag!("sum") >>
    column: delimited!(tag!("("), expr,tag!(")")) >>
    (AggregateFunction::Sum{column})
))));

fn is_digit_char(digit: char) -> bool {
    is_digit(digit as u8)
}

named!(pct_fn<Span, Span>, preceded!(
    alt_complete!(tag!("pct") | tag!("percentile") | tag!("p")),
    take_while_m_n!(2, 2, is_digit_char)
));

named!(p_nn<Span, Positioned<AggregateFunction>>, ws!(
    with_pos!(do_parse!(
        pct: pct_fn >>
        column: delimited!(tag!("("), expr,tag!(")")) >>
        (AggregateFunction::Percentile{
            column,
            percentile: (".".to_owned() + pct.fragment.0).parse::<f64>().unwrap(),
            percentile_str: pct.fragment.0.to_string()
        })
    ))
));

fn string_from_located_span(
    span: LocatedSpan<impl nom::AsBytes>,
) -> Result<String, std::string::FromUtf8Error> {
    String::from_utf8(span.fragment.as_bytes().to_vec())
}

named!(alias<Span, Operator>, map!(
    with_pos!(ws!(do_parse!(
        config: map_res!(map_res!(nom::alpha, string_from_located_span), AliasConfig::matching_string) >>
        (config.render())
    ))),
    Operator::RenderedAlias
));

named!(inline_operator<Span, Operator>, do_parse!(
    peek!(did_you_mean_operator) >>
    res: map!(alt_complete!(parse | json | logfmt | fields | whre | limit | total | split), Operator::Inline) >>
    (res)
));

named!(aggregate_function<Span, Positioned<AggregateFunction>>, do_parse!(
    peek!(did_you_mean_aggregate) >>
    res: alt_complete!(
        count_distinct |
        count |
        min |
        average |
        max |
        sum |
        p_nn) >> (res)
));

named!(operator<Span, Operator>, alt_complete!(
    map!(field_expr, Operator::Inline) |
    inline_operator | sort | alias | multi_aggregate_operator)
);

// count by x,y
// avg(foo) by x

fn default_output(func: &Positioned<AggregateFunction>) -> String {
    match func.into() {
        AggregateFunction::Count { .. } => "_count".to_string(),
        AggregateFunction::Sum { .. } => "_sum".to_string(),
        AggregateFunction::Min { .. } => "_min".to_string(),
        AggregateFunction::Average { .. } => "_average".to_string(),
        AggregateFunction::Max { .. } => "_max".to_string(),
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
            rename_opt.unwrap_or_else(||default_output(&agg_function)),
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
        key_cols: key_cols_opt
            .unwrap_or_default()
            .iter().cloned().map(|col|col.1).collect(),
        aggregate_functions: agg_functions,
     })))
));

named!(sort_mode<Span, SortMode>, alt_complete!(
    map!(
        alt_complete!(tag!("asc") | tag!("ascending")),
        |_|SortMode::Ascending
    ) |
    map!(
        alt_complete!(tag!("desc") | tag!("dsc") | tag!("descending")),
        |_|SortMode::Descending
    )
));

named!(sort<Span, Operator>, ws!(do_parse!(
    tag!("sort") >>
    key_cols_opt: opt!(preceded!(opt!(tag!("by")), sourced_expr_list)) >>
    dir: opt!(sort_mode) >>
    (Operator::Sort(SortOperator{
        sort_cols: key_cols_opt.unwrap_or_default().into_iter().map(|(_source,expr)|expr).collect(),
        direction: dir.unwrap_or(SortMode::Ascending) ,
     })))
));

named!(filter_explicit_and<Span, Search>, do_parse!(
    peek!(ws!(pair!(low_filter, alt!(tag!("AND") | tag!("and"))))) >>
    res: map!(
        return_error!(SyntaxErrors::InvalidBooleanExpression.into(),
            separated_pair!(ws!(low_filter),
            ws!(tag!("AND")),
            ws!(low_filter))), |(l,r)|Search::And(vec![l, r])
    ) >> (res)
));

named!(filter_explicit_or<Span, Search>, do_parse!(
    peek!(ws!(pair!(low_filter, alt!(tag!("OR") | tag!("or"))))) >>
    res: map!(
        return_error!(SyntaxErrors::InvalidBooleanExpression.into(),
            separated_pair!(ws!(mid_filter),
            ws!(tag!("OR")),
            ws!(mid_filter))), |(l,r)|Search::Or(vec![l, r])
    ) >> (res)
));

// Top level:
// Look for OR
// Look for AND
// Implicit AND

/* A list of things is implicitly ANDed together */
named!(filter_implicit_and<Span, Search>, map!(
    separated_nonempty_list!(multispace, low_filter),
    // An empty keyword would match everything, so there's no reason to
    |mut v| {
        v.retain( | k | ! k.no_op());
        Search::And(v)
    }
));

named!(filter_atom<Span, Search>, alt_complete!(
    map!(quoted_string, |s| Search::Keyword(Keyword::new_exact(s.to_string()))) |
    map!(keyword, |s| Search::Keyword(Keyword::new_wildcard(s.trim_matches('*').to_string())))
));

named!(high_filter<Span, Search>, alt_complete!(
  filter_explicit_or |
  mid_filter
));

named!(mid_filter<Span, Search>, alt_complete!(
  filter_explicit_and |
  low_filter
));

named!(low_filter<Span, Search>, alt_complete!(
  filter_not
  | filter_atom
  | delimited!(ws!(tag!("(")), high_filter, ws!(tag!(")")))
));

named!(filter_not<Span, Search>, map!(
        ws!(preceded!(tag!("NOT"), low_filter)),
        |k|Search::Not(Box::new(k))
    )
);

named!(filter_expr<Span, Search>,
    ws!(exact!(alt_complete!(
        filter_explicit_or |
        filter_explicit_and |
        ws!(filter_implicit_and)
))));

named!(query_component<Span, Span>, alt_complete!(
        recognize!(quoted_string) |
        tag!("AND") |
        tag!("NOT") |
        tag!("OR") |
        tag!("(") |
        tag!(")") |
        recognize!(keyword)
    )
);

named!(end_of_query<Span, Span>, alt!(
    ws!(tag!("|")) | ws!(eof!())
));

named!(query_section<Span, Span>, recognize!(ws!(
    many_till!(ws!(query_component), peek!(ws!(end_of_query))))
));

named!(erroring_filter<Span, Search>, add_return_error!(SyntaxErrors::InvalidFilter.into(), filter_expr));

named!(prelexed_query<Span, Search>, ws!(do_parse!(
    res_tuple: flat_map!(query_section, erroring_filter) >>
    (res_tuple)
)));

named!(pub query<Span, Query, SyntaxErrors>, fix_error!(SyntaxErrors, exact!(ws!(do_parse!(
    filter: prelexed_query >>
    operators: ws!(opt!(preceded!(tag!("|"), operator_list))) >>
    (Query{
        search: filter,
        operators: operators.unwrap_or_default()
    }))
))));

named!(pub operator_list<Span, Vec<Operator>>, ws!(separated_nonempty_list!(tag!("|"), operator)));

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
                Err(e) => panic!(format!(
                    "Parse failed, but was expected to succeed: \n{:?}",
                    e
                )),
            }
        }};
    }

    macro_rules! expect_fail {
        ($f:expr, $inp:expr) => {{
            let parse_result = $f(Span::new(CompleteStr($inp)));
            match parse_result {
                Ok(_res) => panic!(format!("Expected parse to fail, but it succeeded")),
                // TODO: enable assertions of specific errors
                Err(_e) => (),
            }
        }};
    }

    #[test]
    fn empty_query_is_query() {
        expect!(prelexed_query, " * ", Search::And(vec![]));
        expect!(prelexed_query, "*", Search::And(vec![]));
        expect!(
            prelexed_query,
            "abc ",
            Search::And(vec![Search::Keyword(Keyword::new_wildcard(
                "abc".to_string()
            ))])
        );
        expect!(
            prelexed_query,
            "(abc )",
            Search::And(vec![Search::Keyword(Keyword::new_wildcard(
                "abc".to_string()
            ))])
        );
    }

    #[test]
    fn parse_keyword_string() {
        expect!(keyword, "abc", "abc".to_string());
        expect!(keyword, "one-two-three", "one-two-three".to_string());
    }

    #[test]
    fn parse_quoted_string() {
        expect!(quoted_string, "\"hello\"", "hello");
        expect!(quoted_string, "'hello'", "hello");
        expect!(quoted_string, r#""test = [*=*] * ""#, "test = [*=*] * ");
        expect_fail!(quoted_string, "\"hello'");
    }

    #[test]
    fn parse_expr() {
        expect!(
            expr,
            "a == b",
            Expr::Binary {
                op: BinaryOp::Comparison(ComparisonOp::Eq),
                left: Box::new(Expr::column("a")),
                right: Box::new(Expr::column("b")),
            }
        );
    }

    #[test]
    fn parse_func_call() {
        expect!(
            field_expr,
            "parseDate(abc) as foo",
            Positioned {
                start_pos: QueryPosition(0),
                end_pos: QueryPosition(21),
                value: InlineOperator::FieldExpression {
                    value: Expr::FunctionCall {
                        name: "parseDate".to_string(),
                        args: vec!(Expr::Column {
                            head: DataAccessAtom::Key("abc".to_string()),
                            rest: vec!()
                        })
                    },
                    name: "foo".to_string()
                }
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
                left: Box::new(Expr::column("a")),
                right: Box::new(Expr::Value(data::Value::Str("b".to_string()))),
            }
        );
    }

    #[test]
    fn parse_expr_ident() {
        expect!(expr, "foo", Expr::column("foo"));
        expect!(
            expr,
            "foo.bar.baz[-10]",
            Expr::Column {
                head: DataAccessAtom::Key("foo".to_owned()),
                rest: vec![
                    DataAccessAtom::Key("bar".to_owned()),
                    DataAccessAtom::Key("baz".to_owned()),
                    DataAccessAtom::Index(-10)
                ]
            }
        )
    }

    #[test]
    fn parse_expr_precedence() {
        expect!(
            expr,
            "2 * 3 + 4 * 5 < 4 / 2 + 1",
            Expr::Binary {
                left: Box::new(Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left: Box::new(Expr::Value(data::Value::Int(2))),
                        op: BinaryOp::Arithmetic(ArithmeticOp::Multiply),
                        right: Box::new(Expr::Value(data::Value::Int(3))),
                    }),
                    op: BinaryOp::Arithmetic(ArithmeticOp::Add),
                    right: Box::new(Expr::Binary {
                        left: Box::new(Expr::Value(data::Value::Int(4))),
                        op: BinaryOp::Arithmetic(ArithmeticOp::Multiply),
                        right: Box::new(Expr::Value(data::Value::Int(5))),
                    }),
                }),
                op: BinaryOp::Comparison(ComparisonOp::Lt),
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left: Box::new(Expr::Value(data::Value::Int(4))),
                        op: BinaryOp::Arithmetic(ArithmeticOp::Divide),
                        right: Box::new(Expr::Value(data::Value::Int(2))),
                    }),
                    op: BinaryOp::Arithmetic(ArithmeticOp::Add),
                    right: Box::new(Expr::Value(data::Value::Int(1))),
                })
            }
        );
    }

    #[test]
    fn parse_ident() {
        expect!(ident, "hello123", "hello123".to_string());
        expect!(ident, "x", "x".to_string());
        expect!(ident, "_x", "_x".to_string());
        expect_fail!(ident, "5x");
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
            Positioned {
                start_pos: QueryPosition(0),
                end_pos: QueryPosition(20),
                value: InlineOperator::Parse {
                    pattern: Keyword::new_wildcard("[key=*]".to_string()),
                    fields: vec!["v".to_string()],
                    input_column: (None, None),
                    no_drop: false
                }
            }
        );
        expect!(
            parse,
            r#"parse "[key=*]" as v nodrop"#,
            Positioned {
                start_pos: QueryPosition(0),
                end_pos: QueryPosition(27),
                value: InlineOperator::Parse {
                    pattern: Keyword::new_wildcard("[key=*]".to_string()),
                    fields: vec!["v".to_string()],
                    input_column: (None, None),
                    no_drop: true
                }
            }
        );
        expect!(
            parse,
            r#"parse "[key=*][val=*]" as k,v nodrop"#,
            Positioned {
                start_pos: QueryPosition(0),
                end_pos: QueryPosition(36),
                value: InlineOperator::Parse {
                    pattern: Keyword::new_wildcard("[key=*][val=*]".to_string()),
                    fields: vec!["k".to_string(), "v".to_string()],
                    input_column: (None, None),
                    no_drop: true
                }
            }
        );
    }

    #[test]
    fn parse_operator() {
        expect!(
            operator,
            r#" 1 as one"#,
            Operator::Inline(Positioned {
                start_pos: QueryPosition(1),
                end_pos: QueryPosition(9),
                value: InlineOperator::FieldExpression {
                    value: Expr::Value(data::Value::Int(1)),
                    name: "one".to_string(),
                },
            })
        );
        expect!(
            operator,
            "  json",
            Operator::Inline(Positioned {
                start_pos: QueryPosition(2),
                end_pos: QueryPosition(6),
                value: InlineOperator::Json { input_column: None }
            })
        );
        expect!(
            operator,
            "  logfmt",
            Operator::Inline(Positioned {
                start_pos: QueryPosition(2),
                end_pos: QueryPosition(8),
                value: InlineOperator::Logfmt { input_column: None }
            })
        );
        expect!(
            operator,
            r#" parse "[key=*]" from field as v"#,
            Operator::Inline(Positioned {
                start_pos: QueryPosition(1),
                end_pos: QueryPosition(32),
                value: InlineOperator::Parse {
                    pattern: Keyword::new_wildcard("[key=*]".to_string()),
                    fields: vec!["v".to_string()],
                    input_column: (
                        Some(Positioned {
                            start_pos: QueryPosition(17),
                            end_pos: QueryPosition(28),
                            value: Expr::column("field")
                        }),
                        None
                    ),
                    no_drop: false
                },
            })
        );
        expect!(
            operator,
            r#" parse "[key=*]" as v from field"#,
            Operator::Inline(Positioned {
                start_pos: QueryPosition(1),
                end_pos: QueryPosition(32),
                value: InlineOperator::Parse {
                    pattern: Keyword::new_wildcard("[key=*]".to_string()),
                    fields: vec!["v".to_string()],
                    input_column: (
                        None,
                        Some(Positioned {
                            start_pos: QueryPosition(22),
                            end_pos: QueryPosition(32),
                            value: Expr::column("field")
                        })
                    ),
                    no_drop: false
                },
            })
        );
    }

    #[test]
    fn parse_limit() {
        expect!(
            operator,
            " limit",
            Operator::Inline(Positioned {
                start_pos: QueryPosition(1),
                end_pos: QueryPosition(6),
                value: InlineOperator::Limit { count: None }
            })
        );
        expect!(
            operator,
            " limit 5",
            Operator::Inline(Positioned {
                start_pos: QueryPosition(1),
                end_pos: QueryPosition(8),
                value: InlineOperator::Limit {
                    count: Some(Positioned {
                        value: 5.0,
                        start_pos: QueryPosition(7),
                        end_pos: QueryPosition(8)
                    })
                }
            })
        );
        expect!(
            operator,
            " limit -5",
            Operator::Inline(Positioned {
                start_pos: QueryPosition(1),
                end_pos: QueryPosition(9),
                value: InlineOperator::Limit {
                    count: Some(Positioned {
                        value: -5.0,
                        start_pos: QueryPosition(7),
                        end_pos: QueryPosition(9)
                    }),
                }
            })
        );
        expect!(
            operator,
            " limit 1e2",
            Operator::Inline(Positioned {
                start_pos: QueryPosition(1),
                end_pos: QueryPosition(10),
                value: InlineOperator::Limit {
                    count: Some(Positioned {
                        value: 1e2,
                        start_pos: QueryPosition(7),
                        end_pos: QueryPosition(10)
                    })
                }
            })
        );
    }

    #[test]
    fn parse_agg_operator() {
        expect!(
            multi_aggregate_operator,
            "count as renamed by x, y",
            Operator::MultiAggregate(MultiAggregateOperator {
                key_cols: vec![Expr::column("x"), Expr::column("y")],
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
                        column: Expr::column("x"),
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
                search: Search::And(vec![]),
                operators: vec![],
            }
        );
        expect!(
            query,
            " filter ",
            Query {
                search: Search::And(vec![Search::Keyword(Keyword::new_wildcard(
                    "filter".to_string()
                ))]),
                operators: vec![],
            }
        );
        expect!(
            query,
            " *abc* ",
            Query {
                search: Search::And(vec![Search::Keyword(Keyword::new_wildcard(
                    "abc".to_string()
                ))]),
                operators: vec![],
            }
        );
        expect!(
            query,
            " abc def \"*ghi*\" ",
            Query {
                search: Search::And(vec![
                    Search::Keyword(Keyword::new_wildcard("abc".to_string())),
                    Search::Keyword(Keyword::new_wildcard("def".to_string())),
                    Search::Keyword(Keyword::new_exact("*ghi*".to_string())),
                ]),
                operators: vec![],
            }
        );
    }

    #[test]
    fn complex_filters() {
        expect!(
            filter_expr,
            "(abc AND def) OR xyz",
            Search::Or(vec![
                Search::And(vec![
                    Search::Keyword(Keyword::new_wildcard("abc".to_string())),
                    Search::Keyword(Keyword::new_wildcard("def".to_string()))
                ]),
                Search::Keyword(Keyword::new_wildcard("xyz".to_string()))
            ])
        );
    }

    #[test]
    fn invalid_filters() {
        expect_fail!(query, "abc AND \"");
        // Would be nice to support this
        expect_fail!(query, "a OR b OR C");
    }

    #[test]
    fn or_filter() {
        expect!(
            filter_expr,
            "abc OR (def OR xyz)",
            Search::Or(vec![
                Search::Keyword(Keyword::new_wildcard("abc".to_string())),
                Search::Or(vec![
                    Search::Keyword(Keyword::new_wildcard("def".to_string())),
                    Search::Keyword(Keyword::new_wildcard("xyz".to_string()))
                ])
            ])
        );
    }

    #[test]
    fn not_filter() {
        expect!(
            filter_not,
            "NOT abc",
            Search::Not(Box::new(Search::Keyword(Keyword::new_wildcard(
                "abc".to_string()
            ))))
        );
        expect!(
            filter_expr,
            "NOT abc",
            Search::And(vec![Search::Not(Box::new(Search::Keyword(
                Keyword::new_wildcard("abc".to_string())
            )))])
        );
        expect!(
            filter_expr,
            "(error OR warn) AND NOT hide",
            Search::And(vec![
                Search::Or(vec![
                    Search::Keyword(Keyword::new_wildcard("error".to_string())),
                    Search::Keyword(Keyword::new_wildcard("warn".to_string()))
                ]),
                Search::Not(Box::new(Search::Keyword(Keyword::new_wildcard(
                    "hide".to_string()
                )))),
            ])
        );
    }

    #[test]
    fn query_operators() {
        let query_str = r#"* | json from col | parse "!123*" as foo | count by foo, foo == 123 | sort by foo dsc "#;
        expect!(
            query,
            query_str,
            Query {
                search: Search::And(vec![]),
                operators: vec![
                    Operator::Inline(Positioned {
                        start_pos: QueryPosition(4),
                        end_pos: QueryPosition(18),
                        value: InlineOperator::Json {
                            input_column: Some(Expr::column("col")),
                        }
                    }),
                    Operator::Inline(Positioned {
                        start_pos: QueryPosition(20),
                        end_pos: QueryPosition(41),
                        value: InlineOperator::Parse {
                            pattern: Keyword::new_wildcard("!123*".to_string()),
                            fields: vec!["foo".to_string()],
                            input_column: (None, None),
                            no_drop: false
                        }
                    }),
                    Operator::MultiAggregate(MultiAggregateOperator {
                        key_col_headers: vec!["foo".to_string(), "foo == 123".to_string()],
                        key_cols: vec![
                            Expr::column("foo"),
                            Expr::Binary {
                                op: BinaryOp::Comparison(ComparisonOp::Eq),
                                left: Box::new(Expr::column("foo")),
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
                        sort_cols: vec![Expr::column("foo")],
                        direction: SortMode::Descending,
                    }),
                ],
            }
        );
    }

    #[test]
    fn logfmt_operator() {
        let query_str = r#"* | logfmt from col | sort by foo dsc "#;
        expect!(
            query,
            query_str,
            Query {
                search: Search::And(vec![]),
                operators: vec![
                    Operator::Inline(Positioned {
                        start_pos: QueryPosition(4),
                        end_pos: QueryPosition(20),
                        value: InlineOperator::Logfmt {
                            input_column: Some(Expr::column("col")),
                        }
                    }),
                    Operator::Sort(SortOperator {
                        sort_cols: vec![Expr::column("foo")],
                        direction: SortMode::Descending,
                    }),
                ],
            }
        );
    }
}
