use crate::lang::{query, Positioned, Query, VALID_AGGREGATES, VALID_INLINE, VALID_OPERATORS};
use annotate_snippets::snippet::{Annotation, AnnotationType, Slice, Snippet, SourceAnnotation};
use failure::Fail;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::convert::From;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use strsim::normalized_levenshtein;

/// Container for the query string that can be used to parse and report errors.
pub struct QueryContainer<'a> {
    pub query: String,
    pub reporter: Box<dyn ErrorReporter + 'a>,
    pub error_count: AtomicUsize,
}

/// Common syntax errors.
#[derive(PartialEq, Debug, FromPrimitive, Fail, Clone)]
pub enum SyntaxErrors {
    #[fail(display = "")]
    StartOfError,
    #[fail(display = "unterminated single quoted string")]
    UnterminatedSingleQuotedString,
    #[fail(display = "unterminated double quoted string")]
    UnterminatedDoubleQuotedString,
    #[fail(display = "expecting an operand for binary operator")]
    MissingOperand,
    #[fail(display = "expecting a name for the field")]
    MissingName,
    #[fail(display = "expecting close parentheses")]
    MissingParen,

    #[fail(display = "Expected an operator")]
    NotAnOperator,

    #[fail(display = "Not an aggregate operator")]
    NotAnAggregateOperator,

    #[fail(display = "Invalid filter")]
    InvalidFilter,

    #[fail(display = "Invalid boolean expression")]
    InvalidBooleanExpression,
}

/// Trait that can be used to report errors by the parser and other layers.
pub trait ErrorBuilder {
    /// Create a SnippetBuilder for the given error
    fn report_error_for<E: ToString>(&self, error: E) -> SnippetBuilder;

    fn get_error_count(&self) -> usize;
}

impl<'a> QueryContainer<'a> {
    pub fn new(query: String, reporter: Box<dyn ErrorReporter + 'a>) -> QueryContainer<'a> {
        QueryContainer {
            query,
            reporter,
            error_count: AtomicUsize::new(0),
        }
    }

    /// Parse the contained query string.
    pub fn parse(&self) -> Result<Query, ()> {
        query(self)
    }
}

impl<'a> ErrorBuilder for QueryContainer<'a> {
    /// Create a SnippetBuilder for the given error
    fn report_error_for<E: ToString>(&self, error: E) -> SnippetBuilder {
        self.error_count.fetch_add(1, Ordering::Relaxed);

        SnippetBuilder {
            query: self,
            data: SnippetData {
                error: error.to_string(),
                source: self.query.to_string(),
                ..Default::default()
            },
        }
    }

    fn get_error_count(&self) -> usize {
        self.error_count.load(Ordering::Relaxed)
    }
}

pub fn did_you_mean(input: &str, choices: &[&str]) -> Option<String> {
    let similarities = choices
        .iter()
        .map(|choice| (choice, normalized_levenshtein(choice, input)));
    let mut candidates: Vec<_> = similarities.filter(|(_op, score)| *score > 0.6).collect();
    candidates.sort_by_key(|(_op, score)| (score * 100_f64) as u16);
    candidates
        .iter()
        .map(|(choice, _score)| (**choice).to_owned())
        .next()
}

impl SyntaxErrors {
    pub fn to_resolution(&self, code_fragment: &str) -> Vec<String> {
        match self {
            SyntaxErrors::InvalidFilter => vec!["Filter was invalid".to_string()],
            SyntaxErrors::StartOfError => Vec::new(),
            SyntaxErrors::NotAnOperator => {
                let mut res = vec![format!("{} is not a valid operator", code_fragment)];
                if let Some(choice) = did_you_mean(code_fragment, &VALID_OPERATORS) {
                    res.push(format!("Did you mean \"{}\"?", choice));
                }
                res
            }
            SyntaxErrors::NotAnAggregateOperator => {
                let mut res = vec![format!(
                    "{} is not a valid aggregate operator",
                    code_fragment
                )];
                if let Some(choice) = did_you_mean(code_fragment, &VALID_AGGREGATES) {
                    res.push(format!("Did you mean \"{}\"?", choice));
                }
                if VALID_INLINE.contains(&code_fragment) {
                    res.push(format!("{} is an inline operator, but only aggregate operators (count, average, etc.) are valid here", code_fragment))
                }
                res
            }

            SyntaxErrors::UnterminatedSingleQuotedString => {
                vec!["Insert a single quote (') to terminate this string".to_string()]
            }
            SyntaxErrors::UnterminatedDoubleQuotedString => {
                vec!["Insert a double quote (\") to terminate this string".to_string()]
            }
            SyntaxErrors::MissingParen => {
                vec!["Insert a right parenthesis to terminate this expression".to_string()]
            }
            SyntaxErrors::MissingOperand => {
                vec!["Add the operand or delete the operator".to_string()]
            }
            SyntaxErrors::MissingName => vec!["Give the value a name".to_string()],
            SyntaxErrors::InvalidBooleanExpression => {
                let mut base = vec![format!(
                    "The boolean expression {} is invalid",
                    code_fragment
                )];
                if code_fragment.contains("and") || code_fragment.contains("or") {
                    base.push("AND and OR must be in UPPER CASE".to_string());
                }
                base
            }
        }
    }
}

/// Converts the ordinal from the nom error object back into a SyntaxError.
impl From<u32> for SyntaxErrors {
    fn from(ord: u32) -> Self {
        // The FromPrimitive trait derived for this enum allows from_u32() to work.
        SyntaxErrors::from_u32(ord).unwrap()
    }
}

/// Callback for handling error Snippets.
pub trait ErrorReporter {
    fn handle_error(&self, _snippet: Snippet) {}
}

/// Container for data that will be used to construct a Snippet
#[derive(Default)]
pub struct SnippetData {
    error: String,
    source: String,
    annotations: Vec<((usize, usize), String)>,
    resolution: Vec<String>,
}

#[must_use = "the send_report() method must eventually be called for this builder"]
pub struct SnippetBuilder<'a> {
    query: &'a QueryContainer<'a>,
    data: SnippetData,
}

impl<'a> SnippetBuilder<'a> {
    /// Adds an annotation to a portion of the query string.  The given position will be
    /// highlighted with the accompanying label.
    pub fn with_code_pointer<T, S: ToString>(mut self, pos: &Positioned<T>, label: S) -> Self {
        self.data
            .annotations
            .push(((pos.range.start, pos.range.end), label.to_string()));
        self
    }

    /// Adds an annotation to a portion of the query string.  The given position will be
    /// highlighted with the accompanying label.
    pub fn with_code_range<S: ToString>(mut self, range: Range<usize>, label: S) -> Self {
        self.data
            .annotations
            .push(((range.start, range.end), label.to_string()));
        self
    }

    /// Add a message to help the user resolve the error.
    pub fn with_resolution<T: ToString>(mut self, resolution: T) -> Self {
        self.data.resolution.push(resolution.to_string());
        self
    }

    /// Add a message to help the user resolve the error.
    pub fn with_resolutions<T: IntoIterator<Item = String>>(mut self, resolutions: T) -> Self {
        self.data.resolution.extend(resolutions.into_iter());
        self
    }

    /// Build and send the Snippet to the ErrorReporter in the QueryContainer.
    pub fn send_report(self) {
        self.query.reporter.handle_error(Snippet {
            title: Some(Annotation {
                label: Some(self.data.error.as_str()),
                id: None,
                annotation_type: AnnotationType::Error,
            }),
            slices: vec![Slice {
                source: self.data.source.as_str(),
                line_start: 1,
                origin: None,
                fold: false,
                annotations: self
                    .data
                    .annotations
                    .iter()
                    .map(|anno| SourceAnnotation {
                        range: anno.0,
                        label: anno.1.as_str(),
                        annotation_type: AnnotationType::Error,
                    })
                    .collect(),
            }],
            footer: self
                .data
                .resolution
                .iter()
                .map(|res| Annotation {
                    label: Some(res),
                    id: None,
                    annotation_type: AnnotationType::Help,
                })
                .collect(),
            opt: Default::default(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn did_you_mean() {
        assert_eq!(
            SyntaxErrors::NotAnAggregateOperator.to_resolution("cont"),
            vec![
                "cont is not a valid aggregate operator",
                "Did you mean \"count\"?"
            ]
        );
        assert_eq!(
            SyntaxErrors::NotAnOperator.to_resolution("cont"),
            vec!["cont is not a valid operator", "Did you mean \"count\"?"]
        );
        assert_eq!(
            SyntaxErrors::NotAnAggregateOperator.to_resolution("parse"),
            vec![
                "parse is not a valid aggregate operator",
                "parse is an inline operator, but only aggregate operators (count, average, etc.) are valid here"
            ]
        );
    }
}
