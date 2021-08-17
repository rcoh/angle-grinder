use crate::lang::{query, Positioned, Query};
use annotate_snippets::snippet::{Annotation, AnnotationType, Slice, Snippet, SourceAnnotation};
use atty::Stream;
use std::env;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use strsim::normalized_levenshtein;

/// Container for the query string that can be used to parse and report errors.
pub struct QueryContainer {
    pub query: String,
    pub reporter: Box<dyn ErrorReporter>,
    pub error_count: AtomicUsize,
}

/// Trait that can be used to report errors by the parser and other layers.
pub trait ErrorBuilder {
    /// Create a SnippetBuilder for the given error
    fn report_error_for<E: ToString>(&self, error: E) -> SnippetBuilder;

    fn get_error_count(&self) -> usize;
}

impl QueryContainer {
    pub fn new(query: String, reporter: Box<dyn ErrorReporter>) -> QueryContainer {
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

impl ErrorBuilder for QueryContainer {
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

/// Callback for handling error Snippets.
pub trait ErrorReporter {
    fn handle_error(&self, _snippet: Snippet) {}
}

/// An ErrorReporter that writes errors related to the query string to the terminal
pub struct TermErrorReporter {}

impl ErrorReporter for TermErrorReporter {
    fn handle_error(&self, mut snippet: Snippet) {
        snippet.opt.color = env::var("NO_COLOR").is_err() && atty::is(Stream::Stderr);
        let dl = annotate_snippets::display_list::DisplayList::from(snippet);

        eprintln!("{}", dl);
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

#[must_use = "the send_report() method must eventually be called for this builder"]
pub struct SnippetBuilder<'a> {
    query: &'a QueryContainer,
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
