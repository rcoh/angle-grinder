use ag::alias::AliasCollection;
use ag::pipeline::{ErrorReporter, OutputMode, Pipeline, QueryContainer, TermErrorReporter};
use annotate_snippets::display_list::FormatOptions;
use annotate_snippets::snippet::{Annotation, AnnotationType, Slice, Snippet};
use human_panic::setup_panic;

use clap::Parser;
#[cfg(feature = "self_update")]
use self_update;
use std::fs::File;
use std::io;
use std::io::{stdout, BufReader};
use std::path::PathBuf;
use thiserror::Error;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use crate::InvalidArgs::{CantSupplyBoth, InvalidFormatString, InvalidOutputMode};

#[derive(Debug, Parser)]
#[command(
    version,
    after_help = "For more details + docs, see https://github.com/rcoh/angle-grinder"
)]
struct Cli {
    /// The query
    #[arg(group = "main")]
    query: Option<String>,

    #[cfg(feature = "self_update")]
    /// Update agrind to the latest published version Github (https://github.com/rcoh/angle-grinder)
    #[arg(long = "self-update", group = "main")]
    update: bool,

    /// Optionally reads from a file instead of Stdin
    #[arg(long = "file", short = 'f')]
    file: Option<String>,

    /// DEPRECATED. Use -o format=... instead. Provide a Rust std::fmt string to format output
    #[arg(long = "format", short = 'm')]
    format: Option<String>,

    /// Set output format. One of (json|legacy|format=<rust fmt str>|logfmt)
    #[arg(
        long = "output",
        short = 'o',
        long_help = "Set output format. Options: \n\
                     - `json`,\n\
                     - `logfmt`\n\
                     - `format=<rust format string>` (eg. -o format='{src} => {dst}'\n\
                     - `legacy` The original output format, auto aligning [k=v]"
    )]
    output: Option<String>,

    #[arg(
        long = "alias-dir",
        short = 'a',
        long_help = "Specifies an alternative directory to use for aliases. Defaults to `.agrind-aliases` in all parent directories."
    )]
    alias_dir: Option<PathBuf>,

    #[arg(long = "no-alias", long_help = "Disables aliases")]
    no_alias: bool,
}

#[derive(Debug, Error)]
pub enum InvalidArgs {
    #[error("Query was missing. Usage: `agrind 'query'`")]
    MissingQuery,

    #[error("Invalid output mode {}. Valid choices: {}", choice, choices)]
    InvalidOutputMode { choice: String, choices: String },

    #[error("Invalid format string. Expected something like `-o format='{{src}} => {{dst}}'`")]
    InvalidFormatString,

    #[error("Can't supply a format string and an output mode")]
    CantSupplyBoth,

    #[error("Can't disable aliases and also set a directory")]
    CantDisableAndOverride,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_panic!();
    let args = Cli::parse();
    #[cfg(feature = "self_update")]
    if args.update {
        return update();
    }
    let (aliases, errors) = match (args.alias_dir, args.no_alias) {
        (Some(dir), false) => AliasCollection::load_aliases_from_dir(&dir)?,
        (None, false) => AliasCollection::load_aliases_ancestors(None)?,
        (Some(_), true) => return Err(InvalidArgs::CantDisableAndOverride.into()),
        (None, true) => (AliasCollection::default(), vec![]),
    };
    let error_reporter = Box::new(TermErrorReporter {});
    for error in errors {
        error_reporter.handle_error(Snippet {
            title: Some(Annotation {
                id: None,
                label: Some(&format!("invalid alias: {}", error.cause)),
                annotation_type: AnnotationType::Warning,
            }),
            footer: vec![],
            slices: vec![Slice {
                source: "",
                line_start: 0,
                origin: Some(error.path.to_str().unwrap()),
                annotations: vec![],
                fold: true,
            }],
            opt: FormatOptions::default(),
        });
    }
    let query = QueryContainer::new_with_aliases(
        args.query.ok_or(InvalidArgs::MissingQuery)?,
        error_reporter,
        aliases,
    );
    let output_mode = match (args.output, args.format) {
        (Some(_output), Some(_format)) => Err(CantSupplyBoth),
        (Some(output), None) => parse_output(&output),
        (None, Some(format)) => Ok(OutputMode::Format(format)),
        (None, None) => parse_output("legacy"),
    }?;
    let pipeline = Pipeline::new(&query, stdout(), output_mode)?;
    match args.file {
        Some(file_name) => {
            let f = File::open(file_name)?;
            pipeline.process(BufReader::new(f))
        }
        None => {
            let stdin = io::stdin();
            let locked = stdin.lock();
            pipeline.process(locked)
        }
    };
    Ok(())
}

fn parse_output(output_param: &str) -> Result<OutputMode, InvalidArgs> {
    // for some args, we split on `=` first
    let (arg, val) = match output_param.find('=') {
        None => (output_param, "="),
        Some(idx) => output_param.split_at(idx),
    };
    let val = &val[1..];

    match (arg, val) {
        ("legacy", "") => Ok(OutputMode::Legacy),
        ("json", "") => Ok(OutputMode::Json),
        ("logfmt", "") => Ok(OutputMode::Logfmt),
        ("format", v) if !v.is_empty() => Ok(OutputMode::Format(v.to_owned())),
        ("format", "") => Err(InvalidFormatString),
        (other, _v) => Err(InvalidOutputMode {
            choice: other.to_owned(),
            choices: "legacy, json, logfmt, format".to_owned(),
        }),
    }
}

#[cfg(feature = "self_update")]
fn update() -> Result<(), Box<dyn std::error::Error>> {
    let crate_version = self_update::cargo_crate_version!();
    let status = self_update::backends::github::Update::configure()
        .repo_owner("rcoh")
        .repo_name("angle-grinder")
        .bin_name("agrind")
        .show_download_progress(true)
        .current_version(crate_version)
        .build()?
        .update()?;

    if crate_version == status.version() {
        println!(
            "Currently running the latest version publicly available ({}). No changes",
            status.version()
        );
    } else {
        println!("Updated to version: {}", status.version());
    }
    Ok(())
}
