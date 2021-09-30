use ag::pipeline::{OutputMode, Pipeline, QueryContainer, TermErrorReporter};
use human_panic::setup_panic;
use quicli::prelude::*;

#[cfg(feature = "self_update")]
use self_update;
use std::fs::File;
use std::io;
use std::io::{stdout, BufReader};
use structopt::StructOpt;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use crate::InvalidArgs::{CantSupplyBoth, InvalidFormatString, InvalidOutputMode};
use structopt::clap::ArgGroup;

// Needed to require either "--self-update" or a query
fn main_arg_group() -> ArgGroup<'static> {
    ArgGroup::with_name("main").required(true)
}

#[derive(Debug, StructOpt)]
#[structopt(
    after_help = "For more details + docs, see https://github.com/rcoh/angle-grinder",
    raw(group = "main_arg_group()")
)]
struct Cli {
    /// The query
    #[structopt(group = "main")]
    query: Option<String>,

    #[cfg(feature = "self_update")]
    /// Update agrind to the latest published version Github (https://github.com/rcoh/angle-grinder)
    #[structopt(long = "self-update", group = "main")]
    update: bool,

    /// Optionally reads from a file instead of Stdin
    #[structopt(long = "file", short = "f")]
    file: Option<String>,

    /// DEPRECATED. Use -o format=... instead. Provide a Rust std::fmt string to format output
    #[structopt(long = "format", short = "m")]
    format: Option<String>,

    /// Set output format. One of (json|legacy|format=<rust fmt str>|logfmt)
    #[structopt(
        long = "output",
        short = "o",
        long_help = "Set output format. Options: \n\
                     - `json`,\n\
                     - `logfmt`\n\
                     - `format=<rust format string>` (eg. -o format='{src} => {dst}'\n\
                     - `legacy` The original output format, auto aligning [k=v]"
    )]
    output: Option<String>,

    #[structopt(flatten)]
    verbosity: Verbosity,
}

#[derive(Debug, Fail)]
pub enum InvalidArgs {
    #[fail(display = "Query was missing. Usage: `agrind 'query'`")]
    MissingQuery,

    #[fail(display = "Invalid output mode {}. Valid choices: {}", choice, choices)]
    InvalidOutputMode { choice: String, choices: String },

    #[fail(
        display = "Invalid format string. Expected something like `-o format='{{src}} => {{dst}}'`"
    )]
    InvalidFormatString,

    #[fail(display = "Can't supply a format string and an output mode")]
    CantSupplyBoth,
}

fn main() -> CliResult {
    setup_panic!();
    let args = Cli::from_args();
    #[cfg(feature = "self_update")]
    if args.update {
        return update();
    }
    let query = QueryContainer::new(
        args.query.ok_or(InvalidArgs::MissingQuery)?,
        Box::new(TermErrorReporter {}),
    );
    args.verbosity.setup_env_logger("agrind")?;
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
fn update() -> CliResult {
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
