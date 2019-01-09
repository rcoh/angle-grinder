use ag::pipeline::{ErrorReporter, Pipeline, QueryContainer};
use annotate_snippets::snippet::Snippet;
use human_panic::setup_panic;
use quicli::prelude::*;
use self_update;
use self_update::cargo_crate_version;
use std::env;
use std::fs::File;
use std::io;
use std::io::BufReader;
use structopt::StructOpt;

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

    /// Update agrind to the latest published version Github (https://github.com/rcoh/angle-grinder)
    #[structopt(long = "self-update", group = "main")]
    update: bool,

    /// Optionally reads from a file instead of Stdin
    #[structopt(long = "file", short = "f")]
    file: Option<String>,
    #[structopt(flatten)]
    verbosity: Verbosity,
}

#[derive(Debug, Fail)]
pub enum InvalidArgs {
    #[fail(display = "Query was missing. Usage: `agrind 'query'`")]
    MissingQuery,
}

/// An ErrorReporter that writes errors related to the query string to the terminal
struct TermErrorReporter {
    formatter: annotate_snippets::formatter::DisplayListFormatter,
}

impl ErrorReporter for TermErrorReporter {
    fn handle_error(&self, snippet: Snippet) {
        let dl = annotate_snippets::display_list::DisplayList::from(snippet);

        eprintln!("{}", self.formatter.format(&dl));
    }
}

fn main() -> CliResult {
    setup_panic!();
    let args = Cli::from_args();
    if args.update {
        return update();
    }
    let query = QueryContainer::new(
        args.query.ok_or(InvalidArgs::MissingQuery)?,
        Box::new(TermErrorReporter {
            formatter: annotate_snippets::formatter::DisplayListFormatter::new(
                env::var("NO_COLOR").is_err(),
            ),
        }),
    );
    args.verbosity.setup_env_logger("agrind")?;
    let pipeline = Pipeline::new(&query)?;
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

fn update() -> CliResult {
    let target = self_update::get_target()?;
    let status = self_update::backends::github::Update::configure()?
        .repo_owner("rcoh")
        .repo_name("angle-grinder")
        .target(&target)
        .bin_name("agrind")
        .show_download_progress(true)
        .current_version(cargo_crate_version!())
        .build()?
        .update()?;

    if cargo_crate_version!() == status.version() {
        println!(
            "Currently running a new version than publicly available ({}). No changes",
            status.version()
        );
    } else {
        println!("Updated to version: {}", status.version());
    }
    Ok(())
}
