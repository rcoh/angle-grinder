extern crate ag;
#[macro_use]
extern crate quicli;

use ag::pipeline::Pipeline;
use quicli::prelude::*;
use std::fs::File;
use std::io;
use std::io::BufReader;

#[derive(Debug, StructOpt)]
struct Cli {
    /// Query
    query: String,
    // Add a positional argument that the user has to supply:
    /// Optionally reads from a file instead of Stdin
    #[structopt(long = "file", short = "f")]
    file: Option<String>,
    /// Pass many times for more log output
    #[structopt(long = "verbose", short = "v", parse(from_occurrences))]
    verbosity: u8,
}

main!(|args: Cli, log_level: verbosity| {
    let pipeline = Pipeline::new(&args.query)?;
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
    }
});
