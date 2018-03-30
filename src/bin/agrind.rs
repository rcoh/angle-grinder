extern crate ag;
extern crate clap;
use ag::pipeline;
use clap::{App, Arg};
use std::io;
use std::io::BufReader;
use std::fs::File;
fn main() {
    let matches = App::new("angle-grinder")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Russell Cohen <rcoh@rcoh.me>")
        .about("Slice and dice log files on the command line")
        .arg(
            Arg::with_name("query")
                .help("ag query string eg: `...`")
                .required(true)
                .index(1),
        )
        .arg(Arg::from_usage(
            "-f --file=[FILE] 'Optionally reads from a file instead of Stdin'",
        ))
        .get_matches();
    let query_str = matches.value_of("query").unwrap();
    match pipeline::Pipeline::new(query_str) {
        Result::Ok(mut pipeline) => match matches.value_of("file") {
            None => {
                let stdin = io::stdin();
                let locked = stdin.lock();
                pipeline.process(locked);
            }
            Some(file) => {
                let f = File::open(file).unwrap();
                let f = BufReader::new(f);
                pipeline.process(f);
            }
        },
        Result::Err(e) => println!("{}", e),
    };
}
