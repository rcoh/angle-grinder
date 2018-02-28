extern crate ag;
extern crate clap;
use ag::pipeline;
use clap::{App, Arg};
use std::io;
fn main() {
    let matches = App::new("angle-grinder")
        .version("0.1.0")
        .author("Russell Cohen <rcoh@rcoh.me>")
        .about("Slice and dice log files on the command line")
        .arg(
            Arg::with_name("query")
                .help("ag query string eg: `...`")
                .required(true)
                .index(1),
        )
        .get_matches();
    let query_str = matches.value_of("query").unwrap();
    match pipeline::Pipeline::new(query_str) {
        Result::Ok(mut pipeline) => pipeline.process(io::stdin()),
        Result::Err(e) => println!("{}", e)
    }
}
