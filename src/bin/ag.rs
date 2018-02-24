extern crate ag;
use ag::inputreader;
use std::env;
fn main() {
    println!("Hello, world!");
    let args = env::args();
    if args.len() == 1 {
        println!("No args")
    }
    inputreader::read_input();

}
