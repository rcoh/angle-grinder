#![cfg(test)]
extern crate ag;
extern crate assert_cli;
extern crate pulldown_cmark;
extern crate test_generator;
extern crate toml;

use serde::Deserialize;
use test_generator::test_resources;

mod code_blocks;

#[derive(Deserialize, Debug)]
struct TestDefinition {
    query: String,
    input: String,
    output: String,
    error: Option<String>,
    notes: Option<String>,
    succeeds: Option<bool>,
}

#[cfg(test)]
mod integration {
    use super::*;
    use ag::pipeline::{ErrorReporter, OutputMode, Pipeline, QueryContainer};
    use assert_cli;
    use std::borrow::Borrow;
    use std::fs;
    use std::io::stdout;
    use toml;

    pub struct EmptyErrorReporter;

    impl ErrorReporter for EmptyErrorReporter {}

    #[test_resources("tests/structured_tests/*.toml")]
    fn integration(path: &str) {
        structured_test(path)
    }

    #[test_resources("tests/structured_tests/aliases/*.toml")]
    fn alias(path: &str) {
        structured_test(path)
    }

    fn structured_test(path: &str) {
        let contents = fs::read_to_string(path).unwrap();
        let conf: TestDefinition = toml::from_str(&contents).unwrap();
        let out: &str = conf.output.borrow();
        let err = conf.error.unwrap_or("".to_string());
        let env = assert_cli::Environment::inherit().insert("RUST_BACKTRACE", "0");
        let mut asserter = assert_cli::Assert::main_binary()
            .with_env(env)
            .stdin(conf.input)
            .with_args(&[&conf.query])
            .stdout()
            .is(out)
            .stderr()
            .is(err.as_str());

        if conf.succeeds.unwrap_or(true) {
            asserter = asserter.succeeds();
        } else {
            asserter = asserter.fails();
        }
        asserter.unwrap();
    }

    #[test]
    fn no_args() {
        assert_cli::Assert::main_binary()
            .fails()
            .and()
            .stderr()
            .contains("[OPTIONS]")
            .unwrap();
    }

    #[test]
    fn parse_failure() {
        assert_cli::Assert::main_binary()
            .with_args(&["* | pasres"])
            .fails()
            .and()
            .stderr()
            .contains("Failed to parse query")
            .unwrap();
    }

    #[test]
    fn test_where_typecheck() {
        assert_cli::Assert::main_binary()
            .with_args(&["* | where 5"])
            .fails()
            .and()
            .stderr()
            .contains("Expected boolean expression, found")
            .unwrap();
    }

    #[test]
    fn test_limit_typecheck() {
        assert_cli::Assert::main_binary()
            .with_args(&["* | limit 0"])
            .fails()
            .and()
            .stderr()
            .contains("Error: Limit must be a non-zero integer, found 0")
            .unwrap();
        assert_cli::Assert::main_binary()
            .with_args(&["* | limit 0.1"])
            .fails()
            .and()
            .stderr()
            .contains("Error: Limit must be a non-zero integer, found 0.1")
            .unwrap();
    }

    #[test]
    fn basic_count() {
        assert_cli::Assert::main_binary()
            .stdin("1\n2\n3\n")
            .with_args(&["* | count"])
            .stdout()
            .is("_count\n--------------\n3")
            .unwrap();
    }

    #[test]
    fn file_input() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                "* | json | count by level",
                "--file",
                "test_files/test_json.log",
            ])
            .stdout()
            .is("level        _count
---------------------------
info         3
error        2
None         1")
            .unwrap();
    }

    #[test]
    fn aggregate_of_aggregate() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                "* | json | count by level | count",
                "--file",
                "test_files/test_json.log",
            ])
            .stdout()
            .is("_count\n--------------\n3")
            .unwrap();
    }

    #[test]
    fn json_from() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                r#"* | parse "* *" as lev, js | json from js | count by level"#,
                "--file",
                "test_files/test_partial_json.log",
            ])
            .stdout()
            .is("level        _count
---------------------------
info         3
error        2
None         1")
            .unwrap();
    }

    #[test]
    fn fields() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                r#"* "error" | parse "* *" as lev, js
                     | json from js
                     | fields except js, lev"#,
                "--file",
                "test_files/test_partial_json.log",
            ])
            .stdout()
            .is("[level=error]        [message=Oh now an error!]
[level=error]        [message=So many more errors!]")
            .unwrap();
    }

    #[test]
    fn parse_plain_text() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                r#"db-1 response | parse "in *ms" as duration"#,
                "--file",
                "test_files/test_parse.log",
            ])
            .stdout()
            .is("[duration=500]
[duration=100]
[duration=102]
[duration=102]
[duration=100]
[duration=100]
[duration=100]
[duration=122]")
            .unwrap();
    }

    #[test]
    fn filter_wildcard() {
        assert_cli::Assert::main_binary()
            .with_args(&[r#""*STAR*""#, "--file", "test_files/filter_test.log"])
            .stdout()
            .is("[INFO] Match a *STAR*!")
            .unwrap();
        assert_cli::Assert::main_binary()
            .with_args(&[r#"*STAR*"#, "--file", "test_files/filter_test.log"])
            .stdout()
            .is("[INFO] Match a *STAR*!
[INFO] Not a STAR!")
            .unwrap();
    }

    #[test]
    fn test_limit() {
        assert_cli::Assert::main_binary()
            .with_args(&[r#"* | limit 2"#, "--file", "test_files/filter_test.log"])
            .stdout()
            .is("[INFO] I am a log!
[WARN] Uh oh, danger ahead! ")
            .unwrap();
    }

    #[test]
    fn custom_format_backcompat() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                "* | logfmt",
                "--format",
                "{level} | {msg:<30} module={module}",
                "--file",
                "test_files/test_logfmt.log",
            ])
            .stdout()
            .is(
                "info | Stopping all fetchers          module=kafka.consumer.ConsumerFetcherManager
info | Starting all fetchers          module=kafka.consumer.ConsumerFetcherManager
warn | Fetcher failed to start        module=kafka.consumer.ConsumerFetcherManager",
            )
            .unwrap();
    }

    #[test]
    fn custom_format() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                "-o",
                "format={level} | {msg:<30} module={module}",
                "--file",
                "test_files/test_logfmt.log",
                "* | logfmt",
            ])
            .stdout()
            .is(
                "info | Stopping all fetchers          module=kafka.consumer.ConsumerFetcherManager
info | Starting all fetchers          module=kafka.consumer.ConsumerFetcherManager
warn | Fetcher failed to start        module=kafka.consumer.ConsumerFetcherManager",
            )
            .unwrap();
    }

    fn ensure_parses(query: &str) {
        let query_container = QueryContainer::new(query.to_string(), Box::new(EmptyErrorReporter));
        Pipeline::new(&query_container, stdout(), OutputMode::Legacy).expect(&format!(
            "Query: `{}` from the README should have parsed",
            query
        ));
        println!("validated {}", query);
    }

    #[test]
    fn validate_readme_examples() {
        let blocks = code_blocks::code_blocks(include_str!("../README.md"));
        for code_block in blocks {
            if code_block.flag == "agrind" {
                ensure_parses(&code_block.code);
            }
        }
    }
}
