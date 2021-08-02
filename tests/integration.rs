#![cfg(test)]
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
    enabled: Option<bool>,
}

#[cfg(test)]
mod integration {
    use super::*;
    use ag::pipeline::{ErrorReporter, OutputMode, Pipeline, QueryContainer};
    use assert_cmd::Command;
    use predicates::prelude::predicate;
    use std::fs;
    use std::io::stdout;
    use toml;

    pub struct EmptyErrorReporter;

    impl ErrorReporter for EmptyErrorReporter {}

    fn run() -> Command {
        Command::cargo_bin("agrind").unwrap()
    }

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
        let err = conf.error.unwrap_or("".to_string());

        if !conf.enabled.unwrap_or(true) {
            return;
        }

        let asserter = run()
            .env("RUST_BACKTRACE", "0")
            .write_stdin(conf.input)
            .args(&[&conf.query])
            .assert();

        let asserter = asserter.stdout(conf.output).stderr(err);

        if conf.succeeds.unwrap_or(true) {
            asserter.code(0);
        } else {
            asserter.failure();
        }
    }

    #[test]
    fn no_args() {
        run()
            .assert()
            .failure()
            .stderr(predicate::str::contains("[OPTIONS]"));
    }

    #[test]
    fn parse_failure() {
        run()
            .args(&["* | pasres"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("Failed to parse query"));
    }

    #[test]
    fn test_where_typecheck() {
        run()
            .args(&["* | where 5"])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Expected boolean expression, found",
            ));
    }

    #[test]
    fn test_limit_typecheck() {
        run()
            .args(&["* | limit 0"])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Error: Limit must be a non-zero integer, found 0",
            ));
        run()
            .args(&["* | limit 0.1"])
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "Error: Limit must be a non-zero integer, found 0.1",
            ));
    }

    #[test]
    fn basic_count() {
        run()
            .write_stdin("1\n2\n3\n")
            .args(&["* | count"])
            .assert()
            .stdout("_count\n--------------\n3\n");
    }

    #[test]
    fn file_input() {
        run()
            .args(&[
                "* | json | count by level",
                "--file",
                "test_files/test_json.log",
            ])
            .assert()
            .stdout(
                "level        _count
---------------------------
info         3
error        2
None         1\n",
            );
    }

    #[test]
    fn filter_wildcard() {
        run()
            .args(&[r#""*STAR*""#, "--file", "test_files/filter_test.log"])
            .assert()
            .stdout("[INFO] Match a *STAR*!\n");
        run()
            .args(&[r#"*STAR*"#, "--file", "test_files/filter_test.log"])
            .assert()
            .stdout("[INFO] Match a *STAR*!\n[INFO] Not a STAR!\n");
    }

    #[test]
    fn test_limit() {
        run()
            .args(&[r#"* | limit 2"#, "--file", "test_files/filter_test.log"])
            .assert()
            .stdout("[INFO] I am a log!\n[WARN] Uh oh, danger ahead!\n");
    }

    #[test]
    fn custom_format_backcompat() {
        run()
            .args(&[
                "* | logfmt",
                "--format",
                "{level} | {msg:<30} module={module}",
                "--file",
                "test_files/test_logfmt.log",
            ])
            .assert()
            .stdout(
                "info | Stopping all fetchers          module=kafka.consumer.ConsumerFetcherManager
info | Starting all fetchers          module=kafka.consumer.ConsumerFetcherManager
warn | Fetcher failed to start        module=kafka.consumer.ConsumerFetcherManager\n",
            );
    }

    #[test]
    fn custom_format() {
        run()
            .args(&[
                "-o",
                "format={level} | {msg:<30} module={module}",
                "--file",
                "test_files/test_logfmt.log",
                "* | logfmt",
            ])
            .assert()
            .stdout(
                "info | Stopping all fetchers          module=kafka.consumer.ConsumerFetcherManager
info | Starting all fetchers          module=kafka.consumer.ConsumerFetcherManager
warn | Fetcher failed to start        module=kafka.consumer.ConsumerFetcherManager\n",
            );
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
