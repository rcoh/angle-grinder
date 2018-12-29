extern crate ag;
extern crate assert_cli;
extern crate pulldown_cmark;
extern crate toml;

#[macro_use]
extern crate serde_derive;

mod code_blocks;

#[derive(Deserialize, Debug)]
struct TestDefinition {
    query: String,
    input: String,
    output: String,
    notes: Option<String>,
}

#[cfg(test)]
mod integration {
    use super::*;
    use ag::pipeline::Pipeline;
    use assert_cli;
    use std::borrow::Borrow;
    use toml;

    fn structured_test(s: &str) {
        let conf: TestDefinition = toml::from_str(s).unwrap();
        let out: &str = conf.output.borrow();
        assert_cli::Assert::main_binary()
            .stdin(conf.input)
            .with_args(&[&conf.query])
            .stdout()
            .is(out)
            .unwrap();
    }

    #[test]
    fn count_distinct_operator() {
        structured_test(include_str!("structured_tests/count_distinct.toml"));
    }

    #[test]
    fn long_aggregate_values() {
        structured_test(include_str!("structured_tests/longlines.toml"));
    }

    #[test]
    fn sum_operator() {
        structured_test(include_str!("structured_tests/sum.toml"));
    }

    #[test]
    fn where_operator() {
        structured_test(include_str!("structured_tests/where-1.toml"));
        structured_test(include_str!("structured_tests/where-2.toml"));
        structured_test(include_str!("structured_tests/where-3.toml"));
    }

    #[test]
    fn sort_order() {
        structured_test(include_str!("structured_tests/sort_order.toml"));
    }

    #[test]
    fn total() {
        structured_test(include_str!("structured_tests/total.toml"));
    }

    #[test]
    fn no_args() {
        assert_cli::Assert::main_binary()
            .fails()
            .and()
            .stderr()
            .contains("[OPTIONS] <query|--self-update>")
            .unwrap();
    }

    #[test]
    fn parse_failure() {
        assert_cli::Assert::main_binary()
            .with_args(&["* | pasres"])
            .fails()
            .and()
            .stderr()
            .contains("Failure parsing")
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
$None$       1")
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
$None$       1")
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
            .with_args(&[
                r#"* | limit 2"#,
                "--file",
                "test_files/filter_test.log",
            ])
            .stdout()
            .is("[INFO] I am a log!
[WARN] Uh oh, danger ahead! ")
            .unwrap();
        assert_cli::Assert::main_binary()
            .with_args(&[
                r#"* | limit -2"#,
                "--file",
                "test_files/filter_test.log",
            ])
            .stdout()
            .is("[INFO] Match a *STAR*!
[INFO] Not a STAR! ")
            .unwrap();
    }

    fn ensure_parses(query: &str) {
        Pipeline::new(query).expect(&format!(
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
