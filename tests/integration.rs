extern crate assert_cli;

extern crate toml;

#[macro_use]
extern crate serde_derive;

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
    use assert_cli;
    use toml;

    fn structured_test(s: &str) {
        let conf: TestDefinition = toml::from_str(s).unwrap();
        assert_cli::Assert::main_binary()
            .stdin(&conf.input)
            .with_args(&[&conf.query])
            .stdout()
            .is(conf.output)
            .unwrap();
    }

    #[test]
    fn test_count_distinct() {
        structured_test(include_str!("structured_tests/count_distinct.toml"));
    }

    #[test]
    fn test_sum() {
        structured_test(include_str!("structured_tests/sum.toml"));
    }

    #[test]
    fn test_sort_order() {
        structured_test(include_str!("structured_tests/sort_order.toml"));
    }

    #[test]
    fn test_no_args() {
        assert_cli::Assert::main_binary()
            .fails()
            .and()
            .stderr()
            .contains("[OPTIONS] <query>")
            .unwrap();
    }

    #[test]
    fn test_parse_failure() {
        assert_cli::Assert::main_binary()
            .with_args(&["* | pasres"])
            .fails()
            .and()
            .stderr()
            .contains("Could not parse")
            .unwrap();
    }

    #[test]
    fn test_basic_count() {
        assert_cli::Assert::main_binary()
            .stdin("1\n2\n3\n")
            .with_args(&["* | count"])
            .stdout()
            .is("_count\n--------------\n3")
            .unwrap();
    }

    #[test]
    fn test_file_input() {
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
    fn test_aggregate_of_aggregate() {
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
    fn test_json_from() {
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
    fn test_fields() {
        assert_cli::Assert::main_binary()
            .with_args(&[
                r#""error" | parse "* *" as lev, js 
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
}
