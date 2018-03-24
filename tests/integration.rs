extern crate assert_cli;

#[cfg(test)]
mod integration {
    use assert_cli;

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
    fn test_basic_count() {
        assert_cli::Assert::main_binary()
            .stdin("1\n2\n3\n")
            .with_args(&["* | count"])
            .stdout()
            .is("_count\n---------\n3")
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
            .is("level       _count\n---------------------
info        3\nerror       2\n$None$      1")
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
            .is("_count\n---------\n3")
            .unwrap();
    }
}
