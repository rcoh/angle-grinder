//! Instructions on adding a new alias:
//! 1. Create a new file for the alias in `aliases`.
//!     1a. The filename is the string to be replaced.
//!     1b. The string inside the file is the replacement.
//! 2. Create a new test config inside `tests/structured_tests/aliases`.
//! 3. Add the test config to the `test_aliases()` test.

use lazy_static::lazy_static;

use crate::errors::{QueryContainer, TermErrorReporter};
use crate::lang::{pipeline_template, Operator};
use include_dir::Dir;
use serde::Deserialize;

const ALIASES_DIR: Dir = include_dir!("aliases");

lazy_static! {
    pub static ref LOADED_ALIASES: Vec<AliasPipeline> = ALIASES_DIR
        .files()
        .map(|file| {
            let config: AliasConfig =
                toml::from_str(file.contents_utf8().expect("load string")).expect("toml valid");
            let reporter = Box::new(TermErrorReporter {});
            let qc = QueryContainer::new(config.template, reporter);
            let pipeline = pipeline_template(&qc).expect("valid alias");

            AliasPipeline {
                keyword: config.keyword,
                pipeline,
            }
        })
        .collect();
    pub static ref LOADED_KEYWORDS: Vec<&'static str> =
        LOADED_ALIASES.iter().map(|a| a.keyword.as_str()).collect();
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct AliasConfig {
    keyword: String,
    template: String,
}

#[derive(Debug)]
pub struct AliasPipeline {
    keyword: String,
    pipeline: Vec<Operator>,
}

impl AliasPipeline {
    pub fn matching_string(s: &str) -> Result<&'static AliasPipeline, ()> {
        LOADED_ALIASES
            .iter()
            .find(|alias| alias.keyword == s)
            .ok_or(())
    }

    /// Render the alias as a string that should parse into a valid operator.
    pub fn render(&self) -> Vec<Operator> {
        self.pipeline.clone()
    }
}
