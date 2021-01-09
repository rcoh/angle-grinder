//! Instructions on adding a new alias:
//! 1. Create a new file for the alias in `aliases`.
//!     1a. The filename is the string to be replaced.
//!     1b. The string inside the file is the replacement.
//! 2. Create a new test config inside `tests/structured_tests/aliases`.
//! 3. Add the test config to the `test_aliases()` test.

use lazy_static::lazy_static;

use include_dir::Dir;
use serde::Deserialize;

const ALIASES_DIR: Dir = include_dir!("aliases");

lazy_static! {
    pub static ref LOADED_ALIASES: Vec<AliasConfig> = ALIASES_DIR
        .files()
        .iter()
        .map(|file| {
            toml::from_str(file.contents_utf8().expect("load string")).expect("toml valid")
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

impl AliasConfig {
    pub fn matching_string(s: String) -> Result<&'static AliasConfig, ()> {
        for alias in LOADED_ALIASES.iter() {
            if alias.keyword != s {
                continue;
            }

            return Ok(alias);
        }

        Err(())
    }

    /// Render the alias as a string that should parse into a valid operator.
    pub fn render(&self) -> String {
        self.template.clone()
    }
}
