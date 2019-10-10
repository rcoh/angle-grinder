//! Instructions on adding a new alias:
//! 1. Create a new file for the alias in `aliases`.
//!     1a. The filename is the string to be replaced.
//!     1b. The string inside the file is the replacement.
//! 2. Create a new test config inside `tests/structured_tests/aliases`.
//! 3. Add the test config to the `test_aliases()` test.
use lazy_static::lazy_static;

use include_dir::Dir;

const ALIASES_DIR: Dir = include_dir!("aliases");

lazy_static! {
    static ref LOADED_ALIASES: Vec<AliasConfig> = ALIASES_DIR
        .files()
        .iter()
        .map(|file| {
            AliasConfig {
                from: file
                    .path()
                    .file_stem()
                    .expect("file stem exists")
                    .to_str()
                    .expect("str conversion"),
                to: file.contents_utf8().expect("load string"),
            }
        })
        .collect();
}

struct AliasConfig {
    from: &'static str,
    to: &'static str,
}

pub fn substitute_aliases(v: &str) -> String {
    let mut v = v.to_string();
    for alias in LOADED_ALIASES.iter() {
        v = v.replace(alias.from, alias.to);
    }
    v
}
