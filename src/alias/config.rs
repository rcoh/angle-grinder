//! Instructions on adding a new alias:
//! 1. Create a new file inside this folder.
//!     1a. The filename is the string to be replaced.
//!     1b. The string inside the file is the replacement.
//! 2. Add the filename to the load_aliases! macro.
//! 3. Create a new test config inside `tests/structured_tests/aliases`.
//! 4. Add the test config to the `test_aliases()` test.
use lazy_static::lazy_static;

macro_rules! load_aliases {
    ($($filename:expr,)+) => {
        vec![
            $(
                AliasConfig {
                    from: stringify!($filename),
                    to: include_str!(stringify!($filename)),
                },
            )*
        ]
    };
}

lazy_static! {
    pub static ref LOADED_ALIASES: Vec<AliasConfig> = load_aliases! {
        apache,
    };
}

pub struct AliasConfig {
    pub from: &'static str,
    pub to: &'static str,
}
