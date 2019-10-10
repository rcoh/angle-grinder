mod config;

use self::config::LOADED_ALIASES;

pub fn substitute_aliases(v: &str) -> String {
    let mut v = v.to_string();
    for alias in LOADED_ALIASES.iter() {
        v = v.replace(alias.from, alias.to);
    }
    v
}
