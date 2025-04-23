//! Instructions on adding a new alias:
//! 1. Create a new file for the alias in `aliases`.
//!     1a. The filename is the string to be replaced.
//!     1b. The string inside the file is the replacement.
//! 2. Create a new test config inside `tests/structured_tests/aliases`.
//! 3. Add the test config to the `test_aliases()` test.

use std::borrow::Cow;
use std::path::{Path, PathBuf};

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
            parse_alias(
                file.contents_utf8().expect("invalid utf-8"),
                file.path(),
                &[],
            )
            .expect("invalid toml")
        })
        .collect();
    pub static ref LOADED_KEYWORDS: Vec<&'static str> =
        LOADED_ALIASES.iter().map(|a| a.keyword.as_str()).collect();
}

#[derive(Debug)]
pub struct InvalidAliasError {
    pub path: PathBuf,
    pub cause: anyhow::Error,
    pub keyword: Option<String>,
    pub contents: Option<String>,
}

fn parse_alias(
    contents: &str,
    path: &Path,
    aliases: &[AliasPipeline],
) -> Result<AliasPipeline, InvalidAliasError> {
    let config: AliasConfig = toml::from_str(contents).map_err(|err| InvalidAliasError {
        path: path.to_owned(),
        cause: err.into(),
        keyword: None,
        contents: Some(contents.to_string()),
    })?;
    let reporter = Box::new(TermErrorReporter {});
    let aliases = AliasCollection {
        aliases: Cow::Borrowed(aliases),
    };
    let qc = QueryContainer::new_with_aliases(config.template, reporter, aliases);
    let keyword = config.keyword;
    let pipeline = pipeline_template(&qc).map_err(|err| InvalidAliasError {
        path: path.to_owned(),
        cause: err.into(),
        keyword: Some(keyword.clone()),
        contents: Some(contents.to_string()),
    })?;

    Ok(AliasPipeline { keyword, pipeline })
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct AliasConfig {
    keyword: String,
    template: String,
}

#[derive(Debug, Clone)]
pub struct AliasPipeline {
    keyword: String,
    pipeline: Vec<Operator>,
}

#[derive(Default)]
pub struct AliasCollection<'a> {
    aliases: Cow<'a, [AliasPipeline]>,
}

#[derive(Default)]
struct AliasAccum {
    valid_aliases: Vec<AliasPipeline>,
    invalid_aliases: Vec<InvalidAliasError>,
}

impl AliasCollection<'_> {
    pub fn get_alias(&self, name: &str) -> Option<&AliasPipeline> {
        self.aliases
            .iter()
            .find(|alias| alias.keyword == name)
            .or_else(|| AliasPipeline::matching_string(name))
    }

    pub fn valid_aliases(&self) -> impl Iterator<Item = &str> {
        self.aliases.iter().map(|a| a.keyword.as_str())
    }
}

impl AliasCollection<'static> {
    pub fn load_aliases_ancestors(
        path: Option<PathBuf>,
    ) -> anyhow::Result<(AliasCollection<'static>, Vec<InvalidAliasError>)> {
        let path = match path {
            Some(path) => path,
            None => std::env::current_dir()?,
        };
        let (valid, invalid) = find_all_aliases(path)?;
        Ok((
            AliasCollection {
                aliases: Cow::Owned(valid),
            },
            invalid,
        ))
    }

    pub fn load_aliases_from_dir(
        path: &Path,
    ) -> anyhow::Result<(AliasCollection<'static>, Vec<InvalidAliasError>)> {
        let mut aliases = AliasAccum::default();
        aliases_from_dir(path, &mut aliases)?;
        Ok((
            AliasCollection {
                aliases: Cow::Owned(aliases.valid_aliases),
            },
            aliases.invalid_aliases,
        ))
    }
}

fn find_local_aliases(dir: &Path, aliases: &mut AliasAccum) -> anyhow::Result<()> {
    if let Some(alias_dir) = dir.read_dir()?.find_map(|file| match file {
        Ok(entry) if entry.file_name() == ".agrind-aliases" => Some(entry),
        _else => None,
    }) {
        aliases_from_dir(&alias_dir.path(), aliases)?;
    }
    Ok(())
}

fn aliases_from_dir(dir: &Path, pipelines: &mut AliasAccum) -> anyhow::Result<()> {
    for entry in dir.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        let contents = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) => {
                pipelines.invalid_aliases.push(InvalidAliasError {
                    keyword: None,
                    path,
                    cause: e.into(),
                    contents: None,
                });
                continue;
            }
        };
        match parse_alias(&contents, &path, &pipelines.valid_aliases) {
            Ok(alias) => pipelines.valid_aliases.push(alias),
            Err(e) => pipelines.invalid_aliases.push(e),
        }
    }
    Ok(())
}

fn find_all_aliases(path: PathBuf) -> anyhow::Result<(Vec<AliasPipeline>, Vec<InvalidAliasError>)> {
    let mut accum = AliasAccum::default();
    for path in path.ancestors() {
        find_local_aliases(path, &mut accum)?;
    }
    Ok((accum.valid_aliases, accum.invalid_aliases))
}

impl AliasPipeline {
    pub fn matching_string(s: &str) -> Option<&'static AliasPipeline> {
        LOADED_ALIASES.iter().find(|alias| alias.keyword == s)
    }

    /// Render the alias as a string that should parse into a valid operator.
    pub fn render(&self) -> Vec<Operator> {
        self.pipeline.clone()
    }
}
