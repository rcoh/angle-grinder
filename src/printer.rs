use crate::data;
use anyhow::Error;
use std::collections::HashMap;
use std::io;
use std::io::Write;

use crate::data::{Aggregate, DisplayConfig, Record, Value, ValueDisplay};
use crate::pipeline::OutputMode;
use crate::render::{RenderConfig, TerminalConfig, TerminalSize};
use itertools::{intersperse, Itertools};
use strfmt::{strfmt_map, FmtError, Formatter};

pub trait AggregatePrinter {
    fn print(&mut self, row: &data::Aggregate, display_config: &DisplayConfig) -> String;
    fn final_print(&mut self, row: &data::Aggregate, display_config: &DisplayConfig) -> String {
        self.print(row, display_config)
    }
}

pub trait RecordPrinter {
    fn print(
        &mut self,
        out: &mut dyn Write,
        row: &Record,
        display_config: &DisplayConfig,
    ) -> io::Result<()>;

    #[cfg(test)]
    fn print_str(&mut self, row: &Record, display_config: &DisplayConfig) -> String {
        let mut out = vec![];
        self.print(&mut out, row, display_config).unwrap();
        String::from_utf8(out).unwrap()
    }
}

struct RecordFromRow<T>(T);

impl<T: RowPrinter> RecordPrinter for RecordFromRow<T> {
    fn print(
        &mut self,
        out: &mut dyn Write,
        row: &Record,
        display_config: &DisplayConfig,
    ) -> io::Result<()> {
        self.0
            .print_row(display_config, out, Some(&row.raw), &mut row.data.iter())
    }
}

struct PrintAggregateAsRows<T>(T);

pub(crate) trait RowPrinter {
    fn print_row(
        &mut self,
        display_config: &DisplayConfig,
        out: &mut dyn Write,
        raw: Option<&str>,
        cols: &mut dyn Iterator<Item = (&String, &Value)>,
    ) -> io::Result<()>;
}

impl<T: RowPrinter> AggregatePrinter for PrintAggregateAsRows<T> {
    fn print(&mut self, _row: &Aggregate, _display_config: &DisplayConfig) -> String {
        "data will be output once the computation is complete...".to_string()
    }

    fn final_print(&mut self, row: &Aggregate, display_config: &DisplayConfig) -> String {
        let mut out = vec![];
        let columns = &row.columns;
        for row in &row.data {
            let mut kv_pairs = columns
                .iter()
                .map(|c| (c, row.get(c).unwrap_or(&Value::None)));
            self.0
                .print_row(display_config, &mut out, None, &mut kv_pairs)
                .expect("writing to string");
            writeln!(&mut out).unwrap();
        }
        String::from_utf8(out).expect("invalid UTF-8 produced")
    }
}

struct LogFmtPrinter;

impl RowPrinter for LogFmtPrinter {
    fn print_row(
        &mut self,
        display_config: &DisplayConfig,
        out: &mut dyn Write,
        _raw: Option<&str>,
        cols: &mut dyn Iterator<Item = (&String, &Value)>,
    ) -> io::Result<()> {
        let columns = intersperse(cols.sorted().map(Some), None);

        for col in columns {
            match col {
                Some((col, data)) => {
                    write!(out, "{}={}", col, ValueDisplay::new(data, display_config))
                }
                None => write!(out, " "),
            }?;
        }
        Ok(())
    }
}

pub fn raw_printer(
    mode: &OutputMode,
    render_config: RenderConfig,
    terminal_config: TerminalConfig,
) -> Result<Box<dyn RecordPrinter + Send>, Error> {
    // suppress warnings until I use these
    let _x = terminal_config.color_enabled;
    let _y = terminal_config.is_tty;
    match mode {
        OutputMode::Logfmt => Ok(Box::new(RecordFromRow(LogFmtPrinter))),
        OutputMode::Legacy => Ok(Box::new(LegacyPrinter::new(render_config, terminal_config))),
        OutputMode::Json => Ok(Box::new(JsonPrinter {})),
        OutputMode::Format(format_str) => Ok(Box::new(FormatPrinter::new(format_str.to_owned())?)),
    }
}

pub fn agg_printer(
    mode: &OutputMode,
    render_config: RenderConfig,
    terminal_config: TerminalConfig,
) -> Result<Box<dyn AggregatePrinter + Send>, Error> {
    match mode {
        OutputMode::Logfmt => Ok(Box::new(PrintAggregateAsRows(LogFmtPrinter))),
        OutputMode::Format(format_str) => Ok(Box::new(PrintAggregateAsRows(FormatPrinter::new(
            format_str.to_owned(),
        )?))),
        OutputMode::Json => Ok(Box::new(JsonPrinter {})),
        OutputMode::Legacy => Ok(Box::new(LegacyPrinter::new(render_config, terminal_config))),
    }
}

struct LegacyPrinter {
    pretty_printer: PrettyPrinter,
}

impl LegacyPrinter {
    pub fn new(render_config: RenderConfig, terminal_config: TerminalConfig) -> Self {
        LegacyPrinter {
            pretty_printer: PrettyPrinter::new(render_config, terminal_config.size),
        }
    }
}

impl RecordPrinter for LegacyPrinter {
    fn print(
        &mut self,
        out: &mut dyn Write,
        row: &Record,
        _display_config: &DisplayConfig,
    ) -> io::Result<()> {
        write!(out, "{}", self.pretty_printer.format_record_as_columns(row))
    }
}

impl AggregatePrinter for LegacyPrinter {
    fn print(&mut self, row: &Aggregate, _display_config: &DisplayConfig) -> String {
        self.pretty_printer.format_aggregate(row)
    }
}

struct FormatPrinter {
    format_str: String,
}

impl FormatPrinter {
    pub fn new(format_str: String) -> Result<Self, Error> {
        let nop_formatter = |mut fmt: Formatter| fmt.str("");
        let _ = strfmt_map(&format_str, &nop_formatter)?;
        Ok(FormatPrinter { format_str })
    }
}

pub fn strformat_record<'a>(
    fmtstr: &str,
    vars: impl Fn(&str) -> &'a Value,
    display_config: &DisplayConfig,
) -> Result<String, FmtError> {
    let formatter = |mut fmt: Formatter| {
        let v = vars(fmt.key);
        fmt.str(&format!("{}", ValueDisplay::new(v, display_config)))
    };

    strfmt_map(fmtstr, &formatter)
}

impl RowPrinter for FormatPrinter {
    fn print_row(
        &mut self,
        display_config: &DisplayConfig,
        out: &mut dyn Write,
        _raw: Option<&str>,
        cols: &mut dyn Iterator<Item = (&String, &Value)>,
    ) -> io::Result<()> {
        let cols: Vec<_> = cols.collect();
        write!(
            out,
            "{}",
            strformat_record(
                &self.format_str,
                |query| cols
                    .iter()
                    .find(|(k, _)| query == *k)
                    .map(|(_k, v)| v)
                    .unwrap_or(&&Value::None),
                display_config,
            )
            .unwrap_or_else(|e| format!("{}", e))
        )
    }
}

impl RecordPrinter for FormatPrinter {
    fn print(
        &mut self,
        out: &mut dyn Write,
        row: &Record,
        _display_config: &DisplayConfig,
    ) -> io::Result<()> {
        write!(
            out,
            "{}",
            match strformat_record(
                &self.format_str,
                |k| row.data.get(k).unwrap_or(&Value::None),
                _display_config
            ) {
                Ok(s) => s,
                Err(e) => format!("{}", e),
            }
        )
    }
}

struct JsonPrinter {}

impl RecordPrinter for JsonPrinter {
    fn print(
        &mut self,
        out: &mut dyn Write,
        row: &Record,
        _display_config: &DisplayConfig,
    ) -> io::Result<()> {
        serde_json::to_writer(out, row).expect("failed to format");
        Ok(())
    }
}

impl AggregatePrinter for JsonPrinter {
    fn print(&mut self, row: &Aggregate, _display_config: &DisplayConfig) -> String {
        let out = serde_json::to_string(row).expect("failed to format");
        out + "\n"
    }
}

struct PrettyPrinter {
    render_config: RenderConfig,
    column_widths: HashMap<String, usize>,
    column_order: Vec<String>,
    term_size: Option<TerminalSize>,
}

// MAYBE TODO: do any terminals not support unicode anymore? If so it would be nice to detect that
// and display "..." instead
const ELLIPSIS: &str = "…";

fn format_with_ellipsis<S: Into<String>>(inp: S, limit: usize) -> String {
    let inp = inp.into();
    if inp.chars().count() > limit {
        format!(
            "{str:.prelimit$}{ellipsis} ",
            str = inp,
            prelimit = limit - ELLIPSIS.chars().count() - 1,
            ellipsis = ELLIPSIS
        )
    } else {
        format!("{:limit$}", inp, limit = limit)
    }
}

impl PrettyPrinter {
    pub fn new(render_config: RenderConfig, term_size: Option<TerminalSize>) -> Self {
        PrettyPrinter {
            render_config,
            term_size,
            column_widths: HashMap::new(),
            column_order: Vec::new(),
        }
    }

    fn compute_column_widths(&self, data: &HashMap<String, data::Value>) -> HashMap<String, usize> {
        data.iter()
            .map(|(column_name, value)| {
                let current_width = *self.column_widths.get(column_name).unwrap_or(&0);
                // 1. If the width would increase, set it to max_buffer
                let value_length = value
                    .render(&self.render_config.display_config)
                    .len()
                    .max(column_name.len());
                let min_column_width = value_length + self.render_config.min_buffer;
                let new_column_width = if min_column_width > current_width {
                    // if we're resizing, go to the max
                    value_length + self.render_config.max_buffer
                } else {
                    current_width
                };
                (column_name.clone(), new_column_width)
            })
            .collect()
    }

    fn new_columns(&self, data: &HashMap<String, data::Value>) -> Vec<String> {
        let mut new_keys: Vec<String> = data
            .keys()
            .filter(|key| !self.column_order.contains(key))
            .cloned()
            .collect();
        new_keys.sort();
        new_keys
    }

    fn projected_width(column_widths: &HashMap<String, usize>) -> usize {
        column_widths
            .iter()
            .map(&|(key, size): (&String, &usize)| {
                let key_len: usize = key.len();
                size + key_len + 3
            })
            .sum()
    }

    fn overflows_term(&self) -> bool {
        let expected = Self::projected_width(&self.column_widths);
        match self.term_size {
            None => false,
            Some(TerminalSize { width, .. }) => expected > (width as usize),
        }
    }

    fn format_record_as_columns(&mut self, record: &data::Record) -> String {
        let new_column_widths = self.compute_column_widths(&(record.data));
        self.column_widths.extend(new_column_widths);
        let new_columns = self.new_columns(&(record.data));
        self.column_order.extend(new_columns);
        if self.column_order.is_empty() {
            return record.raw.trim_end().to_string();
        }

        let no_padding = if self.overflows_term() {
            self.column_widths = HashMap::new();
            self.column_widths = self.compute_column_widths(&(record.data));
            self.column_order = Vec::new();
            self.column_order = self.new_columns(&(record.data));
            self.overflows_term()
        } else {
            false
        };
        let mut strs = self.column_order.iter().map(|column_name| {
            let value = record.data.get(column_name);

            let unpadded = match value {
                Some(value) => format!(
                    "[{}={}]",
                    column_name,
                    value.render(&self.render_config.display_config)
                ),
                None => "".to_string(),
            };
            if no_padding {
                unpadded
            } else {
                format!(
                    "{:width$}",
                    unpadded,
                    width = column_name.len() + 3 + self.column_widths[column_name]
                )
            }
        });

        strs.join("").trim().to_string()
    }

    fn max_width(&self) -> u16 {
        match self.term_size {
            None => 240,
            Some(TerminalSize { width, .. }) => width,
        }
    }

    fn fits_within_term_agg(&self) -> bool {
        let allocated_width = self.max_width() as usize;
        let used_width: usize = self.column_widths.values().sum();
        used_width <= allocated_width
    }

    fn resize_widths_to_fit(
        &self,
        column_widths: &HashMap<String, usize>,
        ordering: &[String],
    ) -> HashMap<String, usize> {
        if !self.fits_within_term_agg() {
            let allocated_width = self.max_width();
            let mut remaining = allocated_width as usize;
            ordering
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let width = column_widths.get(col).unwrap();
                    let col = col.clone();
                    let max_column_width =
                        (remaining as f64 / (self.column_widths.len() - i) as f64) as usize;
                    if *width < max_column_width {
                        remaining -= width;
                        (col, *width)
                    } else {
                        remaining -= max_column_width;
                        (col, max_column_width)
                    }
                })
                .collect()
        } else {
            column_widths.clone()
        }
    }

    fn format_aggregate_row(
        &self,
        columns: &[String],
        row: &HashMap<String, data::Value>,
    ) -> String {
        let mut row = columns.iter().map(|column_name| {
            format_with_ellipsis(
                row.get(column_name)
                    .unwrap_or(&data::Value::None)
                    .render(&self.render_config.display_config),
                self.column_widths[column_name],
            )
        });
        row.join("").trim().to_string()
    }

    pub fn format_aggregate(&mut self, aggregate: &data::Aggregate) -> String {
        if aggregate.data.is_empty() {
            return "No data\n".to_string();
        }

        aggregate.data.iter().for_each(|row| {
            let new_widths = self.compute_column_widths(row);
            self.column_widths.extend(new_widths);
        });

        self.column_widths = self.resize_widths_to_fit(&self.column_widths, &aggregate.columns);
        assert!(self.fits_within_term_agg(), "{:?}", self.column_widths);
        let mut header = aggregate.columns.iter().map(|column_name| {
            format!(
                "{:width$}",
                column_name,
                width = self.column_widths[column_name]
            )
        });
        let header = header.join("");
        let header_len = header.len();
        let header = format!("{}\n{}", header.trim(), "-".repeat(header_len));
        let mut body = aggregate
            .data
            .iter()
            .map(|row| self.format_aggregate_row(&aggregate.columns, row));
        let overlength_str = format!("{}\n{}\n", header, body.join("\n"));
        match self.term_size {
            Some(TerminalSize { height, .. }) => {
                let mut lines = overlength_str.lines().take((height as usize) - 1);
                lines.join("\n") + "\n"
            }
            None => overlength_str,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::*;
    use crate::operator::parse::ParseJson;
    use crate::operator::*;
    use maplit::hashmap;

    impl LegacyPrinter {
        fn print_record(&mut self, record: &Record, _display_config: &DisplayConfig) -> String {
            self.pretty_printer.format_record_as_columns(record)
        }
    }

    #[test]
    fn print_raw() {
        let rec = Record::new("Hello, World!\n");
        let render_config = RenderConfig {
            display_config: DisplayConfig { floating_points: 2 },
            min_buffer: 1,
            max_buffer: 4,
        };
        let display_config = DisplayConfig { floating_points: 2 };
        let mut pp = LegacyPrinter::new(render_config, TerminalConfig::load());
        assert_eq!(pp.print_record(&rec, &display_config), "Hello, World!");
    }

    #[test]
    fn pretty_print_record() {
        let rec = Record::new(r#"{"k1": 5, "k2": 5.5000001, "k3": "str"}"#);
        let display_config = DisplayConfig { floating_points: 2 };
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        let render_config = RenderConfig {
            display_config: DisplayConfig { floating_points: 2 },
            min_buffer: 1,
            max_buffer: 4,
        };
        let mut pp = LegacyPrinter::new(render_config, TerminalConfig::load());
        assert_eq!(
            pp.print_record(&rec, &display_config),
            "[k1=5]     [k2=5.50]    [k3=str]"
        );
        let rec = Record::new(r#"{"k1": 955, "k2": 5.5000001, "k3": "str3"}"#);
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            pp.print_record(&rec, &display_config),
            "[k1=955]   [k2=5.50]    [k3=str3]"
        );
        let rec = Record::new(
            r#"{"k1": "here is a amuch longer stsring", "k2": 5.5000001, "k3": "str3"}"#,
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            pp.print_record(&rec, &display_config),
            "[k1=here is a amuch longer stsring]    [k2=5.50]    [k3=str3]"
        );
        let rec = Record::new(r#"{"k1": 955, "k2": 5.5000001, "k3": "str3"}"#);
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            pp.print_record(&rec, &display_config),
            "[k1=955]                               [k2=5.50]    [k3=str3]"
        );
    }

    #[test]
    fn pretty_print_record_formatted() {
        let rec = Record::new(r#"{"k1": 5, "k2": 5.5000001, "k3": "str"}"#);
        let parser = ParseJson::new(None);
        let display_config = DisplayConfig { floating_points: 2 };
        let rec = parser.process(rec).unwrap().unwrap();
        let mut pp = FormatPrinter::new("{k1:>3} k2={k2:<10.3} k3[{k3}]".to_string()).unwrap();
        assert_eq!(
            pp.print_str(&rec, &display_config),
            "  5 k2=5.5        k3[str]"
        );
        let rec = Record::new(r#"{"k1": 955, "k2": 5.5000001, "k3": "str3"}"#);
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            pp.print_str(&rec, &display_config),
            "955 k2=5.5        k3[str3]"
        );
        let rec = Record::new(
            r#"{"k1": "here is a amuch longer stsring", "k2": 5.5000001, "k3": "str3"}"#,
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap().unwrap();
        assert_eq!(
            pp.print_str(&rec, &display_config),
            "here is a amuch longer stsring k2=5.5        k3[str3]"
        );
    }

    #[test]
    fn pretty_print_record_too_long() {
        let rec = Record::new(r#"{"k1": 5, "k2": 5.5000001, "k3": "str"}"#);
        let parser = ParseJson::new(None);
        let display_config = DisplayConfig { floating_points: 2 };
        let rec = parser.process(rec).unwrap().unwrap();
        let render_config = RenderConfig {
            display_config: DisplayConfig { floating_points: 2 },
            min_buffer: 1,
            max_buffer: 4,
        };
        let mut pp = LegacyPrinter::new(
            render_config,
            TerminalConfig {
                size: Some(TerminalSize {
                    width: 10,
                    height: 2,
                }),
                is_tty: false,
                color_enabled: false,
            },
        );
        assert_eq!(
            pp.print_record(&rec, &display_config),
            "[k1=5][k2=5.50][k3=str]"
        );
    }

    #[test]
    fn pretty_print_aggregate() {
        let agg = Aggregate::new(
            &["kc1".to_string(), "kc2".to_string()],
            "count".to_string(),
            &[
                (
                    hashmap! {
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::Int(100),
                ),
                (
                    hashmap! {
                        "kc1".to_string() => "k300".to_string(),
                        "kc2".to_string() => "k40000".to_string()
                    },
                    Value::Int(500),
                ),
            ],
        );
        assert_eq!(agg.data.len(), 2);
        let mut pp = PrettyPrinter::new(
            RenderConfig {
                display_config: DisplayConfig { floating_points: 2 },
                min_buffer: 2,
                max_buffer: 4,
            },
            Some(TerminalSize {
                width: 100,
                height: 10,
            }),
        );
        println!("{}", pp.format_aggregate(&agg));
        assert_eq!(
            "kc1    kc2       count\n--------------------------\nk1     k2        100\nk300   k40000    500\n",
            pp.format_aggregate(&agg)
        );
    }

    #[test]
    fn pretty_print_aggregate_too_long() {
        let agg = Aggregate::new(
            &["kc1".to_string(), "kc2".to_string()],
            "count".to_string(),
            &[
                (
                    hashmap! {
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k40000 k40000k50000k60000k70000k80000".to_string()
                    },
                    Value::from_string("0bcdefghijklmnopqrztuvwxyz 1bcdefghijklmnopqrztuvwxyz 2bcdefghijklmnopqrztuvwxyz"),
                ),
                (
                    hashmap! {
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::from_string("0bcdefghijklmnopqrztuvwxyz 1bcdefghijklmnopqrztuvwxyz 2bcdefghijklmnopqrztuvwxyz"),
                ),
                (
                    hashmap! {
                        "kc1".to_string() => "k300".to_string(),
                        "kc2".to_string() => "k40000 k40000k50000k60000k70000k80000".to_string()
                    },
                    Value::Int(500),
                ),
            ],
        );
        let max_width = 60;
        let mut pp = PrettyPrinter::new(
            RenderConfig {
                display_config: DisplayConfig { floating_points: 2 },
                min_buffer: 2,
                max_buffer: 4,
            },
            Some(TerminalSize {
                width: max_width as u16,
                height: 10,
            }),
        );
        println!("{}", pp.format_aggregate(&agg));
        let result = pp.format_aggregate(&agg);
        for line in result.lines() {
            assert!(
                line.chars().count() <= max_width as usize,
                "Expected `{}` to be shorter than {} -- it was {}",
                line,
                max_width,
                line.len()
            );
        }
        assert_eq!(
            pp.format_aggregate(&agg),
            "kc1    kc2                       count\n------------------------------------------------------------\nk1     k40000 k40000k50000k6000… 0bcdefghijklmnopqrztuvwxy…\nk1     k2                        0bcdefghijklmnopqrztuvwxy…\nk300   k40000 k40000k50000k6000… 500\n"
        );
    }

    #[test]
    fn test_format_with_ellipsis() {
        assert_eq!(format_with_ellipsis("abcde", 4), "ab… ");
        assert_eq!(format_with_ellipsis("abcde", 10), "abcde     ");
    }
}
