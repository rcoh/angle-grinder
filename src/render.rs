use std;
use std::collections::HashMap;
use data;
use std::io::{stdout, Write};
extern crate terminal_size;
use self::terminal_size::{terminal_size, Height, Width};
use std::time::{Duration, Instant};

pub struct RenderConfig {
    pub floating_points: usize,
    pub min_buffer: usize,
    pub max_buffer: usize,
}

struct TerminalSize {
    height: u16,
    width: u16,
}

struct PrettyPrinter {
    render_config: RenderConfig,
    column_widths: HashMap<String, usize>,
    column_order: Vec<String>,
    term_size: Option<TerminalSize>,
}

impl PrettyPrinter {
    fn new(render_config: RenderConfig, term_size: Option<TerminalSize>) -> Self {
        PrettyPrinter {
            render_config: render_config,
            column_widths: HashMap::new(),
            column_order: Vec::new(),
            term_size: term_size,
        }
    }

    fn compute_column_widths(&self, data: &HashMap<String, data::Value>) -> HashMap<String, usize> {
        data.iter()
            .map(|(column_name, value)| {
                let current_width = *self.column_widths.get(column_name).unwrap_or(&0);
                // 1. If the width would increase, set it to max_buffer
                let value_length = value.render(&self.render_config).len().max(column_name.len());
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
        let mut new_keys: Vec<String> = data.keys()
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
            Some(TerminalSize { height: _, width }) => expected > (width as usize),
        }
    }

    fn format_record(&mut self, record: &data::Record) -> String {
        let new_column_widths = self.compute_column_widths(&(record.data));
        self.column_widths.extend(new_column_widths);
        let new_columns = self.new_columns(&(record.data));
        self.column_order.extend(new_columns);
        if self.column_order.len() == 0 {
            return record.raw.to_string();
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
        let strs: Vec<String> = self.column_order
            .iter()
            .map(|column_name| {
                let value = record.data.get(column_name);

                let unpadded = match value {
                    Some(value) => {
                        format!("[{}={}]", column_name, value.render(&self.render_config))
                    }
                    None => "".to_string(),
                };
                if no_padding {
                    unpadded
                } else {
                    format!(
                        "{:width$}",
                        unpadded,
                        width =
                            column_name.len() + 3 + self.column_widths.get(column_name).unwrap()
                    )
                }
            })
            .collect();
        strs.join("").trim().to_string()
    }

    fn format_aggregate_row(
        &self,
        columns: &Vec<String>,
        row: &HashMap<String, data::Value>,
    ) -> String {
        let row: Vec<String> = columns
            .iter()
            .map(|column_name| {
                format!(
                    "{:width$}",
                    row.get(column_name)
                        .unwrap_or(&data::Value::None)
                        .render(&self.render_config),
                    width = self.column_widths.get(column_name).unwrap()
                )
            })
            .collect();
        row.join("").trim().to_string()
    }

    fn format_aggregate(&mut self, aggregate: &data::Aggregate) -> String {
        aggregate
            .data
            .iter()
            .for_each(|row| self.column_widths = self.compute_column_widths(row));
        let header: Vec<String> = aggregate
            .columns
            .iter()
            .map(|column_name| {
                format!(
                    "{:width$}",
                    column_name,
                    width = self.column_widths.get(column_name).unwrap()
                )
            })
            .collect();
        let header = header.join("");
        let header_len = header.len();
        let header = format!("{}\n{}", header.trim(), "-".repeat(header_len));
        let body: Vec<String> = aggregate
            .data
            .iter()
            .map(|row| self.format_aggregate_row(&aggregate.columns, row))
            .collect();
        let overlength_str = format!("{}\n{}\n", header, body.join("\n"));
        match self.term_size {
            Some(TerminalSize { width: _, height }) => {
                let lines: Vec<&str> = overlength_str.lines().take((height as usize) - 1).collect();
                lines.join("\n") + "\n"
            }
            None => overlength_str,
        }
    }
}

pub struct Renderer {
    pretty_printer: PrettyPrinter,
    update_interval: Duration,
    stdout: std::io::Stdout,

    reset_sequence: String,
    is_tty: bool,
    last_print: Option<Instant>,
}

impl Renderer {
    pub fn new(config: RenderConfig, update_interval: Duration) -> Self {
        let tsize_opt =
            terminal_size().map(|(Width(width), Height(height))| TerminalSize { width, height });
        Renderer {
            is_tty: tsize_opt.is_some(),
            pretty_printer: PrettyPrinter::new(config, tsize_opt),
            stdout: stdout(),
            reset_sequence: "".to_string(),
            last_print: None,
            update_interval,
        }
    }

    pub fn render(&mut self, row: &data::Row, last_row: bool) {
        match row {
            &data::Row::Aggregate(ref aggregate) => {
                if !self.is_tty {
                    if last_row {
                        let output = self.pretty_printer.format_aggregate(&aggregate);
                        write!(self.stdout, "{}", output).unwrap();
                    }
                } else if self.should_print() || last_row {
                    let output = self.pretty_printer.format_aggregate(&aggregate);
                    let num_lines = output.matches("\n").count();
                    write!(self.stdout, "{}{}", self.reset_sequence, output).unwrap();
                    self.reset_sequence = "\x1b[2K\x1b[1A".repeat(num_lines);
                    self.last_print = Some(Instant::now());
                }
            }
            &data::Row::Record(ref record) => {
                let output = self.pretty_printer.format_record(&record);
                write!(self.stdout, "{}\n", output).unwrap();
            }
        }
    }

    pub fn should_print(&self) -> bool {
        self.last_print
            .map(|instant| instant.elapsed() > self.update_interval)
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use data::*;
    use operator::*;
    use super::*;

    #[test]
    fn test_pretty_print_record() {
        let rec = Record::new(r#"{"k1": 5, "k2": 5.5000001, "k3": "str"}"#);
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap();
        let mut pp = PrettyPrinter::new(
            RenderConfig {
                floating_points: 2,
                min_buffer: 1,
                max_buffer: 4,
            },
            None,
        );
        assert_eq!(pp.format_record(&rec), "[k1=5]     [k2=5.50]    [k3=str]");
        let rec = Record::new(r#"{"k1": 955, "k2": 5.5000001, "k3": "str3"}"#);
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap();
        assert_eq!(pp.format_record(&rec), "[k1=955]   [k2=5.50]    [k3=str3]");
        let rec = Record::new(
            r#"{"k1": "here is a amuch longer stsring", "k2": 5.5000001, "k3": "str3"}"#,
        );
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap();
        assert_eq!(
            pp.format_record(&rec),
            "[k1=here is a amuch longer stsring]    [k2=5.50]    [k3=str3]"
        );
        let rec = Record::new(r#"{"k1": 955, "k2": 5.5000001, "k3": "str3"}"#);
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap();
        assert_eq!(
            pp.format_record(&rec),
            "[k1=955]                               [k2=5.50]    [k3=str3]"
        );
    }

    #[test]
    fn test_pretty_print_record_too_long() {
        let rec = Record::new(r#"{"k1": 5, "k2": 5.5000001, "k3": "str"}"#);
        let parser = ParseJson::new(None);
        let rec = parser.process(rec).unwrap();
        let mut pp = PrettyPrinter::new(
            RenderConfig {
                floating_points: 2,
                min_buffer: 1,
                max_buffer: 4,
            },
            Some(TerminalSize {
                width: 10,
                height: 2,
            }),
        );
        assert_eq!(pp.format_record(&rec), "[k1=5][k2=5.50][k3=str]");
    }

    #[test]
    fn test_pretty_print_aggregate() {
        let agg = Aggregate::new(
            vec!["kc1".to_string(), "kc2".to_string()],
            "count".to_string(),
            vec![
                (
                    hashmap!{
                        "kc1".to_string() => "k1".to_string(),
                        "kc2".to_string() => "k2".to_string()
                    },
                    Value::Int(100),
                ),
                (
                    hashmap!{
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
                floating_points: 2,
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

}
