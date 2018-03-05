use std;
use std::collections::HashMap;
use data;
use std::io::{stdout, Write};
extern crate terminal_size;
use self::terminal_size::{terminal_size, Height, Width};

pub struct RenderConfig {
    pub floating_points: usize,
    pub min_buffer: usize,
    pub max_buffer: usize,
}

struct PrettyPrinter {
    render_config: RenderConfig,
    column_widths: HashMap<String, usize>,
    column_order: Vec<String>,
}

impl PrettyPrinter {
    fn new(render_config: RenderConfig) -> Self {
        PrettyPrinter {
            render_config: render_config,
            column_widths: HashMap::new(),
            column_order: Vec::new(),
        }
    }

    fn compute_column_widths(&self, data: &HashMap<String, data::Value>) -> HashMap<String, usize> {
        data.iter()
            .map(|(column_name, value)| {
                let current_width = *self.column_widths.get(column_name).unwrap_or(&0);
                // 1. If the width would increase, set it to max_buffer
                let value_length = value.render(&self.render_config).len();
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

    fn format_record(&mut self, record: &data::Record) -> String {
        let new_column_widths = self.compute_column_widths(&(record.data));
        self.column_widths.extend(new_column_widths);
        let new_columns = self.new_columns(&(record.data));
        self.column_order.extend(new_columns);
        if self.column_order.len() == 0 {
            return record.raw.to_string();
        }
        let strs: Vec<String> = self.column_order
            .iter()
            .map(|column_name| {
                let value = record.data.get(column_name);
                let or_none = data::Value::no_value();
                let value = value.unwrap_or(&or_none);
                let unpadded = format!("[{}={}]", column_name, value.render(&self.render_config));
                format!(
                    "{:width$}",
                    unpadded,
                    width = column_name.len() + 3 + self.column_widths.get(column_name).unwrap()
                )
            })
            .collect();
        strs.join("").trim().to_string()
    }

    fn format_aggregate_row(&self, row: &HashMap<String, data::Value>) -> String {
        let row: Vec<String> = self.column_order
            .iter()
            .map(|column_name| {
                format!(
                    "{:width$}",
                    row.get(column_name).unwrap().render(&self.render_config),
                    width = self.column_widths.get(column_name).unwrap()
                )
            })
            .collect();
        row.join("")
    }

    fn format_aggregate(&mut self, aggregate: &data::Aggregate) -> String {
        let rows = aggregate.rows();
        rows.iter()
            .for_each(|row| self.column_widths = self.compute_column_widths(row));
        let mut cols = aggregate.key_columns.clone();
        cols.push(aggregate.agg_column.clone());
        self.column_order = cols;
        let header: Vec<String> = self.column_order
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
        let header = header + "\n" + &"-".repeat(header_len);
        let body: Vec<String> = rows.iter()
            .map(|row| self.format_aggregate_row(row))
            .collect();
        format!("{}\n{}", header, body.join("\n"))
    }
}

pub struct Renderer {
    pretty_printer: PrettyPrinter,
    stdout: std::io::Stdout,
    reset_sequence: String,
    terminal_size: (Width, Height),
}

impl Renderer {
    pub fn new(config: RenderConfig) -> Self {
        Renderer {
            pretty_printer: PrettyPrinter::new(config),
            stdout: stdout(),
            reset_sequence: "".to_string(),
            terminal_size: terminal_size().unwrap(),
        }
    }

    pub fn render(&mut self, row: data::Row) {
        match row {
            data::Row::Aggregate(aggregate) => {
                let unsafe_output = self.pretty_printer.format_aggregate(&aggregate);
                let &(ref _w, Height(ref t_height)) = &self.terminal_size;
                let t_height = t_height - 1;
                let lines: Vec<&str> = unsafe_output.lines().take(t_height as usize).collect();
                let num_lines = lines.len();
                let output = lines.join("\n");
                write!(self.stdout, "{}{}\n", self.reset_sequence, output).unwrap();
                self.reset_sequence = "\x1b[2K\x1b[1A".repeat(num_lines)
            }
            data::Row::Record(record) => {
                let output = self.pretty_printer.format_record(&record);
                write!(self.stdout, "{}\n", output).unwrap();
            }
        }
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
        let parser = ParseJson {};
        let rec = parser.process(&rec).unwrap();
        let mut pp = PrettyPrinter::new(RenderConfig {
            floating_points: 2,
            min_buffer: 1,
            max_buffer: 4,
        });
        assert_eq!(pp.format_record(&rec), "[k1=5]    [k2=5.50]    [k3=str]");
        let rec = Record::new(r#"{"k1": 955, "k2": 5.5000001, "k3": "str3"}"#);
        let parser = ParseJson {};
        let rec = parser.process(&rec).unwrap();
        assert_eq!(pp.format_record(&rec), "[k1=955]  [k2=5.50]    [k3=str3]");
        let rec = Record::new(
            r#"{"k1": "here is a amuch longer stsring", "k2": 5.5000001, "k3": "str3"}"#,
        );
        let parser = ParseJson {};
        let rec = parser.process(&rec).unwrap();
        assert_eq!(
            pp.format_record(&rec),
            "[k1=here is a amuch longer stsring]    [k2=5.50]    [k3=str3]"
        );
        let rec = Record::new(r#"{"k1": 955, "k2": 5.5000001, "k3": "str3"}"#);
        let parser = ParseJson {};
        let rec = parser.process(&rec).unwrap();
        assert_eq!(
            pp.format_record(&rec),
            "[k1=955]                               [k2=5.50]    [k3=str3]"
        );
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
        let mut pp = PrettyPrinter::new(RenderConfig {
            floating_points: 2,
            min_buffer: 2,
            max_buffer: 4,
        });
        println!("{}", pp.format_aggregate(&agg));
        assert_eq!(
            "kc1   kc2       count  \n-----------------------\nk1    k2        100    \nk300  k40000    500    ",
            pp.format_aggregate(&agg)
        );
    }
}