use crate::data;
use failure::Error;
use std::io::Write;

use crate::data::DisplayConfig;
use crate::printer::Printer;
use std::time::{Duration, Instant};
use terminal_size::{terminal_size, Height, Width};

#[derive(Clone)]
pub struct RenderConfig {
    pub display_config: DisplayConfig,
    pub min_buffer: usize,
    pub max_buffer: usize,
}

impl Default for RenderConfig {
    fn default() -> Self {
        RenderConfig {
            display_config: data::DisplayConfig { floating_points: 2 },
            min_buffer: 1,
            max_buffer: 4,
        }
    }
}

pub struct TerminalConfig {
    pub size: Option<TerminalSize>,
    pub is_tty: bool,
    pub color_enabled: bool,
}

impl TerminalConfig {
    pub fn load() -> Self {
        let tsize_opt =
            terminal_size().map(|(Width(width), Height(height))| TerminalSize { width, height });
        let is_tty = tsize_opt != None;
        TerminalConfig {
            size: tsize_opt,
            is_tty,
            color_enabled: is_tty,
        }
    }
}

#[derive(PartialEq)]
pub struct TerminalSize {
    pub height: u16,
    pub width: u16,
}

pub struct Renderer {
    raw_printer: Box<dyn Printer<data::Record> + Send>,
    agg_printer: Box<dyn Printer<data::Aggregate> + Send>,
    update_interval: Duration,
    stdout: Box<dyn Write + Send>,
    config: RenderConfig,

    reset_sequence: String,
    is_tty: bool,
    last_print: Option<Instant>,
}

impl Renderer {
    pub fn new(
        config: RenderConfig,
        update_interval: Duration,
        raw_printer: Box<dyn Printer<data::Record> + Send>,
        agg_printer: Box<dyn Printer<data::Aggregate> + Send>,
        output: Box<dyn Write + Send>,
    ) -> Self {
        let tsize_opt =
            terminal_size().map(|(Width(width), Height(height))| TerminalSize { width, height });
        Renderer {
            is_tty: tsize_opt.is_some(),
            raw_printer,
            agg_printer,
            config,
            stdout: output,
            reset_sequence: "".to_string(),
            last_print: None,
            update_interval,
        }
    }

    pub fn render(&mut self, row: &data::Row, last_row: bool) -> Result<(), Error> {
        match *row {
            data::Row::Aggregate(ref aggregate) => {
                if !self.is_tty {
                    if last_row {
                        let output = self
                            .agg_printer
                            .final_print(aggregate, &self.config.display_config);
                        write!(self.stdout, "{}", output)?;
                    }
                } else if self.should_print() || last_row {
                    let output = if !last_row {
                        self.agg_printer
                            .print(aggregate, &self.config.display_config)
                    } else {
                        self.agg_printer
                            .final_print(aggregate, &self.config.display_config)
                    };
                    let num_lines = output.matches('\n').count();
                    write!(self.stdout, "{}{}", self.reset_sequence, output)?;
                    self.reset_sequence = "\x1b[2K\x1b[1A".repeat(num_lines);
                    self.last_print = Some(Instant::now());
                }

                Ok(())
            }
            data::Row::Record(ref record) => {
                let output = self.raw_printer.print(record, &self.config.display_config);
                writeln!(self.stdout, "{}", output)?;

                Ok(())
            }
        }
    }

    pub fn should_print(&self) -> bool {
        if !self.is_tty {
            return false;
        }
        self.last_print
            .map(|instant| instant.elapsed() > self.update_interval)
            .unwrap_or(true)
    }
}
