#[macro_use]
extern crate nom;

#[macro_use]
#[cfg(test)]
extern crate maplit;

extern crate crossbeam_channel;

mod data;
mod lang;
mod operator;
mod render;

pub mod pipeline {
    use data::{Record, Row};
    use lang;
    use lang::*;
    use operator;
    use render::{RenderConfig, Renderer};
    use std::io::BufRead;
    use std::time::Duration;

    pub struct Pipeline {
        filter: lang::Search,
        pre_aggregates: Vec<Box<operator::UnaryPreAggOperator>>,
        aggregators: Vec<Box<operator::AggregateOperator>>,
        renderer: Renderer,
    }

    impl Pipeline {
        fn convert_inline(op: lang::InlineOperator) -> Box<operator::UnaryPreAggOperator> {
            match op {
                InlineOperator::Json { input_column } => {
                    Box::new(operator::ParseJson::new(input_column))
                }
                InlineOperator::Parse {
                    pattern,
                    fields,
                    input_column,
                } => Box::new(operator::Parse::new(&pattern, fields, input_column).unwrap()),
                InlineOperator::Fields { fields, mode } => {
                    let omode = match mode {
                        FieldMode::Except => operator::FieldMode::Except,
                        FieldMode::Only => operator::FieldMode::Only,
                    };
                    Box::new(operator::Fields::new(fields, omode))
                }
            }
        }

        fn convert_sort(op: lang::SortOperator) -> Box<operator::AggregateOperator> {
            let mode = match op.direction {
                SortMode::Ascending => operator::SortDirection::Ascending,
                SortMode::Descending => operator::SortDirection::Descending,
            };
            Box::new(operator::Sorter::new(op.sort_cols, mode))
        }

        fn convert_agg(op: lang::AggregateOperator) -> Box<operator::AggregateOperator> {
            match op.aggregate_function {
                AggregateFunction::Count => Box::new(operator::Grouper::<operator::Count>::new(
                    op.key_cols.iter().map(AsRef::as_ref).collect(),
                    &op.output_column,
                    operator::Count::new(),
                )),
                AggregateFunction::Average { column } => {
                    Box::new(operator::Grouper::<operator::Average>::new(
                        op.key_cols.iter().map(AsRef::as_ref).collect(),
                        &op.output_column,
                        operator::Average::empty(column),
                    ))
                }
                AggregateFunction::Sum { column } => {
                    Box::new(operator::Grouper::<operator::Sum>::new(
                        op.key_cols.iter().map(AsRef::as_ref).collect(),
                        &op.output_column,
                        operator::Sum::empty(column),
                    ))
                }
                AggregateFunction::Percentile {
                    column, percentile, ..
                } => Box::new(operator::Grouper::<operator::Percentile>::new(
                    op.key_cols.iter().map(AsRef::as_ref).collect(),
                    &op.output_column,
                    operator::Percentile::empty(column, percentile),
                )),
            }
        }

        pub fn new(pipeline: &str) -> Result<Self, String> {
            let fixed_pipeline = format!("{}!", pipeline); // todo: fix hack
            let parsed = lang::parse_query(&fixed_pipeline)
                .map_err(|e| format!("Could not parse query: {:?}", e));
            let (_, query) = parsed?;
            let mut in_agg = false;
            let mut pre_agg: Vec<Box<operator::UnaryPreAggOperator>> = Vec::new();
            let mut post_agg: Vec<Box<operator::AggregateOperator>> = Vec::new();
            let final_op = {
                let last_op = &(query.operators).last();
                match last_op {
                    &Some(&Operator::Aggregate(ref agg_op)) => {
                        Some(Pipeline::convert_sort(SortOperator {
                            sort_cols: vec![agg_op.output_column.clone()],
                            direction: SortMode::Descending,
                        }))
                    }
                    _other => None,
                }
            };
            for op in query.operators {
                match op {
                    Operator::Inline(inline_op) => if !in_agg {
                        pre_agg.push(Pipeline::convert_inline(inline_op));
                    } else {
                        return Result::Err("non aggregate cannot follow aggregate".to_string());
                    },
                    Operator::Aggregate(agg_op) => {
                        in_agg = true;
                        post_agg.push(Pipeline::convert_agg(agg_op))
                    }
                    Operator::Sort(sort_op) => post_agg.push(Pipeline::convert_sort(sort_op)),
                }
            }
            if let Some(op) = final_op {
                post_agg.push(op)
            };
            Result::Ok(Pipeline {
                filter: query.search,
                pre_aggregates: pre_agg,
                aggregators: post_agg,
                renderer: Renderer::new(
                    RenderConfig {
                        floating_points: 2,
                        min_buffer: 4,
                        max_buffer: 8,
                    },
                    Duration::from_millis(50),
                ),
            })
        }

        fn matches(&self, raw: &str) -> bool {
            match self.filter {
                lang::Search::MatchAll => true,
                lang::Search::MatchFilter(ref filter) => raw.contains(filter),
            }
        }

        pub fn process<T: BufRead>(&mut self, mut buf: T) {
            // This is pretty slow in practice. We could move line splitting until after
            // we find a match. Another option is moving the transformation to String until
            // after we match (staying as Vec<u8> until then)
            let mut line = String::with_capacity(1024);
            while buf.read_line(&mut line).unwrap() > 0 {
                self.proc_str(&(line));
                line.clear();
            }
            // Run the aggregate to ensure it's updated with the latest results
            self.run_agg_pipeline(true);
        }

        fn proc_str(&mut self, s: &str) {
            if self.matches(s) {
                let mut rec = Record::new(s);
                for pre_agg in &self.pre_aggregates {
                    match (*pre_agg).process(rec) {
                        Some(next_rec) => rec = next_rec,
                        None => return,
                    }
                }

                let row = Row::Record(rec);
                if self.aggregators.is_empty() {
                    self.renderer.render(&row, false);
                    return;
                }
                // For every row, send it to the head aggregate
                (*self.aggregators[0]).process(row);
                // Only when we need to render, run the entire pipeline
                if self.renderer.should_print() {
                    self.run_agg_pipeline(false);
                }
            }
        }

        pub fn run_agg_pipeline(&mut self, last_row: bool) {
            if self.aggregators.is_empty() {
                return;
            }
            let mut row = Row::Aggregate((*self.aggregators[0]).emit());
            for agg in self.aggregators[1..].iter_mut() {
                (*agg).process(row);
                row = Row::Aggregate((*agg).emit());
            }
            self.renderer.render(&row, last_row);
        }
    }
}
