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
    use crossbeam_channel::{bounded, Receiver, RecvTimeoutError};
    use data::{Record, Row};
    use lang;
    use lang::*;
    use operator;
    use render::{RenderConfig, Renderer};
    use std::collections::HashMap;
    use std::io::BufRead;
    use std::thread;
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
                    Box::new(operator::Fields::new(&fields, omode))
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

        fn convert_agg_function(func: lang::AggregateFunction) -> Box<operator::AggregateFunction> {
            match func {
                AggregateFunction::Count => Box::new(operator::Count::new()),
                AggregateFunction::Average { column } => Box::new(operator::Average::empty(column)),
                AggregateFunction::Sum { column } => Box::new(operator::Sum::empty(column)),
                AggregateFunction::Percentile {
                    column, percentile, ..
                } => Box::new(operator::Percentile::empty(column, percentile)),
            }
        }

        fn convert_multi_agg(op: lang::MultiAggregateOperator) -> Box<operator::AggregateOperator> {
            let mut agg_map = HashMap::new();
            for (output_column, func) in op.aggregate_functions {
                agg_map.insert(output_column, Pipeline::convert_agg_function(func));
            }
            let key_cols: Vec<&str> = op.key_cols.iter().map(AsRef::as_ref).collect();
            Box::new(operator::MultiGrouper::new(&key_cols[..], agg_map))
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
                    &Some(&Operator::MultiAggregate(ref agg_op)) => {
                        Some(Pipeline::convert_sort(SortOperator {
                            sort_cols: agg_op
                                .aggregate_functions
                                .iter()
                                .map(|&(ref k, _)| k)
                                .cloned()
                                .collect(),
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
                    Operator::MultiAggregate(agg_op) => {
                        in_agg = true;
                        post_agg.push(Pipeline::convert_multi_agg(agg_op))
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

        fn matches(pattern: &lang::Search, raw: &str) -> bool {
            match *pattern {
                lang::Search::MatchAll => true,
                lang::Search::MatchFilter(ref filter) => raw.contains(filter),
            }
        }
        fn render_noagg(mut renderer: Renderer, rx: &Receiver<Row>) {
            loop {
                let next = rx.recv_timeout(Duration::from_millis(50));
                match next {
                    Ok(row) => {
                        renderer.render(&row, false);
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        fn render_aggregate(
            mut head: Box<operator::AggregateOperator>,
            mut rest: Vec<Box<operator::AggregateOperator>>,
            mut renderer: Renderer,
            rx: &Receiver<Row>,
        ) {
            loop {
                let next = rx.recv_timeout(Duration::from_millis(50));
                match next {
                    Ok(row) => {
                        (*head).process(row);
                        if renderer.should_print() {
                            renderer.render(&Pipeline::run_agg_pipeline(&head, &mut rest), false);
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if renderer.should_print() {
                            renderer.render(&Pipeline::run_agg_pipeline(&head, &mut rest), false);
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
            renderer.render(&Pipeline::run_agg_pipeline(&head, &mut rest), true);
        }

        pub fn process<T: BufRead>(self, mut buf: T) {
            let (tx, rx) = bounded(1000);
            let mut aggregators = self.aggregators;
            let preaggs = self.pre_aggregates;
            let search = self.filter;
            let renderer = self.renderer;
            let t = if !aggregators.is_empty() {
                if aggregators.len() == 1 {
                    panic!("Every aggregate pipeline should have a real operator and a sort");
                }
                let head = aggregators.remove(0);
                thread::spawn(move || Pipeline::render_aggregate(head, aggregators, renderer, &rx))
            } else {
                thread::spawn(move || Pipeline::render_noagg(renderer, &rx))
            };

            // This is pretty slow in practice. We could move line splitting until after
            // we find a match. Another option is moving the transformation to String until
            // after we match (staying as Vec<u8> until then)
            let mut line = String::with_capacity(1024);
            while buf.read_line(&mut line).unwrap() > 0 {
                if let Some(row) = Pipeline::proc_preagg(&line, &search, &preaggs) {
                    tx.send(row).unwrap();
                }
                line.clear();
            }
            // Drop tx when causes the thread to exit.
            drop(tx);
            t.join().unwrap();
        }

        fn proc_preagg(
            s: &str,
            pattern: &lang::Search,
            pre_aggs: &[Box<operator::UnaryPreAggOperator>],
        ) -> Option<Row> {
            if Pipeline::matches(pattern, s) {
                let mut rec = Record::new(s);
                for pre_agg in pre_aggs {
                    match (*pre_agg).process(rec) {
                        Some(next_rec) => rec = next_rec,
                        None => return None,
                    }
                }
                Some(Row::Record(rec))
            } else {
                None
            }
        }

        pub fn run_agg_pipeline(
            head: &Box<operator::AggregateOperator>,
            rest: &mut [Box<operator::AggregateOperator>],
        ) -> Row {
            let mut row = Row::Aggregate((*head).emit());
            for agg in (*rest).iter_mut() {
                (*agg).process(row);
                row = Row::Aggregate((*agg).emit());
            }
            row
        }
    }
}
