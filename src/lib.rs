#[macro_use]
extern crate failure;

extern crate crossbeam_channel;

mod data;
mod lang;
mod operator;
mod render;
mod typecheck;

pub mod pipeline {
    use crate::data::{Record, Row};
    use crate::lang;
    use crate::lang::*;
    use crate::operator;
    use crate::render::{RenderConfig, Renderer};
    use crossbeam_channel::{bounded, Receiver, RecvTimeoutError};
    use failure::Error;
    use std::io::BufRead;
    use std::thread;
    use std::time::Duration;

    #[derive(Debug, Fail)]
    pub enum CompileError {
        #[fail(display = "Failure parsing query. Parse failed at: {}", message)]
        Parse { message: String },

        #[fail(display = "Non aggregate operators can't follow aggregate operators")]
        NonAggregateAfterAggregate,

        #[fail(display = "Unexpected failure: {}", message)]
        Unexpected { message: String },
    }

    pub struct Pipeline {
        filter: Vec<regex::Regex>,
        pre_aggregates: Vec<Box<operator::UnaryPreAggOperator>>,
        aggregators: Vec<Box<operator::AggregateOperator>>,
        renderer: Renderer,
    }

    impl Pipeline {
        fn convert_total(op: lang::TotalOperator) -> Box<operator::AggregateOperator> {
            Box::new(operator::Total::new(op.input_column, op.output_column))
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
                AggregateFunction::CountDistinct { column } => {
                    Box::new(operator::CountDistinct::empty(column))
                }
            }
        }

        fn convert_multi_agg(op: lang::MultiAggregateOperator) -> Box<operator::AggregateOperator> {
            let agg_functions = op
                .aggregate_functions
                .into_iter()
                .map(|(k, func)| (k, Pipeline::convert_agg_function(func)));
            let key_cols: Vec<operator::Expr> =
                op.key_cols.into_iter().map(|expr| expr.into()).collect();
            Box::new(operator::MultiGrouper::new(
                &key_cols[..],
                op.key_col_headers,
                agg_functions.collect(),
            ))
        }

        pub fn new(pipeline: &str) -> Result<Self, Error> {
            let parsed = lang::parse_query(pipeline).map_err(|e| CompileError::Parse {
                message: format!("{:?}", e),
            });
            let query = parsed?;
            let filters = query.search.iter().map(lang::Keyword::to_regex).collect();
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
                    Operator::Inline(inline_op) => {
                        let op_builder = inline_op.semantic_analysis()?;

                        if !in_agg {
                            pre_agg.push(op_builder.build());
                        } else {
                            post_agg.push(Box::new(operator::PreAggAdapter::new(op_builder)));
                        }
                    }
                    Operator::MultiAggregate(agg_op) => {
                        in_agg = true;
                        post_agg.push(Pipeline::convert_multi_agg(agg_op))
                    }
                    Operator::Sort(sort_op) => post_agg.push(Pipeline::convert_sort(sort_op)),
                    Operator::Total(total_op) => post_agg.push(Pipeline::convert_total(total_op)),
                }
            }
            if let Some(op) = final_op {
                post_agg.push(op)
            };
            Result::Ok(Pipeline {
                filter: filters,
                pre_aggregates: pre_agg,
                aggregators: post_agg,
                renderer: Renderer::new(
                    RenderConfig {
                        floating_points: 2,
                        min_buffer: 4,
                        max_buffer: 8,
                    },
                    Duration::from_millis(50),
                )
            })
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
            let mut preaggs = self.pre_aggregates;
            let renderer = self.renderer;
            let t = if !aggregators.is_empty() {
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
                if let Some(row) = Pipeline::proc_preagg(&line, &self.filter, &mut preaggs) {
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
            filters: &[regex::Regex],
            pre_aggs: &mut [Box<operator::UnaryPreAggOperator>],
        ) -> Option<Row> {
            if filters.iter().all(|re| re.is_match(s)) {
                let mut rec = Record::new(s);
                for pre_agg in pre_aggs {
                    match (*pre_agg).process_mut(rec) {
                        Ok(Some(next_rec)) => rec = next_rec,
                        Ok(None) => return None,
                        Err(_) => return None,
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
