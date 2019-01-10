#[macro_use]
extern crate failure;
extern crate atty;
extern crate nom_locate;
extern crate num_derive;
extern crate num_traits;

extern crate annotate_snippets;
extern crate crossbeam_channel;

mod data;
mod errors;
mod lang;
mod operator;
mod render;
mod typecheck;

pub mod pipeline {
    use crate::data::{Record, Row};
    pub use crate::errors::{ErrorReporter, QueryContainer};
    use crate::lang::*;
    use crate::operator;
    use crate::render::{RenderConfig, Renderer};
    use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};
    use failure::Error;
    use std::io::BufRead;
    use std::thread;
    use std::time::Duration;

    #[derive(Debug, Fail)]
    pub enum CompileError {
        #[fail(display = "Failed to parse query")]
        Parse,

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
        fn convert_sort(op: SortOperator) -> Box<operator::AggregateOperator> {
            let mode = match op.direction {
                SortMode::Ascending => operator::SortDirection::Ascending,
                SortMode::Descending => operator::SortDirection::Descending,
            };
            Box::new(operator::Sorter::new(op.sort_cols, mode))
        }

        fn convert_multi_agg(
            op: MultiAggregateOperator,
            pipeline: &QueryContainer,
        ) -> Result<Box<operator::AggregateOperator>, ()> {
            let mut agg_functions = Vec::with_capacity(op.aggregate_functions.len());
            let mut has_errors = false;

            for agg in op.aggregate_functions {
                if let Ok(operator_function) = agg.1.semantic_analysis(pipeline) {
                    agg_functions.push((agg.0, operator_function));
                } else {
                    has_errors = true;
                }
            }
            if has_errors {
                return Err(());
            }
            let key_cols: Vec<operator::Expr> =
                op.key_cols.into_iter().map(|expr| expr.into()).collect();
            Ok(Box::new(operator::MultiGrouper::new(
                &key_cols[..],
                op.key_col_headers,
                agg_functions,
            )))
        }

        fn implicit_sort(multi_agg: &MultiAggregateOperator) -> SortOperator {
            SortOperator {
                sort_cols: multi_agg
                    .aggregate_functions
                    .iter()
                    .map(|&(ref k, _)| k)
                    .cloned()
                    .collect(),
                direction: SortMode::Descending,
            }
        }

        pub fn new(pipeline: &QueryContainer) -> Result<Self, Error> {
            let parsed = pipeline.parse().map_err(|_pos| CompileError::Parse);
            let query = parsed?;
            let filters = query.search.iter().map(Keyword::to_regex).collect();
            let mut in_agg = false;
            let mut pre_agg: Vec<Box<operator::UnaryPreAggOperator>> = Vec::new();
            let mut post_agg: Vec<Box<operator::AggregateOperator>> = Vec::new();
            let mut op_iter = query.operators.into_iter().peekable();
            let mut has_errors = false;
            while let Some(op) = op_iter.next() {
                match op {
                    Operator::Inline(inline_op) => {
                        let op_builder = inline_op.semantic_analysis(pipeline)?;

                        if !in_agg {
                            pre_agg.push(op_builder.build());
                        } else {
                            post_agg.push(Box::new(operator::PreAggAdapter::new(op_builder)));
                        }
                    }
                    Operator::MultiAggregate(agg_op) => {
                        in_agg = true;
                        let sorter = Pipeline::implicit_sort(&agg_op);
                        if let Ok(op) = Pipeline::convert_multi_agg(agg_op, pipeline) {
                            post_agg.push(op);

                            let needs_sort = match op_iter.peek() {
                                Some(Operator::Inline(InlineOperator::Limit { .. })) => true,
                                None => true,
                                _ => false,
                            };
                            if needs_sort {
                                post_agg.push(Pipeline::convert_sort(sorter));
                            }
                        } else {
                            has_errors = true;
                        }
                    }
                    Operator::Sort(sort_op) => post_agg.push(Pipeline::convert_sort(sort_op)),
                }
            }
            if has_errors {
                return Err(CompileError::Parse.into());
            }
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
                ),
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
                if self.filter.iter().all(|re| re.is_match(&line)) {
                    Pipeline::proc_preagg(Record::new(&line), &mut preaggs, &tx);
                }
                line.clear();
            }

            // Drain any remaining records from the operators.
            while !preaggs.is_empty() {
                let preagg = preaggs.remove(0);

                for rec in preagg.drain() {
                    Pipeline::proc_preagg(rec, &mut preaggs, &tx);
                }
            }

            // Drop tx when causes the thread to exit.
            drop(tx);
            t.join().unwrap();
        }

        /// Process a record using the pre-agg operators.  The output of the last operator will be
        /// sent to `tx`.
        fn proc_preagg(
            mut rec: Record,
            pre_aggs: &mut [Box<operator::UnaryPreAggOperator>],
            tx: &Sender<Row>,
        ) {
            for pre_agg in pre_aggs {
                match (*pre_agg).process_mut(rec) {
                    Ok(Some(next_rec)) => rec = next_rec,
                    Ok(None) => return,
                    Err(err) => {
                        eprintln!("error: {}", err);
                        return;
                    }
                }
            }
            tx.send(Row::Record(rec)).unwrap();
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
