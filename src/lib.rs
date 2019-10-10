#[macro_use]
extern crate failure;
#[macro_use]
extern crate include_dir;
#[macro_use]
extern crate serde_derive;
extern crate atty;
extern crate nom_locate;
extern crate num_derive;
extern crate num_traits;
extern crate serde;
extern crate toml;

extern crate annotate_snippets;
extern crate crossbeam_channel;

mod alias;
mod data;
mod errors;
mod filter;
mod lang;
mod operator;
mod render;
mod typecheck;

pub mod pipeline {
    use crate::data::{Record, Row};
    pub use crate::errors::{ErrorReporter, QueryContainer};
    use crate::filter;
    use crate::lang::*;
    use crate::operator;
    use crate::render::{RenderConfig, Renderer};
    use crate::typecheck::{TypeCheck, TypeError};
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
        filter: filter::Filter,
        pre_aggregates: Vec<Box<dyn operator::UnaryPreAggOperator>>,
        aggregators: Vec<Box<dyn operator::AggregateOperator>>,
        renderer: Renderer,
    }

    impl Pipeline {
        fn convert_sort(op: SortOperator) -> Box<dyn operator::AggregateOperator> {
            let mode = match op.direction {
                SortMode::Ascending => operator::SortDirection::Ascending,
                SortMode::Descending => operator::SortDirection::Descending,
            };
            Box::new(operator::Sorter::new(op.sort_cols, mode))
        }

        fn convert_multi_agg(
            op: MultiAggregateOperator,
            pipeline: &QueryContainer,
        ) -> Result<Box<dyn operator::AggregateOperator>, TypeError> {
            let mut agg_functions = Vec::with_capacity(op.aggregate_functions.len());

            for agg in op.aggregate_functions {
                let operator_function = agg.1.type_check(pipeline)?;
                agg_functions.push((agg.0, operator_function));
            }
            let key_cols: Vec<operator::Expr> = op
                .key_cols
                .into_iter()
                .map(|expr| expr.type_check(pipeline))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Box::new(operator::MultiGrouper::new(
                &(key_cols)[..],
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

        fn convert_filter(filter: Search) -> filter::Filter {
            match filter {
                Search::And(vec) => {
                    filter::Filter::And(vec.into_iter().map(Pipeline::convert_filter).collect())
                }
                Search::Or(vec) => {
                    filter::Filter::Or(vec.into_iter().map(Pipeline::convert_filter).collect())
                }
                Search::Not(search) => {
                    filter::Filter::Not(Box::new(Pipeline::convert_filter(*search)))
                }
                Search::Keyword(keyword) => filter::Filter::Keyword(keyword.to_regex()),
            }
        }

        pub fn new(pipeline: &QueryContainer, format: Option<String>) -> Result<Self, Error> {
            let parsed = pipeline.parse().map_err(|_pos| CompileError::Parse);
            let query = parsed?;
            let filters = Pipeline::convert_filter(query.search);
            let mut in_agg = false;
            let mut pre_agg: Vec<Box<dyn operator::UnaryPreAggOperator>> = Vec::new();
            let mut post_agg: Vec<Box<dyn operator::AggregateOperator>> = Vec::new();
            let mut op_iter = query.operators.into_iter().peekable();
            let mut has_errors = false;
            while let Some(op) = op_iter.next() {
                match op {
                    Operator::Inline(inline_op) => {
                        let op_builder = inline_op.type_check(pipeline)?;

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
                                Some(Operator::Inline(Positioned {
                                    value: InlineOperator::Limit { .. },
                                    ..
                                })) => true,
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
                        format: format,
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
                        let result = renderer.render(&row, false);

                        if let Err(e) = result {
                            eprintln!("error: {}", e);
                            break;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        fn render_aggregate(
            mut head: Box<dyn operator::AggregateOperator>,
            mut rest: Vec<Box<dyn operator::AggregateOperator>>,
            mut renderer: Renderer,
            rx: &Receiver<Row>,
        ) {
            loop {
                let next = rx.recv_timeout(Duration::from_millis(50));
                match next {
                    Ok(row) => (*head).process(row),
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }

                if renderer.should_print() {
                    let result =
                        renderer.render(&Pipeline::run_agg_pipeline(&head, &mut rest), false);

                    if let Err(e) = result {
                        eprintln!("error: {}", e);
                        return;
                    }
                }
            }
            let result = renderer.render(&Pipeline::run_agg_pipeline(&head, &mut rest), true);

            if let Err(e) = result {
                eprintln!("error: {}", e);
                return;
            }
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
                if self.filter.matches(&line) {
                    if !Pipeline::proc_preagg(Record::new(&line), &mut preaggs, &tx) {
                        break;
                    }
                }
                line.clear();
            }

            // Drain any remaining records from the operators.
            while !preaggs.is_empty() {
                let preagg = preaggs.remove(0);

                for rec in preagg.drain() {
                    if !Pipeline::proc_preagg(rec, &mut preaggs, &tx) {
                        break;
                    }
                }
            }

            // Drop tx when causes the thread to exit.
            drop(tx);
            match t.join() {
                Ok(_) => (),
                Err(e) => println!("Error: {:?}", e),
            }
        }

        /// Process a record using the pre-agg operators.  The output of the last operator will be
        /// sent to `tx`.
        fn proc_preagg(
            mut rec: Record,
            pre_aggs: &mut [Box<dyn operator::UnaryPreAggOperator>],
            tx: &Sender<Row>,
        ) -> bool {
            for pre_agg in pre_aggs {
                match (*pre_agg).process_mut(rec) {
                    Ok(Some(next_rec)) => rec = next_rec,
                    Ok(None) => return true,
                    Err(err) => {
                        eprintln!("error: {}", err);
                        return true;
                    }
                }
            }

            tx.send(Row::Record(rec)).is_ok()
        }

        pub fn run_agg_pipeline(
            head: &Box<dyn operator::AggregateOperator>,
            rest: &mut [Box<dyn operator::AggregateOperator>],
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
