use crate::data::Record;
use crate::operator::{EvalError, OperatorBuilder, UnaryPreAggOperator};
use std::collections::VecDeque;
use std::iter;

/// The state for a limit operator
pub enum Limit {
    Head {
        /// The current index into the input stream.
        index: u64,
        /// The number of rows to pass through before aborting.
        limit: u64,
    },
    Tail {
        /// A circular queue to keep track of the tail of the input stream.
        queue: VecDeque<Record>,
        /// The size of the circular buffer.
        /// XXX Might be better to use a separate type.
        limit: usize,
    },
}

impl UnaryPreAggOperator for Limit {
    fn process_mut(&mut self, rec: Record) -> Result<Option<Record>, EvalError> {
        match self {
            Limit::Head {
                ref mut index,
                limit,
            } => {
                (*index) += 1;

                if index <= limit {
                    Ok(Some(rec))
                } else {
                    Ok(None)
                }
            }
            Limit::Tail {
                ref mut queue,
                limit,
            } => {
                if queue.len() == *limit {
                    queue.pop_front();
                }
                queue.push_back(rec);

                Ok(None)
            }
        }
    }

    fn drain(self: Box<Self>) -> Box<dyn Iterator<Item = Record>> {
        match *self {
            Limit::Head { .. } => Box::new(iter::empty()),
            Limit::Tail { queue, .. } => Box::new(queue.into_iter()),
        }
    }
}

impl LimitDef {
    pub fn new(limit: i64) -> Self {
        LimitDef { limit }
    }
}

/// The definition for a limit operator, which is a positive number used to specify whether
/// the first N rows should be passed through to the downstream operators.  Negative limits are
/// not supported at this time.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct LimitDef {
    limit: i64,
}

impl OperatorBuilder for LimitDef {
    fn build(&self) -> Box<dyn UnaryPreAggOperator> {
        Box::new(if self.limit > 0 {
            Limit::Head {
                index: 0,
                limit: self.limit as u64,
            }
        } else {
            Limit::Tail {
                queue: VecDeque::with_capacity(-self.limit as usize),
                limit: -self.limit as usize,
            }
        })
    }
}
