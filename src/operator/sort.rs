use crate::data;
use crate::data::{Aggregate, Record, Row};
use crate::operator::{AggregateOperator, Data, EvalError, Expr};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

pub struct Sorter {
    columns: Vec<String>,
    state: Vec<Data>,
    #[allow(clippy::type_complexity)]
    ordering: Box<dyn Fn(&Data, &Data) -> Result<Ordering, EvalError> + Send + Sync>,
    direction: SortDirection,
}

impl Sorter {
    pub fn new(exprs: Vec<Expr>, direction: SortDirection) -> Self {
        let ordering = Box::new(Record::ordering(exprs));
        Sorter {
            state: Vec::new(),
            columns: Vec::new(),
            direction,
            ordering,
        }
    }

    fn new_columns(&self, data: &HashMap<String, data::Value>) -> Vec<String> {
        let mut new_keys: Vec<String> = data
            .keys()
            .filter(|key| !self.columns.contains(key))
            .cloned()
            .collect();
        new_keys.sort();
        new_keys
    }
}

impl AggregateOperator for Sorter {
    fn emit(&self) -> data::Aggregate {
        let mut sorted_data = self.state.to_vec();
        let order = &self.ordering;
        // To produce a deterministic sort, we should also sort by the non-key columns
        let second_ordering = Record::ordering_ref(&self.columns);

        // TODO: output errors here
        if self.direction == SortDirection::Ascending {
            sorted_data.sort_by(|l, r| {
                ((order)(l, r))
                    .unwrap_or(Ordering::Less)
                    .then(second_ordering(l, r))
            });
        } else {
            sorted_data.sort_by(|l, r| {
                ((order)(r, l))
                    .unwrap_or(Ordering::Less)
                    .then(second_ordering(l, r))
            });
        }
        Aggregate {
            data: sorted_data,
            columns: self.columns.clone(),
        }
    }

    fn process(&mut self, row: Row) {
        let order = &self.ordering;
        match row {
            Row::Aggregate(agg) => {
                self.columns = agg.columns;
                self.state = agg.data;
            }
            Row::Record(rec) => {
                let new_cols = self.new_columns(&rec.data);
                self.state.push(rec.data);
                self.state
                    .sort_by(|l, r| ((order)(l, r)).unwrap_or(Ordering::Less));
                self.columns.extend(new_cols);
            }
        }
    }
}
