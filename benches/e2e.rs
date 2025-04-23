use ag::alias::AliasCollection;
use ag::pipeline::{ErrorReporter, OutputMode, Pipeline, QueryContainer};
use annotate_snippets::snippet::Snippet;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::Duration;

/// An ErrorReporter that writes errors related to the query string to the terminal
struct NopErrorReporter {}

impl ErrorReporter for NopErrorReporter {
    fn handle_error(&self, _snippet: Snippet) {}
}

struct NopWriter {}

impl Write for NopWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

struct E2eTest {
    name: String,
    query: String,
    file: String,
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let tests = vec![
        E2eTest {
            name: "star".to_owned(),
            query: "*".to_owned(),
            file: "benches/10k.inp".to_owned(),
        },
        E2eTest {
            name: "star-parse-count".to_owned(),
            query: "* | parse '*' as k | count by k".to_owned(),
            file: "benches/10k.inp".to_owned(),
        },
        E2eTest {
            name: "star-count-parse-count".to_owned(),
            query: "* | count | parse '*' as k | count by k".to_owned(),
            file: "benches/1k.inp".to_owned(),
        },
    ];
    tests.into_iter().for_each(|test| {
        let query_container = QueryContainer::new_with_aliases(
            test.query,
            Box::new(NopErrorReporter {}),
            AliasCollection::default(),
        );
        let mut group = c.benchmark_group("e2e_query");
        let num_elems = BufReader::new(File::open(&test.file).unwrap())
            .lines()
            .count();
        group.measurement_time(Duration::from_secs(25));
        group.throughput(Throughput::Elements(num_elems as u64));

        let name = test.name;
        let file = &test.file;
        group.bench_function(name, |b| {
            b.iter(|| {
                let pipeline =
                    Pipeline::new(&query_container, NopWriter {}, OutputMode::Legacy).unwrap();
                let f = File::open(file).unwrap();
                pipeline.process(black_box(BufReader::new(f)))
            })
        });
        group.finish();
    })
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
