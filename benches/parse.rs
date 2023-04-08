use ag::data::Record;
use ag::lang::Keyword;
use ag::operator::UnaryPreAggFunction;

use ag::operator::parse::{Parse, ParseOptions};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

pub fn criterion_benchmark(c: &mut Criterion) {
    let parser = Parse::new(
        Keyword::new_wildcard("IP * > \"*\": * length *".to_string()).to_regex(),
        vec![
            "sender".to_string(),
            "recip".to_string(),
            "ignore".to_string(),
            "length".to_string(),
        ],
        None,
        ParseOptions {
            drop_nonmatching: true,
        },
    );
    let mut group = c.benchmark_group("parse_operator");
    group.throughput(Throughput::Elements(1));
    group.bench_function("ip query", |b| {
        b.iter_batched(
            || {
                Record::new(
                    "17:12:14.214111 IP 10.0.2.243.53938 > \"taotie.canonical.com.http\": \
             Flags [.], ack 56575, win 2375, options [nop,nop,TS val 13651369 ecr 169698010], \
             length 99",
                )
            },
            |rec| parser.process(rec),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
