use std::cell::RefCell;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use db_compare::{counter, diff::IOType, Args, Config};

fn config() -> (Config, RefCell<IOType>) {
    Config::new(&Args {
        config: Some("benches/counter-config.yml".to_string()),
        ..Default::default()
    })
}

fn bench_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("Counter");
    group.sample_size(10);
    let (conf, io) = config();

    group.bench_function("Parallel", |b| {
        b.iter(|| counter::rayon_run(black_box(&conf), black_box(&io)))
    });
    group.bench_function("Sequencial", |b| {
        b.iter(|| counter::run(black_box(&conf), black_box(&io)))
    });

    group.finish();
}

criterion_group!(benches, bench_counter);
criterion_main!(benches);
