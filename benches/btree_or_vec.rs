use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::collections::BTreeMap;

fn insert_remove_btree(list: &[u32]) {
    let mut btree: BTreeMap<u32, u32> = BTreeMap::new();
    for i in list {
        btree.insert(*i, *i);
    }
    for i in list {
        btree.remove(i);
    }
}
fn insert_get_btree(list: &[u32]) {
    let mut btree: BTreeMap<u32, u32> = BTreeMap::new();
    for i in list {
        btree.insert(*i, *i);
    }
    for i in list {
        btree.get(i);
    }
}

fn remove_vec(list: &[u32]) {
    let mut vec: Vec<u32> = Vec::new();
    for i in list {
        vec.push(*i);
    }
    for i in list {
        vec.contains(i);
    }
    for i in list {
        vec.retain(|x| x != i);
    }
}
fn only_contain_vec(list: &[u32]) {
    let mut vec: Vec<u32> = Vec::new();
    for i in list {
        vec.push(*i);
    }
    for i in list {
        vec.contains(i);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let vec = (0..10000).collect::<Vec<u32>>();
    c.bench_function("insert_remove_btree", |b| {
        b.iter(|| insert_remove_btree(black_box(&vec)))
    });
    c.bench_function("insert_get_btree", |b| {
        b.iter(|| insert_get_btree(black_box(&vec)))
    });
    c.bench_function("remove_vec", |b| b.iter(|| remove_vec(black_box(&vec))));
    c.bench_function("only_contain_vec", |b| {
        b.iter(|| only_contain_vec(black_box(&vec)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
