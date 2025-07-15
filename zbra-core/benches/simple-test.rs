use criterion::{black_box, criterion_group, criterion_main, Criterion};
use zbra_core::compression::*;

fn bench_simple(c: &mut Criterion) {
    let data = vec![1, 2, 3, 4, 5];

    c.bench_function("compress_int_array", |b| {
        b.iter(|| compress_int_array(black_box(&data)))
    });
}

criterion_group!(benches, bench_simple);
criterion_main!(benches);
