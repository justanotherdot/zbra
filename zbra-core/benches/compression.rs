use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use zbra_core::binary::BinaryFile;
use zbra_core::compression::*;
use zbra_core::data::{Default, Encoding, IntEncoding};
use zbra_core::logical::{TableSchema, ValueSchema};
use zbra_core::striped::{Column, Table};

fn generate_sequential_data(size: usize) -> Vec<i64> {
    (0..size).map(|i| i as i64).collect()
}

fn generate_random_data(size: usize) -> Vec<i64> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    (0..size)
        .map(|i| {
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            hasher.finish() as i64
        })
        .collect()
}

fn generate_clustered_data(size: usize) -> Vec<i64> {
    // Generate data with clusters around certain values
    let clusters = [100, 1000, 10000, 100000];
    (0..size)
        .map(|i| {
            let cluster = clusters[i % clusters.len()];
            cluster + (i as i64 % 20) - 10 // +/- 10 around cluster center
        })
        .collect()
}

fn generate_time_series_data(size: usize) -> Vec<i64> {
    // Generate realistic time series data (timestamps)
    let base_time = 1640995200000; // 2022-01-01 00:00:00 UTC in milliseconds
    (0..size).map(|i| base_time + (i as i64 * 1000)).collect() // 1 second intervals
}

fn generate_string_data(size: usize, _avg_length: usize) -> Vec<u8> {
    let words = [
        "hello",
        "world",
        "compression",
        "benchmark",
        "performance",
        "test",
        "data",
    ];
    let mut result = Vec::new();

    for i in 0..size {
        let word = words[i % words.len()];
        result.extend_from_slice(word.as_bytes());
        if i < size - 1 {
            result.push(b' ');
        }
    }

    result
}

fn bench_frame_of_reference(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_of_reference");

    for size in [100, 1000, 10000, 100000].iter() {
        let sequential_data = generate_sequential_data(*size);
        let random_data = generate_random_data(*size);
        let clustered_data = generate_clustered_data(*size);
        let time_series_data = generate_time_series_data(*size);

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("sequential_encode", size),
            &sequential_data,
            |b, data| b.iter(|| frame_of_reference_encode(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("random_encode", size),
            &random_data,
            |b, data| b.iter(|| frame_of_reference_encode(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("clustered_encode", size),
            &clustered_data,
            |b, data| b.iter(|| frame_of_reference_encode(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("time_series_encode", size),
            &time_series_data,
            |b, data| b.iter(|| frame_of_reference_encode(black_box(data))),
        );

        // Test decode performance
        let (midpoint, deltas) = frame_of_reference_encode(&sequential_data);
        group.bench_with_input(
            BenchmarkId::new("sequential_decode", size),
            &(midpoint, &deltas),
            |b, (midpoint, deltas)| {
                b.iter(|| frame_of_reference_decode(black_box(*midpoint), black_box(deltas)))
            },
        );
    }

    group.finish();
}

fn bench_zig_zag(c: &mut Criterion) {
    let mut group = c.benchmark_group("zig_zag");

    for size in [100, 1000, 10000, 100000].iter() {
        let mixed_data: Vec<i64> = (0..*size)
            .map(|i| if i % 2 == 0 { i as i64 } else { -(i as i64) })
            .collect();
        let positive_data = generate_sequential_data(*size);
        let negative_data: Vec<i64> = (0..*size).map(|i| -(i as i64)).collect();

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("mixed_encode", size),
            &mixed_data,
            |b, data| b.iter(|| zig_zag_encode(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("positive_encode", size),
            &positive_data,
            |b, data| b.iter(|| zig_zag_encode(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("negative_encode", size),
            &negative_data,
            |b, data| b.iter(|| zig_zag_encode(black_box(data))),
        );

        // Test decode performance
        let encoded = zig_zag_encode(&mixed_data);
        group.bench_with_input(
            BenchmarkId::new("mixed_decode", size),
            &encoded,
            |b, data| b.iter(|| zig_zag_decode(black_box(data))),
        );
    }

    group.finish();
}

fn bench_bp64(c: &mut Criterion) {
    let mut group = c.benchmark_group("bp64");

    for size in [100, 1000, 10000, 100000].iter() {
        // Different bit-width scenarios
        let small_values: Vec<u64> = (0..*size).map(|i| (i % 16) as u64).collect(); // 4-bit values
        let medium_values: Vec<u64> = (0..*size).map(|i| (i % 256) as u64).collect(); // 8-bit values
        let large_values: Vec<u64> = (0..*size).map(|i| (i % 65536) as u64).collect(); // 16-bit values
        let huge_values: Vec<u64> = (0..*size).map(|i| i as u64).collect(); // Full range

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("small_values_pack", size),
            &small_values,
            |b, data| b.iter(|| bp64_pack(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("medium_values_pack", size),
            &medium_values,
            |b, data| b.iter(|| bp64_pack(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("large_values_pack", size),
            &large_values,
            |b, data| b.iter(|| bp64_pack(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("huge_values_pack", size),
            &huge_values,
            |b, data| b.iter(|| bp64_pack(black_box(data))),
        );

        // Test unpack performance
        let packed = bp64_pack(&small_values).unwrap();
        group.bench_with_input(
            BenchmarkId::new("small_values_unpack", size),
            &packed,
            |b, data| b.iter(|| bp64_unpack(black_box(data), black_box(*size))),
        );
    }

    group.finish();
}

fn bench_zstd_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("zstd_compression");

    for size in [1000, 10000, 100000].iter() {
        let text_data = generate_string_data(*size, 10);
        let random_data: Vec<u8> = (0..*size).map(|i| (i % 256) as u8).collect();
        let repetitive_data: Vec<u8> = vec![b'A'; *size];

        group.throughput(Throughput::Bytes(*size as u64));

        for level in [1, 3, 9, 22].iter() {
            let algorithm = CompressionAlgorithm::Zstd { level: *level };

            group.bench_with_input(
                BenchmarkId::new(format!("text_compress_level_{}", level), size),
                &text_data,
                |b, data| b.iter(|| compress_binary(black_box(data), black_box(&algorithm))),
            );

            group.bench_with_input(
                BenchmarkId::new(format!("random_compress_level_{}", level), size),
                &random_data,
                |b, data| b.iter(|| compress_binary(black_box(data), black_box(&algorithm))),
            );

            group.bench_with_input(
                BenchmarkId::new(format!("repetitive_compress_level_{}", level), size),
                &repetitive_data,
                |b, data| b.iter(|| compress_binary(black_box(data), black_box(&algorithm))),
            );

            // Test decompress performance
            let compressed = compress_binary(&text_data, &algorithm).unwrap();
            group.bench_with_input(
                BenchmarkId::new(format!("text_decompress_level_{}", level), size),
                &compressed,
                |b, data| b.iter(|| decompress_binary(black_box(data), black_box(&algorithm))),
            );
        }
    }

    group.finish();
}

fn bench_full_int_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_int_compression");

    for size in [100, 1000, 10000, 100000].iter() {
        let sequential_data = generate_sequential_data(*size);
        let random_data = generate_random_data(*size);
        let clustered_data = generate_clustered_data(*size);
        let time_series_data = generate_time_series_data(*size);

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("sequential_compress", size),
            &sequential_data,
            |b, data| b.iter(|| compress_int_array(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("random_compress", size),
            &random_data,
            |b, data| b.iter(|| compress_int_array(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("clustered_compress", size),
            &clustered_data,
            |b, data| b.iter(|| compress_int_array(black_box(data))),
        );

        group.bench_with_input(
            BenchmarkId::new("time_series_compress", size),
            &time_series_data,
            |b, data| b.iter(|| compress_int_array(black_box(data))),
        );

        // Test decompress performance
        let compressed = compress_int_array(&sequential_data).unwrap();
        group.bench_with_input(
            BenchmarkId::new("sequential_decompress", size),
            &compressed,
            |b, data| b.iter(|| decompress_int_array(black_box(data), black_box(*size))),
        );

        let compressed = compress_int_array(&clustered_data).unwrap();
        group.bench_with_input(
            BenchmarkId::new("clustered_decompress", size),
            &compressed,
            |b, data| b.iter(|| decompress_int_array(black_box(data), black_box(*size))),
        );
    }

    group.finish();
}

fn bench_binary_format_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("binary_format_roundtrip");

    for size in [100, 1000, 10000].iter() {
        let schema = TableSchema::Array {
            default: Default::Allow,
            element: Box::new(ValueSchema::Int {
                default: Default::Allow,
                encoding: Encoding::Int(IntEncoding::Int),
            }),
        };

        let table = Table::Array {
            default: Default::Allow,
            column: Box::new(Column::Int {
                default: Default::Allow,
                encoding: Encoding::Int(IntEncoding::Int),
                values: generate_clustered_data(*size),
            }),
        };

        group.throughput(Throughput::Elements(*size as u64));

        // Test different compression configurations
        let no_compression = CompressionConfig {
            binary_data: CompressionAlgorithm::None,
            strings: CompressionAlgorithm::None,
        };

        let zstd_compression = CompressionConfig {
            binary_data: CompressionAlgorithm::Zstd { level: 3 },
            strings: CompressionAlgorithm::Zstd { level: 3 },
        };

        group.bench_with_input(
            BenchmarkId::new("no_compression_write", size),
            &(&schema, &table, &no_compression),
            |b, (schema, table, compression)| {
                b.iter(|| {
                    let binary_file = BinaryFile::new_with_compression(
                        (*schema).clone(),
                        (*table).clone(),
                        (*compression).clone(),
                    );
                    binary_file.to_bytes()
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("zstd_compression_write", size),
            &(&schema, &table, &zstd_compression),
            |b, (schema, table, compression)| {
                b.iter(|| {
                    let binary_file = BinaryFile::new_with_compression(
                        (*schema).clone(),
                        (*table).clone(),
                        (*compression).clone(),
                    );
                    binary_file.to_bytes()
                })
            },
        );

        // Test read performance
        let binary_file =
            BinaryFile::new_with_compression(schema.clone(), table.clone(), no_compression);
        let bytes = binary_file.to_bytes().unwrap();
        group.bench_with_input(
            BenchmarkId::new("no_compression_read", size),
            &bytes,
            |b, data| b.iter(|| BinaryFile::from_bytes(black_box(data))),
        );

        let binary_file =
            BinaryFile::new_with_compression(schema.clone(), table.clone(), zstd_compression);
        let bytes = binary_file.to_bytes().unwrap();
        group.bench_with_input(
            BenchmarkId::new("zstd_compression_read", size),
            &bytes,
            |b, data| b.iter(|| BinaryFile::from_bytes(black_box(data))),
        );
    }

    group.finish();
}

fn bench_compression_ratios(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_ratios");

    // This benchmark focuses on measuring compression effectiveness
    for size in [1000, 10000, 100000].iter() {
        let sequential_data = generate_sequential_data(*size);
        let random_data = generate_random_data(*size);
        let clustered_data = generate_clustered_data(*size);
        let time_series_data = generate_time_series_data(*size);

        group.throughput(Throughput::Elements(*size as u64));

        // Benchmark that measures both time and compression ratio
        group.bench_with_input(
            BenchmarkId::new("sequential_ratio", size),
            &sequential_data,
            |b, data| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let compressed = compress_int_array(black_box(data)).unwrap();
                        let _decompressed =
                            decompress_int_array(black_box(&compressed), black_box(data.len()))
                                .unwrap();

                        // Calculate compression ratio (this won't affect timing)
                        let original_size = data.len() * 8; // 8 bytes per i64
                        let compressed_size = compressed.len();
                        let ratio = original_size as f64 / compressed_size as f64;

                        // Use black_box to prevent optimization
                        black_box(ratio);
                    }
                    start.elapsed()
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("random_ratio", size),
            &random_data,
            |b, data| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let compressed = compress_int_array(black_box(data)).unwrap();
                        let _decompressed =
                            decompress_int_array(black_box(&compressed), black_box(data.len()))
                                .unwrap();

                        let original_size = data.len() * 8;
                        let compressed_size = compressed.len();
                        let ratio = original_size as f64 / compressed_size as f64;
                        black_box(ratio);
                    }
                    start.elapsed()
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("clustered_ratio", size),
            &clustered_data,
            |b, data| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let compressed = compress_int_array(black_box(data)).unwrap();
                        let _decompressed =
                            decompress_int_array(black_box(&compressed), black_box(data.len()))
                                .unwrap();

                        let original_size = data.len() * 8;
                        let compressed_size = compressed.len();
                        let ratio = original_size as f64 / compressed_size as f64;
                        black_box(ratio);
                    }
                    start.elapsed()
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("time_series_ratio", size),
            &time_series_data,
            |b, data| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let compressed = compress_int_array(black_box(data)).unwrap();
                        let _decompressed =
                            decompress_int_array(black_box(&compressed), black_box(data.len()))
                                .unwrap();

                        let original_size = data.len() * 8;
                        let compressed_size = compressed.len();
                        let ratio = original_size as f64 / compressed_size as f64;
                        black_box(ratio);
                    }
                    start.elapsed()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_frame_of_reference,
    bench_zig_zag,
    bench_bp64,
    bench_zstd_compression,
    bench_full_int_compression,
    bench_binary_format_roundtrip,
    bench_compression_ratios
);

criterion_main!(benches);
