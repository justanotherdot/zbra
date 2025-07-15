use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use zbra_core::binary::BinaryFile;
use zbra_core::compression::*;
use zbra_core::data::{BinaryEncoding, Default, Encoding, IntEncoding};
use zbra_core::logical::{FieldSchema, TableSchema, ValueSchema};
use zbra_core::striped::{Column, FieldColumn, Table};

fn generate_streaming_data(rows: usize, cols: usize) -> Vec<Table> {
    let mut tables = Vec::new();

    for chunk in 0..(rows / 1000).max(1) {
        let base_value = chunk * 1000;
        let chunk_size = if chunk == (rows / 1000) {
            rows % 1000
        } else {
            1000
        };

        if chunk_size == 0 {
            break;
        }

        let mut fields = Vec::new();

        for col in 0..cols {
            let col_name = format!("col_{}", col);
            let values: Vec<i64> = (0..chunk_size)
                .map(|i| base_value as i64 + i as i64 + (col * 100) as i64)
                .collect();

            fields.push(FieldColumn {
                name: col_name,
                column: Column::Int {
                    default: Default::Allow,
                    encoding: Encoding::Int(IntEncoding::Int),
                    values,
                },
            });
        }

        tables.push(Table::Array {
            default: Default::Allow,
            column: Box::new(Column::Struct {
                default: Default::Allow,
                fields,
            }),
        });
    }

    tables
}

fn generate_time_series_stream(rows: usize, series_count: usize) -> Vec<Table> {
    let mut tables = Vec::new();
    let chunk_size = 1000;

    for chunk in 0..(rows / chunk_size).max(1) {
        let base_time = 1640995200000i64 + (chunk * chunk_size * 1000) as i64; // 2022-01-01 + offset
        let actual_chunk_size = if chunk == (rows / chunk_size) {
            rows % chunk_size
        } else {
            chunk_size
        };

        if actual_chunk_size == 0 {
            break;
        }

        let mut fields = Vec::new();

        // Timestamp column
        let timestamps: Vec<i64> = (0..actual_chunk_size)
            .map(|i| base_time + i as i64 * 1000)
            .collect();

        fields.push(FieldColumn {
            name: "timestamp".to_string(),
            column: Column::Int {
                default: Default::Deny,
                encoding: Encoding::Int(IntEncoding::TimeMilliseconds),
                values: timestamps,
            },
        });

        // Value columns (simulate multiple time series)
        for series in 0..series_count {
            let values: Vec<i64> = (0..actual_chunk_size)
                .map(|i| {
                    // Simulate realistic time series with trend + noise
                    let trend = (chunk * chunk_size + i) as i64 * 2;
                    let noise = ((i * 17 + series * 23) % 20) as i64 - 10;
                    1000 + trend + noise
                })
                .collect();

            fields.push(FieldColumn {
                name: format!("series_{}", series),
                column: Column::Int {
                    default: Default::Allow,
                    encoding: Encoding::Int(IntEncoding::Int),
                    values,
                },
            });
        }

        tables.push(Table::Array {
            default: Default::Allow,
            column: Box::new(Column::Struct {
                default: Default::Allow,
                fields,
            }),
        });
    }

    tables
}

fn generate_log_stream(rows: usize) -> Vec<Table> {
    let mut tables = Vec::new();
    let chunk_size = 1000;
    let log_levels = ["DEBUG", "INFO", "WARN", "ERROR"];
    let log_messages = [
        "Processing request",
        "Database query executed",
        "Cache hit",
        "Cache miss",
        "Connection established",
        "Connection closed",
        "Request completed",
        "Error occurred",
    ];

    for chunk in 0..(rows / chunk_size).max(1) {
        let base_time = 1640995200000i64 + (chunk * chunk_size * 1000) as i64;
        let actual_chunk_size = if chunk == (rows / chunk_size) {
            rows % chunk_size
        } else {
            chunk_size
        };

        if actual_chunk_size == 0 {
            break;
        }

        let mut fields = Vec::new();

        // Timestamp
        let timestamps: Vec<i64> = (0..actual_chunk_size)
            .map(|i| base_time + i as i64 * 1000)
            .collect();

        fields.push(FieldColumn {
            name: "timestamp".to_string(),
            column: Column::Int {
                default: Default::Deny,
                encoding: Encoding::Int(IntEncoding::TimeMilliseconds),
                values: timestamps,
            },
        });

        // Log level (as enum-like integers)
        let levels: Vec<i64> = (0..actual_chunk_size)
            .map(|i| (i % log_levels.len()) as i64)
            .collect();

        fields.push(FieldColumn {
            name: "level".to_string(),
            column: Column::Int {
                default: Default::Deny,
                encoding: Encoding::Int(IntEncoding::Int),
                values: levels,
            },
        });

        // Log message (as string data)
        let mut message_data = Vec::new();
        let mut message_lengths = Vec::new();

        for i in 0..actual_chunk_size {
            let message = log_messages[i % log_messages.len()];
            message_data.extend_from_slice(message.as_bytes());
            message_lengths.push(message.len());
        }

        fields.push(FieldColumn {
            name: "message".to_string(),
            column: Column::Binary {
                default: Default::Allow,
                encoding: Encoding::Binary(BinaryEncoding::Utf8),
                lengths: message_lengths,
                data: message_data,
            },
        });

        tables.push(Table::Array {
            default: Default::Allow,
            column: Box::new(Column::Struct {
                default: Default::Allow,
                fields,
            }),
        });
    }

    tables
}

fn bench_streaming_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_write");

    // Test different chunk sizes and compression settings
    for &rows in [1000, 10000, 100000].iter() {
        for &cols in [5, 10, 20].iter() {
            let tables = generate_streaming_data(rows, cols);
            let schema = create_schema_for_streaming_data(cols);

            group.throughput(Throughput::Elements((rows * cols) as u64));

            // No compression
            let no_compression = CompressionConfig {
                binary_data: CompressionAlgorithm::None,
                strings: CompressionAlgorithm::None,
            };

            group.bench_with_input(
                BenchmarkId::new(format!("no_compression_{}x{}", rows, cols), rows * cols),
                &(&tables, &schema, &no_compression),
                |b, (tables, schema, compression)| {
                    b.iter(|| {
                        let mut buffer = Vec::new();
                        for table in tables.iter() {
                            let binary_file = BinaryFile::new_with_compression(
                                (*schema).clone(),
                                table.clone(),
                                (*compression).clone(),
                            );
                            let bytes = binary_file.to_bytes().unwrap();
                            buffer.extend_from_slice(&bytes);
                        }
                        black_box(buffer)
                    })
                },
            );

            // Zstd compression
            let zstd_compression = CompressionConfig {
                binary_data: CompressionAlgorithm::Zstd { level: 3 },
                strings: CompressionAlgorithm::Zstd { level: 3 },
            };

            group.bench_with_input(
                BenchmarkId::new(format!("zstd_compression_{}x{}", rows, cols), rows * cols),
                &(&tables, &schema, &zstd_compression),
                |b, (tables, schema, compression)| {
                    b.iter(|| {
                        let mut buffer = Vec::new();
                        for table in tables.iter() {
                            let binary_file = BinaryFile::new_with_compression(
                                (*schema).clone(),
                                table.clone(),
                                (*compression).clone(),
                            );
                            let bytes = binary_file.to_bytes().unwrap();
                            buffer.extend_from_slice(&bytes);
                        }
                        black_box(buffer)
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_streaming_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_read");

    for &rows in [1000, 10000, 100000].iter() {
        for &cols in [5, 10, 20].iter() {
            let tables = generate_streaming_data(rows, cols);
            let schema = create_schema_for_streaming_data(cols);

            group.throughput(Throughput::Elements((rows * cols) as u64));

            // Pre-serialize data for read benchmarks
            let no_compression = CompressionConfig {
                binary_data: CompressionAlgorithm::None,
                strings: CompressionAlgorithm::None,
            };

            let mut no_compression_data = Vec::new();
            for table in &tables {
                let binary_file = BinaryFile::new_with_compression(
                    schema.clone(),
                    table.clone(),
                    no_compression.clone(),
                );
                let bytes = binary_file.to_bytes().unwrap();
                no_compression_data.push(bytes);
            }

            group.bench_with_input(
                BenchmarkId::new(format!("no_compression_{}x{}", rows, cols), rows * cols),
                &no_compression_data,
                |b, data| {
                    b.iter(|| {
                        let mut results = Vec::new();
                        for chunk in data.iter() {
                            let binary_file = BinaryFile::from_bytes(black_box(chunk)).unwrap();
                            results.push(binary_file);
                        }
                        black_box(results)
                    })
                },
            );

            // Zstd compression
            let zstd_compression = CompressionConfig {
                binary_data: CompressionAlgorithm::Zstd { level: 3 },
                strings: CompressionAlgorithm::Zstd { level: 3 },
            };

            let mut zstd_compression_data = Vec::new();
            for table in &tables {
                let binary_file = BinaryFile::new_with_compression(
                    schema.clone(),
                    table.clone(),
                    zstd_compression.clone(),
                );
                let bytes = binary_file.to_bytes().unwrap();
                zstd_compression_data.push(bytes);
            }

            group.bench_with_input(
                BenchmarkId::new(format!("zstd_compression_{}x{}", rows, cols), rows * cols),
                &zstd_compression_data,
                |b, data| {
                    b.iter(|| {
                        let mut results = Vec::new();
                        for chunk in data.iter() {
                            let binary_file = BinaryFile::from_bytes(black_box(chunk)).unwrap();
                            results.push(binary_file);
                        }
                        black_box(results)
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_time_series_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_series_streaming");

    for &rows in [10000, 100000].iter() {
        for &series_count in [1, 5, 10].iter() {
            let tables = generate_time_series_stream(rows, series_count);
            let schema = create_time_series_schema(series_count);

            group.throughput(Throughput::Elements((rows * (series_count + 1)) as u64));

            let compression = CompressionConfig {
                binary_data: CompressionAlgorithm::Zstd { level: 3 },
                strings: CompressionAlgorithm::Zstd { level: 3 },
            };

            // Write benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("write_{}rows_{}series", rows, series_count), rows),
                &(&tables, &schema, &compression),
                |b, (tables, schema, compression)| {
                    b.iter(|| {
                        let mut total_bytes = 0;
                        for table in tables.iter() {
                            let binary_file = BinaryFile::new_with_compression(
                                (*schema).clone(),
                                table.clone(),
                                (*compression).clone(),
                            );
                            let bytes = binary_file.to_bytes().unwrap();
                            total_bytes += bytes.len();
                        }
                        black_box(total_bytes)
                    })
                },
            );

            // Pre-serialize for read benchmark
            let mut serialized_data = Vec::new();
            for table in &tables {
                let binary_file = BinaryFile::new_with_compression(
                    schema.clone(),
                    table.clone(),
                    compression.clone(),
                );
                let bytes = binary_file.to_bytes().unwrap();
                serialized_data.push(bytes);
            }

            // Read benchmark
            group.bench_with_input(
                BenchmarkId::new(format!("read_{}rows_{}series", rows, series_count), rows),
                &serialized_data,
                |b, data| {
                    b.iter(|| {
                        let mut total_rows = 0;
                        for chunk in data.iter() {
                            let binary_file = BinaryFile::from_bytes(black_box(chunk)).unwrap();
                            if let Some(table) = binary_file.table() {
                                total_rows += table.row_count();
                            }
                        }
                        black_box(total_rows)
                    })
                },
            );
        }
    }

    group.finish();
}

fn bench_log_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_streaming");

    for &rows in [10000, 100000].iter() {
        let tables = generate_log_stream(rows);
        let schema = create_log_schema();

        group.throughput(Throughput::Elements(rows as u64));

        let compression = CompressionConfig {
            binary_data: CompressionAlgorithm::Zstd { level: 3 },
            strings: CompressionAlgorithm::Zstd { level: 3 },
        };

        // Write benchmark
        group.bench_with_input(
            BenchmarkId::new("write", rows),
            &(&tables, &schema, &compression),
            |b, (tables, schema, compression)| {
                b.iter(|| {
                    let mut total_bytes = 0;
                    for table in tables.iter() {
                        let binary_file = BinaryFile::new_with_compression(
                            (*schema).clone(),
                            table.clone(),
                            (*compression).clone(),
                        );
                        let bytes = binary_file.to_bytes().unwrap();
                        total_bytes += bytes.len();
                    }
                    black_box(total_bytes)
                })
            },
        );

        // Pre-serialize for read benchmark
        let mut serialized_data = Vec::new();
        for table in &tables {
            let binary_file = BinaryFile::new_with_compression(
                schema.clone(),
                table.clone(),
                compression.clone(),
            );
            let bytes = binary_file.to_bytes().unwrap();
            serialized_data.push(bytes);
        }

        // Read benchmark
        group.bench_with_input(
            BenchmarkId::new("read", rows),
            &serialized_data,
            |b, data| {
                b.iter(|| {
                    let mut total_rows = 0;
                    for chunk in data.iter() {
                        let binary_file = BinaryFile::from_bytes(black_box(chunk)).unwrap();
                        if let Some(table) = binary_file.table() {
                            total_rows += table.row_count();
                        }
                    }
                    black_box(total_rows)
                })
            },
        );

        // Measure compression effectiveness
        let uncompressed_size: usize = tables.iter().map(|t| estimate_uncompressed_size(t)).sum();
        let compressed_size: usize = serialized_data.iter().map(|d| d.len()).sum();
        let compression_ratio = uncompressed_size as f64 / compressed_size as f64;

        group.bench_with_input(
            BenchmarkId::new("compression_ratio", rows),
            &compression_ratio,
            |b, ratio| {
                b.iter(|| {
                    // This benchmark just reports the compression ratio
                    black_box(*ratio)
                })
            },
        );
    }

    group.finish();
}

// Helper functions for schema creation
fn create_schema_for_streaming_data(cols: usize) -> TableSchema {
    let mut fields = Vec::new();

    for col in 0..cols {
        fields.push(FieldSchema {
            name: format!("col_{}", col),
            schema: ValueSchema::Int {
                default: Default::Allow,
                encoding: Encoding::Int(IntEncoding::Int),
            },
        });
    }

    TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields,
        }),
    }
}

fn create_time_series_schema(series_count: usize) -> TableSchema {
    let mut fields = Vec::new();

    // Timestamp field
    fields.push(FieldSchema {
        name: "timestamp".to_string(),
        schema: ValueSchema::Int {
            default: Default::Deny,
            encoding: Encoding::Int(IntEncoding::TimeMilliseconds),
        },
    });

    // Value fields
    for series in 0..series_count {
        fields.push(FieldSchema {
            name: format!("series_{}", series),
            schema: ValueSchema::Int {
                default: Default::Allow,
                encoding: Encoding::Int(IntEncoding::Int),
            },
        });
    }

    TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields,
        }),
    }
}

fn create_log_schema() -> TableSchema {
    TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![
                FieldSchema {
                    name: "timestamp".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::TimeMilliseconds),
                    },
                },
                FieldSchema {
                    name: "level".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
                FieldSchema {
                    name: "message".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Allow,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
            ],
        }),
    }
}

fn estimate_uncompressed_size(table: &Table) -> usize {
    // Rough estimate of uncompressed size
    match table {
        Table::Array { column, .. } => estimate_column_size(column),
        Table::Map {
            key_column,
            value_column,
            ..
        } => estimate_column_size(key_column) + estimate_column_size(value_column),
        Table::Binary { data, .. } => data.len(),
    }
}

fn estimate_column_size(column: &Column) -> usize {
    match column {
        Column::Unit { count } => *count * 0, // Unit takes no space
        Column::Int { values, .. } => values.len() * 8, // 8 bytes per i64
        Column::Double { values, .. } => values.len() * 8, // 8 bytes per f64
        Column::Binary { data, .. } => data.len(),
        Column::Array {
            lengths, element, ..
        } => {
            let total_elements: usize = lengths.iter().sum();
            total_elements * 8 + estimate_column_size(element) // rough estimate
        }
        Column::Struct { fields, .. } => {
            fields.iter().map(|f| estimate_column_size(&f.column)).sum()
        }
        Column::Enum { tags, variants, .. } => {
            tags.len() * 4
                + variants
                    .iter()
                    .map(|v| estimate_column_size(&v.column))
                    .sum::<usize>()
        }
        Column::Nested { lengths, table, .. } => {
            let total_elements: usize = lengths.iter().sum();
            total_elements * 8 + estimate_uncompressed_size(table)
        }
        Column::Reversed { inner } => estimate_column_size(inner),
    }
}

criterion_group!(
    benches,
    bench_streaming_write,
    bench_streaming_read,
    bench_time_series_streaming,
    bench_log_streaming
);

criterion_main!(benches);
