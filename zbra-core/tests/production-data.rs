// Production data pattern tests for zbra
//
// These tests use realistic data patterns commonly found in production systems:
// - Time series data (metrics, logs)
// - Configuration data (JSON-like structures)
// - User records (mixed data types)
// - Financial transactions
// - IoT sensor data

mod common;
use zbra_core::binary::BinaryFile;
use zbra_core::compression::CompressionConfig;
use zbra_core::data::{BinaryEncoding, Default, Encoding, Field, IntEncoding, Table, Value};
use zbra_core::logical::{FieldSchema, TableSchema, ValueSchema};
use zbra_core::striped;

/// Time series data: timestamps with numeric values
#[test]
fn test_time_series_data() {
    let start_time = 1640995200000i64; // 2022-01-01 00:00:00 UTC
    let interval = 60000; // 1 minute
    let count = 1000;

    // Generate realistic time series data
    let mut records = Vec::new();
    for i in 0..count {
        let timestamp = start_time + (i as i64 * interval);
        let cpu_usage = 20.0 + (i as f64 * 0.1) % 80.0; // Trending CPU usage
        let memory_usage = 512.0 + ((i as f64 * 0.3).sin() * 200.0); // Oscillating memory
        let disk_io = if i % 100 == 0 { 1000.0 } else { 10.0 }; // Periodic spikes

        records.push(Value::Struct(vec![
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(timestamp),
            },
            Field {
                name: "cpu_usage".to_string(),
                value: Value::Double(cpu_usage),
            },
            Field {
                name: "memory_usage".to_string(),
                value: Value::Double(memory_usage),
            },
            Field {
                name: "disk_io".to_string(),
                value: Value::Double(disk_io),
            },
        ]));
    }

    let table = Table::Array(records);

    let schema = TableSchema::Array {
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
                    name: "cpu_usage".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
                FieldSchema {
                    name: "memory_usage".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
                FieldSchema {
                    name: "disk_io".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
            ],
        }),
    };

    // Test full pipeline: logical → striped → binary → compressed → decompressed
    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
    let binary_file = BinaryFile::new(schema.clone(), striped_table);
    let serialized = binary_file.to_bytes().unwrap();
    let deserialized = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_striped = deserialized.table().unwrap();
    let recovered_table = recovered_striped.to_logical().unwrap();

    assert_eq!(table, recovered_table);

    // Verify compression effectiveness on time series data
    let original_size = count * (8 + 8 + 8 + 8); // rough estimate
    let compressed_size = serialized.len();
    println!(
        "Time series compression ratio: {:.2}x ({} → {} bytes)",
        original_size as f64 / compressed_size as f64,
        original_size,
        compressed_size
    );
}

/// Configuration data: nested structures with mixed types
#[test]
fn test_configuration_data() {
    let config = Value::Struct(vec![
        Field {
            name: "database".to_string(),
            value: Value::Struct(vec![
                Field {
                    name: "host".to_string(),
                    value: Value::Binary(b"localhost".to_vec()),
                },
                Field {
                    name: "port".to_string(),
                    value: Value::Int(5432),
                },
                Field {
                    name: "enabled".to_string(),
                    value: Value::Int(1), // Boolean as int
                },
                Field {
                    name: "connection_pool_size".to_string(),
                    value: Value::Int(20),
                },
            ]),
        },
        Field {
            name: "cache".to_string(),
            value: Value::Struct(vec![
                Field {
                    name: "redis_url".to_string(),
                    value: Value::Binary(b"redis://localhost:6379".to_vec()),
                },
                Field {
                    name: "ttl_seconds".to_string(),
                    value: Value::Int(3600),
                },
                Field {
                    name: "max_memory_mb".to_string(),
                    value: Value::Int(512),
                },
            ]),
        },
        Field {
            name: "features".to_string(),
            value: Value::Array(vec![
                Value::Binary(b"authentication".to_vec()),
                Value::Binary(b"rate_limiting".to_vec()),
                Value::Binary(b"analytics".to_vec()),
            ]),
        },
    ]);

    let table = Table::Array(vec![config]);

    let schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![
                FieldSchema {
                    name: "database".to_string(),
                    schema: ValueSchema::Struct {
                        default: Default::Allow,
                        fields: vec![
                            FieldSchema {
                                name: "host".to_string(),
                                schema: ValueSchema::Binary {
                                    default: Default::Allow,
                                    encoding: Encoding::Binary(BinaryEncoding::Utf8),
                                },
                            },
                            FieldSchema {
                                name: "port".to_string(),
                                schema: ValueSchema::Int {
                                    default: Default::Allow,
                                    encoding: Encoding::Int(IntEncoding::Int),
                                },
                            },
                            FieldSchema {
                                name: "enabled".to_string(),
                                schema: ValueSchema::Int {
                                    default: Default::Allow,
                                    encoding: Encoding::Int(IntEncoding::Int),
                                },
                            },
                            FieldSchema {
                                name: "connection_pool_size".to_string(),
                                schema: ValueSchema::Int {
                                    default: Default::Allow,
                                    encoding: Encoding::Int(IntEncoding::Int),
                                },
                            },
                        ],
                    },
                },
                FieldSchema {
                    name: "cache".to_string(),
                    schema: ValueSchema::Struct {
                        default: Default::Allow,
                        fields: vec![
                            FieldSchema {
                                name: "redis_url".to_string(),
                                schema: ValueSchema::Binary {
                                    default: Default::Allow,
                                    encoding: Encoding::Binary(BinaryEncoding::Utf8),
                                },
                            },
                            FieldSchema {
                                name: "ttl_seconds".to_string(),
                                schema: ValueSchema::Int {
                                    default: Default::Allow,
                                    encoding: Encoding::Int(IntEncoding::Int),
                                },
                            },
                            FieldSchema {
                                name: "max_memory_mb".to_string(),
                                schema: ValueSchema::Int {
                                    default: Default::Allow,
                                    encoding: Encoding::Int(IntEncoding::Int),
                                },
                            },
                        ],
                    },
                },
                FieldSchema {
                    name: "features".to_string(),
                    schema: ValueSchema::Array {
                        default: Default::Allow,
                        element: Box::new(ValueSchema::Binary {
                            default: Default::Allow,
                            encoding: Encoding::Binary(BinaryEncoding::Utf8),
                        }),
                    },
                },
            ],
        }),
    };

    // Test roundtrip with nested structures
    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
    let binary_file = BinaryFile::new(schema.clone(), striped_table);
    let serialized = binary_file.to_bytes().unwrap();
    let deserialized = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_striped = deserialized.table().unwrap();
    let recovered_table = recovered_striped.to_logical().unwrap();

    assert_eq!(table, recovered_table);
}

/// User records: typical database-like records with mixed types
#[test]
fn test_user_records() {
    let users = vec![
        Value::Struct(vec![
            Field {
                name: "id".to_string(),
                value: Value::Int(1001),
            },
            Field {
                name: "username".to_string(),
                value: Value::Binary(b"alice_smith".to_vec()),
            },
            Field {
                name: "email".to_string(),
                value: Value::Binary(b"alice@example.com".to_vec()),
            },
            Field {
                name: "age".to_string(),
                value: Value::Int(28),
            },
            Field {
                name: "balance".to_string(),
                value: Value::Double(1250.75),
            },
            Field {
                name: "is_active".to_string(),
                value: Value::Int(1),
            },
            Field {
                name: "created_at".to_string(),
                value: Value::Int(1640995200000),
            },
        ]),
        Value::Struct(vec![
            Field {
                name: "id".to_string(),
                value: Value::Int(1002),
            },
            Field {
                name: "username".to_string(),
                value: Value::Binary(b"bob_jones".to_vec()),
            },
            Field {
                name: "email".to_string(),
                value: Value::Binary(b"bob@example.com".to_vec()),
            },
            Field {
                name: "age".to_string(),
                value: Value::Int(35),
            },
            Field {
                name: "balance".to_string(),
                value: Value::Double(0.00),
            },
            Field {
                name: "is_active".to_string(),
                value: Value::Int(0),
            },
            Field {
                name: "created_at".to_string(),
                value: Value::Int(1640995260000),
            },
        ]),
        Value::Struct(vec![
            Field {
                name: "id".to_string(),
                value: Value::Int(1003),
            },
            Field {
                name: "username".to_string(),
                value: Value::Binary(b"charlie_brown".to_vec()),
            },
            Field {
                name: "email".to_string(),
                value: Value::Binary(b"charlie@example.com".to_vec()),
            },
            Field {
                name: "age".to_string(),
                value: Value::Int(42),
            },
            Field {
                name: "balance".to_string(),
                value: Value::Double(5000.25),
            },
            Field {
                name: "is_active".to_string(),
                value: Value::Int(1),
            },
            Field {
                name: "created_at".to_string(),
                value: Value::Int(1640995320000),
            },
        ]),
    ];

    let table = Table::Array(users);

    let schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![
                FieldSchema {
                    name: "id".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
                FieldSchema {
                    name: "username".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "email".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Allow,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "age".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Allow,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
                FieldSchema {
                    name: "balance".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
                FieldSchema {
                    name: "is_active".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Allow,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
                FieldSchema {
                    name: "created_at".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::TimeMilliseconds),
                    },
                },
            ],
        }),
    };

    // Test with both no compression and compression
    let no_compression = CompressionConfig {
        binary_data: zbra_core::compression::CompressionAlgorithm::None,
        strings: zbra_core::compression::CompressionAlgorithm::None,
    };

    let with_compression = CompressionConfig {
        binary_data: zbra_core::compression::CompressionAlgorithm::Zstd { level: 3 },
        strings: zbra_core::compression::CompressionAlgorithm::Zstd { level: 3 },
    };

    for (name, config) in [
        ("no_compression", no_compression),
        ("with_compression", with_compression),
    ] {
        let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
        let binary_file = BinaryFile::new_with_compression(schema.clone(), striped_table, config);
        let serialized = binary_file.to_bytes().unwrap();
        let deserialized = BinaryFile::from_bytes(&serialized).unwrap();
        let recovered_striped = deserialized.table().unwrap();
        let recovered_table = recovered_striped.to_logical().unwrap();

        assert_eq!(table, recovered_table);
        println!("User records {} size: {} bytes", name, serialized.len());
    }
}

/// Financial transactions: realistic financial data with precision requirements
#[test]
fn test_financial_transactions() {
    let transactions = vec![
        Value::Struct(vec![
            Field {
                name: "transaction_id".to_string(),
                value: Value::Binary(b"tx_001".to_vec()),
            },
            Field {
                name: "from_account".to_string(),
                value: Value::Binary(b"acc_1001".to_vec()),
            },
            Field {
                name: "to_account".to_string(),
                value: Value::Binary(b"acc_1002".to_vec()),
            },
            Field {
                name: "amount_cents".to_string(),
                value: Value::Int(125075), // $1,250.75 in cents
            },
            Field {
                name: "currency".to_string(),
                value: Value::Binary(b"USD".to_vec()),
            },
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(1640995200000),
            },
            Field {
                name: "status".to_string(),
                value: Value::Int(1), // 0=pending, 1=completed, 2=failed
            },
        ]),
        Value::Struct(vec![
            Field {
                name: "transaction_id".to_string(),
                value: Value::Binary(b"tx_002".to_vec()),
            },
            Field {
                name: "from_account".to_string(),
                value: Value::Binary(b"acc_1002".to_vec()),
            },
            Field {
                name: "to_account".to_string(),
                value: Value::Binary(b"acc_1003".to_vec()),
            },
            Field {
                name: "amount_cents".to_string(),
                value: Value::Int(50000), // $500.00 in cents
            },
            Field {
                name: "currency".to_string(),
                value: Value::Binary(b"USD".to_vec()),
            },
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(1640995260000),
            },
            Field {
                name: "status".to_string(),
                value: Value::Int(2), // failed
            },
        ]),
    ];

    let table = Table::Array(transactions);

    let schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![
                FieldSchema {
                    name: "transaction_id".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "from_account".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "to_account".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "amount_cents".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
                FieldSchema {
                    name: "currency".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "timestamp".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::TimeMilliseconds),
                    },
                },
                FieldSchema {
                    name: "status".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
            ],
        }),
    };

    // Test strict schema validation (all fields required)
    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
    let binary_file = BinaryFile::new(schema.clone(), striped_table);
    let serialized = binary_file.to_bytes().unwrap();
    let deserialized = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_striped = deserialized.table().unwrap();
    let recovered_table = recovered_striped.to_logical().unwrap();

    assert_eq!(table, recovered_table);
}

/// IoT sensor data: high-volume, repetitive data with clustering
#[test]
fn test_iot_sensor_data() {
    let mut sensor_readings = Vec::new();
    let base_time = 1640995200000i64;

    // Generate 1000 sensor readings across 10 sensors
    for i in 0..1000 {
        let sensor_id = i % 10; // 10 sensors
        let timestamp = base_time + (i as i64 * 1000); // 1 second intervals

        // Simulate realistic sensor patterns
        let temperature = match sensor_id {
            0..=2 => 20.0 + (i as f64 * 0.01),              // Slowly rising
            3..=5 => 25.0 + ((i as f64 * 0.1).sin() * 5.0), // Oscillating
            6..=8 => 30.0 - (i as f64 * 0.005),             // Slowly falling
            _ => 22.0,                                      // Constant
        };

        let humidity = 50.0 + ((i as f64 * 0.05).cos() * 20.0);

        sensor_readings.push(Value::Struct(vec![
            Field {
                name: "sensor_id".to_string(),
                value: Value::Int(sensor_id),
            },
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(timestamp),
            },
            Field {
                name: "temperature".to_string(),
                value: Value::Double(temperature),
            },
            Field {
                name: "humidity".to_string(),
                value: Value::Double(humidity),
            },
            Field {
                name: "battery_level".to_string(),
                value: Value::Int(100 - (i / 100)), // Slowly draining
            },
        ]));
    }

    let table = Table::Array(sensor_readings);

    let schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![
                FieldSchema {
                    name: "sensor_id".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
                FieldSchema {
                    name: "timestamp".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::TimeMilliseconds),
                    },
                },
                FieldSchema {
                    name: "temperature".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
                FieldSchema {
                    name: "humidity".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
                FieldSchema {
                    name: "battery_level".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Allow,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
            ],
        }),
    };

    // Test compression effectiveness on repetitive IoT data
    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
    let binary_file = BinaryFile::new(schema.clone(), striped_table);
    let serialized = binary_file.to_bytes().unwrap();
    let deserialized = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_striped = deserialized.table().unwrap();
    let recovered_table = recovered_striped.to_logical().unwrap();

    assert_eq!(table, recovered_table);

    // IoT data should compress very well due to clustering
    let estimated_uncompressed = 1000 * (8 + 8 + 8 + 8 + 8); // rough estimate
    let compressed_size = serialized.len();
    println!(
        "IoT sensor data compression ratio: {:.2}x ({} → {} bytes)",
        estimated_uncompressed as f64 / compressed_size as f64,
        estimated_uncompressed,
        compressed_size
    );
}

/// Log data: mixed string and numeric data with enum-like patterns
#[test]
fn test_log_data() {
    let log_levels = ["DEBUG", "INFO", "WARN", "ERROR"];
    let log_messages = [
        "User login successful",
        "Database connection established",
        "Processing request",
        "Cache miss",
        "Request completed",
        "Database query timeout",
        "Invalid authentication token",
        "Internal server error",
    ];

    let mut log_entries = Vec::new();
    let base_time = 1640995200000i64;

    for i in 0..100 {
        let timestamp = base_time + (i as i64 * 1000);
        let level = log_levels[i % log_levels.len()];
        let message = log_messages[i % log_messages.len()];
        let response_time = match level {
            "ERROR" => 5000 + (i % 100), // Slow errors
            "WARN" => 1000 + (i % 500),  // Medium warnings
            _ => 100 + (i % 200),        // Fast normal requests
        };

        log_entries.push(Value::Struct(vec![
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(timestamp),
            },
            Field {
                name: "level".to_string(),
                value: Value::Binary(level.as_bytes().to_vec()),
            },
            Field {
                name: "message".to_string(),
                value: Value::Binary(message.as_bytes().to_vec()),
            },
            Field {
                name: "response_time_ms".to_string(),
                value: Value::Int(response_time as i64),
            },
        ]));
    }

    let table = Table::Array(log_entries);

    let schema = TableSchema::Array {
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
                    schema: ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "message".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Allow,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "response_time_ms".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Allow,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
            ],
        }),
    };

    // Test string compression on repetitive log data
    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
    let binary_file = BinaryFile::new(schema.clone(), striped_table);
    let serialized = binary_file.to_bytes().unwrap();
    let deserialized = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_striped = deserialized.table().unwrap();
    let recovered_table = recovered_striped.to_logical().unwrap();

    assert_eq!(table, recovered_table);

    // Log data should compress well due to repetitive strings
    println!("Log data compressed size: {} bytes", serialized.len());
}
