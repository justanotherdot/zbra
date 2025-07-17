// End-to-end integration tests with validation

use zbra_core::binary::BinaryFile;
use zbra_core::compression::CompressionConfig;
use zbra_core::data::{BinaryEncoding, Default, Encoding, Field, IntEncoding, Table, Value};
use zbra_core::logical::{FieldSchema, TableSchema, ValueSchema};
use zbra_core::striped;

/// Test complete pipeline with date validation at the limit
#[test]
fn test_end_to_end_with_limit_dates() {
    // Create dataset with timestamps right at the validation limit
    let limit_timestamp = 4102444800000; // Exactly Jan 1, 2100

    let records = vec![
        Value::Struct(vec![
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(limit_timestamp), // Exactly at limit
            },
            Field {
                name: "sensor_id".to_string(),
                value: Value::Binary(b"sensor_001".to_vec()),
            },
            Field {
                name: "temperature".to_string(),
                value: Value::Double(22.5),
            },
            Field {
                name: "humidity".to_string(),
                value: Value::Double(45.2),
            },
        ]),
        Value::Struct(vec![
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(limit_timestamp - 60000), // 1 minute before
            },
            Field {
                name: "sensor_id".to_string(),
                value: Value::Binary(b"sensor_002".to_vec()),
            },
            Field {
                name: "temperature".to_string(),
                value: Value::Double(23.1),
            },
            Field {
                name: "humidity".to_string(),
                value: Value::Double(44.8),
            },
        ]),
        Value::Struct(vec![
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(limit_timestamp - 3600000), // 1 hour before
            },
            Field {
                name: "sensor_id".to_string(),
                value: Value::Binary(b"sensor_003".to_vec()),
            },
            Field {
                name: "temperature".to_string(),
                value: Value::Double(21.8),
            },
            Field {
                name: "humidity".to_string(),
                value: Value::Double(46.1),
            },
        ]),
    ];

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
                        encoding: Encoding::Int(IntEncoding::Date),
                    },
                },
                FieldSchema {
                    name: "sensor_id".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
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
            ],
        }),
    };

    // Step 1: Validate schema and data
    assert!(schema.validate().is_ok(), "Schema should be valid");
    assert!(
        table.validate_schema(&schema).is_ok(),
        "Data should validate against schema"
    );

    // Step 2: Convert to striped format
    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();

    // Step 3: Create binary file with compression
    let compression_config = CompressionConfig {
        binary_data: zbra_core::compression::CompressionAlgorithm::Zstd { level: 3 },
        strings: zbra_core::compression::CompressionAlgorithm::Zstd { level: 3 },
    };

    let binary_file =
        BinaryFile::new_with_compression(schema.clone(), striped_table, compression_config);

    // Step 4: Serialize to bytes
    let serialized = binary_file.to_bytes().unwrap();
    println!("Serialized size: {} bytes", serialized.len());

    // Step 5: Deserialize back
    let deserialized_file = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_schema = &deserialized_file.header.schema;
    let recovered_striped = deserialized_file.table().unwrap();

    // Step 6: Convert back to logical format
    let recovered_table = recovered_striped.to_logical().unwrap();

    // Verify everything matches
    assert_eq!(
        *recovered_schema, schema,
        "Schema should round-trip exactly"
    );
    assert_eq!(recovered_table, table, "Data should round-trip exactly");

    // Verify validation still works on recovered data
    assert!(
        recovered_table.validate_schema(recovered_schema).is_ok(),
        "Recovered data should still validate"
    );
}

/// Test that validation prevents invalid data from getting into the pipeline
#[test]
fn test_validation_prevents_invalid_pipeline_entry() {
    // Try to create data with timestamps beyond the limit
    let invalid_timestamp = 4102444800001; // 1ms past limit

    let invalid_record = Value::Struct(vec![
        Field {
            name: "timestamp".to_string(),
            value: Value::Int(invalid_timestamp),
        },
        Field {
            name: "value".to_string(),
            value: Value::Double(42.0),
        },
    ]);

    let invalid_table = Table::Array(vec![invalid_record]);

    let schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![
                FieldSchema {
                    name: "timestamp".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::Date),
                    },
                },
                FieldSchema {
                    name: "value".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
            ],
        }),
    };

    // Validation should catch the invalid timestamp
    let validation_result = invalid_table.validate_schema(&schema);
    assert!(
        validation_result.is_err(),
        "Validation should reject timestamps beyond limit"
    );

    // The striped conversion doesn't validate - it just converts
    // But attempting to create a binary file should work since validation is separate
    let striped_result = striped::Table::from_logical(&schema, &invalid_table);

    // NOTE: The striped layer doesn't validate - validation happens at the logical layer
    // This is by design to keep concerns separated
    if striped_result.is_ok() {
        println!("Striped conversion succeeded (validation is separate from conversion)");
    } else {
        println!("Striped conversion failed: {:?}", striped_result.err());
    }
}

/// Test compression efficiency with validated date data
#[test]
fn test_compression_efficiency_with_validated_dates() {
    // Create a larger dataset with valid timestamps for compression testing
    let base_time = 4102444800000 - (24 * 3600000); // Start 24 hours before limit
    let records: Vec<Value> = (0..1000)
        .map(|i| {
            Value::Struct(vec![
                Field {
                    name: "timestamp".to_string(),
                    value: Value::Int(base_time + (i * 60000)), // Every minute
                },
                Field {
                    name: "metric_name".to_string(),
                    value: Value::Binary(format!("metric_{:03}", i % 10).into_bytes()),
                },
                Field {
                    name: "value".to_string(),
                    value: Value::Double((i as f64 * 0.1) % 100.0),
                },
            ])
        })
        .collect();

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
                        encoding: Encoding::Int(IntEncoding::Date),
                    },
                },
                FieldSchema {
                    name: "metric_name".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Allow,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "value".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
            ],
        }),
    };

    // Validate the large dataset
    assert!(
        table.validate_schema(&schema).is_ok(),
        "Large dataset should validate"
    );

    // Convert and compress
    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
    let binary_file = BinaryFile::new(schema.clone(), striped_table);
    let serialized = binary_file.to_bytes().unwrap();

    // Estimate uncompressed size (rough)
    let estimated_uncompressed = 1000 * (8 + 10 + 8); // timestamp + string + double
    let compression_ratio = estimated_uncompressed as f64 / serialized.len() as f64;

    println!("Compression efficiency test:");
    println!("  Records: 1000");
    println!("  Estimated uncompressed: {} bytes", estimated_uncompressed);
    println!("  Actual compressed: {} bytes", serialized.len());
    println!("  Compression ratio: {:.2}x", compression_ratio);

    // Should achieve reasonable compression
    assert!(
        compression_ratio > 1.5,
        "Should achieve >1.5x compression on time-series data"
    );

    // Verify round-trip still works
    let recovered = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_table = recovered.table().unwrap().to_logical().unwrap();
    assert_eq!(
        recovered_table, table,
        "Large dataset should round-trip correctly"
    );
}

/// Test mixed encoding types work together
#[test]
fn test_mixed_encoding_types_integration() {
    let records = vec![Value::Struct(vec![
        Field {
            name: "date_timestamp".to_string(),
            value: Value::Int(4102444800000), // Date encoding
        },
        Field {
            name: "unix_seconds".to_string(),
            value: Value::Int(4102444800), // Time seconds encoding
        },
        Field {
            name: "counter".to_string(),
            value: Value::Int(12345), // Regular int encoding
        },
        Field {
            name: "utf8_text".to_string(),
            value: Value::Binary(b"Hello, world!".to_vec()), // UTF-8 encoding
        },
        Field {
            name: "binary_data".to_string(),
            value: Value::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]), // Binary encoding
        },
    ])];

    let table = Table::Array(records);

    let schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![
                FieldSchema {
                    name: "date_timestamp".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Deny,
                        encoding: Encoding::Int(IntEncoding::Date),
                    },
                },
                FieldSchema {
                    name: "unix_seconds".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Allow,
                        encoding: Encoding::Int(IntEncoding::TimeSeconds),
                    },
                },
                FieldSchema {
                    name: "counter".to_string(),
                    schema: ValueSchema::Int {
                        default: Default::Allow,
                        encoding: Encoding::Int(IntEncoding::Int),
                    },
                },
                FieldSchema {
                    name: "utf8_text".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Allow,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    },
                },
                FieldSchema {
                    name: "binary_data".to_string(),
                    schema: ValueSchema::Binary {
                        default: Default::Allow,
                        encoding: Encoding::Binary(BinaryEncoding::Binary),
                    },
                },
            ],
        }),
    };

    // Test full pipeline with mixed encodings
    assert!(
        table.validate_schema(&schema).is_ok(),
        "Mixed encodings should validate"
    );

    let striped_table = striped::Table::from_logical(&schema, &table).unwrap();
    let binary_file = BinaryFile::new(schema.clone(), striped_table);
    let serialized = binary_file.to_bytes().unwrap();

    let recovered = BinaryFile::from_bytes(&serialized).unwrap();
    let recovered_table = recovered.table().unwrap().to_logical().unwrap();

    assert_eq!(
        recovered_table, table,
        "Mixed encoding types should round-trip correctly"
    );
}
