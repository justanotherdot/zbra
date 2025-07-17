// Boundary validation tests for zbra date limits

use zbra_core::data::{Default, Encoding, IntEncoding, Value};
use zbra_core::logical::ValueSchema;

/// Test values exactly at the date validation boundaries
#[test]
fn test_date_boundary_conditions() {
    let date_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::Date),
    };

    // Test lower boundary
    let min_valid = Value::Int(0); // Unix epoch start
    assert!(min_valid.validate_schema(&date_schema).is_ok());

    let below_min = Value::Int(-1);
    assert!(below_min.validate_schema(&date_schema).is_err());

    // Test upper boundary
    let max_valid = Value::Int(4102444800000); // Exactly Jan 1, 2100
    assert!(max_valid.validate_schema(&date_schema).is_ok());

    let above_max = Value::Int(4102444800001); // 1ms past limit
    assert!(above_max.validate_schema(&date_schema).is_err());

    // Test edge cases around the limit
    let near_max_valid = Value::Int(4102444799999); // 1ms before limit
    assert!(near_max_valid.validate_schema(&date_schema).is_ok());

    let way_above_max = Value::Int(5000000000000); // Year 2128
    assert!(way_above_max.validate_schema(&date_schema).is_err());
}

/// Test that validation error messages are helpful
#[test]
fn test_date_validation_error_messages() {
    let date_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::Date),
    };

    let invalid_negative = Value::Int(-1000);
    let result = invalid_negative.validate_schema(&date_schema);
    assert!(result.is_err());
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(error_msg.contains("outside valid range"));
    assert!(error_msg.contains("4102444800000"));

    let invalid_future = Value::Int(5000000000000);
    let result = invalid_future.validate_schema(&date_schema);
    assert!(result.is_err());
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(error_msg.contains("outside valid range"));
}

/// Test other time encodings have different or no limits
#[test]
fn test_other_time_encoding_limits() {
    // Time in seconds should allow larger values (different scale)
    let time_seconds_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::TimeSeconds),
    };

    // Should allow large timestamp values
    let large_seconds = Value::Int(4102444800); // Jan 1, 2100 in seconds
    assert!(large_seconds.validate_schema(&time_seconds_schema).is_ok());

    // Time in microseconds
    let time_microseconds_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::TimeMicroseconds),
    };

    let large_microseconds = Value::Int(4102444800000000); // Jan 1, 2100 in microseconds
    assert!(large_microseconds
        .validate_schema(&time_microseconds_schema)
        .is_ok());

    // Regular integers should have no date-specific limits
    let int_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::Int),
    };

    let very_large_int = Value::Int(9223372036854775807); // Max i64
    assert!(very_large_int.validate_schema(&int_schema).is_ok());

    let negative_int = Value::Int(-9223372036854775808); // Min i64
    assert!(negative_int.validate_schema(&int_schema).is_ok());
}

/// Test that the exact limit value works in practice
#[test]
fn test_limit_value_usability() {
    use zbra_core::data::{Field, Table};
    use zbra_core::logical::{FieldSchema, TableSchema};
    use zbra_core::striped;

    // Create a realistic table with timestamps at the limit
    let limit_timestamp = 4102444800000;
    let records = vec![
        Value::Struct(vec![
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(limit_timestamp),
            },
            Field {
                name: "value".to_string(),
                value: Value::Double(42.0),
            },
        ]),
        Value::Struct(vec![
            Field {
                name: "timestamp".to_string(),
                value: Value::Int(limit_timestamp - 1000), // 1 second earlier
            },
            Field {
                name: "value".to_string(),
                value: Value::Double(43.0),
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
                    name: "value".to_string(),
                    schema: ValueSchema::Double {
                        default: Default::Allow,
                    },
                },
            ],
        }),
    };

    // Should validate successfully
    assert!(table.validate_schema(&schema).is_ok());

    // Should convert to striped format successfully
    let striped_table = striped::Table::from_logical(&schema, &table);
    assert!(striped_table.is_ok());

    // Should round-trip successfully
    let recovered_table = striped_table.unwrap().to_logical();
    assert!(recovered_table.is_ok());
    assert_eq!(table, recovered_table.unwrap());
}
