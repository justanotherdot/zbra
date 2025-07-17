// Schema validation tests for zbra
//
// Tests for schema validation, edge cases, and error handling

mod common;

use zbra_core::data::{BinaryEncoding, Default, Encoding, Field, IntEncoding, Value};
use zbra_core::error::SchemaError;
use zbra_core::logical::{FieldSchema, TableSchema, ValueSchema, VariantSchema};

/// Test empty enum schema validation
#[test]
fn test_empty_enum_schema_validation() {
    let empty_enum_schema = ValueSchema::Enum {
        default: Default::Allow,
        variants: vec![],
    };

    // Schema validation should fail for empty enums
    assert!(empty_enum_schema.validate().is_err());

    let result = empty_enum_schema.validate();
    match result {
        Err(SchemaError::UnsupportedType(msg)) => {
            assert!(msg.contains("Empty enum schemas are not supported"));
        }
        _ => panic!("Expected UnsupportedType error for empty enum"),
    }
}

/// Test empty struct schema validation
#[test]
fn test_empty_struct_schema_validation() {
    let empty_struct_schema = ValueSchema::Struct {
        default: Default::Allow,
        fields: vec![],
    };

    // Schema validation should fail for empty structs
    assert!(empty_struct_schema.validate().is_err());

    let result = empty_struct_schema.validate();
    match result {
        Err(SchemaError::UnsupportedType(msg)) => {
            assert!(msg.contains("Empty struct schemas are not supported"));
        }
        _ => panic!("Expected UnsupportedType error for empty struct"),
    }
}

/// Test duplicate field names in struct schema
#[test]
fn test_duplicate_field_names_schema_validation() {
    let duplicate_field_schema = ValueSchema::Struct {
        default: Default::Allow,
        fields: vec![
            FieldSchema {
                name: "field1".to_string(),
                schema: ValueSchema::Int {
                    default: Default::Allow,
                    encoding: Encoding::Int(IntEncoding::Int),
                },
            },
            FieldSchema {
                name: "field1".to_string(), // Duplicate name
                schema: ValueSchema::Int {
                    default: Default::Allow,
                    encoding: Encoding::Int(IntEncoding::Int),
                },
            },
        ],
    };

    // Schema validation should fail for duplicate field names
    assert!(duplicate_field_schema.validate().is_err());

    let result = duplicate_field_schema.validate();
    match result {
        Err(SchemaError::UnsupportedType(msg)) => {
            assert!(msg.contains("Duplicate field name: field1"));
        }
        _ => panic!("Expected UnsupportedType error for duplicate field names"),
    }
}

/// Test duplicate enum tags in schema
#[test]
fn test_duplicate_enum_tags_schema_validation() {
    let duplicate_tag_schema = ValueSchema::Enum {
        default: Default::Allow,
        variants: vec![
            VariantSchema {
                name: "variant1".to_string(),
                tag: 1,
                schema: ValueSchema::Unit,
            },
            VariantSchema {
                name: "variant2".to_string(),
                tag: 1, // Duplicate tag
                schema: ValueSchema::Unit,
            },
        ],
    };

    // Schema validation should fail for duplicate tags
    assert!(duplicate_tag_schema.validate().is_err());

    let result = duplicate_tag_schema.validate();
    match result {
        Err(SchemaError::UnsupportedType(msg)) => {
            assert!(msg.contains("Duplicate enum tag: 1"));
        }
        _ => panic!("Expected UnsupportedType error for duplicate enum tags"),
    }
}

/// Test UTF-8 encoding validation
#[test]
fn test_utf8_encoding_validation() {
    let utf8_schema = ValueSchema::Binary {
        default: Default::Allow,
        encoding: Encoding::Binary(BinaryEncoding::Utf8),
    };

    // Valid UTF-8 should pass
    let valid_utf8 = Value::Binary(b"Hello, World!".to_vec());
    assert!(valid_utf8.validate_schema(&utf8_schema).is_ok());

    // Invalid UTF-8 should fail
    let invalid_utf8 = Value::Binary(vec![0xFF, 0xFE, 0xFD]);
    assert!(invalid_utf8.validate_schema(&utf8_schema).is_err());

    let result = invalid_utf8.validate_schema(&utf8_schema);
    match result {
        Err(SchemaError::UnsupportedType(msg)) => {
            assert!(msg.contains("Invalid UTF-8 encoding"));
        }
        _ => panic!("Expected UnsupportedType error for invalid UTF-8"),
    }
}

/// Test date encoding validation
#[test]
fn test_date_encoding_validation() {
    let date_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::Date),
    };

    // Valid date (Unix timestamp in milliseconds for 2022-01-01)
    let valid_date = Value::Int(1640995200000);
    assert!(valid_date.validate_schema(&date_schema).is_ok());

    // Invalid date (negative)
    let invalid_date_negative = Value::Int(-1);
    assert!(invalid_date_negative.validate_schema(&date_schema).is_err());

    // Invalid date (too far in future)
    let invalid_date_future = Value::Int(5000000000000); // Year 2128
    assert!(invalid_date_future.validate_schema(&date_schema).is_err());

    let result = invalid_date_negative.validate_schema(&date_schema);
    match result {
        Err(SchemaError::UnsupportedType(msg)) => {
            assert!(msg.contains("Date value") && msg.contains("outside valid range"));
        }
        _ => panic!("Expected UnsupportedType error for invalid date"),
    }
}

/// Test valid enum with proper variants
#[test]
fn test_valid_enum_schema() {
    let valid_enum_schema = ValueSchema::Enum {
        default: Default::Allow,
        variants: vec![
            VariantSchema {
                name: "success".to_string(),
                tag: 0,
                schema: ValueSchema::Binary {
                    default: Default::Allow,
                    encoding: Encoding::Binary(BinaryEncoding::Utf8),
                },
            },
            VariantSchema {
                name: "error".to_string(),
                tag: 1,
                schema: ValueSchema::Int {
                    default: Default::Allow,
                    encoding: Encoding::Int(IntEncoding::Int),
                },
            },
        ],
    };

    // Schema validation should pass for valid enum
    assert!(valid_enum_schema.validate().is_ok());

    // Value validation should also work
    let success_value = Value::Enum {
        tag: 0,
        value: Box::new(Value::Binary(b"OK".to_vec())),
    };
    assert!(success_value.validate_schema(&valid_enum_schema).is_ok());

    let error_value = Value::Enum {
        tag: 1,
        value: Box::new(Value::Int(404)),
    };
    assert!(error_value.validate_schema(&valid_enum_schema).is_ok());
}

/// Test valid struct with proper fields
#[test]
fn test_valid_struct_schema() {
    let valid_struct_schema = ValueSchema::Struct {
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
                name: "name".to_string(),
                schema: ValueSchema::Binary {
                    default: Default::Allow,
                    encoding: Encoding::Binary(BinaryEncoding::Utf8),
                },
            },
        ],
    };

    // Schema validation should pass for valid struct
    assert!(valid_struct_schema.validate().is_ok());

    // Value validation should also work
    let valid_struct_value = Value::Struct(vec![
        Field {
            name: "id".to_string(),
            value: Value::Int(123),
        },
        Field {
            name: "name".to_string(),
            value: Value::Binary(b"Alice".to_vec()),
        },
    ]);
    assert!(valid_struct_value
        .validate_schema(&valid_struct_schema)
        .is_ok());
}

/// Test table schema validation
#[test]
fn test_table_schema_validation() {
    // Valid table schema
    let valid_table_schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![FieldSchema {
                name: "field1".to_string(),
                schema: ValueSchema::Int {
                    default: Default::Allow,
                    encoding: Encoding::Int(IntEncoding::Int),
                },
            }],
        }),
    };

    assert!(valid_table_schema.validate().is_ok());

    // Table schema with invalid nested schema
    let invalid_table_schema = TableSchema::Array {
        default: Default::Allow,
        element: Box::new(ValueSchema::Struct {
            default: Default::Allow,
            fields: vec![], // Empty struct - should fail
        }),
    };

    assert!(invalid_table_schema.validate().is_err());
}
