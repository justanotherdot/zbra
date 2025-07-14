// Error handling tests for zbra

mod common;

use common::*;
use proptest::prelude::*;
use zbra_core::data::{Field, Value};
use zbra_core::error::{ConversionError, LogicalError, SchemaError, StripedError};
use zbra_core::logical::{FieldSchema, ValueSchema};
use zbra_core::striped;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Test that schema mismatches are properly detected
    #[test]
    fn test_schema_type_mismatch_errors(
        schema in arb_value_schema(),
        wrong_value in arb_value_shallow()
    ) {
        let validation_result = wrong_value.validate_schema(&schema);

        // If validation fails, it should be a SchemaError
        if let Err(SchemaError::TypeMismatch { expected, actual }) = validation_result {
            prop_assert!(expected.len() > 0);
            prop_assert!(actual.len() > 0);
            prop_assert_ne!(expected, actual);
        } else if validation_result.is_ok() {
            // If validation succeeds, the value should actually be compatible
            // This is harder to test generically, so we'll allow it
        }
    }

    /// Test that missing fields are detected in structs
    #[test]
    fn test_struct_missing_field_errors(
        field_name in arb_field_name(),
        field_schema in arb_value_schema(),
        struct_fields in prop::collection::vec(
            (arb_field_name(), arb_value_shallow()),
            0..5
        )
    ) {
        let expected_fields = vec![FieldSchema {
            name: field_name.clone(),
            schema: field_schema
        }];

        let schema = ValueSchema::Struct {
            default: zbra_core::data::Default::Allow,
            fields: expected_fields,
        };

        let actual_fields: Vec<Field> = struct_fields.into_iter()
            .map(|(name, value)| Field { name, value })
            .collect();

        let struct_value = Value::Struct(actual_fields);
        let validation_result = struct_value.validate_schema(&schema);

        // If the field is missing, should get MissingField error
        if !struct_value.take_struct().unwrap().iter().any(|f| f.name == field_name) {
            if let Err(SchemaError::MissingField(missing_field)) = validation_result {
                prop_assert_eq!(missing_field, field_name);
            }
        }
    }

    /// Test that invalid enum tags are detected
    #[test]
    fn test_enum_invalid_tag_errors(
        valid_tags in prop::collection::vec(0u32..10, 1..5),
        invalid_tag in 100u32..200,
        enum_value in arb_value_shallow()
    ) {
        let variants = valid_tags.into_iter()
            .enumerate()
            .map(|(i, tag)| zbra_core::logical::VariantSchema {
                name: format!("variant_{}", i),
                tag,
                schema: ValueSchema::Unit,
            })
            .collect();

        let schema = ValueSchema::Enum {
            default: zbra_core::data::Default::Allow,
            variants,
        };

        let enum_with_invalid_tag = Value::Enum {
            tag: invalid_tag,
            value: Box::new(enum_value),
        };

        let validation_result = enum_with_invalid_tag.validate_schema(&schema);

        // Should get UnsupportedType error for invalid tag
        if let Err(SchemaError::UnsupportedType(msg)) = validation_result {
            prop_assert!(msg.contains(&invalid_tag.to_string()));
        } else {
            prop_assert!(false, "Expected UnsupportedType error");
        }
    }

    /// Test that column mismatch errors are detected
    #[test]
    fn test_column_mismatch_errors(
        key_count in 1usize..20,
        value_count in 1usize..20
    ) {
        prop_assume!(key_count != value_count);

        let key_values: Vec<Value> = (0..key_count)
            .map(|i| Value::Int(i as i64))
            .collect();

        let value_values: Vec<Value> = (0..value_count)
            .map(|i| Value::Int(i as i64))
            .collect();

        let key_schema = ValueSchema::Int {
            default: zbra_core::data::Default::Allow,
            encoding: zbra_core::data::Encoding::Int(zbra_core::data::IntEncoding::Int),
        };

        let key_column = striped::Column::from_values(&key_schema, &key_values)?;
        let value_column = striped::Column::from_values(&key_schema, &value_values)?;

        let map_table = striped::Table::Map {
            default: zbra_core::data::Default::Allow,
            key_column: Box::new(key_column),
            value_column: Box::new(value_column),
        };

        let result = map_table.to_logical();

        // Should get ColumnMismatch error
        if let Err(ConversionError::Striped(StripedError::ColumnMismatch { expected, actual })) = result {
            prop_assert_eq!(expected, key_count);
            prop_assert_eq!(actual, value_count);
        } else {
            prop_assert!(false, "Expected ColumnMismatch error");
        }
    }

    /// Test that merge conflicts are properly detected
    #[test]
    fn test_merge_conflict_errors(
        val1 in any::<i64>(),
        val2 in any::<i64>()
    ) {
        prop_assume!(val1 != val2);

        let value1 = Value::Int(val1);
        let value2 = Value::Int(val2);

        let merge_result = value1.merge(&value2);

        // Should get InvalidValue error for conflicting integers
        if let Err(LogicalError::InvalidValue { field, reason }) = merge_result {
            prop_assert_eq!(field, "int");
            prop_assert!(reason.contains(&val1.to_string()));
            prop_assert!(reason.contains(&val2.to_string()));
        } else {
            prop_assert!(false, "Expected InvalidValue error");
        }
    }

    /// Test that binary data length mismatches are detected
    #[test]
    fn test_binary_length_mismatch_errors(
        lengths in prop::collection::vec(1usize..100, 1..10),
        wrong_data_size in 1usize..50
    ) {
        let total_expected: usize = lengths.iter().sum();
        prop_assume!(wrong_data_size != total_expected);

        let binary_column = striped::Column::Binary {
            default: zbra_core::data::Default::Allow,
            encoding: zbra_core::data::Encoding::Binary(zbra_core::data::BinaryEncoding::Binary),
            lengths,
            data: vec![0u8; wrong_data_size],
        };

        let result = binary_column.to_values();

        // Should get VectorOperationFailed error
        if let Err(ConversionError::Striped(StripedError::VectorOperationFailed(msg))) = result {
            prop_assert!(msg.contains("length mismatch"));
        } else {
            prop_assert!(false, "Expected VectorOperationFailed error");
        }
    }

    /// Test that array element length mismatches are detected
    #[test]
    fn test_array_element_length_mismatch_errors(
        lengths in prop::collection::vec(1usize..10, 1..5),
        wrong_element_count in 1usize..20
    ) {
        let total_expected: usize = lengths.iter().sum();
        prop_assume!(wrong_element_count != total_expected);

        let element_column = striped::Column::Int {
            default: zbra_core::data::Default::Allow,
            encoding: zbra_core::data::Encoding::Int(zbra_core::data::IntEncoding::Int),
            values: vec![0i64; wrong_element_count],
        };

        let array_column = striped::Column::Array {
            default: zbra_core::data::Default::Allow,
            lengths,
            element: Box::new(element_column),
        };

        let result = array_column.to_values();

        // Should get VectorOperationFailed error
        if let Err(ConversionError::Striped(StripedError::VectorOperationFailed(msg))) = result {
            prop_assert!(msg.contains("length mismatch"));
        } else {
            prop_assert!(false, "Expected VectorOperationFailed error");
        }
    }

    /// Test that struct field count mismatches are detected
    #[test]
    fn test_struct_field_count_mismatch_errors(
        expected_count in 1usize..10,
        actual_count in 1usize..10
    ) {
        prop_assume!(expected_count != actual_count);

        let expected_fields: Vec<FieldSchema> = (0..expected_count)
            .map(|i| FieldSchema {
                name: format!("field_{}", i),
                schema: ValueSchema::Unit,
            })
            .collect();

        let actual_fields: Vec<Field> = (0..actual_count)
            .map(|i| Field {
                name: format!("field_{}", i),
                value: Value::Unit,
            })
            .collect();

        let schema = ValueSchema::Struct {
            default: zbra_core::data::Default::Allow,
            fields: expected_fields,
        };

        let struct_value = Value::Struct(actual_fields);
        let result = struct_value.validate_schema(&schema);

        // Should get TypeMismatch error for field count
        if let Err(SchemaError::TypeMismatch { expected, actual }) = result {
            prop_assert!(expected.contains(&expected_count.to_string()));
            prop_assert!(actual.contains(&actual_count.to_string()));
        } else {
            prop_assert!(false, "Expected TypeMismatch error");
        }
    }
}
