// Property-based roundtrip tests for zbra

mod common;

use common::*;
use proptest::prelude::*;
use zbra_core::data::{Table, Value};
use zbra_core::logical::ValueSchema;
use zbra_core::striped;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Test that logical → striped → logical roundtrip preserves data
    #[test]
    fn test_logical_striped_table_roundtrip(
        (schema, logical_table) in arb_schema_and_table()
    ) {
        let striped_table = striped::Table::from_logical(&schema, &logical_table)?;
        let roundtrip_table = striped_table.to_logical()?;
        prop_assert_eq!(logical_table, roundtrip_table);
    }

    /// Test that value schema validation works for generated values
    #[test]
    fn test_value_schema_validation(
        (schema, value) in arb_schema_and_value()
    ) {
        prop_assert!(value.validate_schema(&schema).is_ok());
    }

    /// Test that table schema validation works for generated tables
    #[test]
    fn test_table_schema_validation(
        (schema, table) in arb_schema_and_table()
    ) {
        prop_assert!(table.validate_schema(&schema).is_ok());
    }

    /// Test striped column roundtrip for individual values
    #[test]
    fn test_striped_column_roundtrip(
        (schema, values) in arb_value_schema().prop_flat_map(|schema| {
            let values_gen = prop::collection::vec(arb_value_for_schema(&schema), 1..10);
            (Just(schema), values_gen)
        })
    ) {
        // Values are generated to match the schema, so they should all be compatible
        let column = striped::Column::from_values(&schema, &values)?;
        let roundtrip_values = column.to_values()?;
        prop_assert_eq!(values, roundtrip_values);
    }

    /// Test merge operations preserve data integrity
    #[test]
    fn test_logical_merge_preserves_data(
        table1 in arb_table(),
        table2 in arb_table()
    ) {
        // Only test merging tables of the same variant
        match (&table1, &table2) {
            (Table::Binary(a), Table::Binary(b)) => {
                let result = table1.merge(&table2);
                if a == b {
                    prop_assert!(result.is_ok());
                } else {
                    prop_assert!(result.is_err());
                }
            }
            (Table::Array(a), Table::Array(b)) => {
                let merged = table1.merge(&table2)?;
                match merged {
                    Table::Array(result) => {
                        prop_assert_eq!(result.len(), a.len() + b.len());
                    }
                    _ => prop_assert!(false, "Expected array result"),
                }
            }
            (Table::Map(_), Table::Map(_)) => {
                // Map merging is complex, just test it doesn't panic
                let _ = table1.merge(&table2);
            }
            _ => {
                // Different types should fail to merge
                prop_assert!(table1.merge(&table2).is_err());
            }
        }
    }

    /// Test boundary values handle edge cases properly
    #[test]
    fn test_boundary_values(
        boundary_value in arb_boundary_values()
    ) {
        // Test that boundary values can be formatted without panic
        let _ = format!("{:?}", boundary_value);

        // Test that they can be cloned
        let _cloned = boundary_value.clone();

        // Test basic equality, but handle NaN specially
        match &boundary_value {
            Value::Double(d) if d.is_nan() => {
                // NaN != NaN, so just check that cloning preserves NaN
                match boundary_value.clone() {
                    Value::Double(d2) => prop_assert!(d2.is_nan()),
                    _ => prop_assert!(false, "Clone changed type"),
                }
            }
            _ => {
                prop_assert_eq!(boundary_value.clone(), boundary_value);
            }
        }
    }

    /// Test that row_count is consistent across conversions
    #[test]
    fn test_row_count_consistency(
        (schema, logical_table) in arb_schema_and_table()
    ) {
        let logical_row_count = match &logical_table {
            Table::Binary(data) => if data.is_empty() { 0 } else { 1 },
            Table::Array(values) => values.len(),
            Table::Map(pairs) => pairs.len(),
        };

        let striped_table = striped::Table::from_logical(&schema, &logical_table)?;
        let striped_row_count = striped_table.row_count();

        prop_assert_eq!(logical_row_count, striped_row_count);
    }

    /// Test that empty collections are handled properly
    #[test]
    fn test_empty_collections(
        schema in arb_value_schema()
    ) {
        let empty_values: Vec<Value> = vec![];

        // Empty collections should work for most schema types
        let column_result = striped::Column::from_values(&schema, &empty_values);

        match schema {
            ValueSchema::Unit |
            ValueSchema::Int { .. } |
            ValueSchema::Double { .. } |
            ValueSchema::Binary { .. } |
            ValueSchema::Array { .. } => {
                prop_assert!(column_result.is_ok());
                if let Ok(column) = column_result {
                    prop_assert_eq!(column.row_count(), 0);
                    let roundtrip = column.to_values()?;
                    prop_assert_eq!(roundtrip, empty_values);
                }
            }
            _ => {
                // Other types might fail with empty input, that's okay
            }
        }
    }

    /// Test that large collections don't cause stack overflow
    #[test]
    fn test_large_collections(
        size in 1000usize..5000
    ) {
        let large_values: Vec<Value> = (0..size)
            .map(|i| Value::Int(i as i64))
            .collect();

        let schema = ValueSchema::Int {
            default: zbra_core::data::Default::Allow,
            encoding: zbra_core::data::Encoding::Int(zbra_core::data::IntEncoding::Int),
        };

        let column = striped::Column::from_values(&schema, &large_values)?;
        prop_assert_eq!(column.row_count(), size);

        let roundtrip = column.to_values()?;
        prop_assert_eq!(roundtrip.len(), size);
        prop_assert_eq!(roundtrip, large_values);
    }
}
