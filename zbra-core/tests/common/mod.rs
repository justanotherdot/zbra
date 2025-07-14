// Test utilities and generators for zbra property-based testing

#![allow(dead_code)]

use proptest::prelude::*;
use zbra_core::data::{BinaryEncoding, Default, Encoding, Field, IntEncoding, Table, Value};
use zbra_core::logical::{FieldSchema, TableSchema, ValueSchema, VariantSchema};

/// Generate Default values
pub fn arb_default() -> impl Strategy<Value = Default> {
    prop_oneof![Just(Default::Allow), Just(Default::Deny),]
}

/// Generate Encoding values
pub fn arb_encoding() -> impl Strategy<Value = Encoding> {
    prop_oneof![
        arb_int_encoding().prop_map(Encoding::Int),
        arb_binary_encoding().prop_map(Encoding::Binary),
    ]
}

/// Generate IntEncoding values
pub fn arb_int_encoding() -> impl Strategy<Value = IntEncoding> {
    prop_oneof![
        Just(IntEncoding::Int),
        Just(IntEncoding::Date),
        Just(IntEncoding::TimeSeconds),
        Just(IntEncoding::TimeMilliseconds),
        Just(IntEncoding::TimeMicroseconds),
    ]
}

/// Generate BinaryEncoding values
pub fn arb_binary_encoding() -> impl Strategy<Value = BinaryEncoding> {
    prop_oneof![Just(BinaryEncoding::Binary), Just(BinaryEncoding::Utf8),]
}

/// Generate reasonable-sized binary data
pub fn arb_binary_data() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..100)
}

/// Generate UTF-8 binary data
pub fn arb_utf8_binary() -> impl Strategy<Value = Vec<u8>> {
    "[a-zA-Z0-9 ]*".prop_map(|s| s.into_bytes())
}

/// Generate a basic Value with limited recursion depth
pub fn arb_value_depth(depth: u32) -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Unit),
        any::<i64>().prop_map(Value::Int),
        any::<f64>().prop_map(Value::Double),
        arb_binary_data().prop_map(Value::Binary),
    ];

    leaf.prop_recursive(depth, 256, 10, move |inner| {
        prop_oneof![
            // Arrays
            prop::collection::vec(inner.clone(), 0..10).prop_map(Value::Array),
            // Structs
            prop::collection::vec(
                (arb_field_name(), inner.clone()).prop_map(|(name, value)| Field { name, value }),
                0..5
            )
            .prop_map(Value::Struct),
            // Enums
            (0u32..10, inner.clone()).prop_map(|(tag, value)| Value::Enum {
                tag,
                value: Box::new(value)
            }),
            // Nested tables - for now, just wrap in arrays
            prop::collection::vec(inner.clone(), 0..5)
                .prop_map(|values| Value::Nested(Box::new(Table::Array(values)))),
            // Reversed
            inner.prop_map(|v| Value::Reversed(Box::new(v))),
        ]
    })
}

/// Generate a reasonable Value (depth 3)
pub fn arb_value() -> impl Strategy<Value = Value> {
    arb_value_depth(3)
}

/// Generate a shallow Value (depth 1) for performance
pub fn arb_value_shallow() -> impl Strategy<Value = Value> {
    arb_value_depth(1)
}

/// Generate field names
pub fn arb_field_name() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("id".to_string()),
        Just("name".to_string()),
        Just("value".to_string()),
        Just("timestamp".to_string()),
        Just("data".to_string()),
        Just("count".to_string()),
        "[a-z][a-z0-9_]*".prop_map(|s| s.to_string()),
    ]
}

/// Generate a ValueSchema with limited recursion depth
pub fn arb_value_schema_depth(depth: u32) -> BoxedStrategy<ValueSchema> {
    let leaf = prop_oneof![
        Just(ValueSchema::Unit),
        (arb_default(), arb_encoding())
            .prop_map(|(default, encoding)| { ValueSchema::Int { default, encoding } }),
        arb_default().prop_map(|default| ValueSchema::Double { default }),
        (arb_default(), arb_encoding())
            .prop_map(|(default, encoding)| { ValueSchema::Binary { default, encoding } }),
    ];

    leaf.prop_recursive(depth, 256, 10, move |inner| {
        prop_oneof![
            // Arrays
            (arb_default(), inner.clone()).prop_map(|(default, element)| ValueSchema::Array {
                default,
                element: Box::new(element)
            }),
            // Structs (must have at least 1 field)
            (
                arb_default(),
                prop::collection::vec(
                    (arb_field_name(), inner.clone())
                        .prop_map(|(name, schema)| FieldSchema { name, schema }),
                    1..5
                )
            )
                .prop_map(|(default, mut fields)| {
                    // Ensure unique field names to avoid schema validation issues
                    fields.sort_by(|a, b| a.name.cmp(&b.name));
                    fields.dedup_by(|a, b| a.name == b.name);
                    // If deduplication removed all fields, add a default one
                    if fields.is_empty() {
                        fields.push(FieldSchema {
                            name: "default_field".to_string(),
                            schema: ValueSchema::Unit,
                        });
                    }
                    ValueSchema::Struct { default, fields }
                }),
            // Enums
            (
                arb_default(),
                prop::collection::vec(
                    (arb_field_name(), 0u32..10, inner.clone())
                        .prop_map(|(name, tag, schema)| VariantSchema { name, tag, schema }),
                    1..5
                )
            )
                .prop_map(|(default, variants)| ValueSchema::Enum { default, variants }),
            // Nested tables - for now, just use array tables
            arb_table_schema_depth(depth.saturating_sub(1)).prop_map(|table| ValueSchema::Nested {
                table: Box::new(table)
            }),
            // Reversed
            inner.prop_map(|inner| ValueSchema::Reversed {
                inner: Box::new(inner)
            }),
        ]
    })
    .boxed()
}

/// Generate a TableSchema with limited recursion depth
pub fn arb_table_schema_depth(depth: u32) -> BoxedStrategy<TableSchema> {
    prop_oneof![
        (arb_default(), arb_encoding())
            .prop_map(|(default, encoding)| { TableSchema::Binary { default, encoding } }),
        (arb_default(), arb_value_schema_depth(depth)).prop_map(|(default, element)| {
            TableSchema::Array {
                default,
                element: Box::new(element),
            }
        }),
        (
            arb_default(),
            arb_value_schema_depth(depth),
            arb_value_schema_depth(depth)
        )
            .prop_map(|(default, key, value)| TableSchema::Map {
                default,
                key: Box::new(key),
                value: Box::new(value)
            }),
    ]
    .boxed()
}

/// Generate a reasonable ValueSchema (depth 2)
pub fn arb_value_schema() -> BoxedStrategy<ValueSchema> {
    arb_value_schema_depth(2)
}

/// Generate a reasonable TableSchema (depth 2)
pub fn arb_table_schema() -> BoxedStrategy<TableSchema> {
    arb_table_schema_depth(2)
}

/// Generate a Table
pub fn arb_table() -> impl Strategy<Value = Table> {
    prop_oneof![
        arb_binary_data().prop_map(Table::Binary),
        prop::collection::vec(arb_value_shallow(), 0..20).prop_map(Table::Array),
        prop::collection::vec((arb_value_shallow(), arb_value_shallow()), 0..10)
            .prop_map(Table::Map),
    ]
}

/// Generate a compatible Value for a given ValueSchema
pub fn arb_value_for_schema(schema: &ValueSchema) -> BoxedStrategy<Value> {
    match schema {
        ValueSchema::Unit => Just(Value::Unit).boxed(),
        ValueSchema::Int { .. } => any::<i64>().prop_map(Value::Int).boxed(),
        ValueSchema::Double { .. } => any::<f64>().prop_map(Value::Double).boxed(),
        ValueSchema::Binary { encoding, .. } => match encoding {
            Encoding::Binary(BinaryEncoding::Binary) => {
                arb_binary_data().prop_map(Value::Binary).boxed()
            }
            Encoding::Binary(BinaryEncoding::Utf8) => {
                arb_utf8_binary().prop_map(Value::Binary).boxed()
            }
            _ => arb_binary_data().prop_map(Value::Binary).boxed(),
        },
        ValueSchema::Array { element, .. } => {
            let element_gen = arb_value_for_schema(element);
            prop::collection::vec(element_gen, 0..10)
                .prop_map(Value::Array)
                .boxed()
        }
        ValueSchema::Struct { fields, .. } => {
            if fields.is_empty() {
                // Empty structs are not supported - this should not happen if schema validation works
                Just(Value::Unit).boxed()
            } else {
                // Generate all fields for the struct
                let field_gens: Vec<_> = fields
                    .iter()
                    .map(|field_schema| {
                        let name = field_schema.name.clone();
                        let value_gen = arb_value_for_schema(&field_schema.schema);
                        value_gen.prop_map(move |value| Field {
                            name: name.clone(),
                            value,
                        })
                    })
                    .collect();

                // Combine all field generators into a single struct generator
                match field_gens.len() {
                    1 => field_gens[0]
                        .clone()
                        .prop_map(|field| Value::Struct(vec![field]))
                        .boxed(),
                    2 => (field_gens[0].clone(), field_gens[1].clone())
                        .prop_map(|(f1, f2)| Value::Struct(vec![f1, f2]))
                        .boxed(),
                    3 => (
                        field_gens[0].clone(),
                        field_gens[1].clone(),
                        field_gens[2].clone(),
                    )
                        .prop_map(|(f1, f2, f3)| Value::Struct(vec![f1, f2, f3]))
                        .boxed(),
                    4 => (
                        field_gens[0].clone(),
                        field_gens[1].clone(),
                        field_gens[2].clone(),
                        field_gens[3].clone(),
                    )
                        .prop_map(|(f1, f2, f3, f4)| Value::Struct(vec![f1, f2, f3, f4]))
                        .boxed(),
                    _ => {
                        // For more than 4 fields, fall back to generating just the first field
                        // This is a limitation of proptest's tuple support
                        let field_schema = &fields[0];
                        let name = field_schema.name.clone();
                        let value_gen = arb_value_for_schema(&field_schema.schema);
                        value_gen
                            .prop_map(move |value| {
                                Value::Struct(vec![Field {
                                    name: name.clone(),
                                    value,
                                }])
                            })
                            .boxed()
                    }
                }
            }
        }
        ValueSchema::Enum { variants, .. } => {
            if variants.is_empty() {
                Just(Value::Unit).boxed()
            } else {
                let variant = variants[0].clone();
                let tag = variant.tag;
                let value_gen = arb_value_for_schema(&variant.schema);
                value_gen
                    .prop_map(move |value| Value::Enum {
                        tag,
                        value: Box::new(value),
                    })
                    .boxed()
            }
        }
        ValueSchema::Nested { table } => arb_table_for_schema(table)
            .prop_map(|table| Value::Nested(Box::new(table)))
            .boxed(),
        ValueSchema::Reversed { inner } => {
            // For reversed schemas, we need to generate a Reversed value
            // containing the inner value that matches the inner schema
            arb_value_for_schema(inner)
                .prop_map(|value| Value::Reversed(Box::new(value)))
                .boxed()
        }
    }
}

/// Generate a compatible Table for a given TableSchema
pub fn arb_table_for_schema(schema: &TableSchema) -> BoxedStrategy<Table> {
    match schema {
        TableSchema::Binary { encoding, .. } => match encoding {
            Encoding::Binary(BinaryEncoding::Binary) => {
                arb_binary_data().prop_map(Table::Binary).boxed()
            }
            Encoding::Binary(BinaryEncoding::Utf8) => {
                arb_utf8_binary().prop_map(Table::Binary).boxed()
            }
            _ => arb_binary_data().prop_map(Table::Binary).boxed(),
        },
        TableSchema::Array { element, .. } => {
            let element_gen = arb_value_for_schema(element);
            prop::collection::vec(element_gen, 0..20)
                .prop_map(Table::Array)
                .boxed()
        }
        TableSchema::Map { key, value, .. } => {
            let key_gen = arb_value_for_schema(key);
            let value_gen = arb_value_for_schema(value);
            prop::collection::vec((key_gen, value_gen), 0..10)
                .prop_map(Table::Map)
                .boxed()
        }
    }
}

/// Generate a schema and compatible data pair
pub fn arb_schema_and_table() -> impl Strategy<Value = (TableSchema, Table)> {
    arb_table_schema().prop_flat_map(|schema| {
        let table_gen = arb_table_for_schema(&schema);
        (Just(schema), table_gen)
    })
}

/// Generate a schema and compatible value pair
pub fn arb_schema_and_value() -> impl Strategy<Value = (ValueSchema, Value)> {
    arb_value_schema().prop_flat_map(|schema| {
        let value_gen = arb_value_for_schema(&schema);
        (Just(schema), value_gen)
    })
}

/// Generate boundary values for testing edge cases
pub fn arb_boundary_values() -> impl Strategy<Value = Value> {
    prop_oneof![
        // Integer boundaries
        Just(Value::Int(i64::MIN)),
        Just(Value::Int(i64::MAX)),
        Just(Value::Int(0)),
        Just(Value::Int(-1)),
        Just(Value::Int(1)),
        // Float boundaries
        Just(Value::Double(f64::MIN)),
        Just(Value::Double(f64::MAX)),
        Just(Value::Double(f64::NEG_INFINITY)),
        Just(Value::Double(f64::INFINITY)),
        Just(Value::Double(f64::NAN)),
        Just(Value::Double(0.0)),
        Just(Value::Double(-0.0)),
        // Empty collections
        Just(Value::Binary(Vec::new())),
        Just(Value::Array(Vec::new())),
        Just(Value::Struct(Vec::new())),
        // Large collections
        prop::collection::vec(Just(Value::Unit), 100..200).prop_map(Value::Array),
        prop::collection::vec(0u8..255, 1000..2000).prop_map(Value::Binary),
    ]
}

/// Roundtrip test utility - like zebra's trippingIO
pub fn test_roundtrip<T, E, F, G>(to_fn: F, from_fn: G, original: T) -> Result<(), String>
where
    T: PartialEq + std::fmt::Debug + Clone,
    E: std::fmt::Debug,
    F: FnOnce(T) -> Result<T, E>,
    G: FnOnce(T) -> Result<T, E>,
{
    let original_clone = original.clone();
    let intermediate =
        to_fn(original).map_err(|e| format!("Forward conversion failed: {:?}", e))?;
    let roundtrip =
        from_fn(intermediate).map_err(|e| format!("Reverse conversion failed: {:?}", e))?;

    if original_clone == roundtrip {
        Ok(())
    } else {
        Err(format!(
            "Roundtrip failed: {:?} != {:?}",
            original_clone, roundtrip
        ))
    }
}
