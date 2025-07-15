// Logical layer - human-readable representation

use crate::data::{Default, Encoding, Field, Table, Value};
use crate::error::{LogicalError, SchemaError};
use serde::{Deserialize, Serialize};

/// Schema definition for tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableSchema {
    Binary {
        default: Default,
        encoding: Encoding,
    },
    Array {
        default: Default,
        element: Box<ValueSchema>,
    },
    Map {
        default: Default,
        key: Box<ValueSchema>,
        value: Box<ValueSchema>,
    },
}

/// Schema definition for values
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueSchema {
    Unit,
    Int {
        default: Default,
        encoding: Encoding,
    },
    Double {
        default: Default,
    },
    Binary {
        default: Default,
        encoding: Encoding,
    },
    Array {
        default: Default,
        element: Box<ValueSchema>,
    },
    Struct {
        default: Default,
        fields: Vec<FieldSchema>,
    },
    Enum {
        default: Default,
        variants: Vec<VariantSchema>,
    },
    Nested {
        table: Box<TableSchema>,
    },
    Reversed {
        inner: Box<ValueSchema>,
    },
}

/// Schema for struct fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub schema: ValueSchema,
}

/// Schema for enum variants
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VariantSchema {
    pub name: String,
    pub tag: u32,
    pub schema: ValueSchema,
}

/// Logical operations on tables
impl Table {
    /// Validate table against schema
    pub fn validate_schema(&self, schema: &TableSchema) -> Result<(), SchemaError> {
        match (self, schema) {
            (Table::Binary(_), TableSchema::Binary { .. }) => Ok(()),
            (Table::Array(values), TableSchema::Array { element, .. }) => {
                for value in values {
                    value.validate_schema(element)?;
                }
                Ok(())
            }
            (Table::Map(pairs), TableSchema::Map { key, value, .. }) => {
                for (k, v) in pairs {
                    k.validate_schema(key)?;
                    v.validate_schema(value)?;
                }
                Ok(())
            }
            _ => Err(SchemaError::TypeMismatch {
                expected: format!("{:?}", schema),
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Merge two tables of the same type
    pub fn merge(&self, other: &Table) -> Result<Table, LogicalError> {
        match (self, other) {
            (Table::Binary(a), Table::Binary(b)) => {
                if a == b {
                    Ok(Table::Binary(a.clone()))
                } else {
                    Err(LogicalError::StructureMismatch(
                        "Cannot merge different binary values".to_string(),
                    ))
                }
            }
            (Table::Array(a), Table::Array(b)) => {
                let mut merged = a.clone();
                merged.extend(b.clone());
                Ok(Table::Array(merged))
            }
            (Table::Map(a), Table::Map(b)) => {
                let mut merged_pairs = a.clone();

                for (new_key, new_value) in b {
                    match merged_pairs.iter_mut().find(|(k, _)| k == new_key) {
                        Some((_, existing_value)) => {
                            *existing_value = existing_value.merge(new_value)?;
                        }
                        None => {
                            merged_pairs.push((new_key.clone(), new_value.clone()));
                        }
                    }
                }
                Ok(Table::Map(merged_pairs))
            }
            _ => Err(LogicalError::StructureMismatch(format!(
                "Cannot merge tables of different types: {:?} and {:?}",
                self, other
            ))),
        }
    }

    /// Get the default table for a schema
    pub fn default_for_schema(schema: &TableSchema) -> Table {
        match schema {
            TableSchema::Binary { .. } => Table::Binary(Vec::new()),
            TableSchema::Array { .. } => Table::Array(Vec::new()),
            TableSchema::Map { .. } => Table::Map(Vec::new()),
        }
    }
}

/// Logical operations on values
impl Value {
    /// Validate value against schema
    pub fn validate_schema(&self, schema: &ValueSchema) -> Result<(), SchemaError> {
        match (self, schema) {
            (Value::Unit, ValueSchema::Unit) => Ok(()),
            (Value::Int(_), ValueSchema::Int { .. }) => Ok(()),
            (Value::Double(_), ValueSchema::Double { .. }) => Ok(()),
            (Value::Binary(_), ValueSchema::Binary { .. }) => Ok(()),
            (Value::Array(values), ValueSchema::Array { element, .. }) => {
                for value in values {
                    value.validate_schema(element)?;
                }
                Ok(())
            }
            (
                Value::Struct(fields),
                ValueSchema::Struct {
                    fields: field_schemas,
                    ..
                },
            ) => {
                if fields.len() != field_schemas.len() {
                    return Err(SchemaError::TypeMismatch {
                        expected: format!("struct with {} fields", field_schemas.len()),
                        actual: format!("struct with {} fields", fields.len()),
                    });
                }
                for (field, field_schema) in fields.iter().zip(field_schemas.iter()) {
                    if field.name != field_schema.name {
                        return Err(SchemaError::MissingField(field_schema.name.clone()));
                    }
                    field.value.validate_schema(&field_schema.schema)?;
                }
                Ok(())
            }
            (Value::Enum { tag, value }, ValueSchema::Enum { variants, .. }) => {
                if let Some(variant) = variants.iter().find(|v| v.tag == *tag) {
                    value.validate_schema(&variant.schema)
                } else {
                    Err(SchemaError::UnsupportedType(format!("enum tag {}", tag)))
                }
            }
            (
                Value::Nested(table),
                ValueSchema::Nested {
                    table: table_schema,
                },
            ) => table.validate_schema(table_schema),
            (Value::Reversed(value), ValueSchema::Reversed { inner }) => {
                value.validate_schema(inner)
            }
            _ => Err(SchemaError::TypeMismatch {
                expected: format!("{:?}", schema),
                actual: format!("{:?}", self),
            }),
        }
    }

    /// Merge two values of compatible types
    pub fn merge(&self, other: &Value) -> Result<Value, LogicalError> {
        match (self, other) {
            // Primitive values must be identical to merge
            (Value::Unit, Value::Unit) => Ok(Value::Unit),
            (Value::Int(a), Value::Int(b)) => {
                if a == b {
                    Ok(Value::Int(*a))
                } else {
                    Err(LogicalError::InvalidValue {
                        field: "int".to_string(),
                        reason: format!("Cannot merge different integers: {} and {}", a, b),
                    })
                }
            }
            (Value::Double(a), Value::Double(b)) => {
                if (a - b).abs() < f64::EPSILON {
                    Ok(Value::Double(*a))
                } else {
                    Err(LogicalError::InvalidValue {
                        field: "double".to_string(),
                        reason: format!("Cannot merge different doubles: {} and {}", a, b),
                    })
                }
            }
            (Value::Binary(a), Value::Binary(b)) => {
                if a == b {
                    Ok(Value::Binary(a.clone()))
                } else {
                    Err(LogicalError::InvalidValue {
                        field: "binary".to_string(),
                        reason: "Cannot merge different binary values".to_string(),
                    })
                }
            }
            // Arrays can be concatenated
            (Value::Array(a), Value::Array(b)) => {
                let mut merged = a.clone();
                merged.extend(b.clone());
                Ok(Value::Array(merged))
            }
            // Structs merge field by field
            (Value::Struct(a), Value::Struct(b)) => {
                if a.len() != b.len() {
                    return Err(LogicalError::StructureMismatch(
                        "Cannot merge structs with different field counts".to_string(),
                    ));
                }
                let mut merged_fields = Vec::new();
                for (field_a, field_b) in a.iter().zip(b.iter()) {
                    if field_a.name != field_b.name {
                        return Err(LogicalError::StructureMismatch(format!(
                            "Field name mismatch: {} vs {}",
                            field_a.name, field_b.name
                        )));
                    }
                    let merged_value = field_a.value.merge(&field_b.value)?;
                    merged_fields.push(Field {
                        name: field_a.name.clone(),
                        value: merged_value,
                    });
                }
                Ok(Value::Struct(merged_fields))
            }
            // Enums must have same tag and value
            (
                Value::Enum {
                    tag: tag_a,
                    value: val_a,
                },
                Value::Enum {
                    tag: tag_b,
                    value: val_b,
                },
            ) => {
                if tag_a != tag_b {
                    return Err(LogicalError::InvalidValue {
                        field: "enum".to_string(),
                        reason: format!(
                            "Cannot merge enums with different tags: {} vs {}",
                            tag_a, tag_b
                        ),
                    });
                }
                let merged_value = val_a.merge(val_b)?;
                Ok(Value::Enum {
                    tag: *tag_a,
                    value: Box::new(merged_value),
                })
            }
            // Nested tables
            (Value::Nested(a), Value::Nested(b)) => {
                let merged_table = a.merge(b)?;
                Ok(Value::Nested(Box::new(merged_table)))
            }
            // Reversed values
            (Value::Reversed(a), Value::Reversed(b)) => {
                let merged_inner = a.merge(b)?;
                Ok(Value::Reversed(Box::new(merged_inner)))
            }
            _ => Err(LogicalError::StructureMismatch(format!(
                "Cannot merge values of different types: {:?} and {:?}",
                self, other
            ))),
        }
    }

    /// Get the default value for a schema
    pub fn default_for_schema(schema: &ValueSchema) -> Value {
        match schema {
            ValueSchema::Unit => Value::Unit,
            ValueSchema::Int { .. } => Value::Int(0),
            ValueSchema::Double { .. } => Value::Double(0.0),
            ValueSchema::Binary { .. } => Value::Binary(Vec::new()),
            ValueSchema::Array { .. } => Value::Array(Vec::new()),
            ValueSchema::Struct { fields, .. } => {
                let default_fields = fields
                    .iter()
                    .map(|field_schema| Field {
                        name: field_schema.name.clone(),
                        value: Value::default_for_schema(&field_schema.schema),
                    })
                    .collect();
                Value::Struct(default_fields)
            }
            ValueSchema::Enum { variants, .. } => {
                if let Some(first_variant) = variants.first() {
                    Value::Enum {
                        tag: first_variant.tag,
                        value: Box::new(Value::default_for_schema(&first_variant.schema)),
                    }
                } else {
                    // TODO: handle empty enum case
                    Value::Unit
                }
            }
            ValueSchema::Nested { table } => {
                Value::Nested(Box::new(Table::default_for_schema(table)))
            }
            ValueSchema::Reversed { inner } => {
                Value::Reversed(Box::new(Value::default_for_schema(inner)))
            }
        }
    }
}

/// Type-safe extractors for values
impl Value {
    pub fn take_int(&self) -> Result<i64, LogicalError> {
        match self {
            Value::Int(n) => Ok(*n),
            _ => Err(LogicalError::InvalidValue {
                field: "value".to_string(),
                reason: format!("Expected int, got {:?}", self),
            }),
        }
    }

    pub fn take_double(&self) -> Result<f64, LogicalError> {
        match self {
            Value::Double(d) => Ok(*d),
            _ => Err(LogicalError::InvalidValue {
                field: "value".to_string(),
                reason: format!("Expected double, got {:?}", self),
            }),
        }
    }

    pub fn take_binary(&self) -> Result<&Vec<u8>, LogicalError> {
        match self {
            Value::Binary(b) => Ok(b),
            _ => Err(LogicalError::InvalidValue {
                field: "value".to_string(),
                reason: format!("Expected binary, got {:?}", self),
            }),
        }
    }

    pub fn take_array(&self) -> Result<&Vec<Value>, LogicalError> {
        match self {
            Value::Array(a) => Ok(a),
            _ => Err(LogicalError::InvalidValue {
                field: "value".to_string(),
                reason: format!("Expected array, got {:?}", self),
            }),
        }
    }

    pub fn take_struct(&self) -> Result<&Vec<Field>, LogicalError> {
        match self {
            Value::Struct(s) => Ok(s),
            _ => Err(LogicalError::InvalidValue {
                field: "value".to_string(),
                reason: format!("Expected struct, got {:?}", self),
            }),
        }
    }
}

/// Type-safe extractors for tables
impl Table {
    pub fn take_binary(&self) -> Result<&Vec<u8>, LogicalError> {
        match self {
            Table::Binary(b) => Ok(b),
            _ => Err(LogicalError::InvalidValue {
                field: "table".to_string(),
                reason: format!("Expected binary table, got {:?}", self),
            }),
        }
    }

    pub fn take_array(&self) -> Result<&Vec<Value>, LogicalError> {
        match self {
            Table::Array(a) => Ok(a),
            _ => Err(LogicalError::InvalidValue {
                field: "table".to_string(),
                reason: format!("Expected array table, got {:?}", self),
            }),
        }
    }

    pub fn take_map(&self) -> Result<&Vec<(Value, Value)>, LogicalError> {
        match self {
            Table::Map(m) => Ok(m),
            _ => Err(LogicalError::InvalidValue {
                field: "table".to_string(),
                reason: format!("Expected map table, got {:?}", self),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::IntEncoding;

    #[test]
    fn test_value_validation() {
        let schema = ValueSchema::Int {
            default: Default::Allow,
            encoding: Encoding::Int(IntEncoding::Int),
        };

        let value = Value::Int(42);
        assert!(value.validate_schema(&schema).is_ok());

        let wrong_value = Value::Double(42.0);
        assert!(wrong_value.validate_schema(&schema).is_err());
    }

    #[test]
    fn test_value_merge() {
        let val1 = Value::Int(42);
        let val2 = Value::Int(42);
        let merged = val1.merge(&val2).unwrap();
        assert_eq!(merged, Value::Int(42));

        let val3 = Value::Int(24);
        assert!(val1.merge(&val3).is_err());
    }

    #[test]
    fn test_array_merge() {
        let arr1 = Value::Array(vec![Value::Int(1), Value::Int(2)]);
        let arr2 = Value::Array(vec![Value::Int(3), Value::Int(4)]);
        let merged = arr1.merge(&arr2).unwrap();

        match merged {
            Value::Array(values) => {
                assert_eq!(values.len(), 4);
                assert_eq!(values[0], Value::Int(1));
                assert_eq!(values[3], Value::Int(4));
            }
            _ => panic!("Expected array"),
        }
    }
}
