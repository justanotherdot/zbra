// Striped layer - in-memory columnar representation

use crate::data::{Default, Encoding, Field, Table as LogicalTable, Value};
use crate::error::{ConversionError, StripedError};
use crate::logical::{TableSchema, ValueSchema};

/// Striped table representation - columnar storage
#[derive(Debug, Clone, PartialEq)]
pub enum Table {
    Binary {
        default: Default,
        encoding: Encoding,
        data: Vec<u8>,
    },
    Array {
        default: Default,
        column: Box<Column>,
    },
    Map {
        default: Default,
        key_column: Box<Column>,
        value_column: Box<Column>,
    },
}

/// Striped column representation for values
#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    Unit {
        count: usize,
    },
    Int {
        default: Default,
        encoding: Encoding,
        values: Vec<i64>,
    },
    Double {
        default: Default,
        values: Vec<f64>,
    },
    Binary {
        default: Default,
        encoding: Encoding,
        lengths: Vec<usize>,
        data: Vec<u8>,
    },
    Array {
        default: Default,
        lengths: Vec<usize>,
        element: Box<Column>,
    },
    Struct {
        default: Default,
        fields: Vec<FieldColumn>,
    },
    Enum {
        default: Default,
        tags: Vec<u32>,
        variants: Vec<VariantColumn>,
    },
    Nested {
        lengths: Vec<usize>,
        table: Box<Table>,
    },
    Reversed {
        inner: Box<Column>,
    },
}

/// Field in a striped struct
#[derive(Debug, Clone, PartialEq)]
pub struct FieldColumn {
    pub name: String,
    pub column: Column,
}

/// Variant in a striped enum
#[derive(Debug, Clone, PartialEq)]
pub struct VariantColumn {
    pub name: String,
    pub tag: u32,
    pub column: Column,
}

/// Convert logical table to striped format
impl Table {
    pub fn from_logical(
        schema: &TableSchema,
        logical: &LogicalTable,
    ) -> Result<Self, ConversionError> {
        match (schema, logical) {
            (TableSchema::Binary { default, encoding }, LogicalTable::Binary(data)) => {
                Ok(Table::Binary {
                    default: default.clone(),
                    encoding: encoding.clone(),
                    data: data.clone(),
                })
            }
            (TableSchema::Array { default, element }, LogicalTable::Array(values)) => {
                let column = Column::from_values(element, values)?;
                Ok(Table::Array {
                    default: default.clone(),
                    column: Box::new(column),
                })
            }
            (
                TableSchema::Map {
                    default,
                    key,
                    value,
                },
                LogicalTable::Map(pairs),
            ) => {
                let keys: Vec<Value> = pairs.iter().map(|(k, _)| k.clone()).collect();
                let values: Vec<Value> = pairs.iter().map(|(_, v)| v.clone()).collect();

                let key_column = Column::from_values(key, &keys)?;
                let value_column = Column::from_values(value, &values)?;

                Ok(Table::Map {
                    default: default.clone(),
                    key_column: Box::new(key_column),
                    value_column: Box::new(value_column),
                })
            }
            _ => Err(ConversionError::Schema(
                crate::error::SchemaError::TypeMismatch {
                    expected: format!("{:?}", schema),
                    actual: format!("{:?}", logical),
                },
            )),
        }
    }

    /// Convert striped table back to logical format
    pub fn to_logical(&self) -> Result<LogicalTable, ConversionError> {
        match self {
            Table::Binary { data, .. } => Ok(LogicalTable::Binary(data.clone())),
            Table::Array { column, .. } => {
                let values = column.to_values()?;
                Ok(LogicalTable::Array(values))
            }
            Table::Map {
                key_column,
                value_column,
                ..
            } => {
                let keys = key_column.to_values()?;
                let values = value_column.to_values()?;

                if keys.len() != values.len() {
                    return Err(ConversionError::Striped(StripedError::ColumnMismatch {
                        expected: keys.len(),
                        actual: values.len(),
                    }));
                }

                let pairs = keys.into_iter().zip(values.into_iter()).collect();
                Ok(LogicalTable::Map(pairs))
            }
        }
    }

    /// Get the number of rows in the table
    pub fn row_count(&self) -> usize {
        match self {
            Table::Binary { data, .. } => {
                if data.is_empty() {
                    0
                } else {
                    1
                }
            }
            Table::Array { column, .. } => column.row_count(),
            Table::Map { key_column, .. } => key_column.row_count(),
        }
    }
}

/// Convert logical values to striped columns
impl Column {
    pub fn from_values(schema: &ValueSchema, values: &[Value]) -> Result<Self, ConversionError> {
        match schema {
            ValueSchema::Unit => {
                // TODO: validate all values are Unit
                Ok(Column::Unit {
                    count: values.len(),
                })
            }
            ValueSchema::Int { default, encoding } => {
                let mut int_values = Vec::new();
                for value in values {
                    match value {
                        Value::Int(n) => int_values.push(*n),
                        _ => {
                            return Err(ConversionError::Schema(
                                crate::error::SchemaError::TypeMismatch {
                                    expected: "int".to_string(),
                                    actual: format!("{:?}", value),
                                },
                            ))
                        }
                    }
                }
                Ok(Column::Int {
                    default: default.clone(),
                    encoding: encoding.clone(),
                    values: int_values,
                })
            }
            ValueSchema::Double { default } => {
                let mut double_values = Vec::new();
                for value in values {
                    match value {
                        Value::Double(d) => double_values.push(*d),
                        _ => {
                            return Err(ConversionError::Schema(
                                crate::error::SchemaError::TypeMismatch {
                                    expected: "double".to_string(),
                                    actual: format!("{:?}", value),
                                },
                            ))
                        }
                    }
                }
                Ok(Column::Double {
                    default: default.clone(),
                    values: double_values,
                })
            }
            ValueSchema::Binary { default, encoding } => {
                let mut lengths = Vec::new();
                let mut data = Vec::new();

                for value in values {
                    match value {
                        Value::Binary(bytes) => {
                            lengths.push(bytes.len());
                            data.extend_from_slice(bytes);
                        }
                        _ => {
                            return Err(ConversionError::Schema(
                                crate::error::SchemaError::TypeMismatch {
                                    expected: "binary".to_string(),
                                    actual: format!("{:?}", value),
                                },
                            ))
                        }
                    }
                }

                Ok(Column::Binary {
                    default: default.clone(),
                    encoding: encoding.clone(),
                    lengths,
                    data,
                })
            }
            ValueSchema::Array { default, element } => {
                let mut lengths = Vec::new();
                let mut all_elements = Vec::new();

                for value in values {
                    match value {
                        Value::Array(arr) => {
                            lengths.push(arr.len());
                            all_elements.extend(arr.clone());
                        }
                        _ => {
                            return Err(ConversionError::Schema(
                                crate::error::SchemaError::TypeMismatch {
                                    expected: "array".to_string(),
                                    actual: format!("{:?}", value),
                                },
                            ))
                        }
                    }
                }

                let element_column = Column::from_values(element, &all_elements)?;
                Ok(Column::Array {
                    default: default.clone(),
                    lengths,
                    element: Box::new(element_column),
                })
            }
            ValueSchema::Struct { default, fields } => {
                // Reject empty structs following zebra's approach
                if fields.is_empty() {
                    return Err(ConversionError::Schema(
                        crate::error::SchemaError::UnsupportedType(
                            "Empty structs are not supported".to_string(),
                        ),
                    ));
                }

                let mut field_columns = Vec::new();

                for field_schema in fields {
                    let mut field_values = Vec::new();

                    for value in values {
                        match value {
                            Value::Struct(struct_fields) => {
                                if let Some(field) =
                                    struct_fields.iter().find(|f| f.name == field_schema.name)
                                {
                                    field_values.push(field.value.clone());
                                } else {
                                    return Err(ConversionError::Schema(
                                        crate::error::SchemaError::MissingField(
                                            field_schema.name.clone(),
                                        ),
                                    ));
                                }
                            }
                            _ => {
                                return Err(ConversionError::Schema(
                                    crate::error::SchemaError::TypeMismatch {
                                        expected: "struct".to_string(),
                                        actual: format!("{:?}", value),
                                    },
                                ))
                            }
                        }
                    }

                    let field_column = Column::from_values(&field_schema.schema, &field_values)?;
                    field_columns.push(FieldColumn {
                        name: field_schema.name.clone(),
                        column: field_column,
                    });
                }

                Ok(Column::Struct {
                    default: default.clone(),
                    fields: field_columns,
                })
            }
            ValueSchema::Enum { default, variants } => {
                let mut tags = Vec::new();
                let mut variant_data: Vec<Vec<Value>> = vec![Vec::new(); variants.len()];

                for value in values {
                    match value {
                        Value::Enum { tag, value } => {
                            tags.push(*tag);

                            if let Some(variant_index) = variants.iter().position(|v| v.tag == *tag)
                            {
                                variant_data[variant_index].push((**value).clone());
                            } else {
                                return Err(ConversionError::Schema(
                                    crate::error::SchemaError::UnsupportedType(format!(
                                        "enum tag {}",
                                        tag
                                    )),
                                ));
                            }
                        }
                        _ => {
                            return Err(ConversionError::Schema(
                                crate::error::SchemaError::TypeMismatch {
                                    expected: "enum".to_string(),
                                    actual: format!("{:?}", value),
                                },
                            ))
                        }
                    }
                }

                let mut variant_columns = Vec::new();
                for (i, variant_schema) in variants.iter().enumerate() {
                    let column = Column::from_values(&variant_schema.schema, &variant_data[i])?;
                    variant_columns.push(VariantColumn {
                        name: variant_schema.name.clone(),
                        tag: variant_schema.tag,
                        column,
                    });
                }

                Ok(Column::Enum {
                    default: default.clone(),
                    tags,
                    variants: variant_columns,
                })
            }
            ValueSchema::Nested {
                table: table_schema,
            } => {
                let mut lengths = Vec::new();
                let mut all_logical_tables = Vec::new();

                for value in values {
                    match value {
                        Value::Nested(table_value) => {
                            match table_value.as_ref() {
                                LogicalTable::Array(arr) => {
                                    lengths.push(arr.len());
                                }
                                LogicalTable::Map(pairs) => {
                                    lengths.push(pairs.len());
                                }
                                LogicalTable::Binary(data) => {
                                    lengths.push(data.len());
                                }
                            }
                            all_logical_tables.push(table_value.as_ref().clone());
                        }
                        _ => {
                            return Err(ConversionError::Schema(
                                crate::error::SchemaError::TypeMismatch {
                                    expected: "nested".to_string(),
                                    actual: format!("{:?}", value),
                                },
                            ))
                        }
                    }
                }

                // For nested tables, we need to preserve the individual tables
                // Create a single logical table that contains all nested tables concatenated
                let merged_table = match table_schema.as_ref() {
                    TableSchema::Binary { .. } => {
                        // For binary tables, concatenate all binary data
                        let mut all_data = Vec::new();
                        for table in &all_logical_tables {
                            match table {
                                LogicalTable::Binary(data) => all_data.extend_from_slice(data),
                                _ => {
                                    return Err(ConversionError::Schema(
                                        crate::error::SchemaError::TypeMismatch {
                                            expected: "binary table".to_string(),
                                            actual: format!("{:?}", table),
                                        },
                                    ))
                                }
                            }
                        }
                        LogicalTable::Binary(all_data)
                    }
                    TableSchema::Array { .. } => {
                        // For array tables, concatenate all array elements
                        let mut all_elements = Vec::new();
                        for table in &all_logical_tables {
                            match table {
                                LogicalTable::Array(elements) => {
                                    all_elements.extend_from_slice(elements)
                                }
                                _ => {
                                    return Err(ConversionError::Schema(
                                        crate::error::SchemaError::TypeMismatch {
                                            expected: "array table".to_string(),
                                            actual: format!("{:?}", table),
                                        },
                                    ))
                                }
                            }
                        }
                        LogicalTable::Array(all_elements)
                    }
                    TableSchema::Map { .. } => {
                        // For map tables, concatenate all map pairs
                        let mut all_pairs = Vec::new();
                        for table in &all_logical_tables {
                            match table {
                                LogicalTable::Map(pairs) => all_pairs.extend_from_slice(pairs),
                                _ => {
                                    return Err(ConversionError::Schema(
                                        crate::error::SchemaError::TypeMismatch {
                                            expected: "map table".to_string(),
                                            actual: format!("{:?}", table),
                                        },
                                    ))
                                }
                            }
                        }
                        LogicalTable::Map(all_pairs)
                    }
                };

                // Convert the merged logical table to striped format
                let nested_table = Table::from_logical(table_schema, &merged_table)?;

                Ok(Column::Nested {
                    lengths,
                    table: Box::new(nested_table),
                })
            }
            ValueSchema::Reversed { inner } => {
                // Unwrap all reversed values to get the inner values
                let mut inner_values = Vec::new();
                for value in values {
                    match value {
                        Value::Reversed(inner_value) => {
                            inner_values.push(inner_value.as_ref().clone());
                        }
                        _ => {
                            return Err(ConversionError::Schema(
                                crate::error::SchemaError::TypeMismatch {
                                    expected: "reversed".to_string(),
                                    actual: format!("{:?}", value),
                                },
                            ))
                        }
                    }
                }

                let inner_column = Column::from_values(inner, &inner_values)?;
                Ok(Column::Reversed {
                    inner: Box::new(inner_column),
                })
            }
        }
    }

    /// Convert striped column back to logical values
    pub fn to_values(&self) -> Result<Vec<Value>, ConversionError> {
        match self {
            Column::Unit { count } => Ok(vec![Value::Unit; *count]),
            Column::Int { values, .. } => Ok(values.iter().map(|&n| Value::Int(n)).collect()),
            Column::Double { values, .. } => Ok(values.iter().map(|&d| Value::Double(d)).collect()),
            Column::Binary { lengths, data, .. } => {
                let mut result = Vec::new();
                let mut offset = 0;

                for &length in lengths {
                    if offset + length > data.len() {
                        return Err(ConversionError::Striped(
                            StripedError::VectorOperationFailed(
                                "Binary data length mismatch".to_string(),
                            ),
                        ));
                    }

                    let bytes = data[offset..offset + length].to_vec();
                    result.push(Value::Binary(bytes));
                    offset += length;
                }

                // Ensure we consumed exactly all the data
                if offset != data.len() {
                    return Err(ConversionError::Striped(
                        StripedError::VectorOperationFailed(
                            "Binary data length mismatch".to_string(),
                        ),
                    ));
                }

                Ok(result)
            }
            Column::Array {
                lengths, element, ..
            } => {
                let element_values = element.to_values()?;
                let mut result = Vec::new();
                let mut offset = 0;

                for &length in lengths {
                    if offset + length > element_values.len() {
                        return Err(ConversionError::Striped(
                            StripedError::VectorOperationFailed(
                                "Array element length mismatch".to_string(),
                            ),
                        ));
                    }

                    let array_elements = element_values[offset..offset + length].to_vec();
                    result.push(Value::Array(array_elements));
                    offset += length;
                }

                // Ensure we consumed exactly all the element values
                if offset != element_values.len() {
                    return Err(ConversionError::Striped(
                        StripedError::VectorOperationFailed(
                            "Array element length mismatch".to_string(),
                        ),
                    ));
                }

                Ok(result)
            }
            Column::Struct { fields, .. } => {
                debug_assert!(
                    !fields.is_empty(),
                    "Struct columns must have at least one field"
                );

                let row_count = fields[0].column.row_count();
                let mut result = Vec::new();

                for row_idx in 0..row_count {
                    let mut struct_fields = Vec::new();

                    for field_column in fields {
                        let field_values = field_column.column.to_values()?;
                        if row_idx >= field_values.len() {
                            return Err(ConversionError::Striped(
                                StripedError::VectorOperationFailed(
                                    "Struct field row count mismatch".to_string(),
                                ),
                            ));
                        }

                        struct_fields.push(Field {
                            name: field_column.name.clone(),
                            value: field_values[row_idx].clone(),
                        });
                    }

                    result.push(Value::Struct(struct_fields));
                }

                Ok(result)
            }
            Column::Enum { tags, variants, .. } => {
                // Get values for all variant columns
                let mut variant_values = Vec::new();
                for variant in variants {
                    variant_values.push(variant.column.to_values()?);
                }

                // Transpose the variant columns (each row becomes a vector of values, one per variant)
                let row_count = tags.len();
                let mut transposed_values = Vec::new();
                for row_idx in 0..row_count {
                    let mut row_values = Vec::new();
                    for (variant_idx, _variant) in variants.iter().enumerate() {
                        let values = &variant_values[variant_idx];
                        if row_idx < values.len() {
                            row_values.push(values[row_idx].clone());
                        } else {
                            // Use default value for this variant if no value exists
                            row_values.push(Value::Unit); // TODO: use proper default
                        }
                    }
                    transposed_values.push(row_values);
                }

                let mut result = Vec::new();
                for (row_idx, &tag) in tags.iter().enumerate() {
                    if let Some(variant_idx) = variants.iter().position(|v| v.tag == tag) {
                        let row_values = &transposed_values[row_idx];
                        result.push(Value::Enum {
                            tag,
                            value: Box::new(row_values[variant_idx].clone()),
                        });
                    } else {
                        return Err(ConversionError::Schema(
                            crate::error::SchemaError::UnsupportedType(format!("enum tag {}", tag)),
                        ));
                    }
                }

                Ok(result)
            }
            Column::Nested { lengths, table, .. } => {
                let table_logical = table.to_logical()?;
                let mut result = Vec::new();

                // Split the table according to the lengths to reconstruct individual nested tables
                let mut offset = 0;
                for &length in lengths {
                    let nested_table = match &table_logical {
                        LogicalTable::Binary(data) => {
                            let end_offset = offset + length;
                            if end_offset > data.len() {
                                return Err(ConversionError::Striped(
                                    StripedError::VectorOperationFailed(
                                        "Binary nested table length mismatch".to_string(),
                                    ),
                                ));
                            }
                            let slice = data[offset..end_offset].to_vec();
                            offset = end_offset;
                            LogicalTable::Binary(slice)
                        }
                        LogicalTable::Array(elements) => {
                            let end_offset = offset + length;
                            if end_offset > elements.len() {
                                return Err(ConversionError::Striped(
                                    StripedError::VectorOperationFailed(
                                        "Array nested table length mismatch".to_string(),
                                    ),
                                ));
                            }
                            let slice = elements[offset..end_offset].to_vec();
                            offset = end_offset;
                            LogicalTable::Array(slice)
                        }
                        LogicalTable::Map(pairs) => {
                            let end_offset = offset + length;
                            if end_offset > pairs.len() {
                                return Err(ConversionError::Striped(
                                    StripedError::VectorOperationFailed(
                                        "Map nested table length mismatch".to_string(),
                                    ),
                                ));
                            }
                            let slice = pairs[offset..end_offset].to_vec();
                            offset = end_offset;
                            LogicalTable::Map(slice)
                        }
                    };
                    result.push(Value::Nested(Box::new(nested_table)));
                }

                Ok(result)
            }
            Column::Reversed { inner } => {
                let inner_values = inner.to_values()?;
                Ok(inner_values
                    .into_iter()
                    .map(|v| Value::Reversed(Box::new(v)))
                    .collect())
            }
        }
    }

    /// Get the number of rows in the column
    pub fn row_count(&self) -> usize {
        match self {
            Column::Unit { count } => *count,
            Column::Int { values, .. } => values.len(),
            Column::Double { values, .. } => values.len(),
            Column::Binary { lengths, .. } => lengths.len(),
            Column::Array { lengths, .. } => lengths.len(),
            Column::Struct { fields, .. } => {
                debug_assert!(
                    !fields.is_empty(),
                    "Struct columns must have at least one field"
                );
                fields[0].column.row_count()
            }
            Column::Enum { tags, .. } => tags.len(),
            Column::Nested { lengths, .. } => lengths.len(),
            Column::Reversed { inner } => inner.row_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{BinaryEncoding, IntEncoding};
    use crate::logical::ValueSchema;

    #[test]
    fn test_int_column_conversion() {
        let schema = ValueSchema::Int {
            default: Default::Allow,
            encoding: Encoding::Int(IntEncoding::Int),
        };

        let values = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let column = Column::from_values(&schema, &values).unwrap();

        match &column {
            Column::Int {
                values: int_values, ..
            } => {
                assert_eq!(int_values, &vec![1, 2, 3]);
            }
            _ => panic!("Expected Int column"),
        }

        let reconstructed = column.to_values().unwrap();
        assert_eq!(reconstructed, values);
    }

    #[test]
    fn test_binary_column_conversion() {
        let schema = ValueSchema::Binary {
            default: Default::Allow,
            encoding: Encoding::Binary(BinaryEncoding::Binary),
        };

        let values = vec![
            Value::Binary(vec![1, 2, 3]),
            Value::Binary(vec![4, 5]),
            Value::Binary(vec![6]),
        ];
        let column = Column::from_values(&schema, &values).unwrap();

        match &column {
            Column::Binary { lengths, data, .. } => {
                assert_eq!(lengths, &vec![3, 2, 1]);
                assert_eq!(data, &vec![1, 2, 3, 4, 5, 6]);
            }
            _ => panic!("Expected Binary column"),
        }

        let reconstructed = column.to_values().unwrap();
        assert_eq!(reconstructed, values);
    }

    #[test]
    fn test_array_column_conversion() {
        let schema = ValueSchema::Array {
            default: Default::Allow,
            element: Box::new(ValueSchema::Int {
                default: Default::Allow,
                encoding: Encoding::Int(IntEncoding::Int),
            }),
        };

        let values = vec![
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Array(vec![Value::Int(3)]),
            Value::Array(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
        ];
        let column = Column::from_values(&schema, &values).unwrap();

        match &column {
            Column::Array {
                lengths, element, ..
            } => {
                assert_eq!(lengths, &vec![2, 1, 3]);
                match element.as_ref() {
                    Column::Int {
                        values: int_values, ..
                    } => {
                        assert_eq!(int_values, &vec![1, 2, 3, 4, 5, 6]);
                    }
                    _ => panic!("Expected Int element column"),
                }
            }
            _ => panic!("Expected Array column"),
        }

        let reconstructed = column.to_values().unwrap();
        assert_eq!(reconstructed, values);
    }
}
