// Binary layer - compressed disk/wire format

use crate::data::{BinaryEncoding, Default, Encoding, IntEncoding};
use crate::error::{BinaryError, Result};
use crate::logical::TableSchema;
use crate::striped::{Column, FieldColumn, Table, VariantColumn};
use std::io::{Read, Write};

/// Binary format constants
///
/// Magic number format: "||_ZBRA||vvvvv||" where vvvvv is the 5-digit version
/// - Version 1: "||_ZBRA||00001||" (current)
///
/// The version is embedded directly in the magic number, so no separate version
/// field is needed.
const MAGIC_NUMBER: &[u8; 16] = b"||_ZBRA||00001||";

/// Binary format header
#[derive(Debug, Clone)]
pub struct Header {
    pub schema: TableSchema,
}

/// Binary format file structure
///
/// File layout:
/// ```text
/// [Magic Number: 16 bytes] "||_ZBRA||00001||"
/// [Schema Size: 4 bytes] uncompressed_size (little-endian u32)
/// [Schema Size: 4 bytes] compressed_size (little-endian u32)  
/// [Schema Data: N bytes] JSON-encoded TableSchema (compressed with Snappy in future)
/// [Block Count: 4 bytes] number of blocks (little-endian u32)
/// [Block 0: Variable] row_count + striped table data
/// [Block 1: Variable] ...
/// ```
#[derive(Debug, Clone)]
pub struct BinaryFile {
    pub header: Header,
    pub blocks: Vec<Block>,
}

/// Binary data block
#[derive(Debug, Clone)]
pub struct Block {
    pub row_count: u32,
    pub table: Table,
}

impl BinaryFile {
    /// Create a new binary file from a schema and striped table
    pub fn new(schema: TableSchema, table: Table) -> Self {
        let header = Header { schema };
        let row_count = table.row_count() as u32;
        let blocks = vec![Block { row_count, table }];
        BinaryFile { header, blocks }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut writer = Vec::new();
        self.write_to(&mut writer)?;
        Ok(writer)
    }

    /// Write to a writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        // Write magic number
        writer.write_all(MAGIC_NUMBER)?;

        // Serialize schema to JSON
        let schema_json = serde_json::to_string(&self.header.schema)
            .map_err(|e| BinaryError::SerializationError(e.to_string()))?;
        let schema_bytes = schema_json.as_bytes();

        // Write schema as sized byte array
        write_sized_byte_array(writer, schema_bytes)?;

        // Write blocks
        write_u32(writer, self.blocks.len() as u32)?;
        for block in &self.blocks {
            block.write_to(writer)?;
        }

        Ok(())
    }

    /// Read from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut reader = std::io::Cursor::new(data);
        Self::read_from(&mut reader)
    }

    /// Read from a reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // Check magic number
        let mut magic = [0u8; 16];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC_NUMBER {
            return Err(BinaryError::InvalidMagicNumber);
        }

        // Read schema
        let schema_bytes = read_sized_byte_array(reader)?;
        let schema_json = String::from_utf8(schema_bytes)
            .map_err(|e| BinaryError::DeserializationError(e.to_string()))?;
        let schema: TableSchema = serde_json::from_str(&schema_json)
            .map_err(|e| BinaryError::DeserializationError(e.to_string()))?;

        let header = Header { schema };

        // Read blocks
        let block_count = read_u32(reader)?;
        let mut blocks = Vec::with_capacity(block_count as usize);
        for _ in 0..block_count {
            blocks.push(Block::read_from(reader)?);
        }

        Ok(BinaryFile { header, blocks })
    }

    /// Get the table from the first block (for simple cases)
    pub fn table(&self) -> Option<&Table> {
        self.blocks.first().map(|block| &block.table)
    }
}

impl Block {
    /// Write block to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_u32(writer, self.row_count)?;
        self.table.write_to(writer)?;
        Ok(())
    }

    /// Read block from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let row_count = read_u32(reader)?;
        let table = Table::read_from(reader)?;
        Ok(Block { row_count, table })
    }
}

impl Table {
    /// Write table to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            Table::Binary {
                default,
                encoding,
                data,
            } => {
                write_u8(writer, 0)?; // Binary table tag
                default.write_to(writer)?;
                encoding.write_to(writer)?;
                write_sized_byte_array(writer, data)?;
            }
            Table::Array { default, column } => {
                write_u8(writer, 1)?; // Array table tag
                default.write_to(writer)?;
                column.write_to(writer)?;
            }
            Table::Map {
                default,
                key_column,
                value_column,
            } => {
                write_u8(writer, 2)?; // Map table tag
                default.write_to(writer)?;
                key_column.write_to(writer)?;
                value_column.write_to(writer)?;
            }
        }
        Ok(())
    }

    /// Read table from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let tag = read_u8(reader)?;
        match tag {
            0 => {
                let default = Default::read_from(reader)?;
                let encoding = Encoding::read_from(reader)?;
                let data = read_sized_byte_array(reader)?;
                Ok(Table::Binary {
                    default,
                    encoding,
                    data,
                })
            }
            1 => {
                let default = Default::read_from(reader)?;
                let column = Box::new(Column::read_from(reader)?);
                Ok(Table::Array { default, column })
            }
            2 => {
                let default = Default::read_from(reader)?;
                let key_column = Box::new(Column::read_from(reader)?);
                let value_column = Box::new(Column::read_from(reader)?);
                Ok(Table::Map {
                    default,
                    key_column,
                    value_column,
                })
            }
            _ => Err(BinaryError::InvalidTableTag(tag)),
        }
    }
}

impl Column {
    /// Write column to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            Column::Unit { count } => {
                write_u8(writer, 0)?; // Unit column tag
                write_u32(writer, *count as u32)?;
            }
            Column::Int {
                default,
                encoding,
                values,
            } => {
                write_u8(writer, 1)?; // Int column tag
                default.write_to(writer)?;
                encoding.write_to(writer)?;
                write_int_array(writer, values)?;
            }
            Column::Double { default, values } => {
                write_u8(writer, 2)?; // Double column tag
                default.write_to(writer)?;
                // Convert f64 to i64 bits for compression
                let int_values: Vec<i64> = values.iter().map(|f| f.to_bits() as i64).collect();
                write_int_array(writer, &int_values)?;
            }
            Column::Binary {
                default,
                encoding,
                lengths,
                data,
            } => {
                write_u8(writer, 3)?; // Binary column tag
                default.write_to(writer)?;
                encoding.write_to(writer)?;
                write_int_array_usize(writer, lengths)?;
                write_sized_byte_array(writer, data)?;
            }
            Column::Array {
                default,
                lengths,
                element,
            } => {
                write_u8(writer, 4)?; // Array column tag
                default.write_to(writer)?;
                write_int_array_usize(writer, lengths)?;
                element.write_to(writer)?;
            }
            Column::Struct { default, fields } => {
                write_u8(writer, 5)?; // Struct column tag
                default.write_to(writer)?;
                write_u32(writer, fields.len() as u32)?;
                for field in fields {
                    field.write_to(writer)?;
                }
            }
            Column::Enum {
                default,
                tags,
                variants,
            } => {
                write_u8(writer, 6)?; // Enum column tag
                default.write_to(writer)?;
                write_u32_array(writer, tags)?;
                write_u32(writer, variants.len() as u32)?;
                for variant in variants {
                    variant.write_to(writer)?;
                }
            }
            Column::Nested { lengths, table } => {
                write_u8(writer, 7)?; // Nested column tag
                write_int_array_usize(writer, lengths)?;
                table.write_to(writer)?;
            }
            Column::Reversed { inner } => {
                write_u8(writer, 8)?; // Reversed column tag
                inner.write_to(writer)?;
            }
        }
        Ok(())
    }

    /// Read column from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let tag = read_u8(reader)?;
        match tag {
            0 => {
                let count = read_u32(reader)? as usize;
                Ok(Column::Unit { count })
            }
            1 => {
                let default = Default::read_from(reader)?;
                let encoding = Encoding::read_from(reader)?;
                let values = read_int_array(reader)?;
                Ok(Column::Int {
                    default,
                    encoding,
                    values,
                })
            }
            2 => {
                let default = Default::read_from(reader)?;
                let int_values = read_int_array(reader)?;
                let values: Vec<f64> = int_values
                    .iter()
                    .map(|i| f64::from_bits(*i as u64))
                    .collect();
                Ok(Column::Double { default, values })
            }
            3 => {
                let default = Default::read_from(reader)?;
                let encoding = Encoding::read_from(reader)?;
                let lengths = read_int_array_usize(reader)?;
                let data = read_sized_byte_array(reader)?;
                Ok(Column::Binary {
                    default,
                    encoding,
                    lengths,
                    data,
                })
            }
            4 => {
                let default = Default::read_from(reader)?;
                let lengths = read_int_array_usize(reader)?;
                let element = Box::new(Column::read_from(reader)?);
                Ok(Column::Array {
                    default,
                    lengths,
                    element,
                })
            }
            5 => {
                let default = Default::read_from(reader)?;
                let field_count = read_u32(reader)? as usize;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    fields.push(FieldColumn::read_from(reader)?);
                }
                Ok(Column::Struct { default, fields })
            }
            6 => {
                let default = Default::read_from(reader)?;
                let tags = read_u32_array(reader)?;
                let variant_count = read_u32(reader)? as usize;
                let mut variants = Vec::with_capacity(variant_count);
                for _ in 0..variant_count {
                    variants.push(VariantColumn::read_from(reader)?);
                }
                Ok(Column::Enum {
                    default,
                    tags,
                    variants,
                })
            }
            7 => {
                let lengths = read_int_array_usize(reader)?;
                let table = Box::new(Table::read_from(reader)?);
                Ok(Column::Nested { lengths, table })
            }
            8 => {
                let inner = Box::new(Column::read_from(reader)?);
                Ok(Column::Reversed { inner })
            }
            _ => Err(BinaryError::InvalidColumnTag(tag)),
        }
    }
}

impl FieldColumn {
    /// Write field column to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_string(writer, &self.name)?;
        self.column.write_to(writer)?;
        Ok(())
    }

    /// Read field column from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let name = read_string(reader)?;
        let column = Column::read_from(reader)?;
        Ok(FieldColumn { name, column })
    }
}

impl VariantColumn {
    /// Write variant column to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        write_string(writer, &self.name)?;
        write_u32(writer, self.tag)?;
        self.column.write_to(writer)?;
        Ok(())
    }

    /// Read variant column from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let name = read_string(reader)?;
        let tag = read_u32(reader)?;
        let column = Column::read_from(reader)?;
        Ok(VariantColumn { name, tag, column })
    }
}

impl Default {
    /// Write default to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            Default::Allow => write_u8(writer, 0),
            Default::Deny => write_u8(writer, 1),
        }
    }

    /// Read default from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        match read_u8(reader)? {
            0 => Ok(Default::Allow),
            1 => Ok(Default::Deny),
            tag => Err(BinaryError::InvalidDefaultTag(tag)),
        }
    }
}

impl Encoding {
    /// Write encoding to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            Encoding::Int(int_enc) => {
                write_u8(writer, 0)?;
                int_enc.write_to(writer)?;
            }
            Encoding::Binary(bin_enc) => {
                write_u8(writer, 1)?;
                bin_enc.write_to(writer)?;
            }
        }
        Ok(())
    }

    /// Read encoding from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        match read_u8(reader)? {
            0 => Ok(Encoding::Int(IntEncoding::read_from(reader)?)),
            1 => Ok(Encoding::Binary(BinaryEncoding::read_from(reader)?)),
            tag => Err(BinaryError::InvalidEncodingTag(tag)),
        }
    }
}

impl IntEncoding {
    /// Write int encoding to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            IntEncoding::Int => write_u8(writer, 0),
            IntEncoding::Date => write_u8(writer, 1),
            IntEncoding::TimeSeconds => write_u8(writer, 2),
            IntEncoding::TimeMilliseconds => write_u8(writer, 3),
            IntEncoding::TimeMicroseconds => write_u8(writer, 4),
        }
    }

    /// Read int encoding from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        match read_u8(reader)? {
            0 => Ok(IntEncoding::Int),
            1 => Ok(IntEncoding::Date),
            2 => Ok(IntEncoding::TimeSeconds),
            3 => Ok(IntEncoding::TimeMilliseconds),
            4 => Ok(IntEncoding::TimeMicroseconds),
            tag => Err(BinaryError::InvalidIntEncodingTag(tag)),
        }
    }
}

impl BinaryEncoding {
    /// Write binary encoding to writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        match self {
            BinaryEncoding::Binary => write_u8(writer, 0),
            BinaryEncoding::Utf8 => write_u8(writer, 1),
        }
    }

    /// Read binary encoding from reader
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        match read_u8(reader)? {
            0 => Ok(BinaryEncoding::Binary),
            1 => Ok(BinaryEncoding::Utf8),
            tag => Err(BinaryError::InvalidBinaryEncodingTag(tag)),
        }
    }
}

// Basic I/O primitives

fn write_u8<W: Write>(writer: &mut W, value: u8) -> Result<()> {
    writer.write_all(&[value])?;
    Ok(())
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<()> {
    let bytes = s.as_bytes();
    write_u32(writer, bytes.len() as u32)?;
    writer.write_all(bytes)?;
    Ok(())
}

fn read_string<R: Read>(reader: &mut R) -> Result<String> {
    let len = read_u32(reader)? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| BinaryError::DeserializationError(e.to_string()))
}

/// Write a sized byte array (future: will use Snappy compression)
///
/// Format:
/// - uncompressed_size: u32 (little-endian)
/// - compressed_size: u32 (little-endian)
/// - data: compressed_size bytes
///
/// Currently no compression is applied (compressed_size == uncompressed_size)
fn write_sized_byte_array<W: Write>(writer: &mut W, data: &[u8]) -> Result<()> {
    write_u32(writer, data.len() as u32)?; // uncompressed size
    write_u32(writer, data.len() as u32)?; // compressed size (same for now)
    writer.write_all(data)?;
    Ok(())
}

/// Read a sized byte array (future: will decompress with Snappy)
fn read_sized_byte_array<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let _uncompressed_size = read_u32(reader)?;
    let compressed_size = read_u32(reader)? as usize;
    let mut buf = vec![0u8; compressed_size];
    reader.read_exact(&mut buf)?;
    // For now, no decompression
    Ok(buf)
}

/// Write integer array (simplified - no compression yet)
///
/// TODO: Implement the full zbra compression pipeline:
/// 1. Frame-of-reference encoding (subtract midpoint)
/// 2. Zig-zag encoding (signed to unsigned)  
/// 3. BP64 bit-packing (64-element chunks)
///
/// Currently just writes length + raw values
fn write_int_array<W: Write>(writer: &mut W, values: &[i64]) -> Result<()> {
    write_u32(writer, values.len() as u32)?;
    for &value in values {
        writer.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}

/// Read integer array (simplified - no decompression yet)
fn read_int_array<R: Read>(reader: &mut R) -> Result<Vec<i64>> {
    let len = read_u32(reader)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        values.push(i64::from_le_bytes(buf));
    }
    Ok(values)
}

fn write_int_array_usize<W: Write>(writer: &mut W, values: &[usize]) -> Result<()> {
    let i64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
    write_int_array(writer, &i64_values)
}

fn read_int_array_usize<R: Read>(reader: &mut R) -> Result<Vec<usize>> {
    let i64_values = read_int_array(reader)?;
    Ok(i64_values.iter().map(|&v| v as usize).collect())
}

fn write_u32_array<W: Write>(writer: &mut W, values: &[u32]) -> Result<()> {
    write_u32(writer, values.len() as u32)?;
    for &value in values {
        write_u32(writer, value)?;
    }
    Ok(())
}

fn read_u32_array<R: Read>(reader: &mut R) -> Result<Vec<u32>> {
    let len = read_u32(reader)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_u32(reader)?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{BinaryEncoding, Default, Encoding, IntEncoding};
    use crate::logical::TableSchema;
    use crate::striped::{Column, Table};

    #[test]
    fn test_binary_roundtrip_simple() {
        // Create a simple integer array
        let schema = TableSchema::Array {
            default: Default::Allow,
            element: Box::new(crate::logical::ValueSchema::Int {
                default: Default::Allow,
                encoding: Encoding::Int(IntEncoding::Int),
            }),
        };

        let table = Table::Array {
            default: Default::Allow,
            column: Box::new(Column::Int {
                default: Default::Allow,
                encoding: Encoding::Int(IntEncoding::Int),
                values: vec![1, 2, 3, 4, 5],
            }),
        };

        // Create binary file
        let binary_file = BinaryFile::new(schema.clone(), table.clone());

        // Serialize to bytes
        let bytes = binary_file.to_bytes().expect("Failed to serialize");

        // Deserialize from bytes
        let deserialized = BinaryFile::from_bytes(&bytes).expect("Failed to deserialize");

        // Check that we got back what we put in
        assert_eq!(deserialized.header.schema, schema);
        assert_eq!(deserialized.blocks.len(), 1);
        assert_eq!(deserialized.blocks[0].table, table);
    }

    #[test]
    fn test_binary_roundtrip_struct() {
        use crate::logical::{FieldSchema, ValueSchema};

        // Create a struct array
        let schema = TableSchema::Array {
            default: Default::Allow,
            element: Box::new(ValueSchema::Struct {
                default: Default::Allow,
                fields: vec![
                    FieldSchema {
                        name: "id".to_string(),
                        schema: ValueSchema::Int {
                            default: Default::Allow,
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
            }),
        };

        let table = Table::Array {
            default: Default::Allow,
            column: Box::new(Column::Struct {
                default: Default::Allow,
                fields: vec![
                    crate::striped::FieldColumn {
                        name: "id".to_string(),
                        column: Column::Int {
                            default: Default::Allow,
                            encoding: Encoding::Int(IntEncoding::Int),
                            values: vec![1, 2, 3],
                        },
                    },
                    crate::striped::FieldColumn {
                        name: "name".to_string(),
                        column: Column::Binary {
                            default: Default::Allow,
                            encoding: Encoding::Binary(BinaryEncoding::Utf8),
                            lengths: vec![5, 3, 7],
                            data: b"AliceBobCharlie".to_vec(),
                        },
                    },
                ],
            }),
        };

        // Create binary file
        let binary_file = BinaryFile::new(schema.clone(), table.clone());

        // Serialize and deserialize
        let bytes = binary_file.to_bytes().expect("Failed to serialize");
        let deserialized = BinaryFile::from_bytes(&bytes).expect("Failed to deserialize");

        // Verify roundtrip
        assert_eq!(deserialized.header.schema, schema);
        assert_eq!(deserialized.blocks[0].table, table);
    }
}
