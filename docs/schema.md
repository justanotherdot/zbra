# Zbra Schema System

## Table of Contents
- [Schema Overview](#schema-overview)
- [Schema Types](#schema-types)
- [Value Types](#value-types)
- [Encoding System](#encoding-system)
- [Default Values](#default-values)
- [Schema Evolution](#schema-evolution)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Schema Overview

Zbra uses a hierarchical schema system that describes the structure and encoding of columnar data. The schema is embedded in the binary file header and serialized as JSON, providing both human readability and natural extensibility.

### Schema Architecture

```
TableSchema (root)
├── Binary tables (raw byte data)
├── Array tables (homogeneous collections)
└── Map tables (key-value structures)
    └── ValueSchema (describes array elements/map values)
        ├── Primitive types (Int, Double, Binary)
        ├── Complex types (Struct, Enum, Array, Map)
        └── Nested tables
```

### Core Principles

1. **Type safety** - All data types are explicitly declared
2. **Encoding flexibility** - Multiple encodings per logical type
3. **Null handling** - Explicit default value policies
4. **Compression awareness** - Schema informs compression decisions
5. **Evolution friendly** - JSON-based schema supports safe extensions

## Schema Types

### TableSchema

The root schema type that describes the overall table structure:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableSchema {
    /// Raw binary data with encoding hints
    Binary {
        default: Default,
        encoding: Encoding,
    },
    /// Homogeneous array of values
    Array {
        default: Default,
        element: Box<ValueSchema>,
    },
    /// Key-value map structure
    Map {
        default: Default,
        key: Box<ValueSchema>,
        value: Box<ValueSchema>,
    },
}
```

**Usage patterns:**
- **Binary**: Log files, image data, serialized objects
- **Array**: Time series, lists, vectors
- **Map**: Configuration, metadata, sparse data

### ValueSchema

Describes the schema for individual values within arrays or maps:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueSchema {
    /// 64-bit signed integer
    Int {
        default: Default,
        encoding: Encoding,
    },
    /// 64-bit floating point
    Double {
        default: Default,
    },
    /// Variable-length binary data
    Binary {
        default: Default,
        encoding: Encoding,
    },
    /// Fixed-schema structure
    Struct {
        default: Default,
        fields: Vec<FieldSchema>,
    },
    /// Tagged union type
    Enum {
        default: Default,
        variants: Vec<VariantSchema>,
    },
    /// Nested homogeneous array
    Array {
        default: Default,
        element: Box<ValueSchema>,
    },
    /// Nested key-value map
    Map {
        default: Default,
        key: Box<ValueSchema>,
        value: Box<ValueSchema>,
    },
    /// Nested table reference
    Nested {
        default: Default,
        table: Box<TableSchema>,
    },
}
```

## Value Types

### Primitive Types

#### Int
64-bit signed integers with multiple encoding options:

```rust
ValueSchema::Int {
    default: Default::Allow,
    encoding: Encoding::Int(IntEncoding::Int),
}
```

**Supported encodings:**
- `IntEncoding::Int` - Standard 64-bit signed integer
- `IntEncoding::Date` - Days since epoch
- `IntEncoding::TimeSeconds` - Unix timestamp (seconds)
- `IntEncoding::TimeMilliseconds` - Unix timestamp (milliseconds)
- `IntEncoding::TimeMicroseconds` - Unix timestamp (microseconds)

#### Double
64-bit IEEE 754 floating point numbers:

```rust
ValueSchema::Double {
    default: Default::Allow,
}
```

#### Binary
Variable-length byte arrays with encoding hints:

```rust
ValueSchema::Binary {
    default: Default::Allow,
    encoding: Encoding::Binary(BinaryEncoding::Utf8),
}
```

**Supported encodings:**
- `BinaryEncoding::Binary` - Raw byte data
- `BinaryEncoding::Utf8` - UTF-8 encoded text

### Complex Types

#### Struct
Fixed-schema records with named fields:

```rust
ValueSchema::Struct {
    default: Default::Allow,
    fields: vec![
        FieldSchema {
            name: "id".to_string(),
            schema: ValueSchema::Int { /* ... */ },
        },
        FieldSchema {
            name: "name".to_string(),
            schema: ValueSchema::Binary { /* ... */ },
        },
    ],
}
```

#### Enum
Tagged unions with named variants:

```rust
ValueSchema::Enum {
    default: Default::Allow,
    variants: vec![
        VariantSchema {
            name: "Success".to_string(),
            tag: 0,
            schema: ValueSchema::Int { /* ... */ },
        },
        VariantSchema {
            name: "Error".to_string(),
            tag: 1,
            schema: ValueSchema::Binary { /* ... */ },
        },
    ],
}
```

#### Array
Homogeneous collections with element schema:

```rust
ValueSchema::Array {
    default: Default::Allow,
    element: Box::new(ValueSchema::Int { /* ... */ }),
}
```

#### Map
Key-value collections with typed keys and values:

```rust
ValueSchema::Map {
    default: Default::Allow,
    key: Box::new(ValueSchema::Binary { /* ... */ }),
    value: Box::new(ValueSchema::Int { /* ... */ }),
}
```

## Encoding System

### Integer Encodings

**Standard Integer:**
```rust
Encoding::Int(IntEncoding::Int)
```
- Raw 64-bit signed integer
- Full range: -2^63 to 2^63-1
- Compressed using frame-of-reference + zig-zag + BP64

**Date Encoding:**
```rust
Encoding::Int(IntEncoding::Date)
```
- Days since Unix epoch (1970-01-01)
- Range: ~584 million years
- Optimized for date arithmetic

**Time Encodings:**
```rust
Encoding::Int(IntEncoding::TimeSeconds)      // Unix timestamp (seconds)
Encoding::Int(IntEncoding::TimeMilliseconds) // Unix timestamp (milliseconds)  
Encoding::Int(IntEncoding::TimeMicroseconds) // Unix timestamp (microseconds)
```
- Different precision levels for time data
- Compression benefits from temporal locality

### Binary Encodings

**Raw Binary:**
```rust
Encoding::Binary(BinaryEncoding::Binary)
```
- Arbitrary byte sequences
- No character encoding assumptions
- Compressed using Zstd

**UTF-8 Text:**
```rust
Encoding::Binary(BinaryEncoding::Utf8)
```
- Valid UTF-8 encoded text
- Enables text-aware optimizations
- Dictionary compression for repeated strings

## Default Values

The `Default` enum controls how missing/null values are handled:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Default {
    /// Allow null/missing values
    Allow,
    /// Reject null/missing values (fail on encounter)
    Deny,
}
```

**Default::Allow:**
- Permits null values in the data
- Null-friendly compression (sparse representations)
- Optional fields in structs

**Default::Deny:**
- Guarantees non-null values
- Denser compression (no null tracking)
- Required fields in structs

## Schema Evolution

Schema evolution allows formats to change over time while maintaining backward and forward compatibility. Zbra uses JSON serialization with serde to provide natural schema evolution capabilities.

### Evolution Principles

**Forward Compatibility:** Old readers can process new files
**Backward Compatibility:** New readers can process old files
**Graceful Degradation:** Unknown fields are ignored, missing fields use defaults

### Safe Evolution Patterns

#### 1. Adding Optional Fields

**Before:**
```rust
#[derive(Serialize, Deserialize)]
pub struct CompressionConfig {
    pub binary_data: CompressionAlgorithm,
    pub strings: CompressionAlgorithm,
}
```

**After:**
```rust
#[derive(Serialize, Deserialize)]
pub struct CompressionConfig {
    pub binary_data: CompressionAlgorithm,
    pub strings: CompressionAlgorithm,
    
    // Safe: old readers ignore this field
    #[serde(default)]
    pub per_column_config: Option<HashMap<String, CompressionAlgorithm>>,
}
```

#### 2. Adding New Enum Variants

**Before:**
```rust
#[derive(Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Zstd { level: i32 },
}
```

**After:**
```rust
#[derive(Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    None,
    Zstd { level: i32 },
    
    // Safe: old readers will get deserialization error and can handle gracefully
    Lz4,
    Snappy,
}
```

#### 3. Adding New Encoding Types

**Before:**
```rust
#[derive(Serialize, Deserialize)]
pub enum IntEncoding {
    Int,
    Date,
    TimeSeconds,
}
```

**After:**
```rust
#[derive(Serialize, Deserialize)]
pub enum IntEncoding {
    Int,
    Date,
    TimeSeconds,
    
    // Safe: represents new time precision
    TimeNanoseconds,
}
```

#### 4. Extending Schema Structures

**Before:**
```rust
#[derive(Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub schema: ValueSchema,
}
```

**After:**
```rust
#[derive(Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub schema: ValueSchema,
    
    // Safe: old readers ignore metadata
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,
    
    #[serde(default)]
    pub nullable: bool,
}
```

### Breaking Changes (Require Version Bump)

These changes require incrementing the format version in the magic number:

1. **Removing fields** - Old readers expect them
2. **Changing field types** - Breaks deserialization
3. **Changing required to optional** - Changes semantics
4. **Reordering enum variants** - Changes tag values
5. **Renaming fields** - Breaks field lookup

### Evolution Strategy

**Version 1 (Current):** `||_ZBRA||00001||`
- JSON schema with serde defaults
- Compression config in header
- Basic type system

**Version 2 (Future):** `||_ZBRA||00002||`
- Could add unified header format
- Enhanced compression algorithms
- Advanced type system features

**Version Detection:**
```rust
const MAGIC_NUMBER: &[u8; 16] = b"||_ZBRA||00001||";

fn detect_version(magic: &[u8]) -> Result<u32> {
    if magic[0..8] != b"||_ZBRA||"[..] {
        return Err(InvalidMagicNumber);
    }
    
    let version_bytes = &magic[8..13];
    let version_str = std::str::from_utf8(version_bytes)?;
    Ok(version_str.parse()?)
}
```

### Migration Strategy

**For compatible changes:**
1. Add fields with `#[serde(default)]`
2. Test with old files
3. Document compatibility notes

**For breaking changes:**
1. Increment version number
2. Implement version-specific readers
3. Provide migration tools

## Best Practices

### Schema Design

**1. Be explicit about encodings:**
```rust
// Good: explicit encoding
ValueSchema::Int {
    default: Default::Allow,
    encoding: Encoding::Int(IntEncoding::TimeSeconds),
}

// Avoid: assuming default encoding
```

**2. Use appropriate default policies:**
```rust
// For required fields
ValueSchema::Int {
    default: Default::Deny,  // Fail on null
    encoding: Encoding::Int(IntEncoding::Int),
}

// For optional fields  
ValueSchema::Int {
    default: Default::Allow,  // Accept null
    encoding: Encoding::Int(IntEncoding::Int),
}
```

**3. Choose efficient nesting:**
```rust
// Good: flat structure
ValueSchema::Struct {
    default: Default::Allow,
    fields: vec![
        FieldSchema { name: "timestamp".to_string(), schema: time_schema },
        FieldSchema { name: "value".to_string(), schema: value_schema },
    ],
}

// Avoid: excessive nesting
ValueSchema::Struct {
    fields: vec![
        FieldSchema { 
            name: "data".to_string(), 
            schema: ValueSchema::Struct { /* deeply nested */ }
        }
    ],
}
```

### Evolution Guidelines

**1. Always use `#[serde(default)]` for new fields:**
```rust
#[derive(Serialize, Deserialize)]
pub struct MySchema {
    pub existing_field: String,
    
    #[serde(default)]
    pub new_field: Option<String>,
}
```

**2. Document compatibility in code:**
```rust
/// CompressionConfig defines compression settings for zbra files.
/// 
/// Evolution notes:
/// - v1: Added basic binary_data and strings compression
/// - v2: Added per_column_config (optional, backward compatible)
#[derive(Serialize, Deserialize)]
pub struct CompressionConfig {
    pub binary_data: CompressionAlgorithm,
    pub strings: CompressionAlgorithm,
    
    /// Per-column compression overrides (added in v2)
    #[serde(default)]
    pub per_column_config: Option<HashMap<String, CompressionAlgorithm>>,
}
```

**3. Test evolution scenarios:**
```rust
#[test]
fn test_schema_evolution() {
    // Test old schema can read new files
    let old_config = CompressionConfig {
        binary_data: CompressionAlgorithm::Zstd { level: 3 },
        strings: CompressionAlgorithm::Zstd { level: 3 },
    };
    
    let json = serde_json::to_string(&old_config).unwrap();
    
    // New code should read old JSON with defaults
    let new_config: CompressionConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(new_config.per_column_config, None);
}
```

## Examples

### Time Series Data

```rust
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
                name: "value".to_string(),
                schema: ValueSchema::Double {
                    default: Default::Allow,
                },
            },
            FieldSchema {
                name: "quality".to_string(),
                schema: ValueSchema::Enum {
                    default: Default::Allow,
                    variants: vec![
                        VariantSchema {
                            name: "Good".to_string(),
                            tag: 0,
                            schema: ValueSchema::Int {
                                default: Default::Allow,
                                encoding: Encoding::Int(IntEncoding::Int),
                            },
                        },
                        VariantSchema {
                            name: "Bad".to_string(),
                            tag: 1,
                            schema: ValueSchema::Binary {
                                default: Default::Allow,
                                encoding: Encoding::Binary(BinaryEncoding::Utf8),
                            },
                        },
                    ],
                },
            },
        ],
    }),
};
```

### Configuration Data

```rust
let schema = TableSchema::Map {
    default: Default::Allow,
    key: Box::new(ValueSchema::Binary {
        default: Default::Deny,
        encoding: Encoding::Binary(BinaryEncoding::Utf8),
    }),
    value: Box::new(ValueSchema::Enum {
        default: Default::Allow,
        variants: vec![
            VariantSchema {
                name: "String".to_string(),
                tag: 0,
                schema: ValueSchema::Binary {
                    default: Default::Allow,
                    encoding: Encoding::Binary(BinaryEncoding::Utf8),
                },
            },
            VariantSchema {
                name: "Integer".to_string(),
                tag: 1,
                schema: ValueSchema::Int {
                    default: Default::Allow,
                    encoding: Encoding::Int(IntEncoding::Int),
                },
            },
            VariantSchema {
                name: "Array".to_string(),
                tag: 2,
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
```

### Log Data

```rust
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
                schema: ValueSchema::Enum {
                    default: Default::Deny,
                    variants: vec![
                        VariantSchema {
                            name: "DEBUG".to_string(),
                            tag: 0,
                            schema: ValueSchema::Int {
                                default: Default::Allow,
                                encoding: Encoding::Int(IntEncoding::Int),
                            },
                        },
                        VariantSchema {
                            name: "INFO".to_string(),
                            tag: 1,
                            schema: ValueSchema::Int {
                                default: Default::Allow,
                                encoding: Encoding::Int(IntEncoding::Int),
                            },
                        },
                        VariantSchema {
                            name: "WARN".to_string(),
                            tag: 2,
                            schema: ValueSchema::Int {
                                default: Default::Allow,
                                encoding: Encoding::Int(IntEncoding::Int),
                            },
                        },
                        VariantSchema {
                            name: "ERROR".to_string(),
                            tag: 3,
                            schema: ValueSchema::Int {
                                default: Default::Allow,
                                encoding: Encoding::Int(IntEncoding::Int),
                            },
                        },
                    ],
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
                name: "metadata".to_string(),
                schema: ValueSchema::Map {
                    default: Default::Allow,
                    key: Box::new(ValueSchema::Binary {
                        default: Default::Deny,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    }),
                    value: Box::new(ValueSchema::Binary {
                        default: Default::Allow,
                        encoding: Encoding::Binary(BinaryEncoding::Utf8),
                    }),
                },
            },
        ],
    }),
};
```

## Performance Considerations

**Schema Impact on Compression:**
- Integer encodings affect compression ratios
- Default policies influence null handling overhead
- Struct vs. nested tables affect memory layout
- Enum tag distribution affects bit-packing efficiency

**Schema Complexity:**
- Deeply nested structures increase parsing overhead
- Wide structs with many fields impact memory usage
- Complex enum variants affect dispatch performance
- Map schemas have higher key lookup costs

**Evolution Performance:**
- Adding optional fields has minimal overhead
- New enum variants require deserialization updates
- Schema changes affecting hot paths need careful benchmarking
- Version detection adds small parsing cost