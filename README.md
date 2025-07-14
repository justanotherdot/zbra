# Zbra

**Zbra is a columnar binary format for immutable time-series datasets** with three key architectural layers:

## Core Concepts

### 1. **Three-Layer Architecture**
- **Logical**: Human-readable types (JSON-like)
- **Striped**: In-memory columnar format 
- **Binary**: Compressed disk format

### 2. **Advanced Type System**
- Full **sum types** (Rust-like enums), not just optionals
- **Nested structures** with schema evolution
- **Default value handling** for missing columns

### 3. **Sophisticated In-Memory Columnar Format**
The "Striped" layer provides Arrow-like in-memory columnar representation:
```
Table = Binary | Array Column | Map Column Column
Column = Unit | Int Vector | Double Vector | Enum Tags+Variants | Struct Fields | Nested | Reversed
```
- Vectorized storage with zero-copy operations
- Struct-of-Arrays decomposition for complex types
- Tag arrays for efficient sum type representation

### 4. **Custom Compression Stack**
- Frame-of-reference encoding
- Zig-zag encoding for signed integers  
- Bit packing for small values
- Snappy for strings with column locality

### 5. **Time-Series Domain Model**
- EntityId/AttributeId/Time/FactsetId structure
- Optimized for entity-attribute-time queries
- Tombstone deletion markers

**Key Innovation**: Unlike Parquet/Arrow, Zbra supports full algebraic data types (sum types) while maintaining columnar storage efficiency through tag arrays and variant columns.

The core idea is **type-safe, high-compression columnar storage** for immutable analytical datasets with streaming processing capabilities.

## Future Enhancements

- **Arrow Compatibility**: Potential interoperability with Apache Arrow's in-memory format for broader ecosystem integration