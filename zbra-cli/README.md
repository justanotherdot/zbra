# zbra CLI

Command-line tool for working with zbra columnar data format.

## Installation

```bash
cargo build --release --bin zbra-cli
# Binary will be at target/release/zbra-cli
```

## Quick Start

```bash
# Create example data files
zbra example

# Show information about the data
zbra info people.json

# Convert to striped (columnar) format
zbra convert --input people.json --output people.striped --to striped

# Convert to binary format
zbra convert --input people.json --output people.zbra --to binary

# Validate data against schema
zbra validate --data people.json --schema schema.json
```

## Commands

### `zbra example`

Creates example data files for testing and learning.

```bash
zbra example --output examples/
```

Creates:
- `people.json` - Person records with id, name, age
- `numbers.json` - Simple integer array

### `zbra info <file>`

Shows information about a data file including schema validation and row counts.

```bash
zbra info people.json
zbra info people.zbra  # For binary files
```

Output:
```
File info for: people.json
Format: JSON
Schema type: array
Schema default: allow
Schema validation: PASS
Row count: 3
Striped row count: 3
```

### `zbra convert`

Converts data between formats.

```bash
zbra convert --input people.json --output people.striped --to striped
zbra convert --input people.json --output people.zbra --to binary
zbra convert --input people.zbra --output output.json --from binary --to json
```

Supported formats:
- `json` - Human-readable JSON format (input)
- `logical` - zbra logical format (intermediate)
- `striped` - zbra columnar format (JSON-serialized for debugging)
- `binary` - zbra binary format (.zbra files for efficient storage)

### `zbra validate`

Validates data against a schema file.

```bash
zbra validate --data people.json --schema person_schema.json
```

Output includes:
- Schema validation result
- Striped conversion test
- Roundtrip integrity test

## Format Hierarchy

Zbra uses a **four-layer architecture** with different formats for different purposes:

### 1. **JSON Format** (Human Input)
- **Purpose**: Human-authored data input and schema definitions
- **Usage**: Initial data creation, testing, debugging
- **Example**: `people.json`

### 2. **Logical Format** (Internal Representation)
- **Purpose**: Internal validation and type checking
- **Usage**: Intermediate processing, schema validation
- **Example**: Used internally during conversions

### 3. **Striped Format** (Debugging View)
- **Purpose**: JSON-serialized columnar view for inspection and debugging
- **Usage**: Understanding data layout, debugging conversions, development
- **Example**: `people.striped` - shows how data is organized in columns
- **NOTE**: This is JSON for human readability, not for production storage

### 4. **Binary Format** (Production Storage)
- **Purpose**: Efficient compressed disk/wire format
- **Usage**: Production storage, data exchange, performance-critical applications
- **Example**: `people.zbra` - compact binary files with magic number `||_ZBRA||00001||`

### Format Usage Guidelines

**For Development & Testing:**
```bash
# Create human-readable data
zbra example

# Inspect columnar structure
zbra convert --input people.json --output people.striped --to striped

# Debug conversions
zbra info people.striped
```

**For Production:**
```bash
# Create efficient binary files
zbra convert --input people.json --output people.zbra --to binary

# Read binary files
zbra info people.zbra
zbra convert --input people.zbra --output output.json --from binary --to json
```

## Data Format

The CLI expects JSON files with this structure:

```json
{
  "schema": {
    "type": "array",
    "default": "allow",
    "element": {
      "type": "struct",
      "default": "allow",
      "fields": [
        {
          "name": "id",
          "schema": {
            "type": "int",
            "default": "allow",
            "encoding": "int"
          }
        },
        {
          "name": "name",
          "schema": {
            "type": "binary",
            "default": "allow",
            "encoding": "utf8"
          }
        }
      ]
    }
  },
  "data": [
    {"struct": {"id": 1, "name": "Alice"}},
    {"struct": {"id": 2, "name": "Bob"}}
  ]
}
```

### Schema Types

**Table Schemas:**
- `array` - Array of values with `element` schema
- `binary` - Binary data with encoding
- `map` - Key-value pairs (not yet implemented)

**Value Schemas:**
- `unit` - Unit/null value
- `int` - 64-bit integers with encoding
- `double` - 64-bit floating point
- `binary` - Binary data with encoding
- `array` - Array of values
- `struct` - Structured record with named fields

**Encodings:**
- `int` - Plain integer
- `date` - Date as integer
- `time_seconds` - Time in seconds
- `time_milliseconds` - Time in milliseconds
- `time_microseconds` - Time in microseconds
- `binary` - Raw binary data
- `utf8` - UTF-8 encoded text

**Defaults:**
- `allow` - Allow default values
- `deny` - Require explicit values

## Examples

### Binary Format Workflow

```bash
# 1. Create example data
zbra example

# 2. Convert to binary format
zbra convert --input people.json --output people.zbra --to binary

# 3. Inspect binary file
zbra info people.zbra
# Output:
# Format: Binary (.zbra)
# Schema type: Array { default: Allow, element: Struct { ... } }
# Total rows: 3
# Block count: 1
# Block 0: 3 rows

# 4. Convert binary back to JSON
zbra convert --input people.zbra --output restored.json --from binary --to json

# 5. Verify roundtrip integrity
diff people.json restored.json
```

### Striped Format Inspection

```bash
# Convert to striped format for debugging
zbra convert --input people.json --output people.striped --to striped

# Inspect columnar structure
zbra info people.striped
# Shows how data is organized in columns - useful for debugging
```

### Person Records

```json
{
  "schema": {
    "type": "array",
    "default": "allow",
    "element": {
      "type": "struct",
      "default": "allow",
      "fields": [
        {
          "name": "id",
          "schema": {"type": "int", "default": "allow", "encoding": "int"}
        },
        {
          "name": "name",
          "schema": {"type": "binary", "default": "allow", "encoding": "utf8"}
        },
        {
          "name": "age",
          "schema": {"type": "int", "default": "allow", "encoding": "int"}
        }
      ]
    }
  },
  "data": [
    {"struct": {"id": 1, "name": "Alice", "age": 30}},
    {"struct": {"id": 2, "name": "Bob", "age": 25}},
    {"struct": {"id": 3, "name": "Charlie", "age": 35}}
  ]
}
```

### Simple Numbers

```json
{
  "schema": {
    "type": "array",
    "default": "allow",
    "element": {
      "type": "int",
      "default": "allow",
      "encoding": "int"
    }
  },
  "data": [1, 2, 3, 4, 5, 10, 20, 30]
}
```

## Implementation Status

**Current (working):**
- JSON schema definition and parsing
- Logical ↔ Striped ↔ Binary conversion
- Schema validation
- Basic value types (int, double, binary, array, struct)
- Example generation
- Binary file format (.zbra files)
- CLI support for all format conversions

**Future:**
- Advanced compression pipeline (frame-of-reference, zig-zag, bit-packing)
- Map tables
- Enum types
- Reversed types
- Nested tables
- Streaming operations

## Error Handling

The CLI provides detailed error messages for:
- Invalid JSON syntax
- Schema validation failures
- Type mismatches
- Missing required fields
- Conversion errors

All errors include context about what went wrong and where.