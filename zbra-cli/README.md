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

# Validate data against schema
zbra validate --data people.json --schema schema.json
```

## Commands

### `zbra example`

Creates example data files for testing and learning.

NOTE: This command will be removed once binary format is implemented.

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
```

Output:
```
File info for: people.json
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
zbra convert --input data.json --output data.logical --to logical
```

Supported formats:
- `json` - Human-readable JSON format (input)
- `logical` - zbra logical format (intermediate)
- `striped` - zbra columnar format (output)

### `zbra validate`

Validates data against a schema file.

```bash
zbra validate --data people.json --schema person_schema.json
```

Output includes:
- Schema validation result
- Striped conversion test
- Roundtrip integrity test

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
```\n\n### Schema Types\n\n**Table Schemas:**\n- `array` - Array of values with `element` schema\n- `binary` - Binary data with encoding\n- `map` - Key-value pairs (not yet implemented)\n\n**Value Schemas:**\n- `unit` - Unit/null value\n- `int` - 64-bit integers with encoding\n- `double` - 64-bit floating point\n- `binary` - Binary data with encoding\n- `array` - Array of values\n- `struct` - Structured record with named fields\n\n**Encodings:**\n- `int` - Plain integer\n- `date` - Date as integer\n- `time_seconds` - Time in seconds\n- `time_milliseconds` - Time in milliseconds\n- `time_microseconds` - Time in microseconds\n- `binary` - Raw binary data\n- `utf8` - UTF-8 encoded text\n\n**Defaults:**\n- `allow` - Allow default values\n- `deny` - Require explicit values\n\n## Examples\n\n### Person Records\n\n```json\n{\n  \"schema\": {\n    \"type\": \"array\",\n    \"default\": \"allow\",\n    \"element\": {\n      \"type\": \"struct\",\n      \"default\": \"allow\",\n      \"fields\": [\n        {\n          \"name\": \"id\",\n          \"schema\": {\"type\": \"int\", \"default\": \"allow\", \"encoding\": \"int\"}\n        },\n        {\n          \"name\": \"name\",\n          \"schema\": {\"type\": \"binary\", \"default\": \"allow\", \"encoding\": \"utf8\"}\n        },\n        {\n          \"name\": \"age\",\n          \"schema\": {\"type\": \"int\", \"default\": \"allow\", \"encoding\": \"int\"}\n        }\n      ]\n    }\n  },\n  \"data\": [\n    {\"struct\": {\"id\": 1, \"name\": \"Alice\", \"age\": 30}},\n    {\"struct\": {\"id\": 2, \"name\": \"Bob\", \"age\": 25}},\n    {\"struct\": {\"id\": 3, \"name\": \"Charlie\", \"age\": 35}}\n  ]\n}\n```\n\n### Simple Numbers\n\n```json\n{\n  \"schema\": {\n    \"type\": \"array\",\n    \"default\": \"allow\",\n    \"element\": {\n      \"type\": \"int\",\n      \"default\": \"allow\",\n      \"encoding\": \"int\"\n    }\n  },\n  \"data\": [1, 2, 3, 4, 5, 10, 20, 30]\n}\n```\n\n## Implementation Status\n\n**Current (working):**\n- JSON schema definition and parsing\n- Logical ↔ Striped conversion\n- Schema validation\n- Basic value types (int, double, binary, array, struct)\n- Example generation\n\n**Future:**\n- Binary file format (.zbra files)\n- Compression pipeline\n- Map tables\n- Enum types\n- Reversed types\n- Nested tables\n- Streaming operations\n\n## Error Handling\n\nThe CLI provides detailed error messages for:\n- Invalid JSON syntax\n- Schema validation failures\n- Type mismatches\n- Missing required fields\n- Conversion errors\n\nAll errors include context about what went wrong and where."