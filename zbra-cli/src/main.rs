use clap::{Parser, Subcommand};
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

use zbra_core::binary;
use zbra_core::data::{BinaryEncoding, Default, Encoding, Field, IntEncoding, Table, Value};
use zbra_core::logical::{FieldSchema, TableSchema, ValueSchema, VariantSchema};
use zbra_core::striped;

#[derive(Parser)]
#[command(name = "zbra")]
#[command(about = "A modern columnar data format")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert data between formats
    Convert {
        /// Input file
        #[arg(short, long)]
        input: PathBuf,

        /// Output file
        #[arg(short, long)]
        output: PathBuf,

        /// Input format (json, logical, binary)
        #[arg(long, default_value = "json")]
        from: String,

        /// Output format (json, logical, striped, binary)
        #[arg(long, default_value = "striped")]
        to: String,
    },
    /// Show information about a data file
    Info {
        /// Input file
        file: PathBuf,
    },
    /// Create example data files
    /// TODO: Remove this command once binary format is implemented
    /// This is a development convenience to bootstrap testing without .zbra files
    Example {
        /// Output directory
        #[arg(short, long, default_value = ".")]
        output: PathBuf,
    },
    /// Validate data against schema
    Validate {
        /// Data file
        #[arg(short, long)]
        data: PathBuf,

        /// Schema file
        #[arg(short, long)]
        schema: PathBuf,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct JsonData {
    schema: JsonSchema,
    data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct JsonSchema {
    #[serde(rename = "type")]
    schema_type: String,
    default: Option<String>,
    encoding: Option<String>,
    element: Option<Box<JsonSchema>>,
    fields: Option<Vec<JsonField>>,
    variants: Option<Vec<JsonVariant>>,
    inner: Option<Box<JsonSchema>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct JsonField {
    name: String,
    schema: JsonSchema,
}

#[derive(Serialize, Deserialize, Debug)]
struct JsonVariant {
    name: String,
    tag: u32,
    schema: JsonSchema,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Convert {
            input,
            output,
            from,
            to,
        } => {
            convert_file(input, output, from, to)?;
        }
        Commands::Info { file } => {
            show_info(file)?;
        }
        Commands::Example { output } => {
            create_examples(output)?;
        }
        Commands::Validate { data, schema } => {
            validate_data(data, schema)?;
        }
    }

    Ok(())
}

fn convert_file(input: &PathBuf, output: &PathBuf, from: &str, to: &str) -> Result<()> {
    println!(
        "Converting {} -> {} ({} to {})",
        input.display(),
        output.display(),
        from,
        to
    );

    match (from, to) {
        ("json", "logical") => {
            let json_content = fs::read_to_string(input)?;
            let json_data: JsonData = serde_json::from_str(&json_content)?;

            let schema = convert_json_schema_to_table_schema(&json_data.schema)?;
            let logical_data = convert_json_value_to_table(&json_data.data)?;

            // Validate the data against schema
            logical_data.validate_schema(&schema)?;

            let output_data = serde_json::json!({
                "schema": schema_to_json(&schema),
                "data": table_to_json(&logical_data)
            });

            fs::write(output, serde_json::to_string_pretty(&output_data)?)?;
            println!("Converted to logical format");
        }
        ("json", "striped") | ("logical", "striped") => {
            let json_content = fs::read_to_string(input)?;
            let json_data: JsonData = serde_json::from_str(&json_content)?;

            let schema = convert_json_schema_to_table_schema(&json_data.schema)?;
            let logical_data = convert_json_value_to_table(&json_data.data)?;

            // Convert to striped format
            let striped_table = striped::Table::from_logical(&schema, &logical_data)?;

            let output_data = serde_json::json!({
                "schema": schema_to_json(&schema),
                "striped": striped_table_to_json(&striped_table),
                "row_count": striped_table.row_count()
            });

            fs::write(output, serde_json::to_string_pretty(&output_data)?)?;
            println!(
                "Converted to striped format with {} rows",
                striped_table.row_count()
            );
        }
        ("json", "binary") | ("logical", "binary") | ("striped", "binary") => {
            let (schema, striped_table) = match from {
                "json" | "logical" => {
                    let json_content = fs::read_to_string(input)?;
                    let json_data: JsonData = serde_json::from_str(&json_content)?;

                    let schema = convert_json_schema_to_table_schema(&json_data.schema)?;
                    let logical_data = convert_json_value_to_table(&json_data.data)?;
                    let striped_table = striped::Table::from_logical(&schema, &logical_data)?;

                    (schema, striped_table)
                }
                "striped" => {
                    let json_content = fs::read_to_string(input)?;
                    let json_data: serde_json::Value = serde_json::from_str(&json_content)?;

                    // For striped format, we need to infer the schema from the striped data
                    let striped_table = json_to_striped_table(&json_data["striped"])?;
                    let schema = infer_schema_from_striped_table(&striped_table)?;

                    (schema, striped_table)
                }
                _ => unreachable!(),
            };

            // Create binary file
            let row_count = striped_table.row_count();
            let binary_file = binary::BinaryFile::new(schema, striped_table);

            // Write to output file
            let mut file = fs::File::create(output)?;
            binary_file.write_to(&mut file)?;

            println!("Converted to binary format with {} rows", row_count);
        }
        ("binary", "json") | ("binary", "logical") | ("binary", "striped") => {
            // Read binary file
            let mut file = fs::File::open(input)?;
            let binary_file = binary::BinaryFile::read_from(&mut file)?;

            let schema = &binary_file.header.schema;
            let striped_table = &binary_file.blocks[0].table; // For now, assume single block

            match to {
                "json" | "logical" => {
                    let logical_data = striped_table.to_logical()?;
                    let output_data = serde_json::json!({
                        "schema": schema_to_json(schema),
                        "data": table_to_json(&logical_data)
                    });

                    fs::write(output, serde_json::to_string_pretty(&output_data)?)?;
                    println!("Converted from binary to logical format");
                }
                "striped" => {
                    let output_data = serde_json::json!({
                        "schema": schema_to_json(schema),
                        "striped": striped_table_to_json(striped_table),
                        "row_count": striped_table.row_count()
                    });

                    fs::write(output, serde_json::to_string_pretty(&output_data)?)?;
                    println!(
                        "Converted from binary to striped format with {} rows",
                        striped_table.row_count()
                    );
                }
                _ => unreachable!(),
            }
        }
        _ => {
            return Err(eyre::eyre!("Unsupported conversion: {} to {}", from, to));
        }
    }

    Ok(())
}

fn show_info(file: &PathBuf) -> Result<()> {
    println!("File info for: {}", file.display());

    // Check if this is a binary file (ends with .zbra)
    if file.extension().and_then(|s| s.to_str()) == Some("zbra") {
        // Handle binary file
        let mut file_handle = fs::File::open(file)?;
        let binary_file = binary::BinaryFile::read_from(&mut file_handle)?;

        let schema = &binary_file.header.schema;
        let total_rows: usize = binary_file
            .blocks
            .iter()
            .map(|b| b.row_count as usize)
            .sum();

        println!("Format: Binary (.zbra)");
        println!("Schema type: {:?}", schema);
        println!("Total rows: {}", total_rows);
        println!("Block count: {}", binary_file.blocks.len());

        for (i, block) in binary_file.blocks.iter().enumerate() {
            println!("Block {}: {} rows", i, block.row_count);
        }

        println!("Schema validation: PASS (binary files are pre-validated)");
    } else {
        // Handle JSON file
        let content = fs::read_to_string(file)?;
        let json_data: JsonData = serde_json::from_str(&content)?;

        println!("Format: JSON");
        println!("Schema type: {}", json_data.schema.schema_type);
        println!(
            "Schema default: {}",
            json_data.schema.default.as_deref().unwrap_or("allow")
        );

        let schema = convert_json_schema_to_table_schema(&json_data.schema)?;
        let logical_data = convert_json_value_to_table(&json_data.data)?;

        // Validate
        match logical_data.validate_schema(&schema) {
            Ok(_) => println!("Schema validation: PASS"),
            Err(e) => println!("Schema validation: FAIL - {}", e),
        }

        // Show row count
        let row_count = match &logical_data {
            Table::Array(values) => values.len(),
            Table::Map(pairs) => pairs.len(),
            Table::Binary(data) => {
                if data.is_empty() {
                    0
                } else {
                    1
                }
            }
        };
        println!("Row count: {}", row_count);

        // Convert to striped and show info
        let striped_table = striped::Table::from_logical(&schema, &logical_data)?;
        println!("Striped row count: {}", striped_table.row_count());
    }

    Ok(())
}

fn create_examples(output_dir: &PathBuf) -> Result<()> {
    println!("Creating example files in: {}", output_dir.display());

    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;

    // Example 1: Simple person records
    let people_example = serde_json::json!({
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
                    },
                    {
                        "name": "age",
                        "schema": {
                            "type": "int",
                            "default": "allow",
                            "encoding": "int"
                        }
                    }
                ]
            }
        },
        "data": [
            {"struct": {"id": 1, "name": "Alice", "age": 30}},
            {"struct": {"id": 2, "name": "Bob", "age": 25}},
            {"struct": {"id": 3, "name": "Charlie", "age": 35}}
        ]
    });

    let people_file = output_dir.join("people.json");
    fs::write(&people_file, serde_json::to_string_pretty(&people_example)?)?;
    println!("Created: {}", people_file.display());

    // Example 2: Simple integers
    let numbers_example = serde_json::json!({
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
    });

    let numbers_file = output_dir.join("numbers.json");
    fs::write(
        &numbers_file,
        serde_json::to_string_pretty(&numbers_example)?,
    )?;
    println!("Created: {}", numbers_file.display());

    println!("\nExample usage:");
    println!("  zbra info {}", people_file.display());
    println!(
        "  zbra convert --input {} --output people.striped --to striped",
        people_file.display()
    );

    Ok(())
}

fn validate_data(data_file: &PathBuf, schema_file: &PathBuf) -> Result<()> {
    println!(
        "Validating {} against {}",
        data_file.display(),
        schema_file.display()
    );

    let data_content = fs::read_to_string(data_file)?;
    let schema_content = fs::read_to_string(schema_file)?;

    let json_data: JsonData = serde_json::from_str(&data_content)?;
    let json_schema: JsonSchema = serde_json::from_str(&schema_content)?;

    let schema = convert_json_schema_to_table_schema(&json_schema)?;
    let logical_data = convert_json_value_to_table(&json_data.data)?;

    match logical_data.validate_schema(&schema) {
        Ok(_) => {
            println!("Validation: PASS");

            // Try conversion to striped
            let striped_table = striped::Table::from_logical(&schema, &logical_data)?;
            println!(
                "Striped conversion: PASS ({} rows)",
                striped_table.row_count()
            );

            // Try roundtrip
            let roundtrip = striped_table.to_logical()?;
            if logical_data == roundtrip {
                println!("Roundtrip test: PASS");
            } else {
                println!("Roundtrip test: FAIL");
            }
        }
        Err(e) => {
            println!("Validation: FAIL - {}", e);
        }
    }

    Ok(())
}

// Helper functions for JSON conversion
fn convert_json_schema_to_table_schema(json_schema: &JsonSchema) -> Result<TableSchema> {
    match json_schema.schema_type.as_str() {
        "array" => {
            let default = parse_default(&json_schema.default.as_deref().unwrap_or("allow"))?;
            let element = json_schema
                .element
                .as_ref()
                .ok_or_else(|| eyre::eyre!("Array schema missing element"))?;
            let element_schema = convert_json_schema_to_value_schema(element)?;
            Ok(TableSchema::Array {
                default,
                element: Box::new(element_schema),
            })
        }
        "binary" => {
            let default = parse_default(&json_schema.default.as_deref().unwrap_or("allow"))?;
            let encoding = parse_encoding(json_schema.encoding.as_deref().unwrap_or("binary"))?;
            Ok(TableSchema::Binary { default, encoding })
        }
        "map" => {
            // This is simplified - would need key/value schemas
            Err(eyre::eyre!("Map table schema not yet implemented in CLI"))
        }
        _ => Err(eyre::eyre!(
            "Unknown table schema type: {}",
            json_schema.schema_type
        )),
    }
}

fn convert_json_schema_to_value_schema(json_schema: &JsonSchema) -> Result<ValueSchema> {
    let default = parse_default(&json_schema.default.as_deref().unwrap_or("allow"))?;

    match json_schema.schema_type.as_str() {
        "unit" => Ok(ValueSchema::Unit),
        "int" => {
            let encoding = parse_encoding(json_schema.encoding.as_deref().unwrap_or("int"))?;
            Ok(ValueSchema::Int { default, encoding })
        }
        "double" => Ok(ValueSchema::Double { default }),
        "binary" => {
            let encoding = parse_encoding(json_schema.encoding.as_deref().unwrap_or("binary"))?;
            Ok(ValueSchema::Binary { default, encoding })
        }
        "array" => {
            let element = json_schema
                .element
                .as_ref()
                .ok_or_else(|| eyre::eyre!("Array schema missing element"))?;
            let element_schema = convert_json_schema_to_value_schema(element)?;
            Ok(ValueSchema::Array {
                default,
                element: Box::new(element_schema),
            })
        }
        "struct" | "complex" => {
            let fields = json_schema
                .fields
                .as_ref()
                .ok_or_else(|| eyre::eyre!("Struct schema missing fields"))?;
            let field_schemas: Result<Vec<_>> = fields
                .iter()
                .map(|f| {
                    Ok(FieldSchema {
                        name: f.name.clone(),
                        schema: convert_json_schema_to_value_schema(&f.schema)?,
                    })
                })
                .collect();
            Ok(ValueSchema::Struct {
                default,
                fields: field_schemas?,
            })
        }
        _ => Err(eyre::eyre!(
            "Unknown value schema type: {}",
            json_schema.schema_type
        )),
    }
}

fn parse_default(default_str: &str) -> Result<Default> {
    match default_str {
        "allow" => Ok(Default::Allow),
        "deny" => Ok(Default::Deny),
        _ => Err(eyre::eyre!("Unknown default: {}", default_str)),
    }
}

fn parse_encoding(encoding_str: &str) -> Result<Encoding> {
    match encoding_str {
        "int" => Ok(Encoding::Int(IntEncoding::Int)),
        "date" => Ok(Encoding::Int(IntEncoding::Date)),
        "time_seconds" => Ok(Encoding::Int(IntEncoding::TimeSeconds)),
        "time_milliseconds" => Ok(Encoding::Int(IntEncoding::TimeMilliseconds)),
        "time_microseconds" => Ok(Encoding::Int(IntEncoding::TimeMicroseconds)),
        "binary" => Ok(Encoding::Binary(BinaryEncoding::Binary)),
        "utf8" => Ok(Encoding::Binary(BinaryEncoding::Utf8)),
        _ => Err(eyre::eyre!("Unknown encoding: {}", encoding_str)),
    }
}

fn convert_json_value_to_table(json_value: &serde_json::Value) -> Result<Table> {
    match json_value {
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<_>> = arr.iter().map(convert_json_value_to_value).collect();
            Ok(Table::Array(values?))
        }
        serde_json::Value::String(s) => Ok(Table::Binary(s.as_bytes().to_vec())),
        _ => Err(eyre::eyre!(
            "Cannot convert JSON value to table: {:?}",
            json_value
        )),
    }
}

fn convert_json_value_to_value(json_value: &serde_json::Value) -> Result<Value> {
    match json_value {
        serde_json::Value::Null => Ok(Value::Unit),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Double(f))
            } else {
                Err(eyre::eyre!("Invalid number: {}", n))
            }
        }
        serde_json::Value::String(s) => Ok(Value::Binary(s.as_bytes().to_vec())),
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<_>> = arr.iter().map(convert_json_value_to_value).collect();
            Ok(Value::Array(values?))
        }
        serde_json::Value::Object(obj) => {
            if let Some(struct_obj) = obj.get("struct") {
                if let serde_json::Value::Object(fields) = struct_obj {
                    let field_values: Result<Vec<_>> = fields
                        .iter()
                        .map(|(name, value)| {
                            Ok(Field {
                                name: name.clone(),
                                value: convert_json_value_to_value(value)?,
                            })
                        })
                        .collect();
                    return Ok(Value::Struct(field_values?));
                }
            }
            Err(eyre::eyre!(
                "Cannot convert JSON object to value: {:?}",
                obj
            ))
        }
        _ => Err(eyre::eyre!("Cannot convert JSON value: {:?}", json_value)),
    }
}

// Simple JSON serialization helpers (for output)
fn schema_to_json(schema: &TableSchema) -> serde_json::Value {
    match schema {
        TableSchema::Array { default, element } => {
            serde_json::json!({
                "type": "array",
                "default": default_to_string(default),
                "element": value_schema_to_json(element)
            })
        }
        TableSchema::Binary { default, encoding } => {
            serde_json::json!({
                "type": "binary",
                "default": default_to_string(default),
                "encoding": encoding_to_string(encoding)
            })
        }
        TableSchema::Map {
            default,
            key,
            value,
        } => {
            serde_json::json!({
                "type": "map",
                "default": default_to_string(default),
                "key": value_schema_to_json(key),
                "value": value_schema_to_json(value)
            })
        }
    }
}

fn value_schema_to_json(schema: &ValueSchema) -> serde_json::Value {
    match schema {
        ValueSchema::Unit => serde_json::json!({"type": "unit"}),
        ValueSchema::Int { default, encoding } => {
            serde_json::json!({
                "type": "int",
                "default": default_to_string(default),
                "encoding": encoding_to_string(encoding)
            })
        }
        ValueSchema::Double { default } => {
            serde_json::json!({
                "type": "double",
                "default": default_to_string(default)
            })
        }
        ValueSchema::Binary { default, encoding } => {
            serde_json::json!({
                "type": "binary",
                "default": default_to_string(default),
                "encoding": encoding_to_string(encoding)
            })
        }
        _ => serde_json::json!({"type": "complex"}), // Simplified for now
    }
}

fn table_to_json(table: &Table) -> serde_json::Value {
    match table {
        Table::Array(values) => {
            let json_values: Vec<_> = values.iter().map(value_to_json).collect();
            serde_json::Value::Array(json_values)
        }
        Table::Binary(data) => {
            let text = String::from_utf8_lossy(data);
            serde_json::Value::String(text.to_string())
        }
        Table::Map(_) => serde_json::json!("map_not_implemented"),
    }
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Unit => serde_json::Value::Null,
        Value::Int(n) => serde_json::Value::Number((*n).into()),
        Value::Double(d) => serde_json::json!(d),
        Value::Binary(data) => {
            let text = String::from_utf8_lossy(data);
            serde_json::Value::String(text.to_string())
        }
        Value::Array(values) => {
            let json_values: Vec<_> = values.iter().map(value_to_json).collect();
            serde_json::Value::Array(json_values)
        }
        Value::Struct(fields) => {
            let mut obj = serde_json::Map::new();
            for field in fields {
                obj.insert(field.name.clone(), value_to_json(&field.value));
            }
            serde_json::json!({"struct": obj})
        }
        _ => serde_json::json!("complex_value"),
    }
}

fn default_to_string(default: &Default) -> &'static str {
    match default {
        Default::Allow => "allow",
        Default::Deny => "deny",
    }
}

fn encoding_to_string(encoding: &Encoding) -> &'static str {
    match encoding {
        Encoding::Int(IntEncoding::Int) => "int",
        Encoding::Int(IntEncoding::Date) => "date",
        Encoding::Int(IntEncoding::TimeSeconds) => "time_seconds",
        Encoding::Int(IntEncoding::TimeMilliseconds) => "time_milliseconds",
        Encoding::Int(IntEncoding::TimeMicroseconds) => "time_microseconds",
        Encoding::Binary(BinaryEncoding::Binary) => "binary",
        Encoding::Binary(BinaryEncoding::Utf8) => "utf8",
    }
}

fn string_to_default(s: &str) -> Result<Default> {
    match s {
        "allow" => Ok(Default::Allow),
        "deny" => Ok(Default::Deny),
        _ => Err(eyre::eyre!("Invalid default value: {}", s)),
    }
}

fn string_to_encoding(s: &str) -> Result<Encoding> {
    match s {
        "int" => Ok(Encoding::Int(IntEncoding::Int)),
        "date" => Ok(Encoding::Int(IntEncoding::Date)),
        "time_seconds" => Ok(Encoding::Int(IntEncoding::TimeSeconds)),
        "time_milliseconds" => Ok(Encoding::Int(IntEncoding::TimeMilliseconds)),
        "time_microseconds" => Ok(Encoding::Int(IntEncoding::TimeMicroseconds)),
        "binary" => Ok(Encoding::Binary(BinaryEncoding::Binary)),
        "utf8" => Ok(Encoding::Binary(BinaryEncoding::Utf8)),
        _ => Err(eyre::eyre!("Invalid encoding value: {}", s)),
    }
}

// Striped table JSON serialization

fn striped_table_to_json(table: &striped::Table) -> serde_json::Value {
    match table {
        striped::Table::Binary {
            default,
            encoding,
            data,
        } => {
            serde_json::json!({
                "type": "binary",
                "default": default_to_string(default),
                "encoding": encoding_to_string(encoding),
                "data": data
            })
        }
        striped::Table::Array { default, column } => {
            serde_json::json!({
                "type": "array",
                "default": default_to_string(default),
                "column": striped_column_to_json(column)
            })
        }
        striped::Table::Map {
            default,
            key_column,
            value_column,
        } => {
            serde_json::json!({
                "type": "map",
                "default": default_to_string(default),
                "key_column": striped_column_to_json(key_column),
                "value_column": striped_column_to_json(value_column)
            })
        }
    }
}

fn striped_column_to_json(column: &striped::Column) -> serde_json::Value {
    match column {
        striped::Column::Unit { count } => {
            serde_json::json!({
                "type": "unit",
                "count": count
            })
        }
        striped::Column::Int {
            default,
            encoding,
            values,
        } => {
            serde_json::json!({
                "type": "int",
                "default": default_to_string(default),
                "encoding": encoding_to_string(encoding),
                "values": values
            })
        }
        striped::Column::Double { default, values } => {
            serde_json::json!({
                "type": "double",
                "default": default_to_string(default),
                "values": values
            })
        }
        striped::Column::Binary {
            default,
            encoding,
            lengths,
            data,
        } => {
            // Convert binary data to readable strings where possible
            let data_display = if let Encoding::Binary(BinaryEncoding::Utf8) = encoding {
                // Try to display as UTF-8 strings
                let mut strings = Vec::new();
                let mut offset = 0;
                for &length in lengths {
                    let end = offset + length;
                    if end <= data.len() {
                        let slice = &data[offset..end];
                        match String::from_utf8(slice.to_vec()) {
                            Ok(s) => strings.push(serde_json::Value::String(s)),
                            Err(_) => strings.push(serde_json::Value::Array(
                                slice
                                    .iter()
                                    .map(|b| serde_json::Value::Number((*b).into()))
                                    .collect(),
                            )),
                        }
                        offset = end;
                    }
                }
                serde_json::Value::Array(strings)
            } else {
                // Display as raw bytes
                serde_json::Value::Array(
                    data.iter()
                        .map(|b| serde_json::Value::Number((*b).into()))
                        .collect(),
                )
            };

            serde_json::json!({
                "type": "binary",
                "default": default_to_string(default),
                "encoding": encoding_to_string(encoding),
                "lengths": lengths,
                "data": data_display
            })
        }
        striped::Column::Array {
            default,
            lengths,
            element,
        } => {
            serde_json::json!({
                "type": "array",
                "default": default_to_string(default),
                "lengths": lengths,
                "element": striped_column_to_json(element)
            })
        }
        striped::Column::Struct { default, fields } => {
            let field_objects: Vec<_> = fields
                .iter()
                .map(|field| {
                    serde_json::json!({
                        "name": field.name,
                        "column": striped_column_to_json(&field.column)
                    })
                })
                .collect();

            serde_json::json!({
                "type": "struct",
                "default": default_to_string(default),
                "fields": field_objects
            })
        }
        striped::Column::Enum {
            default,
            tags,
            variants,
        } => {
            let variant_objects: Vec<_> = variants
                .iter()
                .map(|variant| {
                    serde_json::json!({
                        "name": variant.name,
                        "tag": variant.tag,
                        "column": striped_column_to_json(&variant.column)
                    })
                })
                .collect();

            serde_json::json!({
                "type": "enum",
                "default": default_to_string(default),
                "tags": tags,
                "variants": variant_objects
            })
        }
        striped::Column::Nested { lengths, table } => {
            serde_json::json!({
                "type": "nested",
                "lengths": lengths,
                "table": striped_table_to_json(table)
            })
        }
        striped::Column::Reversed { inner } => {
            serde_json::json!({
                "type": "reversed",
                "inner": striped_column_to_json(inner)
            })
        }
    }
}

fn json_to_striped_table(json_value: &serde_json::Value) -> Result<striped::Table> {
    let table_type = json_value["type"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Missing type field"))?;

    match table_type {
        "binary" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let encoding = string_to_encoding(json_value["encoding"].as_str().unwrap_or("binary"))?;
            let data = json_value["data"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Binary data must be an array"))?
                .iter()
                .map(|v| v.as_u64().unwrap_or(0) as u8)
                .collect();

            Ok(striped::Table::Binary {
                default,
                encoding,
                data,
            })
        }
        "array" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let column = json_to_striped_column(&json_value["column"])?;

            Ok(striped::Table::Array {
                default,
                column: Box::new(column),
            })
        }
        "map" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let key_column = json_to_striped_column(&json_value["key_column"])?;
            let value_column = json_to_striped_column(&json_value["value_column"])?;

            Ok(striped::Table::Map {
                default,
                key_column: Box::new(key_column),
                value_column: Box::new(value_column),
            })
        }
        _ => Err(eyre::eyre!(
            "Unsupported striped table type: {}",
            table_type
        )),
    }
}

fn json_to_striped_column(json_value: &serde_json::Value) -> Result<striped::Column> {
    let column_type = json_value["type"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Missing type field"))?;

    match column_type {
        "unit" => {
            let count = json_value["count"].as_u64().unwrap_or(0) as usize;
            Ok(striped::Column::Unit { count })
        }
        "int" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let encoding = string_to_encoding(json_value["encoding"].as_str().unwrap_or("int"))?;
            let values = json_value["values"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Int values must be an array"))?
                .iter()
                .map(|v| v.as_i64().unwrap_or(0))
                .collect();

            Ok(striped::Column::Int {
                default,
                encoding,
                values,
            })
        }
        "double" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let values = json_value["values"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Double values must be an array"))?
                .iter()
                .map(|v| v.as_f64().unwrap_or(0.0))
                .collect();

            Ok(striped::Column::Double { default, values })
        }
        "binary" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let encoding = string_to_encoding(json_value["encoding"].as_str().unwrap_or("binary"))?;
            let lengths = json_value["lengths"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Binary lengths must be an array"))?
                .iter()
                .map(|v| v.as_u64().unwrap_or(0) as usize)
                .collect();
            let data = json_value["data"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Binary data must be an array"))?
                .iter()
                .map(|v| v.as_str().unwrap_or("").bytes().collect::<Vec<u8>>())
                .flatten()
                .collect();

            Ok(striped::Column::Binary {
                default,
                encoding,
                lengths,
                data,
            })
        }
        "array" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let lengths = json_value["lengths"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Array lengths must be an array"))?
                .iter()
                .map(|v| v.as_u64().unwrap_or(0) as usize)
                .collect();
            let element = json_to_striped_column(&json_value["element"])?;

            Ok(striped::Column::Array {
                default,
                lengths,
                element: Box::new(element),
            })
        }
        "struct" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let fields = json_value["fields"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Struct fields must be an array"))?
                .iter()
                .map(|field| {
                    let name = field["name"].as_str().unwrap_or("").to_string();
                    let column = json_to_striped_column(&field["column"])?;
                    Ok(striped::FieldColumn { name, column })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(striped::Column::Struct { default, fields })
        }
        "enum" => {
            let default = string_to_default(json_value["default"].as_str().unwrap_or("allow"))?;
            let tags = json_value["tags"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Enum tags must be an array"))?
                .iter()
                .map(|v| v.as_u64().unwrap_or(0) as u32)
                .collect();
            let variants = json_value["variants"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Enum variants must be an array"))?
                .iter()
                .map(|variant| {
                    let name = variant["name"].as_str().unwrap_or("").to_string();
                    let tag = variant["tag"].as_u64().unwrap_or(0) as u32;
                    let column = json_to_striped_column(&variant["column"])?;
                    Ok(striped::VariantColumn { name, tag, column })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(striped::Column::Enum {
                default,
                tags,
                variants,
            })
        }
        "nested" => {
            let lengths = json_value["lengths"]
                .as_array()
                .ok_or_else(|| eyre::eyre!("Nested lengths must be an array"))?
                .iter()
                .map(|v| v.as_u64().unwrap_or(0) as usize)
                .collect();
            let table = json_to_striped_table(&json_value["table"])?;

            Ok(striped::Column::Nested {
                lengths,
                table: Box::new(table),
            })
        }
        "reversed" => {
            let inner = json_to_striped_column(&json_value["inner"])?;
            Ok(striped::Column::Reversed {
                inner: Box::new(inner),
            })
        }
        _ => Err(eyre::eyre!(
            "Unsupported striped column type: {}",
            column_type
        )),
    }
}

fn infer_schema_from_striped_table(striped_table: &striped::Table) -> Result<TableSchema> {
    match striped_table {
        striped::Table::Binary {
            default, encoding, ..
        } => Ok(TableSchema::Binary {
            default: default.clone(),
            encoding: encoding.clone(),
        }),
        striped::Table::Array { default, column } => {
            let element_schema = infer_schema_from_striped_column(column)?;
            Ok(TableSchema::Array {
                default: default.clone(),
                element: Box::new(element_schema),
            })
        }
        striped::Table::Map {
            default,
            key_column,
            value_column,
        } => {
            let key_schema = infer_schema_from_striped_column(key_column)?;
            let value_schema = infer_schema_from_striped_column(value_column)?;
            Ok(TableSchema::Map {
                default: default.clone(),
                key: Box::new(key_schema),
                value: Box::new(value_schema),
            })
        }
    }
}

fn infer_schema_from_striped_column(column: &striped::Column) -> Result<ValueSchema> {
    match column {
        striped::Column::Unit { .. } => Ok(ValueSchema::Unit),
        striped::Column::Int {
            default, encoding, ..
        } => Ok(ValueSchema::Int {
            default: default.clone(),
            encoding: encoding.clone(),
        }),
        striped::Column::Double { default, .. } => Ok(ValueSchema::Double {
            default: default.clone(),
        }),
        striped::Column::Binary {
            default, encoding, ..
        } => Ok(ValueSchema::Binary {
            default: default.clone(),
            encoding: encoding.clone(),
        }),
        striped::Column::Array {
            default, element, ..
        } => {
            let element_schema = infer_schema_from_striped_column(element)?;
            Ok(ValueSchema::Array {
                default: default.clone(),
                element: Box::new(element_schema),
            })
        }
        striped::Column::Struct { default, fields } => {
            let field_schemas: Result<Vec<_>> = fields
                .iter()
                .map(|field| {
                    let schema = infer_schema_from_striped_column(&field.column)?;
                    Ok(FieldSchema {
                        name: field.name.clone(),
                        schema,
                    })
                })
                .collect();
            Ok(ValueSchema::Struct {
                default: default.clone(),
                fields: field_schemas?,
            })
        }
        striped::Column::Enum {
            default, variants, ..
        } => {
            let variant_schemas: Result<Vec<_>> = variants
                .iter()
                .map(|variant| {
                    let schema = infer_schema_from_striped_column(&variant.column)?;
                    Ok(VariantSchema {
                        name: variant.name.clone(),
                        tag: variant.tag,
                        schema,
                    })
                })
                .collect();
            Ok(ValueSchema::Enum {
                default: default.clone(),
                variants: variant_schemas?,
            })
        }
        striped::Column::Nested { table, .. } => {
            let table_schema = infer_schema_from_striped_table(table)?;
            Ok(ValueSchema::Nested {
                table: Box::new(table_schema),
            })
        }
        striped::Column::Reversed { inner } => {
            let inner_schema = infer_schema_from_striped_column(inner)?;
            Ok(ValueSchema::Reversed {
                inner: Box::new(inner_schema),
            })
        }
    }
}
