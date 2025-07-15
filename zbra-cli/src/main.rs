use clap::{Parser, Subcommand};
use eyre::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

use zbra_core::data::{BinaryEncoding, Default, Encoding, Field, IntEncoding, Table, Value};
use zbra_core::logical::{FieldSchema, TableSchema, ValueSchema};
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

        /// Input format (json, logical)
        #[arg(long, default_value = "json")]
        from: String,

        /// Output format (json, logical, striped)
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
    default: String,
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
                "striped": format!("{:?}", striped_table),
                "row_count": striped_table.row_count()
            });

            fs::write(output, serde_json::to_string_pretty(&output_data)?)?;
            println!(
                "Converted to striped format with {} rows",
                striped_table.row_count()
            );
        }
        _ => {
            return Err(eyre::eyre!("Unsupported conversion: {} to {}", from, to));
        }
    }

    Ok(())
}

fn show_info(file: &PathBuf) -> Result<()> {
    println!("File info for: {}", file.display());

    let content = fs::read_to_string(file)?;
    let json_data: JsonData = serde_json::from_str(&content)?;

    println!("Schema type: {}", json_data.schema.schema_type);
    println!("Schema default: {}", json_data.schema.default);

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
            let default = parse_default(&json_schema.default)?;
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
            let default = parse_default(&json_schema.default)?;
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
    let default = parse_default(&json_schema.default)?;

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
        "struct" => {
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
