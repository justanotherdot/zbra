// Core type definitions for zbra

use serde::{Deserialize, Serialize};

/// Core value types in zbra
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    // Phase 1 - Essential types
    Unit,
    Int(i64),
    Double(f64),
    Binary(Vec<u8>),
    Array(Vec<Value>),
    Struct(Vec<Field>),
    // Phase 2 - Nice-to-haves
    Enum { tag: u32, value: Box<Value> },
    Nested(Box<Table>),
    Reversed(Box<Value>),
}

/// Named field in a struct
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub value: Value,
}

/// Table representation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Table {
    Binary(Vec<u8>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

/// Encoding specification for primitive types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Encoding {
    Int(IntEncoding),
    Binary(BinaryEncoding),
}

/// Integer encoding variants
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IntEncoding {
    Int,
    Date,
    TimeSeconds,
    TimeMilliseconds,
    TimeMicroseconds,
}

/// Binary encoding variants
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryEncoding {
    Binary,
    Utf8,
}

/// Default value handling
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Default {
    Allow,
    Deny,
}

#[cfg(test)]
mod tests {
    // TODO: meaningful tests when conversions and behavior are implemented
}
