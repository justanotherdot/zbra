// Error types for zbra

use std::error::Error;
use std::fmt;

/// Core conversion errors across zbra layers
#[derive(Debug)]
pub enum ConversionError {
    Schema(SchemaError),
    Logical(LogicalError),
    Striped(StripedError),
    Binary(BinaryError),
}

/// Schema validation and compatibility errors
#[derive(Debug)]
pub enum SchemaError {
    TypeMismatch { expected: String, actual: String },
    MissingField(String),
    IncompatibleSchema { source: String, target: String },
    InvalidEncoding(String),
    UnsupportedType(String),
}

/// Logical layer representation errors
#[derive(Debug)]
pub enum LogicalError {
    InvalidValue { field: String, reason: String },
    StructureMismatch(String),
    ValidationFailure(String),
}

/// Striped (columnar) format errors
#[derive(Debug)]
pub enum StripedError {
    ColumnMismatch { expected: usize, actual: usize },
    InvalidColumnType(String),
    CompressionError(String),
    VectorOperationFailed(String),
}

/// Binary format encoding/decoding errors
#[derive(Debug)]
pub enum BinaryError {
    InvalidHeader,
    CorruptedData(String),
    UnsupportedVersion(u32),
    DecompressionFailure(String),
    SerializationFailure(String),
}

// Error trait implementations

impl Error for ConversionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ConversionError::Schema(e) => Some(e),
            ConversionError::Logical(e) => Some(e),
            ConversionError::Striped(e) => Some(e),
            ConversionError::Binary(e) => Some(e),
        }
    }
}

impl Error for SchemaError {}
impl Error for LogicalError {}
impl Error for StripedError {}
impl Error for BinaryError {}

// Display implementations

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConversionError::Schema(e) => write!(f, "Schema error: {}", e),
            ConversionError::Logical(e) => write!(f, "Logical layer error: {}", e),
            ConversionError::Striped(e) => write!(f, "Striped format error: {}", e),
            ConversionError::Binary(e) => write!(f, "Binary format error: {}", e),
        }
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {}, got {}", expected, actual)
            }
            SchemaError::MissingField(field) => {
                write!(f, "Missing required field: {}", field)
            }
            SchemaError::IncompatibleSchema { source, target } => {
                write!(
                    f,
                    "Incompatible schemas: cannot convert from {} to {}",
                    source, target
                )
            }
            SchemaError::InvalidEncoding(encoding) => {
                write!(f, "Invalid encoding: {}", encoding)
            }
            SchemaError::UnsupportedType(type_name) => {
                write!(f, "Unsupported type: {}", type_name)
            }
        }
    }
}

impl fmt::Display for LogicalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalError::InvalidValue { field, reason } => {
                write!(f, "Invalid value for field '{}': {}", field, reason)
            }
            LogicalError::StructureMismatch(msg) => {
                write!(f, "Structure mismatch: {}", msg)
            }
            LogicalError::ValidationFailure(msg) => {
                write!(f, "Validation failed: {}", msg)
            }
        }
    }
}

impl fmt::Display for StripedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StripedError::ColumnMismatch { expected, actual } => {
                write!(
                    f,
                    "Column count mismatch: expected {}, got {}",
                    expected, actual
                )
            }
            StripedError::InvalidColumnType(type_name) => {
                write!(f, "Invalid column type: {}", type_name)
            }
            StripedError::CompressionError(msg) => {
                write!(f, "Compression error: {}", msg)
            }
            StripedError::VectorOperationFailed(msg) => {
                write!(f, "Vector operation failed: {}", msg)
            }
        }
    }
}

impl fmt::Display for BinaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryError::InvalidHeader => {
                write!(f, "Invalid binary format header")
            }
            BinaryError::CorruptedData(msg) => {
                write!(f, "Corrupted data: {}", msg)
            }
            BinaryError::UnsupportedVersion(version) => {
                write!(f, "Unsupported format version: {}", version)
            }
            BinaryError::DecompressionFailure(msg) => {
                write!(f, "Decompression failed: {}", msg)
            }
            BinaryError::SerializationFailure(msg) => {
                write!(f, "Serialization failed: {}", msg)
            }
        }
    }
}

// Convenience From implementations for error composition

impl From<SchemaError> for ConversionError {
    fn from(error: SchemaError) -> Self {
        ConversionError::Schema(error)
    }
}

impl From<LogicalError> for ConversionError {
    fn from(error: LogicalError) -> Self {
        ConversionError::Logical(error)
    }
}

impl From<StripedError> for ConversionError {
    fn from(error: StripedError) -> Self {
        ConversionError::Striped(error)
    }
}

impl From<BinaryError> for ConversionError {
    fn from(error: BinaryError) -> Self {
        ConversionError::Binary(error)
    }
}

#[cfg(test)]
mod tests {
    // TODO: error handling tests
}
