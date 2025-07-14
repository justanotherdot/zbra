# Zbra Error Handling Strategy

## Design Philosophy

Zbra is designed as a library for embedding in other applications, prioritizing simplicity and clarity over complex error handling infrastructure.

### Library-First Design Principles

- **Keep it simple** - Errors should be easy for consuming applications to handle
- **Clear error types** - Enable pattern matching and specific error handling
- **Good messages** - Provide actionable information for human operators
- **Let users decide** - Don't impose error codes or structured logging; let consuming applications build their own on top

## Error Architecture

### Hierarchical Composition

Following Zebra's proven pattern, errors compose hierarchically where higher-level errors wrap lower-level ones:

```rust
pub enum ConversionError {
    Schema(SchemaError),
    Binary(BinaryError), 
    Striped(StripedError),
}
```

### Layer-Specific Error Types

Errors are organized by architectural layer:

- **`SchemaError`** - Schema validation and compatibility issues
- **`BinaryError`** - Binary format encoding/decoding failures
- **`StripedError`** - Columnar format operations
- **`LogicalError`** - Logical representation validation
- **`ConversionError`** - Cross-layer transformation failures

### Standard Rust Error Handling

- Use standard Rust `Error` trait with manual `Display` implementations
- Only consider `thiserror` if `Display` boilerplate becomes excessive
- Explicit `Result` types - no exceptions or panics in normal operation
- Rich context in error messages for debugging

## Implementation Guidelines

### Error Messages

- Include sufficient context for debugging (file paths, schema details, position info)
- Make messages actionable - what went wrong and why
- Use consistent formatting and terminology
- Avoid technical jargon where possible

### Error Composition

```rust
// Higher-level errors wrap lower-level ones
pub enum BinaryDecodeError {
    InvalidHeader(String),
    CompressionFailure(CompressionError),
    SchemaValidation(SchemaError),
}
```

### Error Context

- Preserve important context as errors bubble up through layers
- Include relevant data (entity IDs, attribute names, file positions)
- Maintain error chains for root cause analysis

## What We Don't Include (Initially)

### Structured Error Codes
- Skip machine-readable error codes initially
- Let consuming applications add their own error classification
- Focus on clear error types for programmatic handling

### Complex Error Infrastructure
- No error aggregation or multiple error collection
- No severity levels or error categories beyond types
- No built-in logging or telemetry integration

### Recovery Mechanisms
- Keep errors simple and explicit
- Let consuming applications decide on retry/recovery strategies
- Fail fast with clear error information

## Future Considerations

As zbra matures and we build higher-level components (like a streaming query engine), we may add:

- Error codes for specific use cases
- Error aggregation for validation scenarios
- Performance context in errors
- Structured metadata for observability

But for the core library, simplicity and clarity are the priorities.

## Example Error Hierarchy

```rust
// Core conversion errors
pub enum ConversionError {
    Schema(SchemaError),
    Logical(LogicalError),
    Striped(StripedError),
    Binary(BinaryError),
}

// Schema-specific errors
pub enum SchemaError {
    TypeMismatch { expected: String, actual: String },
    MissingField(String),
    IncompatibleSchema { source: String, target: String },
}

// Binary format errors  
pub enum BinaryError {
    InvalidHeader,
    CorruptedData(String),
    UnsupportedVersion(u32),
    DecompressionFailure(String),
}
```

This approach provides the foundation for robust error handling while keeping the library focused and easy to integrate.