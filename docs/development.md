# Zbra Development Plan

## Implementation Strategy

### Three-Layer Architecture Porting Order

1. **Core Type System (Foundation)** - `zbra-core/src/types.rs`
   - `Value` and `Table` enums
   - Schema definitions and encoding specifications
   - Basic type safety before any data operations

2. **Logical Layer** - `zbra-core/src/logical.rs`
   - JSON-like representation for human readability
   - Schema evolution and default handling
   - Working data model for testing

3. **Striped (In-Memory) Layer** - `zbra-core/src/striped.rs`
   - Column structs: `Unit`, `Int`, `Double`, `Enum`, `Struct`, `Nested`
   - Vector storage with zero-copy operations
   - Performance-critical columnar representation

4. **Conversion Pipeline**
   - Logical ↔ Striped transformations
   - Bridge between human-readable and performance formats

5. **Binary Layer** - `zbra-core/src/binary.rs`
   - Serialization/deserialization
   - Compression pipeline (frame-of-reference, zig-zag, bit-packing)

## Core Type System Implementation

### Phase 1 - MVP Types (Essential)
1. `Unit` - null/empty values
2. `Int64` - with date/time encodings  
3. `Float64` - IEEE 754 doubles
4. `Binary` - byte arrays with UTF-8 support
5. `Array<T>` - homogeneous sequences
6. `Struct` - named field records

### Phase 2 - Extensions (Nice-to-Have)
1. `Map<K,V>` - key-value pairs
2. `Enum` - tagged unions (sum types)
3. `Nested` - recursive table embedding

## Testing Strategy

### Framework
- **Property-based testing** with `proptest` (Rust's QuickCheck equivalent)
- **High-volume testing** (1000+ test cases per property)
- **Roundtrip testing** as core validation strategy

### Test Structure
```
zbra-core/src/
├── types.rs          # Core types + unit tests
├── logical.rs        # + unit tests 
├── striped.rs        # + unit tests
└── binary.rs         # + unit tests

tests/
├── roundtrip.rs      # Cross-layer integration tests
├── schema.rs         # Schema validation tests
└── compatibility.rs  # Schema evolution tests

benches/              # Performance benchmarks
examples/             # Usage examples
```

### Core Testing Pattern
**Three-layer roundtrip validation:**
```rust
logical -> striped -> binary -> striped -> logical
```

### Test Categories
1. **Roundtrip Tests** (Critical)
   - Logical ↔ Striped ↔ Binary conversions
   - Full pipeline integrity validation

2. **Schema Evolution Tests**
   - Backward compatibility
   - Schema expansion/contraction
   - Default value handling

3. **Error Boundary Tests**
   - Invalid schema rejection
   - Data/schema mismatch handling
   - Type coercion failures

4. **Integration Tests**
   - CLI roundtrip testing
   - Golden file validation
   - Cross-format compatibility

## Implementation Timeline

### Week 1-2: Foundation ✅ **COMPLETED**
- [x] Core type definitions (`types.rs`)
- [x] Basic roundtrip test framework
- [x] MVP types: `Unit`, `Int64`, `Float64`, `Binary`

### Week 3: Logical Layer ✅ **COMPLETED**
- [x] Schema validation & error handling
- [x] JSON-like representation
- [x] Default value system

### Week 4: Striped Layer ✅ **COMPLETED**
- [x] Columnar in-memory format
- [x] Vector storage implementation
- [x] Zero-copy operations

### Week 5: Conversion Pipeline ✅ **COMPLETED**
- [x] Logical ↔ Striped transformations
- [x] Performance optimization
- [x] Memory efficiency validation

### Week 6: Binary Layer ✅ **COMPLETED**
- [x] Serialization format
- [x] Basic compression pipeline
- [x] Full roundtrip validation

### Week 7+: Extensions (In Progress)
- [x] Basic CLI tool implementation
- [x] Binary file format (.zbra files)
- [x] CLI support for all format conversions
- [ ] Complex types (`Map`, `Enum`, `Nested`)
- [ ] Advanced compression (SIMD optimization)
- [ ] Performance benchmarking

## Testing Quality Metrics

- **100% roundtrip coverage** for all type combinations
- **95% error path coverage**
- **Performance regression detection** (±5%)
- **1000+ test cases** per property (following Zebra's approach)

## Dependencies

### Required Crates
- `proptest` - Property-based testing
- `serde` - Serialization framework
- `bstr` - Binary string handling (already in Cargo.toml)

### Future Dependencies
- SIMD crates for compression optimization
- Compression libraries (snappy, etc.)
- Benchmarking frameworks

## Key Principles

1. **Test-driven development** - Write roundtrip tests before implementation
2. **Incremental validation** - Each layer must work before moving to next
3. **Performance awareness** - Profile and benchmark throughout development
4. **Type safety first** - Leverage Rust's type system for correctness
5. **Backwards compatibility** - Maintain compatibility with original Zebra format