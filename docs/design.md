# Zbra Design Document

## Performance Tradeoffs and Future Optimizations

### Current Performance Characteristics

**Zebra vs Arrow Tradeoffs:**

**Zebra optimized for:**
- Storage efficiency (superior compression ratios)
- Streaming large datasets with bounded memory
- Complex nested data with full sum types
- Memory-constrained scenarios

**Arrow optimized for:**
- In-memory analytical query performance
- Vectorized operations with SIMD
- Simple write operations
- Memory-rich analytics workloads

### SIMD Optimization Opportunities

**Current State:**
- Basic SSE4.2 compilation flag enabled
- Limited use of `__builtin_clzll()` for bit manipulation
- No explicit SIMD vectorization in performance-critical paths

**Major Optimization Targets:**

1. **Integer Packing/Unpacking (BP64) - High Priority**
   - Current: Sequential 64-element chunks
   - Opportunity: AVX2 (4x int64) or AVX-512 (8x int64) vectorization
   - Expected: 4-8x speedup for compression/decompression

2. **Zig-Zag Encoding/Decoding - High Priority**
   - Current: Scalar bit operations `(n >> 1) ^ (-(n & 1))`
   - Opportunity: Vectorized bit manipulation
   - Expected: 4-8x speedup

3. **Min/Max Finding - Medium Priority**
   - Current: Sequential loops in `zebra_midpoint()`
   - Opportunity: Horizontal SIMD min/max operations
   - Expected: 8-16x speedup

4. **Array Operations - Medium Priority**
   - Memory copies, comparisons, merging operations
   - Vectorized comparisons and conditional moves

### Recommended SIMD Implementation Strategy

**Phase 1: Runtime SIMD Detection**
```rust
enum SimdLevel {
    None,
    Sse42,
    Avx2,
    Avx512,
}
```

**Phase 2: BP64 Vectorization**
- Focus on core bit packing operations
- AVX2 and AVX-512 implementations with scalar fallback

**Phase 3: Utility Operations**
- Zig-zag encoding, min/max finding, transformations

**Phase 4: Advanced Optimizations**
- Vectorized merge operations
- SIMD string operations
- Cache-aware prefetching

### Streaming vs In-Memory Performance

**Key Insight:** Zebra's streaming architecture should translate well to in-memory processing by adjusting chunk sizes:

- **Small chunks:** Streaming mode for memory-constrained scenarios
- **Large chunks:** In-memory mode when memory allows
- **Same benefits:** Zero-copy operations, vectorized columns, memory efficiency

**Potential advantages over Arrow:**
- Better memory utilization through compression
- Richer type system (full sum types)
- Configurable memory/performance tradeoffs

### Compression Algorithm Modernization

**Current Zbra Pipeline (2015-era):**
1. Frame-of-reference encoding (offset by midpoint)
2. Zig-zag encoding (signed to unsigned conversion)
3. BP64 bit packing (64-element chunks with minimum bits)
4. Snappy compression (for string/binary data)

**Modern 2025 Alternatives:**

**Enhanced Encoding:**
- **Daniel Lemire's FastPFOR** - Successor to BP64 with better performance
- **Stream VByte** - More efficient than zig-zag for small integers
- **Delta encoding + Delta-of-delta** - Optimized for time series patterns
- **Dictionary encoding** - For repeated string/categorical values

**Advanced Compression:**
- **Zstd** - Better compression ratios than Snappy, similar speed
- **LZ4** - Ultra-fast decompression for hot data
- **Brotli** - Maximum compression for cold storage

**Potential Dependencies:**
```toml
# Modern compression libraries
snap = "1.1"           # Snappy (compatibility)
zstd = "0.13"          # Modern general compression  
lz4_flex = "0.11"      # Ultra-fast decompression
brotli = "3.4"         # Maximum compression

# Modern encoding algorithms
fastpfor = "0.1"       # Lemire's integer encoding
stream-vbyte = "0.1"   # Modern variable-byte encoding
roaring = "0.10"       # Compressed bitmaps
```

**Implementation Strategy:**
- Maintain compatibility with original Zebra format (zbra is the successor)
- Add modern algorithms as opt-in alternatives
- Benchmark against original implementation
- Support hybrid compression (different algorithms per column type)

### Format Hierarchy and Architecture

**Zbra's Four-Layer Format Design:**

Zbra implements a carefully designed four-layer architecture where each format serves a specific purpose:

1. **JSON Format** (Human Interface)
   - **Purpose**: Human-authored data input and schema definitions
   - **Usage**: Initial data creation, testing, debugging
   - **Performance**: Not optimized for speed, designed for readability

2. **Logical Format** (Internal Representation)
   - **Purpose**: Internal validation and type checking
   - **Usage**: Intermediate processing, schema validation
   - **Performance**: Optimized for correctness, not speed

3. **Striped Format** (Development/Debugging)
   - **Purpose**: JSON-serialized columnar view for inspection and debugging
   - **Usage**: Understanding data layout, debugging conversions, development
   - **Performance**: JSON serialization for human readability - NOT for production storage
   - **Key Insight**: This is columnar data represented as JSON for debugging purposes

4. **Binary Format** (Production Storage)
   - **Purpose**: Efficient compressed disk/wire format
   - **Usage**: Production storage, data exchange, performance-critical applications
   - **Performance**: Optimized for space and speed with compression pipeline

**Format Selection Guidelines:**
- **Development**: JSON → Striped (for debugging) → Binary (for testing)
- **Production**: JSON → Binary (direct, bypassing striped JSON)
- **Debugging**: Any format → Striped (for inspection) → Continue processing

### Parsing Strategy and Performance

**Zbra's Zero-Parse Architecture:**

**Primary Data Path (Performance Critical):**
```
Binary → Striped → Analytics
```
- Binary format designed for DMA-like transfers
- Direct memory mapping with minimal processing
- Header contains schema, data blocks are raw compressed bytes
- No JSON parsing in hot paths - just decompression

**Secondary Tooling Path (Human Interface):**
```
JSON Schema → Logical → Striped
JSON Text Format → Logical → Striped  
```
- JSON used only for schema definitions (.zschema files)
- Text format (.ztxt) for debugging and human readability
- Logical layer is restructuring/validation, not primary data representation

**Modern JSON Parsing Opportunities (Lemire's simdjson):**

**Where it COULD help:**
- Schema parsing (.zschema files) - one-time cost
- Text format import (.ztxt files) - tooling/debugging
- Metadata and configuration parsing
- CLI tool operations

**Where it's NOT needed:**
- Primary data path (Binary ↔ Striped conversions)
- Hot performance paths (compression/decompression)
- Runtime data operations

**Key Insight:** Zbra optimizes for maximum DMA-like transfers in the performance path, using JSON only for human-facing tooling. SIMD JSON parsing would improve developer experience but not core analytical performance.

### Future Research Areas

1. **SIMD Compression Pipeline**
   - End-to-end vectorization of frame-of-reference + zig-zag + bit-packing
   - SIMD-aware block layouts

2. **Adaptive Compression**
   - Runtime selection of compression strategies based on data patterns
   - SIMD-optimized format detection

3. **Columnar Query Engine**
   - SIMD-vectorized analytical operations
   - JIT compilation for hot paths

4. **Memory Management**
   - NUMA-aware allocation
   - SIMD-optimized memory pools

### Advanced Compression Architecture

#### Per-Column Compression (Following Apache Arrow)

**Current State:**
- Single compression algorithm applied to entire dataset
- Basic Zstd compression with configurable level

**FUTURE Enhancement - Per-Column Compression:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnCompressionConfig {
    /// Column name or index
    pub column_id: String,
    /// Compression algorithm for this specific column
    pub algorithm: CompressionAlgorithm,
    /// Column-specific compression parameters
    pub params: CompressionParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionParams {
    /// Dictionary encoding threshold (repeated values)
    pub dict_threshold: f32,
    /// Run-length encoding threshold (consecutive values)
    pub rle_threshold: f32,
    /// Bit-packing optimization for integers
    pub bit_packing: bool,
}
```

**Column-Specific Optimization Strategies:**
- **Integer columns**: Frame-of-reference + zig-zag + BP64 + Zstd
- **String columns**: Dictionary encoding + Zstd
- **Categorical columns**: Dictionary encoding + bit-packing
- **Time series**: Delta encoding + delta-of-delta + Zstd
- **Binary data**: Direct Zstd or LZ4 for hot paths

**Apache Arrow Compatibility:**
Arrow supports very granular compression control at the column level, allowing different algorithms per column type. This approach would make zbra compatible with Arrow's compression model while maintaining zbra's superior compression ratios.

#### Per-Chunk Adaptive Compression

**Key Insight:** Different data chunks within the same column may benefit from different compression strategies.

**FUTURE Enhancement - Per-Chunk Compression:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkCompressionMetadata {
    /// Chunk identifier
    pub chunk_id: u32,
    /// Detected data pattern
    pub pattern: DataPattern,
    /// Chosen compression algorithm
    pub algorithm: CompressionAlgorithm,
    /// Compression ratio achieved
    pub ratio: f32,
    /// Decompression speed hint
    pub speed_hint: SpeedHint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataPattern {
    /// Mostly sequential integers
    Sequential,
    /// High entropy random data
    Random,
    /// Repeated values
    Repetitive,
    /// Sparse data with many nulls
    Sparse,
    /// Time series with trends
    TimeSeries,
}
```

**Adaptive Algorithm Selection:**
- **Sequential data**: Frame-of-reference + bit-packing
- **Random data**: Direct Zstd compression
- **Repetitive data**: Run-length encoding + dictionary
- **Sparse data**: Bitmap compression + value arrays
- **Time series**: Delta-of-delta + specialized encoding

**Implementation Strategy:**
1. **Analysis Phase**: Sample first N values to detect pattern
2. **Algorithm Selection**: Choose optimal compression based on pattern
3. **Metadata Storage**: Store compression choice in chunk header
4. **Decompression**: Use stored metadata to select decompression algorithm

#### Runtime Compression Configuration

**FUTURE Enhancement - Dynamic Compression:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCompressionConfig {
    /// Global fallback compression
    pub default: CompressionAlgorithm,
    /// Per-column overrides
    pub column_configs: Vec<ColumnCompressionConfig>,
    /// Enable adaptive per-chunk compression
    pub adaptive_chunks: bool,
    /// Performance vs compression trade-off
    pub optimization_target: OptimizationTarget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OptimizationTarget {
    /// Maximize compression ratio
    MinimizeSpace,
    /// Maximize decompression speed
    MaximizeSpeed,
    /// Balance compression and speed
    Balanced,
}
```

**Configuration Sources:**
- **File headers**: Embedded compression metadata
- **Runtime detection**: Automatic pattern recognition
- **User overrides**: API-specified compression preferences
- **Profile-guided**: Historical performance data

#### Memory and Performance Implications

**Memory Efficiency:**
- **Per-column**: Allows optimal memory usage per data type
- **Per-chunk**: Enables streaming with bounded memory
- **Adaptive**: Reduces memory pressure through better compression

**Performance Characteristics:**
- **Compression time**: Slightly higher due to analysis overhead
- **Decompression speed**: Potentially faster with algorithm specialization
- **Storage efficiency**: Significantly better compression ratios
- **Query performance**: Faster due to reduced I/O from better compression

**SIMD Optimization Opportunities:**
- **Pattern detection**: Vectorized data analysis
- **Algorithm selection**: SIMD-accelerated heuristics
- **Compression pipelines**: Vectorized encoding/decoding
- **Memory operations**: SIMD-optimized data movement

#### Production Implementation Path

**Phase 1: Per-Column Compression**
- Implement column-specific compression algorithms
- Add compression metadata to binary format headers
- Maintain backward compatibility with current format

**Phase 2: Adaptive Chunks**
- Add chunk-level compression analysis
- Implement pattern detection algorithms
- Add chunk compression metadata

**Phase 3: Runtime Configuration**
- Implement dynamic compression selection
- Add performance profiling and feedback
- Optimize for different workload patterns

**Phase 4: Advanced Features**
- Cross-column compression (shared dictionaries)
- Predictive compression based on schema
- Integration with query engines for optimal decompression

### Performance Measurement Strategy

1. **Establish baselines** for current scalar implementations
2. **Incremental SIMD adoption** with A/B performance testing
3. **Cross-platform benchmarks** (x86_64, ARM64)
4. **Memory usage profiling** alongside performance metrics
5. **Comparison benchmarks** against Arrow for equivalent operations