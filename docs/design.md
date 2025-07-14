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

### Performance Measurement Strategy

1. **Establish baselines** for current scalar implementations
2. **Incremental SIMD adoption** with A/B performance testing
3. **Cross-platform benchmarks** (x86_64, ARM64)
4. **Memory usage profiling** alongside performance metrics
5. **Comparison benchmarks** against Arrow for equivalent operations