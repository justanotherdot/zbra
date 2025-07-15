# Zbra Compression Benchmarks

This directory contains performance benchmarks for the zbra compression pipeline.

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Use the benchmark runner script
./bin/bench                    # Run all benchmarks
./bin/bench compression        # Run compression algorithm benchmarks
./bin/bench streaming          # Run streaming I/O benchmarks
./bin/bench quick             # Run quick benchmark suite

# Run specific benchmark groups
cargo bench frame_of_reference
cargo bench zig_zag  
cargo bench bp64
cargo bench zstd_compression
cargo bench full_int_compression
cargo bench binary_format_roundtrip
cargo bench compression_ratios
cargo bench streaming_write
cargo bench streaming_read
cargo bench time_series_streaming
cargo bench log_streaming

# Generate HTML reports
cargo bench --features html_reports
```

## Benchmark Groups

### Individual Algorithm Benchmarks

**`frame_of_reference`** - Frame-of-reference encoding performance
- Tests different data patterns: sequential, random, clustered, time series
- Measures both encode and decode performance
- Shows how data distribution affects compression speed

**`zig_zag`** - Zig-zag encoding performance  
- Tests mixed positive/negative, all positive, all negative data
- Measures conversion between signed and unsigned integers
- Shows impact of sign distribution on performance

**`bp64`** - BP64 bit-packing performance
- Tests different bit-width scenarios (4-bit, 8-bit, 16-bit, full range)
- Measures both pack and unpack performance
- Shows how value range affects compression efficiency

**`zstd_compression`** - Zstd compression performance
- Tests different compression levels (1, 3, 9, 22)
- Tests different data types (text, random, repetitive)
- Measures both compression and decompression speed

### Pipeline Benchmarks

**`full_int_compression`** - Complete integer compression pipeline
- Tests frame-of-reference → zig-zag → BP64 → final result
- Measures full compression and decompression roundtrip
- Shows end-to-end performance for different data patterns

**`binary_format_roundtrip`** - Binary format read/write performance
- Tests complete file serialization/deserialization
- Compares no compression vs Zstd compression
- Measures real-world file I/O performance

**`compression_ratios`** - Compression effectiveness measurement
- Measures both speed and compression ratio
- Tests different data patterns for compression effectiveness
- Shows trade-offs between speed and compression

### Streaming I/O Benchmarks

**`streaming_write`** - Streaming write performance
- Tests chunked data writing with different compression settings
- Measures throughput for different column counts and row counts
- Compares no compression vs Zstd compression
- Simulates real-world streaming scenarios

**`streaming_read`** - Streaming read performance
- Tests chunked data reading with different compression settings
- Measures deserialization throughput
- Tests memory usage patterns during streaming
- Validates streaming architecture benefits

**`time_series_streaming`** - Time series specific streaming
- Tests realistic time series data patterns
- Multiple series with timestamps
- Optimized for temporal data compression
- Measures both read and write performance

**`log_streaming`** - Log data streaming
- Tests structured log data (timestamp, level, message)
- Mixed data types (integers, strings)
- Realistic log message patterns
- Measures compression effectiveness for log data

## Data Patterns

Benchmarks test various data patterns to understand compression performance:

**Sequential Data** - `0, 1, 2, 3, 4, ...`
- Best case for frame-of-reference encoding
- Excellent compression ratios
- Realistic for counter/ID data

**Random Data** - Pseudo-random values
- Worst case for compression
- Tests compression overhead
- Realistic for hash values/UUIDs

**Clustered Data** - Values grouped around centers
- Realistic for sensor data/measurements
- Good compression with frame-of-reference
- Tests clustering benefits

**Time Series Data** - Monotonic timestamps
- Realistic for time-based data
- Excellent compression due to temporal locality
- Tests delta encoding benefits

## Performance Expectations

### Frame-of-Reference Encoding
- **Sequential data**: Very fast, linear time complexity
- **Random data**: Slower due to median calculation
- **Clustered data**: Good performance and compression
- **Time series**: Excellent performance, minimal deltas

### Zig-Zag Encoding
- **Mixed signs**: Best compression improvement
- **All positive**: Minimal overhead
- **All negative**: Moderate compression improvement

### BP64 Bit-Packing
- **Small values (4-bit)**: Excellent compression, fast
- **Medium values (8-bit)**: Good compression, fast
- **Large values (16-bit)**: Moderate compression
- **Full range (64-bit)**: Minimal compression, overhead

### Zstd Compression
- **Level 1**: Fast compression, moderate ratios
- **Level 3**: Balanced speed/compression (default)
- **Level 9**: Slower compression, better ratios
- **Level 22**: Very slow, best ratios

## Expected Results

### Compression Ratios
- **Sequential integers**: 10-50x compression
- **Time series**: 20-100x compression  
- **Clustered data**: 5-20x compression
- **Random data**: 1-2x compression (overhead)

### Performance Targets
- **Frame-of-reference**: >1M elements/second
- **Zig-zag**: >10M elements/second
- **BP64**: >1M elements/second
- **Full pipeline**: >100K elements/second

## Optimization Notes

### Current Implementation
- Scalar implementations (no SIMD)
- Basic bit-packing algorithm
- Standard Zstd compression
- Single-threaded processing

### Future Optimizations
- SIMD vectorization for hot paths
- Parallel processing for large datasets
- Cache-friendly memory layouts
- Specialized algorithms for common patterns

## Benchmark Tips

### Accurate Measurements
- Run benchmarks in release mode
- Close other applications to reduce noise
- Run multiple times and average results
- Use consistent hardware/environment

### Interpreting Results
- Focus on throughput (elements/second)
- Compare relative performance between patterns
- Consider both speed and compression ratio
- Look for performance regressions

### Optimization Workflow
1. Run baseline benchmarks
2. Implement optimization
3. Run benchmarks again
4. Compare results and validate improvements
5. Profile if performance is unexpectedly poor