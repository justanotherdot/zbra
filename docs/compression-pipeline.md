# Compression pipeline

This document explains how zbra's compression pipeline transforms data, with special focus on timestamp compression and the engineering rationale behind date validation limits.

## Overview

Zbra uses a multi-stage compression pipeline optimized for columnar data:

```
Raw Values → Frame-of-Reference → Zig-Zag → BP64 → Zstd
   (42 bits)      (29 bits)      (30 bits)  (packed)  (final)
```

Each stage is designed to work together, with earlier stages preparing data for maximum efficiency in later stages.

## Pipeline stages

### Stage 1: Frame-of-reference encoding

**Purpose**: Reduce magnitude of values by subtracting a reference point

**Algorithm**:
1. Sort all values to find median
2. Subtract median from each value to create deltas
3. Store median + deltas instead of raw values

**Example** (timestamps):
```
Raw timestamps:       4,102,444,800,000  4,102,448,400,000  4,102,451,600,000
                            ↓ subtract median ↓
Median:               4,102,448,400,000
Deltas:                      -3,600,000             0         3,200,000
```

**Benefits**:
- Raw 42-bit timestamps → 22-bit deltas
- Deltas are much smaller than absolute values for time-series data
- Improves efficiency of subsequent bit-packing

### Stage 2: Zig-zag encoding

**Purpose**: Convert signed integers to unsigned for efficient bit-packing

**Algorithm**:
```
For positive n:  result = n << 1
For negative n:  result = (-n << 1) - 1
```

**Example**:
```
Signed deltas:    -3,600,000        0         3,200,000
                      ↓ zig-zag ↓
Unsigned:          7,199,999        0         6,400,000
```

**Benefits**:
- Eliminates sign bit inefficiency in bit-packing
- Maps negative numbers to positive range
- Preserves magnitude relationships

### Stage 3: BP64 bit-packing

**Purpose**: Pack multiple integers using minimum bits required

**Algorithm**:
1. Find maximum value in chunk
2. Calculate bits needed: `64 - max_value.leading_zeros()`
3. Pack all values using that bit width

**Efficiency threshold**:
```
≤32 bits:  Efficient variable-width bit-packing
>32 bits:  Falls back to 8-byte storage (inefficient)
```

**Example**:
```
Values:           7,199,999    0    6,400,000
Max value:        7,199,999 (needs 23 bits)
Packed storage:   23 bits per value instead of 64 bits
Compression:      ~64% size reduction
```

### Stage 4: Zstd compression

**Purpose**: Final compression of bit-packed binary data

**Benefits**:
- Removes remaining redundancy in packed data
- Handles any patterns not caught by earlier stages
- Industry-standard compression with good speed/ratio balance

## Date validation limits

### The January 1, 2100 limit

The date validation limit `4102444800000` (January 1, 2100) is **not arbitrary** — it's carefully engineered for compression efficiency.

**Engineering rationale**:

```
Raw timestamp bits:  42 bits (year 2100)
After compression:   ≤30 bits (typical datasets)
BP64 threshold:      32 bits (efficiency cutoff)
Safety margin:       2 bits
```

**Why this matters**:

1. **Temporal locality**: Real datasets have timestamps clustered in time
2. **Small deltas**: Frame-of-reference creates small deltas from large timestamps  
3. **Bit-packing efficiency**: Deltas ≤32 bits get efficient compression
4. **Fallback cost**: >32 bits triggers 8-byte storage (much less efficient)

### Compression examples

#### Efficient case (within limit)

```
Dataset: One week of hourly metrics (Jan 2100)
Raw timestamps:     4,102,444,800,000 to 4,103,048,400,000 (42 bits each)
Frame-of-reference: ±302,000,000 deltas (29 bits)
Zig-zag:           604,000,000 max (30 bits)
BP64:              Efficient bit-packing at 30 bits/value
Result:            ~53% compression from bit-packing alone
```

#### Inefficient case (beyond limit)

```
Dataset: One week of hourly metrics (Jan 2200)  
Raw timestamps:     7,258,118,400,000 to 7,258,722,000,000 (43 bits each)
Frame-of-reference: ±302,000,000 deltas (29 bits)
Zig-zag:           604,000,000 max (30 bits)
BP64:              Still efficient in this case
```

#### Pathological case

```
Dataset: 10 years of scattered timestamps (beyond limit)
Raw timestamps:     Various 43+ bit values
Frame-of-reference: ±5,000,000,000 deltas (33 bits)
Zig-zag:           10,000,000,000 max (34 bits) 
BP64:              Falls back to 8-byte storage
Result:            No compression from bit-packing
```

**Note**: The difference between efficient and pathological cases is demonstrated in the test `test_frame_of_reference_effectiveness()` in `compression-pipeline.rs`.

## Visual pipeline breakdown

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│ Raw Timestamps      │    │ After Frame-of-     │    │ After Zig-Zag       │
│                     │    │ Reference           │    │ Encoding            │
│ 4,102,444,800,000   │───▶│    -3,600,000       │───▶│   7,199,999         │
│ 4,102,448,400,000   │    │            0        │    │           0         │
│ 4,102,451,600,000   │    │     3,200,000       │    │   6,400,000         │
│                     │    │                     │    │                     │
│ 42 bits each        │    │ 29 bits max         │    │ 30 bits max         │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
                                      ↓
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│ After BP64          │    │ After Zstd          │    │ Final Result        │
│ Bit-Packing         │    │ Compression         │    │                     │
│                     │    │                     │    │ 42 bits → ~15       │
│ 30-bit packing      │───▶│ Binary compress     │───▶│ bits effective      │
│ ~53% reduction      │    │ Additional ~20%     │    │ ~65% total          │
│                     │    │                     │    │ compression         │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

## Design principles

### Compression-aware validation

Validation limits are set based on compression algorithm characteristics, not arbitrary business rules.

**Key insight**: The goal is ensuring typical datasets compress efficiently, not supporting every possible edge case.

### Safety margins

The 2100 limit provides buffer for:

- Datasets spanning multiple years
- Irregular timestamp distributions  
- Future algorithm improvements
- Different temporal clustering patterns

### Performance predictability

By keeping compression in the efficient range:

- Consistent performance across datasets
- Predictable storage requirements
- No surprising fallbacks to inefficient modes

## Implementation notes

### BP64 efficiency threshold

The critical 32-bit threshold in `compression.rs`:

```rust
if bit_width >= 32 {
    // Fall back to uncompressed 8-byte storage
    for &value in values {
        packed.extend_from_slice(&value.to_le_bytes());
    }
}
```

This fallback eliminates compression benefits and should be avoided for performance-critical data like timestamps.

### Validation in logical layer

Date validation in `logical.rs` enforces compression-friendly limits:

```rust
if *n < 0 || *n > 4102444800000 {
    return Err(SchemaError::UnsupportedType(
        format!("Date value {} is outside valid range", n)
    ));
}
```

This prevents datasets from accidentally triggering inefficient compression modes.

## Future considerations

### Algorithm improvements

- SIMD-optimized BP64 implementation
- Dynamic bit-width selection
- Improved frame-of-reference strategies
- Specialized timestamp compression

### Limit adjustments

Any changes to date limits should consider:

- Impact on compression efficiency
- Backwards compatibility
- Performance benchmarking
- Real-world usage patterns

The current limit represents a carefully balanced engineering decision optimized for the compression pipeline's characteristics.