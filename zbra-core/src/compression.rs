// Compression algorithms for zbra binary format
//
// This module implements the core compression pipeline:
// 1. Frame-of-reference encoding (integers)
// 2. Zig-zag encoding (signed to unsigned)
// 3. BP64 bit-packing (64-element chunks)
// 4. Zstd compression (binary data)

use crate::error::{BinaryError, Result};
use serde::{Deserialize, Serialize};

/// Compression algorithms supported by zbra
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// Zstd compression with configurable level (1-22)
    Zstd { level: i32 },
    // FUTURE: Additional compression algorithms
    // Lz4,
    // Snappy,
    // Brotli { level: u32 },
}

/// Configuration for compression settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Compression for binary data (byte arrays)
    pub binary_data: CompressionAlgorithm,
    /// Compression for string data
    pub strings: CompressionAlgorithm,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            binary_data: CompressionAlgorithm::Zstd { level: 3 },
            strings: CompressionAlgorithm::Zstd { level: 3 },
        }
    }
}

/// Frame-of-reference encoding for integers
///
/// This reduces the magnitude of values by subtracting a reference point (midpoint),
/// which improves compression ratios for subsequent bit-packing.
pub fn frame_of_reference_encode(values: &[i64]) -> (i64, Vec<i64>) {
    if values.is_empty() {
        return (0, Vec::new());
    }

    // Use median as reference point for better compression
    let mut sorted = values.to_vec();
    sorted.sort();
    let midpoint = if sorted.len() % 2 == 0 {
        // Handle overflow in midpoint calculation by using wrapping arithmetic
        let a = sorted[sorted.len() / 2 - 1];
        let b = sorted[sorted.len() / 2];
        // Avoid overflow by using the average formula: a + (b - a) / 2
        a.wrapping_add(b.wrapping_sub(a) / 2)
    } else {
        sorted[sorted.len() / 2]
    };

    // Subtract midpoint from all values, handling overflow
    let deltas = values.iter().map(|&v| v.wrapping_sub(midpoint)).collect();

    (midpoint, deltas)
}

/// Decode frame-of-reference encoded values
pub fn frame_of_reference_decode(midpoint: i64, deltas: &[i64]) -> Vec<i64> {
    deltas
        .iter()
        .map(|&delta| midpoint.wrapping_add(delta))
        .collect()
}

/// Zig-zag encoding converts signed integers to unsigned
///
/// This brings small negative numbers closer to zero, improving compression.
/// Formula: (n << 1) ^ (n >> 63)
pub fn zig_zag_encode(values: &[i64]) -> Vec<u64> {
    values
        .iter()
        .map(|&n| {
            // Proper zig-zag encoding: handle overflow with wrapping
            let shifted = (n as u64) << 1;
            let sign_bit = (n >> 63) as u64;
            shifted ^ sign_bit
        })
        .collect()
}

/// Decode zig-zag encoded values back to signed integers
pub fn zig_zag_decode(values: &[u64]) -> Vec<i64> {
    values
        .iter()
        .map(|&n| {
            // Proper zig-zag decoding
            let shifted = (n >> 1) as i64;
            let sign_mask = -((n & 1) as i64);
            shifted ^ sign_mask
        })
        .collect()
}

/// BP64 bit-packing for 64-element chunks
///
/// This packs integers using the minimum number of bits required for the maximum value.
/// Currently implements a simplified version - FUTURE: optimize with SIMD
pub fn bp64_pack(values: &[u64]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    // Find maximum value to determine bit width
    let max_value = *values.iter().max().unwrap();
    let bit_width = if max_value == 0 {
        1
    } else {
        64 - max_value.leading_zeros()
    } as u8;

    let mut packed = Vec::new();
    packed.push(bit_width); // Store bit width as first byte

    if bit_width == 0 {
        return Ok(packed);
    }

    // For very large bit widths, use a simpler approach
    if bit_width >= 32 {
        // Just store as little-endian 8-byte values
        for &value in values {
            packed.extend_from_slice(&value.to_le_bytes());
        }
        return Ok(packed);
    }

    // Pack values using bit_width bits per value
    let mut bit_buffer = 0u64;
    let mut bits_in_buffer = 0u32;

    for &value in values {
        // Mask the value to fit in bit_width bits
        let mask = (1u64 << bit_width) - 1;
        let masked_value = value & mask;

        // Add value to bit buffer
        bit_buffer |= masked_value << bits_in_buffer;
        bits_in_buffer += bit_width as u32;

        // Extract complete bytes
        while bits_in_buffer >= 8 {
            packed.push(bit_buffer as u8);
            bit_buffer >>= 8;
            bits_in_buffer -= 8;
        }
    }

    // Handle remaining bits
    if bits_in_buffer > 0 {
        packed.push(bit_buffer as u8);
    }

    Ok(packed)
}

/// Unpack BP64 bit-packed values
pub fn bp64_unpack(packed: &[u8], count: usize) -> Result<Vec<u64>> {
    if packed.is_empty() {
        return Ok(Vec::new());
    }

    let bit_width = packed[0];
    if bit_width == 0 || count == 0 {
        return Ok(vec![0; count]);
    }

    let data = &packed[1..];
    let mut values = Vec::with_capacity(count);

    // For very large bit widths, read as little-endian 8-byte values
    if bit_width >= 32 {
        for i in 0..count {
            let start = i * 8;
            if start + 8 <= data.len() {
                let bytes = &data[start..start + 8];
                let value = u64::from_le_bytes(bytes.try_into().unwrap());
                values.push(value);
            } else {
                values.push(0);
            }
        }
        return Ok(values);
    }

    let mut bit_buffer = 0u64;
    let mut bits_in_buffer = 0u32;
    let mut byte_index = 0;

    let mask = (1u64 << bit_width) - 1;

    for _ in 0..count {
        // Fill buffer with enough bits
        while bits_in_buffer < bit_width as u32 && byte_index < data.len() {
            bit_buffer |= (data[byte_index] as u64) << bits_in_buffer;
            bits_in_buffer += 8;
            byte_index += 1;
        }

        // Extract value
        let value = bit_buffer & mask;
        values.push(value);

        // Remove used bits
        bit_buffer >>= bit_width;
        bits_in_buffer -= bit_width as u32;
    }

    Ok(values)
}

/// Compress binary data using the specified algorithm
pub fn compress_binary(data: &[u8], algorithm: &CompressionAlgorithm) -> Result<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd { level } => zstd::bulk::compress(data, *level)
            .map_err(|e| BinaryError::CompressionError(format!("Zstd compression failed: {}", e))), // FUTURE: Add other compression algorithms
    }
}

/// Decompress binary data using the specified algorithm
pub fn decompress_binary(data: &[u8], algorithm: &CompressionAlgorithm) -> Result<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Zstd { .. } => {
            zstd::bulk::decompress(data, data.len() * 4) // Estimate decompressed size
                .map_err(|e| {
                    BinaryError::DecompressionError(format!("Zstd decompression failed: {}", e))
                })
        } // FUTURE: Add other compression algorithms
    }
}

/// Full integer compression pipeline
pub fn compress_int_array(values: &[i64]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(Vec::new());
    }

    // Step 1: Frame-of-reference encoding
    let (midpoint, deltas) = frame_of_reference_encode(values);

    // Step 2: Zig-zag encoding
    let unsigned_values = zig_zag_encode(&deltas);

    // Step 3: BP64 bit-packing
    let packed = bp64_pack(&unsigned_values)?;

    // Combine midpoint and packed data
    let mut result = Vec::new();
    result.extend_from_slice(&midpoint.to_le_bytes());
    result.extend_from_slice(&(packed.len() as u32).to_le_bytes());
    result.extend_from_slice(&packed);

    Ok(result)
}

/// Full integer decompression pipeline
pub fn decompress_int_array(data: &[u8], count: usize) -> Result<Vec<i64>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    if data.len() < 12 {
        return Err(BinaryError::DecompressionError(
            "Invalid compressed data length".to_string(),
        ));
    }

    // Extract midpoint
    let midpoint = i64::from_le_bytes(data[0..8].try_into().unwrap());

    // Extract packed data length
    let packed_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

    if data.len() < 12 + packed_len {
        return Err(BinaryError::DecompressionError(
            "Insufficient data for packed array".to_string(),
        ));
    }

    let packed = &data[12..12 + packed_len];

    // Step 1: BP64 bit-unpacking
    let unsigned_values = bp64_unpack(packed, count)?;

    // Step 2: Zig-zag decoding
    let deltas = zig_zag_decode(&unsigned_values);

    // Step 3: Frame-of-reference decoding
    let values = frame_of_reference_decode(midpoint, &deltas);

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_frame_of_reference_roundtrip() {
        let values = vec![100, 102, 98, 101, 99, 103, 97];
        let (midpoint, deltas) = frame_of_reference_encode(&values);
        let decoded = frame_of_reference_decode(midpoint, &deltas);
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_zig_zag_roundtrip() {
        let values = vec![-5, -1, 0, 1, 5, -100, 100];
        let encoded = zig_zag_encode(&values);
        let decoded = zig_zag_decode(&encoded);
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_bp64_roundtrip() {
        let values = vec![0, 1, 2, 15, 255, 1000];
        let packed = bp64_pack(&values).unwrap();
        let unpacked = bp64_unpack(&packed, values.len()).unwrap();
        assert_eq!(values, unpacked);
    }

    #[test]
    fn test_full_int_compression_roundtrip() {
        let values = vec![100, 102, 98, 101, 99, 103, 97, -5, -1, 0];
        let compressed = compress_int_array(&values).unwrap();
        let decompressed = decompress_int_array(&compressed, values.len()).unwrap();
        assert_eq!(values, decompressed);
    }

    #[test]
    fn test_zstd_compression_roundtrip() {
        let data = b"Hello, world! This is a test string for compression.";
        let algorithm = CompressionAlgorithm::Zstd { level: 3 };
        let compressed = compress_binary(data, &algorithm).unwrap();
        let decompressed = decompress_binary(&compressed, &algorithm).unwrap();
        assert_eq!(data.to_vec(), decompressed);
    }

    proptest! {
        #[test]
        fn test_frame_of_reference_property(values in prop::collection::vec(any::<i64>(), 0..100)) {
            let (midpoint, deltas) = frame_of_reference_encode(&values);
            let decoded = frame_of_reference_decode(midpoint, &deltas);
            prop_assert_eq!(values, decoded);
        }

        #[test]
        fn test_zig_zag_property(values in prop::collection::vec(any::<i64>(), 0..100)) {
            let encoded = zig_zag_encode(&values);
            let decoded = zig_zag_decode(&encoded);
            prop_assert_eq!(values, decoded);
        }

        #[test]
        fn test_bp64_property(values in prop::collection::vec(0u64..1000u64, 0..100)) {
            let packed = bp64_pack(&values).unwrap();
            let unpacked = bp64_unpack(&packed, values.len()).unwrap();
            prop_assert_eq!(values, unpacked);
        }

        #[test]
        fn test_full_compression_property(values in prop::collection::vec(any::<i64>(), 0..100)) {
            let compressed = compress_int_array(&values).unwrap();
            let decompressed = decompress_int_array(&compressed, values.len()).unwrap();
            prop_assert_eq!(values, decompressed);
        }
    }
}
