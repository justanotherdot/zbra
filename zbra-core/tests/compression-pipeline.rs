// Verify compression pipeline works as documented

use zbra_core::compression::*;

/// Test the compression pipeline on realistic timestamp data
#[test]
fn test_timestamp_compression_pipeline() {
    // Example from documentation: one week of hourly data starting Jan 1, 2100
    let base_time = 4102444800000i64; // Jan 1, 2100
    let mut timestamps = Vec::new();

    // Generate 168 hours (1 week) with some jitter
    for i in 0..168 {
        let timestamp = base_time + (i * 3600000); // Add hours in milliseconds
        timestamps.push(timestamp);
    }

    println!(
        "Testing compression pipeline with {} timestamps",
        timestamps.len()
    );
    println!(
        "Range: {} to {}",
        timestamps[0],
        timestamps[timestamps.len() - 1]
    );

    // Step 1: Frame-of-reference encoding
    let (midpoint, deltas) = frame_of_reference_encode(&timestamps);
    let max_delta_magnitude = deltas.iter().map(|&d| d.abs()).max().unwrap();
    let delta_bits = if max_delta_magnitude == 0 {
        1
    } else {
        64 - max_delta_magnitude.leading_zeros()
    };

    println!("After frame-of-reference:");
    println!("  Midpoint: {}", midpoint);
    println!("  Max delta magnitude: {}", max_delta_magnitude);
    println!("  Delta bits needed: {}", delta_bits);

    // Should be much smaller than 42 bits for this dataset
    assert!(
        delta_bits <= 32,
        "Deltas should fit in ≤32 bits for efficient compression"
    );

    // Step 2: Zig-zag encoding
    let unsigned_deltas = zig_zag_encode(&deltas);
    let max_unsigned = *unsigned_deltas.iter().max().unwrap();
    let unsigned_bits = if max_unsigned == 0 {
        1
    } else {
        64 - max_unsigned.leading_zeros()
    };

    println!("After zig-zag encoding:");
    println!("  Max unsigned value: {}", max_unsigned);
    println!("  Unsigned bits needed: {}", unsigned_bits);

    // Should still be ≤32 bits
    assert!(unsigned_bits <= 32, "Zig-zag values should fit in ≤32 bits");

    // Step 3: BP64 bit-packing
    let packed = bp64_pack(&unsigned_deltas).unwrap();
    let original_size = timestamps.len() * 8; // 8 bytes per i64
    let packed_size = packed.len();
    let compression_ratio = original_size as f64 / packed_size as f64;

    println!("After BP64 bit-packing:");
    println!("  Original size: {} bytes", original_size);
    println!("  Packed size: {} bytes", packed_size);
    println!("  Compression ratio: {:.2}x", compression_ratio);

    // Should achieve significant compression
    assert!(
        compression_ratio > 1.5,
        "Should achieve >1.5x compression from bit-packing"
    );

    // Verify roundtrip works
    let unpacked = bp64_unpack(&packed, timestamps.len()).unwrap();
    let decoded_deltas = zig_zag_decode(&unpacked);
    let recovered_timestamps = frame_of_reference_decode(midpoint, &decoded_deltas);

    assert_eq!(
        timestamps, recovered_timestamps,
        "Roundtrip should preserve data exactly"
    );
}

/// Test that the 32-bit threshold actually matters
#[test]
fn test_compression_efficiency_threshold() {
    // Test efficient case: small values (≤32 bits)
    let small_values: Vec<u64> = (0..64).map(|i| i * 1000).collect();
    let small_packed = bp64_pack(&small_values).unwrap();
    let small_ratio = (small_values.len() * 8) as f64 / small_packed.len() as f64;

    // Test inefficient case: large values (>32 bits)
    let large_values: Vec<u64> = (0..64).map(|i| (1u64 << 40) + i).collect(); // 40+ bits
    let large_packed = bp64_pack(&large_values).unwrap();
    let large_ratio = (large_values.len() * 8) as f64 / large_packed.len() as f64;

    println!("Small values (≤32 bits): {:.2}x compression", small_ratio);
    println!("Large values (>32 bits): {:.2}x compression", large_ratio);

    // Small values should compress much better
    assert!(
        small_ratio > 2.0,
        "Small values should achieve >2x compression"
    );
    assert!(
        large_ratio < 1.1,
        "Large values should achieve minimal compression"
    );
    assert!(
        small_ratio > large_ratio * 2.0,
        "Small values should compress much better than large values"
    );
}

/// Test frame-of-reference effectiveness on timestamps
///
/// This test demonstrates the core engineering insight behind the date validation limits:
/// - Clustered timestamps (typical use case) compress efficiently (≤32 bits)
/// - Scattered timestamps (pathological case) exceed the efficiency threshold (>32 bits)
///
/// This justifies the Jan 1, 2100 limit: it ensures typical datasets stay in the
/// efficient compression range while preventing accidental inefficient compression.
#[test]
fn test_frame_of_reference_effectiveness() {
    // Create timestamps clustered around Jan 1, 2100
    let base = 4102444800000i64;
    let clustered_timestamps: Vec<i64> = (0..100)
        .map(|i| base + (i * 60000)) // Every minute for 100 minutes
        .collect();

    // Test frame-of-reference on clustered data
    let (_midpoint, deltas) = frame_of_reference_encode(&clustered_timestamps);
    let max_delta = deltas.iter().map(|&d| d.abs()).max().unwrap();
    let delta_bits = if max_delta == 0 {
        1
    } else {
        64 - max_delta.leading_zeros()
    };

    println!("Clustered timestamps frame-of-reference:");
    println!(
        "  Original max: {}",
        clustered_timestamps.iter().max().unwrap()
    );
    println!(
        "  Original bits: {}",
        64 - clustered_timestamps.iter().max().unwrap().leading_zeros()
    );
    println!("  Delta max: {}", max_delta);
    println!("  Delta bits: {}", delta_bits);

    // Verify frame-of-reference reduces bit requirements for clustered data
    let raw_bits = 64 - clustered_timestamps.iter().max().unwrap().leading_zeros();
    assert!(
        delta_bits < raw_bits,
        "Frame-of-reference should reduce bit requirements"
    );
    assert!(
        delta_bits <= 32,
        "Clustered deltas should stay within BP64 efficiency range (≤32 bits)"
    );

    // Compare to scattered timestamps spanning years
    let scattered_timestamps: Vec<i64> = vec![
        1577836800000, // 2020
        4102444800000, // 2100
        7258118400000, // 2200
    ];

    let (_, scattered_deltas) = frame_of_reference_encode(&scattered_timestamps);
    let scattered_max_delta = scattered_deltas.iter().map(|&d| d.abs()).max().unwrap();
    let scattered_delta_bits = if scattered_max_delta == 0 {
        1
    } else {
        64 - scattered_max_delta.leading_zeros()
    };

    println!("Scattered timestamps frame-of-reference:");
    println!("  Delta max: {}", scattered_max_delta);
    println!("  Delta bits: {}", scattered_delta_bits);

    // Verify scattered data performs worse than clustered data
    assert!(
        scattered_delta_bits > delta_bits,
        "Scattered timestamps should need more bits than clustered"
    );

    // The key insight: this difference justifies the validation limits
    assert!(
        delta_bits <= 32,
        "Clustered data should stay in efficient range"
    );
    assert!(
        scattered_delta_bits > 32,
        "Scattered data should exceed efficient range (>32 bits)"
    );
}

/// Test full integer compression pipeline
#[test]
fn test_full_integer_compression_pipeline() {
    // Use the same timestamp data from our examples
    let base_time = 4102444800000i64;
    let values: Vec<i64> = (0..168).map(|i| base_time + (i * 3600000)).collect();

    // Compress using the full pipeline
    let compressed = compress_int_array(&values).unwrap();
    let original_size = values.len() * 8;
    let compressed_size = compressed.len();
    let ratio = original_size as f64 / compressed_size as f64;

    println!("Full compression pipeline:");
    println!("  Original: {} bytes", original_size);
    println!("  Compressed: {} bytes", compressed_size);
    println!("  Ratio: {:.2}x", ratio);

    // Should achieve good compression
    assert!(
        ratio > 1.5,
        "Full pipeline should achieve >1.5x compression"
    );

    // Verify roundtrip
    let decompressed = decompress_int_array(&compressed, values.len()).unwrap();
    assert_eq!(
        values, decompressed,
        "Full pipeline roundtrip should preserve data"
    );
}

/// Test that our documented examples are accurate
#[test]
fn test_documented_compression_examples() {
    // Example from docs: "One week of hourly metrics (Jan 2100)"
    let jan_2100_base = 4102444800000i64;
    let week_end = jan_2100_base + (7 * 24 * 3600000); // Add 7 days in milliseconds

    let weekly_data: Vec<i64> = (0..168) // 7 days * 24 hours
        .map(|i| jan_2100_base + (i * 3600000)) // Hourly intervals
        .collect();

    // Test the documented claims
    let (_, deltas) = frame_of_reference_encode(&weekly_data);
    let max_delta = deltas.iter().map(|&d| d.abs()).max().unwrap();
    let delta_bits = if max_delta == 0 {
        1
    } else {
        64 - max_delta.leading_zeros()
    };

    println!("Weekly data analysis (matches docs):");
    println!("  Range: {} to {}", jan_2100_base, week_end);
    println!(
        "  Raw timestamp bits: {}",
        64 - jan_2100_base.leading_zeros()
    );
    println!("  Max delta: ±{}", max_delta);
    println!("  Delta bits: {}", delta_bits);

    // Verify documentation claims
    assert_eq!(
        64 - jan_2100_base.leading_zeros(),
        42,
        "Raw timestamps should be 42 bits (as documented)"
    );
    assert!(delta_bits <= 30, "Delta bits should be ≤30 (as documented)");
    assert!(
        delta_bits <= 32,
        "Should stay in efficient BP64 range (as documented)"
    );
}
