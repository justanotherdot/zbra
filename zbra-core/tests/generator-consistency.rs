// Test generator consistency for all encoding types

mod common;

use common::*;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use zbra_core::data::{Default, Encoding, IntEncoding};
use zbra_core::logical::ValueSchema;

/// Test that our constrained generators only produce valid values
#[test]
fn test_date_generator_produces_valid_values() {
    let date_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::Date),
    };

    // Generate 1000 values and ensure they all validate
    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_value_for_schema(&date_schema);

    for _ in 0..1000 {
        let value = strategy.new_tree(&mut runner).unwrap().current();
        assert!(
            value.validate_schema(&date_schema).is_ok(),
            "Generated value {:?} should validate against date schema",
            value
        );
    }
}

/// Test that time encoding generators respect their limits
#[test]
fn test_time_encoding_generators() {
    let test_cases = vec![
        (IntEncoding::TimeSeconds, 4102444800i64), // Jan 1, 2100 in seconds
        (IntEncoding::TimeMilliseconds, 4102444800000i64), // Jan 1, 2100 in milliseconds
        (IntEncoding::TimeMicroseconds, 4102444800000000i64), // Jan 1, 2100 in microseconds
        (IntEncoding::Date, 4102444800000i64),     // Jan 1, 2100 in milliseconds
    ];

    let mut runner = proptest::test_runner::TestRunner::default();

    for (encoding, expected_max) in test_cases {
        let schema = ValueSchema::Int {
            default: Default::Allow,
            encoding: Encoding::Int(encoding.clone()),
        };

        let strategy = arb_value_for_schema(&schema);

        // Generate many values and check they're within expected ranges
        for _ in 0..100 {
            let value = strategy.new_tree(&mut runner).unwrap().current();

            if let zbra_core::data::Value::Int(n) = value {
                assert!(n >= 0, "Generated timestamp should be non-negative: {}", n);
                assert!(
                    n <= expected_max,
                    "Generated value {} for {:?} exceeds expected max {}",
                    n,
                    encoding,
                    expected_max
                );

                // Should validate successfully
                assert!(
                    value.validate_schema(&schema).is_ok(),
                    "Generated value {} should validate for {:?}",
                    n,
                    encoding
                );
            } else {
                panic!("Expected Int value, got {:?}", value);
            }
        }
    }
}

/// Test that regular int encoding allows full range
#[test]
fn test_regular_int_generator() {
    let int_schema = ValueSchema::Int {
        default: Default::Allow,
        encoding: Encoding::Int(IntEncoding::Int),
    };

    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_value_for_schema(&int_schema);

    let mut found_negative = false;
    let mut found_large_positive = false;

    // Generate many values to ensure we get good coverage
    for _ in 0..1000 {
        let value = strategy.new_tree(&mut runner).unwrap().current();

        if let zbra_core::data::Value::Int(n) = value {
            if n < 0 {
                found_negative = true;
            }
            if n > 5000000000000i64 {
                // Beyond date limits
                found_large_positive = true;
            }

            // Should always validate
            assert!(
                value.validate_schema(&int_schema).is_ok(),
                "Int value {} should always validate",
                n
            );
        } else {
            panic!("Expected Int value, got {:?}", value);
        }

        if found_negative && found_large_positive {
            break;
        }
    }

    // Regular ints should generate both negative and very large values
    assert!(
        found_negative,
        "Regular int generator should produce negative values"
    );
    assert!(
        found_large_positive,
        "Regular int generator should produce large values beyond date limits"
    );
}

/// Test schema-value pair generation consistency
#[test]
fn test_schema_value_pair_consistency() {
    let mut runner = proptest::test_runner::TestRunner::default();
    let strategy = arb_schema_and_value();

    // Generate 100 schema-value pairs and ensure they're consistent
    for _ in 0..100 {
        let (schema, value) = strategy.new_tree(&mut runner).unwrap().current();

        assert!(
            value.validate_schema(&schema).is_ok(),
            "Generated value {:?} should validate against generated schema {:?}",
            value,
            schema
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property test: all generated values for date schemas should validate
    #[test]
    fn prop_date_values_always_validate(
        value in arb_value_for_schema(&ValueSchema::Int {
            default: Default::Allow,
            encoding: Encoding::Int(IntEncoding::Date),
        })
    ) {
        let schema = ValueSchema::Int {
            default: Default::Allow,
            encoding: Encoding::Int(IntEncoding::Date),
        };

        prop_assert!(value.validate_schema(&schema).is_ok());
    }

    /// Property test: all schema-value pairs should be consistent
    #[test]
    fn prop_schema_value_pairs_consistent(
        (schema, value) in arb_schema_and_value()
    ) {
        prop_assert!(value.validate_schema(&schema).is_ok());
    }
}
