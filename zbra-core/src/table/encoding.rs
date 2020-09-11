use bstr::{ByteVec, FromUtf8Error};

// should this close over values?
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum Binary {
    Binary,
    Utf8,
}

// Potentially we want to support epochs other than 1600-03-01, and possibly
// even time zones, but we keep it super simple for now.

pub enum Int {
    Int,
    /// days since 1600-03-01
    Date,
    /// seconds since 1600-03-01
    TimeSeconds,
    /// milliseconds since 1600-03-01
    TimeMilliseconds,
    /// microseconds since 1600-03-01
    TimeMicroseconds,
}

// not sure what type to put for ByteString just yet.
// you can treat this like a firewall.
pub fn validate_binary(kind: Binary, bytes: Vec<u8>) -> Result<(), FromUtf8Error> {
    match kind {
        Binary::Binary => (),
        Binary::Utf8 => bytes.into_string().map(|_| ())?,
    };
    Ok(())
}

pub fn validate_int(kind: Int, int: i64) -> Result<(), TimeError> {
    match kind {
        Int::Int => (),
        Int::Date => decode_date(int).map(|_| ())?,
        Int::TimeSeconds => decode_time_seconds(int).map(|_| ())?,
        Int::TimeMilliseconds => decode_time_milliseconds(int).map(|_| ())?,
        Int::TimeMicroseconds => decode_time_microseconds(int).map(|_| ())?,
    };
    Ok(())
}

pub fn decode_date(int: i64) -> Result<Date, TimeError> {
    time::from_days(time::Days(int))
}
