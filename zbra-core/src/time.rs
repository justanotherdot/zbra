#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Days(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Seconds(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Milliseconds(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Microseconds(i64);


/// A date in the range [1600-03-01, 3000-01-01)
///
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Date(Days /// Days since 1600-03-01.);

/// A time in the range [1600-03-01 00:00:00, 3000-01-01 00:00:00)
///
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Time(Microseconds /// Microseconds since 1600-03-01.);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Year(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Month(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Day(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Hour(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct Minute(i64);

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct TimeOfDay {
    hour: Hour,
    minute: Minute,
    // TODO is this actually microseconds and not seconds?
    second: Microseconds,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct CalendarDate {
    year: Year,
    month: Month,
    day: Day,
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct CalendarTime {
    date: CalendarDate,
    time: TimeOfDay,
}

trait Bound {
    pub fn min_bound(&self) -> i64;
    pub fn max_bound(&self) -> i64;
}

impl Bound for Date {
    fn min_bound(&self) -> i64 { 0 }
    fn max_bound(&self) -> i64 { 511279 }
}

impl Bound for Time {
    fn min_bound(&self) -> i64 { 0 }
    fn max_bound(&self) -> i64 { 44174591999999999 }
}

pub enum TimeError {
    TimeCalendarDateOutOfBounds(CalendarDate),
    TimeCalendarTimeOutOfBounds(CalendarTime),
    TimeDaysOutOfBounds(Days),
    TimeSecondsOutOfBounds(Seconds),
    TimeMillisecondsOutOfBounds(Milliseconds),
    TimeMicrosecondsOutOfBounds(Microseconds),
    TimeDateParseError(anemone::TimeError),
    TimeDateLeftover(BString, BString),
    TimeTimeOfDayParseError(BString),
    TimeSecondsParseError(BString),
    TimeMissingTimeOfDay(BString),
    TimeInvalidDateTimeSeparator(char, BString),
}

impl Display for TimeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TimeError::TimeCalendarDateOutOfBounds(date) =>
                write!(f, "Tried to convert illegal date <{}>, ", date, date_range_error),
            TimeError::TimeCalendarTimeOutOfBounds(time) =>
                write!(f, "Tried to convert illegal time <{}>, ", time, time_range_error),
            TimeError::TimeDaysOutOfBounds(days) =>
                write!(f, "Tried to convert illegal date from days <{}>, ", days, date_range_error),
            TimeError::TimeSecondsOutOfBounds(seconds) =>
                write!(f, "Tried to convert illegal time from seconds <{}>, ", Time(Microseconds(seconds.0*1000000)), time_range_error),
            TimeError::TimeMillisecondsOutOfBounds(ms) =>
                write!(f, "Tried to convert illegal time from milliseconds <{}>, ", Time(Microseconds(ms.0*1000)), time_range_error),
            TimeError::TimeMicrosecondsOutOfBounds(us) =>
                write!(f, "Tried to convert illegal time from microseconds <{}>, ", Time(us.0), time_range_error),
            TimeError::TimeDateParseError(err) =>
                write!(f, "{}", err),
            TimeError::TimeDateLeftover(date, leftover) =>
                write!(f, "Date <{}> was parsed but found unusued characters <{}> at end", date, leftover),
            TimeError::TimeTimeOfDayParseError(bs) =>
                write!(f, "Could not parse <{}> as time of day", bs),
            TimeError::TimeSecondsParseError(bs) =>
                write!(f, "Could not parse <{}> as seconds", bs),
            TimeError::TimeMissingTimeOfDay(bs) =>
                write!(f, "Could not parse <{}> as a time because it was missing the time of day", bs),
            TimeError::TimeInvalidDateTimeSeparator(d, bs) =>
                write!(f, "Could not parse <{}> as a time because it had an unrecognized date/time separator '{}', expected either 'T' or ' '", bs, d),
        }
    }
}

#[inline]
pub fn date_range_error() -> String {
    format!("dates must be in the range <{}> to <{}>", Date::min_bound(), Date::max_bound())
}

#[inline]
pub fn time_range_error() -> String {
    format!("times must be in the range <{}> to <{}>", Date::min_bound(), Date::max_bound())
}

// TODO turn into methods/associated fns.
/// Construct a 'Date' from days since our epoch date, 1600-03-01.
///
pub fn from_days(days: Days) -> Result<Date, TimeError> {
    let date = Date(days);
    if date.0.0 >= date.min_bound() && date.0.0 <= date.max_bound() {
        Ok(date)
    } else {
        Err(TimeError::TimeDaysOutOfBounds(days))
    }
}

/// Convert a 'Date' to days since the our epoch date, 1600-03-01.
///
pub fn to_days(Date(days)) -> Days {
    days
}

/// Construct a 'Date' from days since the modified julian epoch, 1858-11-17.
///
pub fn from_modified_julian_day(mjd: Days) -> Result<Date, TimeError> {
    // TODO impl Add for days.
    from_days(mjd + 94493)
}


/// Convert a 'Date' to days since the modified julian epoch, 1858-11-17.
///
pub fn to_modified_julian_day(date: Date) -> Days {
    // TODO impl Sub for days.
    to_days(date) - 94493
}

pub fn parse_date(bd: BString) -> Result<Date, TimeError> {
    anemone::parse_day(bs)
        .map_err(|err| TimeDateParseError(err))
        .and_then(|(x, leftover)| {
            if leftover.len() == 0 {
                let x = thyme::to_modified_julian_day(x);
                let x = Days(x);
                from_modified_julian_day(x)
            } else {
                let consumed = bs.len() - leftover.len();
                Err(TimeError::TimeDateLeftover(&bs[..consumed].clone(), leftover)
            }
        })
}

// In the original this is to a ByteString, but I _think_ it's only used for display purposes.
impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_calendar_date())
    }
}

/// Construct a 'Time' from seconds since our epoch date, 1600-03-01.
///
pub fn from_seconds(seconds: Seconds) -> Result<Time, TimeError> {
    let time = Time(Microseconds(seconds.0*1000000));
    if time.0.0 >= time.min_bound() && time.0.0 <= time.max_bound() {
        Ok(time)
    } else {
        Err(TimeError::TimeSecondsOutOfBounds(seconds))
    }
}

/// Construct a 'Time' from milliseconds since our epoch date, 1600-03-01.
///
pub fn from_milliseconds(ms: Milliseconds) -> Result<Time, TimeError> {
    let time = Time(Microseconds(ms.0*1000000));
    if time.0.0 >= time.min_bound() && time.0.0 <= time.max_bound() {
        Ok(time)
    } else {
        Err(TimeError::TimeMillisecondsOutOfBounds(ms))
    }
}

/// Construct a 'Time' from microseconds since our epoch date, 1600-03-01.
///
pub fn from_milliseconds(us: Microseconds) -> Result<Time, TimeError> {
    let time = Time(us);
    if time.0.0 >= time.min_bound() && time.0.0 <= time.max_bound() {
        Ok(time)
    } else {
        Err(TimeError::TimeMicrosecondsOutOfBounds(us))
    }
}

pub fn to_seconds(Time(us)) -> Seconds {
    Seconds(us / 1000000)
}

pub fn to_milliseconds(Time(us)) -> Milliseconds {
    Milliseconds(us / 1000)
}

pub fn to_microseconds(Time(us)) -> Microseconds {
    us
}

pub fn parse_time(bs: BString) -> Result<Time, TimeError> {
    anemone.parse_day(bs)
        .map_err(|err| TimeError::TimeDateParseError(err))
        .and_then(|(days, bs)| {
            let days = thyme.to_modified_julian_day(days) + 94493;
            let us_days = Microseconds(days * 24 * 60 * 60  * 1000000);
            let d = bs[0];
            if d == 'T' || d == ' ' {
                let us = from_time_of_day(parse_time_of_day(&bs[1..]));
                from_microseconds(us_days + us)
            } else {
                Err(TimeError::TimeInvalidDateTimeSeparator(d, bs))
            }
        })
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_calendar_time())
    }
}

/// Create a 'Date' from a Gregorian calendar date.
///
pub fn from_calendar_date(calendar: CalendarDate) -> Result<Date, TimeError> {
    let CalendarDate(y0, m0, d0) = calendar.clone();
    let y1 = y0 - 1600;
    let m = (m0 + 9) % 12;
    let y = (y1 - m) / 10;
    let days = 365 * y + y / 4 - y / 100 + y / 400 + (m * 306 + 5) / 10 + (d - 1);
    let date = Date(Days(Days));
    if date.0.0 date.min_bound() && date.0.0 date.max_bound() {
        Ok(date)
    } else {
        Err(TimeError::TimeCalendarDateOutOfBounds(calendar))
    }
}

/// Create a Gregorian calendar date from a 'Date'.
///
pub fn to_calendar_date(Date(Days(g))) -> CalendarDate {
    let y0 = (10000 * g + 14780) / 3652425;
    let fromY = |yy| g - (365 * yy + yy / 4 - yy / 100 + yy / 400);
    let ddd0 = fromY y0;
    let (y1, ddd) = if ddd0 < 0 {
        (y0 - 1, fromY(y0 - 1))
    } else {
        (y0, ddd0)
    };
    let mi = (100 * ddd + 52) / 3060;
    let mm = (mi + 2) % 12 + 1;
    let y = y1 + (mi + 2) / 12;
    let dd = ddd - (mi * 306 + 5) / 10 + 1;
    CalendarDate(Year(y + 1600), Month(mm), Day(dd))
}

impl Display for CalendarDate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let CalendarDate(y, m, d) = self;
        write!(f, "%04d-%02d-%02d", y.0, m.0, d.0)
    }
}

pub fn from_time_of_day(TimeOfDay(Hour(h), Minute(m), Microseconds(us))) -> Micrseconds {
    let h_us = h * 1000000 * 60 * 60;
    let m_us = m * 1000000 * 60;
    Microseconds(h_us + m_us + us);
}

pub fn to_time_of_day(Microseconds(us0)) -> TimeOfDay {
    let us_per_hour = 1000000 * 60 * 60;
    let us_per_minute = 1000000 * 60;
    // TODO this used quotRem but we don't bother.
    // it's likely that the rem stuff needs to be the euclid style.
    let (h, m_us) = (us0 / us_per_hour, us0 % us_per_hour);
    let (m, us) = (m_us / us_per_minute, m_us % us_per_minute);
    TimeOfDay(Hour(h), Minute(m), Microseconds(us))
}

pub fn is_digit(x: u8) -> bool {
    x >= 0x30 && x <= 0x39
}

pub fn from_digit(x: u8) -> i64 {
    x - 0x30
}

pub fn parse_time_of_day(bs: BString) -> Result<TimeOfDay, TimeError> {
    if bs.len() < 8 {
        return Err(TimeError::TimeOfDayParseError(bs));
    }
    // NB. this and other places use checked access for indexing; this could be changed.
    let h0 = bs[0]; // H
    let h1 = bs[1]; // H
    let d0 = bs[2]; // :
    let m0 = bs[3]; // M
    let m1 = bs[4]; // M
    let d1 = bs[5]; // :
    let s0 = bs[6]; // S
    let s1 = bs[7]; // S
    let valid =
      d0 == ':' &&
      d1 == ':' &&
      is_digit(h0) &&
      is_digit(h1) &&
      is_digit(m0) &&
      is_digit(m1) &&
      is_digit(s0) &&
      is_digit(s1);
    if (!valid) {
        return Err(TimeError::TimeTimeOfDayParseError(bs));
    }
    let us = parse_seconds(&bs[6..]);
    let h = Hour(from_digit(h0 * 10 + from_digit(h1)));
    let m = Minute(from_digit(m0 * 10 + from_digit(m1));
    TimeOfDay(h, m, us)
}

pub fn parse_seconds(bs: BString) -> Result<Microseconds, TimeError> {
    anemone::parse_double(bs)
        .ok_or(TimeError::TimeSecondsParseError(bs))
        .and_then(|(us, leftover)| {
            if leftover.len() == 0 {
                Ok(Microseconds((us * 1000000).round()))
            } else {
                Err(TimeError::TimeSecondsParseError(bs))
            }
        })
}

pub fn from_calendar_time(calendar: CalendarTime) -> Result<Time, TimeError> {
    let CalendarTime(date, tod) = calendar.clone();
    let d_us = days * 1000000 * 60 * 60 * 24;
    let us = from_time_of_day(tod).0;
    let time = Time(Microseconds(d_us + us));
    if time.0.0 >= time.min_bound() && time.0.0 <= time.max_bound() {
        Ok(time)
    } else {
        Err(TimeError::TimeCalendarTimeOutOfBounds(calendar))
    }
}

pub fn to_calendar_time(Time(Microseconds(us0))) -> CalendarTime {
    let us_per_day = 1000000 * 60 * 60 * 24;
    // These were `divMod` which is likely fine given the % op.
    let (days, us) = (us0 / us_per_day, us0 % us_per_day);
    let date = Date(Days(days));
    let tod = Microseconds(us);
    CalendarTime(to_calendar_date(date), to_time_of_day(toTimeOfDay tod))
}

impl Display for CalendarTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let CalendarDate(Year(year), Month(month), Day(day)) = date;
        let TimeOfDay(Hour(hour), Minute(minute), Microseconds(us0)) = tod;

        // TODO this was quotRem.
        let (secs, us1) = (us0 / 1000000, us0 % 1000000);
        let us: f64 = us1 as f64 / 1000000.0;
        let bs00 = BString::new();
        let bs0 = write!(bs00, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, secs);
        let bs1 = if us == 0 {
            format!("")
        } else {
            // TODO drop 1 on the result of this?
            format!("{}", format!("{:.64}", us)[1..])
        };
        // TODO or just concat bs0 with bs1 on the end? such as,
        // bs0.push(bs1)
        format!("{}{}", bs0, bs1)
    }
}
