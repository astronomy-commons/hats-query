//! DALI timestamps.

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};

/// Conversion of a timestamp to its IVOA string representation.
pub trait DateTimeToIvoa {
    /// Formats as a DALI timestamp.
    fn to_ivoa(&self) -> String;
}

impl<Tz: TimeZone> DateTimeToIvoa for DateTime<Tz> {
    /// DALI timestamp converted to UTC, with microseconds, e.g. "2026-11-16T10:00:00.123456Z".
    ///
    /// Microseconds are consistent with pyvo's `IVOA_DATETIME_FORMAT`, "%Y-%m-%dT%H:%M:%S.%fZ".
    fn to_ivoa(&self) -> String {
        self.with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Micros, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, Timelike as _};

    #[test]
    fn utc() {
        let date_time = Utc
            .with_ymd_and_hms(2026, 11, 16, 10, 0, 0)
            .unwrap()
            .with_nanosecond(123_456_789)
            .unwrap();
        assert_eq!(date_time.to_ivoa(), "2026-11-16T10:00:00.123456Z");
    }

    #[test]
    fn other_timezone() {
        let date_time = FixedOffset::east_opt(2 * 3600)
            .unwrap()
            .with_ymd_and_hms(2026, 11, 16, 12, 0, 0)
            .unwrap();
        assert_eq!(date_time.to_ivoa(), "2026-11-16T10:00:00.000000Z");
    }
}
