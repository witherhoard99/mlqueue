//! Daily windows with fixed UTC offsets. Start is inclusive, end exclusive.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RunWindow {
    start: i64,
    end: i64,
    offset: i64,
}

const MINUTE: i64 = 60_000;
const DAY: i64 = 24 * 60 * MINUTE;

fn parse_time(value: &str) -> Result<i64, String> {
    let [h1, h2, b':', m1, m2] = value.as_bytes() else {
        return Err("expected HH:MM".into());
    };
    if ![h1, h2, m1, m2].iter().all(|digit| digit.is_ascii_digit()) {
        return Err("expected HH:MM".into());
    }
    let hour = i64::from((h1 - b'0') * 10 + h2 - b'0');
    let minute = i64::from((m1 - b'0') * 10 + m2 - b'0');
    if hour > 23 || minute > 59 {
        return Err("time is outside 00:00-23:59".into());
    }
    Ok((hour * 60 + minute) * MINUTE)
}

impl FromStr for RunWindow {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (times, offset) = value.split_once('@').unwrap_or((value, "Z"));
        let (start, end) = times.split_once('-').ok_or("expected HH:MM-HH:MM@+HH:MM")?;
        let start = parse_time(start)?;
        let end = parse_time(end)?;
        if start == end {
            return Err("start and end must differ; clear the window to allow any time".into());
        }
        let offset = match offset {
            "Z" => 0,
            text if text.starts_with('+') => parse_time(&text[1..])?,
            text if text.starts_with('-') => -parse_time(&text[1..])?,
            _ => return Err("UTC offset must be Z or signed HH:MM".into()),
        };
        Ok(Self { start, end, offset })
    }
}

impl RunWindow {
    pub fn contains(&self, now_ms: i64) -> bool {
        let local =
            (i128::from(now_ms) + i128::from(self.offset)).rem_euclid(i128::from(DAY)) as i64;
        if self.start < self.end {
            local >= self.start && local < self.end
        } else {
            local >= self.start || local < self.end
        }
    }

    /// Return the current window's closing time, or `None` outside the window.
    pub fn closing_at(&self, now_ms: i64) -> Option<i64> {
        if !self.contains(now_ms) {
            return None;
        }
        let local = i128::from(now_ms) + i128::from(self.offset);
        let day = local.div_euclid(i128::from(DAY));
        let next_day =
            self.start > self.end && local.rem_euclid(i128::from(DAY)) >= i128::from(self.start);
        i64::try_from(
            (day + i128::from(next_day)) * i128::from(DAY) + i128::from(self.end)
                - i128::from(self.offset),
        )
        .ok()
    }
}

impl fmt::Display for RunWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let start = self.start / MINUTE;
        let end = self.end / MINUTE;
        let offset = self.offset.abs() / MINUTE;
        write!(
            f,
            "{:02}:{:02}-{:02}:{:02}@{}{:02}:{:02}",
            start / 60,
            start % 60,
            end / 60,
            end % 60,
            if self.offset < 0 { '-' } else { '+' },
            offset / 60,
            offset % 60
        )
    }
}

impl TryFrom<String> for RunWindow {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
impl From<RunWindow> for String {
    fn from(value: RunWindow) -> Self {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daytime_boundaries() {
        let w: RunWindow = "09:00-17:00".parse().unwrap();
        assert!(!w.contains(9 * 60 * MINUTE - 1));
        assert!(w.contains(9 * 60 * MINUTE));
        assert_eq!(w.closing_at(10 * 60 * MINUTE), Some(17 * 60 * MINUTE));
        assert!(!w.contains(17 * 60 * MINUTE));
        assert_eq!(w.closing_at(17 * 60 * MINUTE), None);
    }

    #[test]
    fn overnight_and_negative_epochs() {
        let w: RunWindow = "23:00-07:00@Z".parse().unwrap();
        assert!(w.contains(0));
        assert_eq!(w.closing_at(0), Some(7 * 60 * MINUTE));
        assert_eq!(w.closing_at(23 * 60 * MINUTE), Some(DAY + 7 * 60 * MINUTE));
        assert_eq!(w.closing_at(-1), Some(7 * 60 * MINUTE));
        assert!(!w.contains(7 * 60 * MINUTE));
        assert!(!w.contains(23 * 60 * MINUTE - 1));
    }

    #[test]
    fn fixed_offset_and_serde() {
        let w: RunWindow = "23:00-07:00@-04:00".parse().unwrap();
        assert!(!w.contains(3 * 60 * MINUTE - 1));
        assert!(w.contains(3 * 60 * MINUTE));
        assert_eq!(w.closing_at(3 * 60 * MINUTE), Some(11 * 60 * MINUTE));
        assert!(!w.contains(11 * 60 * MINUTE));
        let json = serde_json::to_string(&w).unwrap();
        assert_eq!(serde_json::from_str::<RunWindow>(&json).unwrap(), w);
        assert_eq!(
            "09:00-10:00@+05:30"
                .parse::<RunWindow>()
                .unwrap()
                .closing_at(4 * 60 * MINUTE),
            Some((4 * 60 + 30) * MINUTE)
        );
    }

    #[test]
    fn rejects_invalid_windows() {
        for value in [
            "",
            "7:00-09:00",
            "24:00-07:00",
            "23:60-07:00",
            "07:00-07:00",
            "23:00-07:00@America/New_York",
            "23:00-07:00@+24:00",
            "23:00-07:00@Z@Z",
            "é:00-07:00",
        ] {
            assert!(value.parse::<RunWindow>().is_err(), "{value}");
            assert!(serde_json::from_value::<RunWindow>(serde_json::json!(value)).is_err());
        }
    }
}
