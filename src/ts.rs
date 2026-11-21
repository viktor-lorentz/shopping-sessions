//! Timestamp and number parsing exactly as rules.md defines them.

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;

#[allow(dead_code)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[allow(dead_code)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

#[inline]
fn dig(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

fn num(s: &[u8]) -> Option<i64> {
    let mut v = 0i64;
    for &b in s {
        v = v * 10 + dig(b)?;
    }
    Some(v)
}

const CUM: [i64; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];
const DIM: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const LEAPS_TO_1969: i64 = 1969 / 4 - 1969 / 100 + 1969 / 400;

/// "YYYY-MM-DDTHH:MM:SS" (19 bytes) -> local epoch ms.
#[inline(always)]
fn civil(s: &[u8]) -> Option<i64> {
    let s: &[u8; 19] = s[..19].try_into().unwrap();
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let d = |i: usize| s[i].wrapping_sub(b'0');
    let ds = [d(0), d(1), d(2), d(3), d(5), d(6), d(8), d(9), d(11), d(12), d(14), d(15), d(17), d(18)];
    let mut bad = 0u8;
    for &x in &ds {
        bad |= (x > 9) as u8;
    }
    if bad != 0 {
        return None;
    }
    let v = |a: usize, b: usize| (ds[a] as i64) * 10 + ds[b] as i64;
    let y = v(0, 1) * 100 + v(2, 3);
    let mo = v(4, 5);
    let dd = v(6, 7);
    let h = v(8, 9);
    let mi = v(10, 11);
    let se = v(12, 13);
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || dd < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) as i64;
    let dim = if mo == 2 { 28 + leap } else { DIM[mo as usize] };
    if dd > dim {
        return None;
    }
    let y1 = y - 1;
    let days = 365 * (y - 1970) + (y1 / 4 - y1 / 100 + y1 / 400) - LEAPS_TO_1969
        + CUM[(mo - 1) as usize]
        + if mo > 2 { leap } else { 0 }
        + dd
        - 1;
    Some(days * DAY + h * 3_600_000 + mi * 60_000 + se * 1000)
}

/// Returns epoch ms, or None for a bad timestamp. Huge F3 values return i64::MAX (outside any window).
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(|b| b.is_ascii_digit()) {
        let mut i = 0;
        while i < s.len() && s[i] == b'0' {
            i += 1;
        }
        let sig = &s[i..];
        if sig.len() > 19 {
            return Some(i64::MAX);
        }
        let mut v: u64 = 0;
        for &b in sig {
            v = v * 10 + (b - b'0') as u64; // <= 19 digits fits in u64
        }
        return Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 });
    }
    match s.len() {
        20 => {
            if s[19] != b'Z' {
                return None;
            }
            civil(&s[..19])
        }
        25 => {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let hh = num(&s[20..22])?;
            let mm = num(&s[23..25])?;
            if hh > 23 || mm > 59 {
                return None;
            }
            let local = civil(&s[..19])?;
            Some(local - sign * (hh * 60 + mm) * 60_000)
        }
        _ => None,
    }
}

/// price_cents for a purchase: ASCII digits only, 1..=10_000_000.
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    let sig = &s[i..];
    if sig.is_empty() || sig.len() > 8 {
        return None;
    }
    let mut v = 0u32;
    for &b in sig {
        v = v * 10 + (b - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn civil_matches_reference() {
        for y in [1, 4, 100, 400, 1900, 1969, 1970, 1999, 2000, 2024, 2025, 2026, 2027, 2028, 2100, 2400, 9999] {
            for mo in 1..=12 {
                for d in 1..=31 {
                    let ok = d <= days_in_month(y, mo);
                    let st = format!("{:04}-{:02}-{:02}T13:45:59Z", y, mo, d);
                    let got = parse_ts(st.as_bytes());
                    if !ok {
                        assert_eq!(got, None, "{}", st);
                        continue;
                    }
                    let want = days_from_civil(y, mo, d) * DAY + 13 * 3_600_000 + 45 * 60_000 + 59_000;
                    if y >= 1 {
                        assert_eq!(got, Some(want), "{}", st);
                    }
                }
            }
        }
        for y in 1900..2101 {
            for mo in 1..=12 {
                let st = format!("{:04}-{:02}-01T00:00:00Z", y, mo);
                assert_eq!(parse_ts(st.as_bytes()), Some(days_from_civil(y, mo, 1) * DAY));
            }
        }
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"2026-10-03T24:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:60:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:00:60Z"), None);
        assert_eq!(parse_ts(b"2026-13-03T23:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-00-03T23:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-00T23:00:00Z"), None);
        assert_eq!(parse_ts(b"2027-02-29T00:00:00Z"), None);
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"2026-10-03 23:00:00Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:00:00z"), None);
        assert_eq!(parse_ts(b"2026-1a-03T23:00:00Z"), None);
    }
}
