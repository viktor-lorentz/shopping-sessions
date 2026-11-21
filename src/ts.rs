//! Timestamp and number parsing exactly as rules.md defines them.

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

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

/// "YYYY-MM-DDTHH:MM:SS" (19 bytes) -> local epoch ms.
fn civil(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = num(&s[0..4])?;
    let mo = num(&s[5..7])?;
    let d = num(&s[8..10])?;
    let h = num(&s[11..13])?;
    let mi = num(&s[14..16])?;
    let se = num(&s[17..19])?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    Some(days_from_civil(y, mo, d) * DAY + h * 3_600_000 + mi * 60_000 + se * 1000)
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
