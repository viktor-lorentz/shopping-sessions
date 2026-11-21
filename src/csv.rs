//! Single-pass line tokenizer for the rules.md CSV dialect.
//! Finds ',', '"' and '\n' with 32-byte masks (AVX2 on x86, SWAR elsewhere), then:
//! - exactly N-1 commas before the line end, else malformed (this also covers empty lines);
//! - a field wrapped in quotes (len >= 2, first and last byte '"') is unwrapped;
//! - the line is well-formed only if its quote count equals 2 x wrapped fields
//!   (so any other quote, including one inside a wrapped field, makes it malformed).

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn masks_at(p: *const u8) -> (u32, u32, u32) {
    use core::arch::x86_64::*;
    unsafe {
        let v = _mm256_loadu_si256(p as *const __m256i);
        let c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8))) as u32;
        let n = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\n' as i8))) as u32;
        let q = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8))) as u32;
        (c, n, q)
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
fn masks_at(p: *const u8) -> (u32, u32, u32) {
    #[inline(always)]
    fn eqmask(x: u64, c: u8) -> u32 {
        let t = x ^ (0x0101_0101_0101_0101u64 * c as u64);
        // exact: high bit set in each byte of t that is zero
        let z = !(((t & 0x7F7F_7F7F_7F7F_7F7F).wrapping_add(0x7F7F_7F7F_7F7F_7F7F)) | t | 0x7F7F_7F7F_7F7F_7F7F);
        ((z >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) as u32
    }
    let (mut c, mut n, mut q) = (0u32, 0u32, 0u32);
    for k in 0..4 {
        let x = unsafe { (p.add(8 * k) as *const u64).read_unaligned() };
        let x = u64::from_le(x);
        c |= eqmask(x, b',') << (8 * k);
        n |= eqmask(x, b'\n') << (8 * k);
        q |= eqmask(x, b'"') << (8 * k);
    }
    (c, n, q)
}

/// Masks for buf[p..p+32]; bytes past the end of buf count as none of the three.
#[inline(always)]
fn masks(buf: &[u8], p: usize) -> (u32, u32, u32, usize) {
    if p + 32 <= buf.len() {
        let (c, n, q) = masks_at(unsafe { buf.as_ptr().add(p) });
        (c, n, q, 32)
    } else {
        let mut tmp = [0u8; 32];
        let k = buf.len() - p;
        tmp[..k].copy_from_slice(&buf[p..]);
        let (c, n, q) = masks_at(tmp.as_ptr());
        (c, n, q, k)
    }
}

pub struct Lines<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Lines<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Lines { buf, pos: 0 }
    }

    /// None at the end of the buffer; Some(None) for a malformed line; Some(Some(fields)) otherwise.
    #[inline(always)]
    pub fn next<const N: usize>(&mut self) -> Option<Option<[&'a [u8]; N]>> {
        let buf = self.buf;
        let start = self.pos;
        if start >= buf.len() {
            return None;
        }
        let mut commas = [0usize; N];
        let mut nc = 0usize;
        let mut quotes = 0u32;
        let mut p = start;
        let end;
        loop {
            let (mc, mn, mq, valid) = masks(buf, p);
            let lim = if mn != 0 { mn.trailing_zeros() as usize } else { valid };
            let below = if lim >= 32 { u32::MAX } else { (1u32 << lim) - 1 };
            let mut c = mc & below;
            while c != 0 {
                let i = c.trailing_zeros() as usize;
                if nc < N {
                    commas[nc] = p + i;
                }
                nc += 1;
                c &= c - 1;
            }
            quotes += (mq & below).count_ones();
            if mn != 0 {
                end = p + lim;
                break;
            }
            if valid < 32 {
                end = buf.len();
                break;
            }
            p += 32;
        }
        self.pos = end + 1;
        if nc != N - 1 {
            return Some(None);
        }
        let mut out: [&'a [u8]; N] = [&[]; N];
        let mut s = start;
        let mut wrapped = 0u32;
        for k in 0..N {
            let e = if k < N - 1 { commas[k] } else { end };
            let f = &buf[s..e];
            if quotes > 0 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                out[k] = &f[1..f.len() - 1];
                wrapped += 1;
            } else {
                out[k] = f;
            }
            s = e + 1;
        }
        if quotes != 2 * wrapped {
            return Some(None);
        }
        Some(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reference: the straightforward dialect implementation.
    fn reference<const N: usize>(line: &[u8]) -> Option<Vec<Vec<u8>>> {
        let parts: Vec<&[u8]> = line.split(|&b| b == b',').collect();
        if parts.len() != N {
            return None;
        }
        let mut out = vec![];
        for f in parts {
            if f.contains(&b'"') {
                if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' && !f[1..f.len() - 1].contains(&b'"') {
                    out.push(f[1..f.len() - 1].to_vec());
                } else {
                    return None;
                }
            } else {
                out.push(f.to_vec());
            }
        }
        Some(out)
    }

    #[test]
    fn matches_reference_on_random_lines() {
        let alphabet = [b'a', b'1', b',', b'"', b'\r', b' ', b'Z', b'-'];
        let mut seed: u64 = 12345;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..20000 {
            // build a buffer of several lines with random lengths (some > 32 bytes)
            let nlines = (rnd() % 5) as usize + 1;
            let mut buf = vec![];
            let mut lines = vec![];
            for _ in 0..nlines {
                let len = (rnd() % 70) as usize;
                let mut l = vec![];
                for _ in 0..len {
                    l.push(alphabet[(rnd() % alphabet.len() as u64) as usize]);
                }
                buf.extend_from_slice(&l);
                lines.push(l);
                buf.push(b'\n');
            }
            if rnd() % 2 == 0 && !lines.last().unwrap().is_empty() {
                buf.pop(); // last line without trailing newline
            }
            let mut it = Lines::new(&buf);
            for l in &lines {
                let got = it.next::<4>().expect("line");
                let want = reference::<4>(l);
                let got = got.map(|a| a.iter().map(|f| f.to_vec()).collect::<Vec<_>>());
                assert_eq!(got, want, "line {:?}", String::from_utf8_lossy(l));
            }
            assert!(it.next::<4>().is_none());
        }
    }
}
