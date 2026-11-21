//! Fixed-width key table for short IDs. An ID of at most 15 bytes maps one-to-one onto two
//! u64 words (k0, k1), so lookups compare exact keys without touching the original bytes.
//! Longer IDs return None from `key_of`; callers fall back to a byte-slice HashMap.

pub const EMPTY: u64 = u64::MAX;

#[inline(always)]
fn ld8(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}
#[inline(always)]
fn ld4(b: &[u8], at: usize) -> u64 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap()) as u64
}

/// Injective encoding of a byte string of length <= 15 into (k0, k1).
#[inline(always)]
pub fn key_of(b: &[u8]) -> Option<(u64, u64)> {
    let n = b.len();
    if n > 15 {
        return None;
    }
    let lenbits = (n as u64) << 56;
    if n >= 8 {
        let k0 = ld8(b, 0);
        let tail = ld8(b, n - 8);
        let k1 = if n == 8 { 0 } else { tail >> (8 * (16 - n)) };
        Some((k0, k1 | lenbits))
    } else if n >= 4 {
        Some((ld4(b, 0) | (ld4(b, n - 4) << 32), lenbits))
    } else if n > 0 {
        Some(((b[0] as u64) | ((b[n / 2] as u64) << 8) | ((b[n - 1] as u64) << 16), lenbits))
    } else {
        Some((0, 0))
    }
}

#[inline(always)]
pub fn mix(k0: u64, k1: u64, seed: u64) -> u64 {
    let mut h = (k0 ^ seed).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h = (h ^ (h >> 32) ^ k1).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    h ^= h >> 29;
    h = h.wrapping_mul(0x1656_67B1_9E37_79F9);
    h ^ (h >> 32)
}

/// Open-addressing table; each entry is [k0, k1, v2, v3], empty when v2 == EMPTY.
pub struct KT {
    mask: usize,
    len: usize,
    pub e: Vec<[u64; 4]>,
}

impl KT {
    pub fn with_capacity(n: usize) -> KT {
        let cap = (n.max(8) * 2).next_power_of_two();
        KT { mask: cap - 1, len: 0, e: vec![[0, 0, EMPTY, 0]; cap] }
    }

    #[inline(always)]
    fn slot(&self, k0: u64, k1: u64) -> usize {
        (mix(k0, k1, 0x5851_F42D_4C95_7F2D) as usize) & self.mask
    }

    #[inline(always)]
    pub fn get(&self, k0: u64, k1: u64) -> Option<&[u64; 4]> {
        let mut i = self.slot(k0, k1);
        loop {
            let e = &self.e[i];
            if e[2] == EMPTY {
                return None;
            }
            if e[0] == k0 && e[1] == k1 {
                return Some(e);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Returns the entry and whether it was newly inserted (then v2/v3 must be set by the caller,
    /// and v2 must not be EMPTY).
    pub fn entry(&mut self, k0: u64, k1: u64) -> (&mut [u64; 4], bool) {
        if (self.len + 1) * 3 > self.e.len() * 2 {
            self.grow();
        }
        let mut i = self.slot(k0, k1);
        loop {
            let e = self.e[i];
            if e[2] == EMPTY {
                self.len += 1;
                self.e[i] = [k0, k1, 0, 0];
                return (&mut self.e[i], true);
            }
            if e[0] == k0 && e[1] == k1 {
                return (&mut self.e[i], false);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.e, vec![[0, 0, EMPTY, 0]; (self.mask + 1) * 2]);
        self.mask = self.e.len() - 1;
        for e in old {
            if e[2] != EMPTY {
                let mut i = self.slot(e[0], e[1]);
                while self.e[i][2] != EMPTY {
                    i = (i + 1) & self.mask;
                }
                self.e[i] = e;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn injective_small() {
        let mut seen = std::collections::HashMap::new();
        let alpha = [b'a', b'b', b'\0', b'"', 0xFF];
        // all strings up to length 4 over a small alphabet, plus padded variants
        let mut stack: Vec<Vec<u8>> = vec![vec![]];
        while let Some(s) = stack.pop() {
            let k = key_of(&s).unwrap();
            if let Some(prev) = seen.insert(k, s.clone()) {
                panic!("collision {:?} {:?}", prev, s);
            }
            if s.len() < 4 {
                for &c in &alpha {
                    let mut t = s.clone();
                    t.push(c);
                    stack.push(t);
                }
            }
        }
        for n in 0..=15usize {
            for m in 0..=15usize {
                if n != m {
                    assert_ne!(key_of(&vec![0u8; n]), key_of(&vec![0u8; m]));
                }
            }
        }
        // position sensitivity for every length up to 15
        for n in 1..=15usize {
            let base = vec![b'x'; n];
            let kb = key_of(&base).unwrap();
            for i in 0..n {
                let mut t = base.clone();
                t[i] = b'y';
                assert_ne!(key_of(&t).unwrap(), kb, "len {} pos {}", n, i);
            }
        }
    }
}
