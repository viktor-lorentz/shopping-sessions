mod parquet;
mod ts;

use memchr::{memchr, memchr_iter};
use parquet::{write_file, Column};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use ts::{parse_price, parse_ts, DAY, WEEK};

type B = &'static [u8];

// ---------- hashing ----------
#[inline]
fn h64(b: &[u8]) -> u64 {
    const K: u64 = 0x517c_c1b7_2722_0a95;
    let mut h: u64 = 0x9E37_79B9_7F4A_7C15 ^ (b.len() as u64);
    let mut c = b.chunks_exact(8);
    for w in &mut c {
        h = (h.rotate_left(5) ^ u64::from_le_bytes(w.try_into().unwrap())).wrapping_mul(K);
    }
    let r = c.remainder();
    if !r.is_empty() {
        let mut t = [0u8; 8];
        t[..r.len()].copy_from_slice(r);
        h = (h.rotate_left(5) ^ u64::from_le_bytes(t)).wrapping_mul(K);
    }
    h = (h ^ (h >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^ (h >> 33)
}

#[derive(Default)]
struct BH(u64);
impl Hasher for BH {
    fn write(&mut self, b: &[u8]) {
        self.0 = h64(b);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}
type Map<V> = HashMap<B, V, BuildHasherDefault<BH>>;
fn new_map<V>() -> Map<V> {
    HashMap::with_hasher(BuildHasherDefault::default())
}

// ---------- csv ----------
#[inline]
fn unq(f: &[u8]) -> Option<&[u8]> {
    if memchr(b'"', f).is_none() {
        return Some(f);
    }
    if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' && memchr(b'"', &f[1..f.len() - 1]).is_none() {
        Some(&f[1..f.len() - 1])
    } else {
        None
    }
}

#[inline]
fn split_fields<const N: usize>(line: B) -> Option<[B; N]> {
    let mut out: [B; N] = [&[]; N];
    let mut n = 0;
    let mut start = 0;
    for i in memchr_iter(b',', line) {
        if n >= N - 1 {
            return None;
        }
        out[n] = unq(&line[start..i])?;
        n += 1;
        start = i + 1;
    }
    if n != N - 1 {
        return None;
    }
    out[n] = unq(&line[start..])?;
    Some(out)
}

fn for_each_line(buf: B, mut f: impl FnMut(B)) {
    let mut pos = 0;
    while pos < buf.len() {
        let end = match memchr(b'\n', &buf[pos..]) {
            Some(i) => pos + i,
            None => buf.len(),
        };
        f(&buf[pos..end]);
        pos = end + 1;
    }
}

// ---------- parallel helpers ----------
fn par<T: Send, F: Fn(usize) -> T + Sync>(n: usize, f: F) -> Vec<T> {
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..n).map(|i| { let f = &f; s.spawn(move || f(i)) }).collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

struct Chunk {
    buf: B,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv"))
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// mmap every file in dir, drop the header line, cut into newline-aligned chunks.
fn chunks_of(dir: &Path, chunk_size: usize) -> Vec<Chunk> {
    let mut out = Vec::new();
    for p in list_csv(dir) {
        let f = match std::fs::File::open(&p) { Ok(f) => f, Err(_) => continue };
        let len = f.metadata().map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            continue;
        }
        let mm: &'static memmap2::Mmap = Box::leak(Box::new(unsafe { memmap2::Mmap::map(&f).unwrap() }));
        let data: B = &mm[..];
        let hdr_end = match memchr(b'\n', data) { Some(i) => i + 1, None => continue };
        let mut start = hdr_end;
        while start < data.len() {
            let mut end = (start + chunk_size).min(data.len());
            if end < data.len() {
                end = match memchr(b'\n', &data[end..]) { Some(i) => end + i + 1, None => data.len() };
            }
            out.push(Chunk { buf: &data[start..end] });
            start = end;
        }
    }
    out
}

fn read_static(p: &Path) -> B {
    Box::leak(std::fs::read(p).unwrap_or_default().into_boxed_slice())
}

// ---------- records ----------
#[derive(Clone, Copy)]
struct Ev {
    eid_h: u64,
    eid: B,
    ingest: i64,
    ts: i64,
    ph: u64,
    name: B,
    cat: u8,
    ty: u8, // 0 view 1 cart 2 purchase
    price: u32,
    valid: bool,
}

#[derive(Clone, Copy)]
struct Ck {
    ph: u64,
    ts: i64,
    camp: u32,
    cid: B,
}

struct Ctx {
    ws: i64,
    we: i64,
    products: Map<u8>,
    ncat: usize,
    camps: Map<u32>,
    persons: Map<(u64, B)>,
}

#[inline]
fn person_of(cx: &Ctx, uid: B) -> (u64, B) {
    match cx.persons.get(uid) {
        Some(&(h, n)) => (h, n),
        None => (h64(uid), uid),
    }
}

const P: usize = 256;

fn parse_events(buf: B, cx: &Ctx, out: &mut [Vec<Ev>]) {
    for_each_line(buf, |line| {
        let f = match split_fields::<7>(line) { Some(f) => f, None => return };
        let [eid, uid, tsb, ing, ety, prod, price] = f;
        if eid.is_empty() {
            return;
        }
        let eid_h = h64(eid);
        let ingest = parse_ts(ing).unwrap_or(i64::MAX);
        let mut e = Ev { eid_h, eid, ingest, ts: 0, ph: 0, name: &[], cat: 0, ty: 0, price: 0, valid: false };
        'v: {
            if uid.is_empty() || prod.is_empty() {
                break 'v;
            }
            let ts = match parse_ts(tsb) { Some(t) => t, None => break 'v };
            if ts < cx.ws || ts >= cx.we {
                break 'v;
            }
            let ty = match ety {
                b"view" => 0,
                b"cart" => 1,
                b"purchase" => 2,
                _ => break 'v,
            };
            let mut pr = 0;
            if ty == 2 {
                pr = match parse_price(price) { Some(p) => p, None => break 'v };
            }
            if ingest.saturating_sub(ts) > DAY {
                break 'v;
            }
            let (ph, name) = person_of(cx, uid);
            e.ts = ts;
            e.ph = ph;
            e.name = name;
            e.ty = ty;
            e.price = pr;
            e.cat = *cx.products.get(prod).unwrap_or(&(cx.ncat as u8));
            e.valid = true;
        }
        out[(eid_h as usize) % P].push(e);
    });
}

fn parse_clicks(buf: B, cx: &Ctx, out: &mut [Vec<Ck>]) {
    for_each_line(buf, |line| {
        let [cid, uid, camp, tsb] = match split_fields::<4>(line) { Some(f) => f, None => return };
        if cid.is_empty() || uid.is_empty() {
            return;
        }
        let ts = match parse_ts(tsb) { Some(t) => t, None => return };
        if ts < cx.ws - WEEK || ts >= cx.we {
            return;
        }
        let camp = match cx.camps.get(camp) { Some(&c) => c, None => return };
        let (ph, _) = person_of(cx, uid);
        out[(ph as usize) % P].push(Ck { ph, ts, camp, cid });
    });
}

// ---------- accumulators ----------
struct Acc {
    // user_daily rows
    u_name: Vec<B>,
    u_day: Vec<i32>,
    u_sessions: Vec<i64>,
    u_events: Vec<i64>,
    u_rev: Vec<i64>,
    u_cat: Vec<u8>, // 255 = null
    // campaign tables [camp * ndays + day]
    c_sessions: Vec<u64>,
    c_purch: Vec<u64>,
    c_rev: Vec<u64>,
    c_assist: Vec<u64>,
    // scratch for the sliding window
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
}

fn json_str(s: &str, key: &str) -> Option<String> {
    let i = s.find(key)?;
    let rest = &s[i + key.len()..];
    let q1 = rest.find('"')?;
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"')?;
    Some(rest[..q2].to_string())
}

fn main() {
    let t0 = Instant::now();
    let timing = std::env::var("SS_TIMING").is_ok();
    let lap = |name: &str| {
        if timing {
            eprintln!("[{:>8.1} ms] {}", t0.elapsed().as_secs_f64() * 1000.0, name);
        }
    };
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nthreads: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
    });
    let nthreads = nthreads.max(1);

    // manifest
    let man = String::from_utf8_lossy(&std::fs::read(data.join("manifest.json")).unwrap()).to_string();
    let ws = parse_ts(json_str(&man, "\"window_start\"").unwrap().as_bytes()).unwrap();
    let we = parse_ts(json_str(&man, "\"window_end\"").unwrap().as_bytes()).unwrap();

    // products -> categories (sorted bytewise; "unknown" competes as one more name)
    let prod_buf = read_static(&data.join("products.csv"));
    let mut prod_rows: Vec<(B, B)> = Vec::new();
    let mut first = true;
    for_each_line(prod_buf, |l| {
        if first {
            first = false;
            return;
        }
        if let Some([p, c]) = split_fields::<2>(l) {
            prod_rows.push((p, c));
        }
    });
    let mut cat_names: Vec<B> = prod_rows.iter().map(|r| r.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort();
    cat_names.dedup();
    let ncat = cat_names.len() - 1; // ncat is the "unknown" index only if unknown sorts last; handle below
    let unknown_idx = cat_names.iter().position(|c| *c == b"unknown").unwrap();
    let _ = ncat;
    let ncat_total = cat_names.len();
    assert!(ncat_total < 255);
    let mut products = new_map::<u8>();
    for (p, c) in &prod_rows {
        let ci = cat_names.iter().position(|x| x == c).unwrap();
        products.insert(*p, ci as u8);
    }

    // campaigns
    let camp_buf = read_static(&data.join("campaigns.csv"));
    let mut camp_ids: Vec<B> = Vec::new();
    let mut camp_ch: Vec<B> = Vec::new();
    let mut camps = new_map::<u32>();
    let mut first = true;
    for_each_line(camp_buf, |l| {
        if first {
            first = false;
            return;
        }
        if let Some([c, ch]) = split_fields::<2>(l) {
            if !camps.contains_key(c) {
                camps.insert(c, camp_ids.len() as u32);
                camp_ids.push(c);
                camp_ch.push(ch);
            }
        }
    });
    let ncamp = camp_ids.len();
    camp_ids.push(b"organic");
    camp_ch.push(b"organic");
    lap("setup");

    // ---- links ----
    let link_chunks = chunks_of(&data.join("links"), 8 << 20);
    let next = AtomicUsize::new(0);
    let link_parts: Vec<Vec<(B, B)>> = par(nthreads, |_| {
        let mut v = Vec::new();
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= link_chunks.len() {
                break;
            }
            for_each_line(link_chunks[i].buf, |l| {
                if let Some([a, b]) = split_fields::<2>(l) {
                    if !a.is_empty() && !b.is_empty() {
                        v.push((a, b));
                    }
                }
            });
        }
        v
    });
    let mut ids = new_map::<u32>();
    let mut names: Vec<B> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(p: &mut Vec<u32>, mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for part in &link_parts {
        for &(a, b) in part {
            let mut get = |s: B| -> u32 {
                *ids.entry(s).or_insert_with(|| {
                    names.push(s);
                    parent.push(names.len() as u32 - 1);
                    names.len() as u32 - 1
                })
            };
            let ia = get(a);
            let ib = get(b);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    let n = names.len();
    let mut best: Vec<B> = vec![&[]; n];
    let mut has = vec![false; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if !has[r] || names[i] < best[r] {
            best[r] = names[i];
            has[r] = true;
        }
    }
    let mut persons = new_map::<(u64, B)>();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        persons.insert(names[i], (h64(best[r]), best[r]));
    }
    lap("links");

    let cx = Ctx { ws, we, products, ncat: unknown_idx, camps, persons };

    // ---- events + clicks parse ----
    let ev_chunks = chunks_of(&data.join("events"), 8 << 20);
    let ck_chunks = chunks_of(&data.join("clicks"), 8 << 20);
    let next_e = AtomicUsize::new(0);
    let next_c = AtomicUsize::new(0);
    let parsed: Vec<(Vec<Vec<Ev>>, Vec<Vec<Ck>>)> = par(nthreads, |_| {
        let mut eb: Vec<Vec<Ev>> = (0..P).map(|_| Vec::new()).collect();
        let mut cb: Vec<Vec<Ck>> = (0..P).map(|_| Vec::new()).collect();
        loop {
            let i = next_e.fetch_add(1, Ordering::Relaxed);
            if i >= ev_chunks.len() {
                break;
            }
            parse_events(ev_chunks[i].buf, &cx, &mut eb);
        }
        loop {
            let i = next_c.fetch_add(1, Ordering::Relaxed);
            if i >= ck_chunks.len() {
                break;
            }
            parse_clicks(ck_chunks[i].buf, &cx, &mut cb);
        }
        (eb, cb)
    });
    lap("parse");

    // ---- dedup (by eid bucket) -> scatter survivors by person bucket ----
    let next_b = AtomicUsize::new(0);
    let survivors: Vec<Vec<Vec<Ev>>> = par(nthreads, |_| {
        let mut outb: Vec<Vec<Ev>> = (0..P).map(|_| Vec::new()).collect();
        loop {
            let b = next_b.fetch_add(1, Ordering::Relaxed);
            if b >= P {
                break;
            }
            let mut m: Map<Ev> = new_map();
            for t in &parsed {
                for e in &t.0[b] {
                    match m.get_mut(e.eid) {
                        Some(cur) => {
                            if e.ingest < cur.ingest {
                                *cur = *e;
                            }
                        }
                        None => {
                            m.insert(e.eid, *e);
                        }
                    }
                }
            }
            for (_, e) in m {
                if e.valid {
                    outb[(e.ph as usize) % P].push(e);
                }
            }
        }
        outb
    });
    lap("dedup");

    // ---- sessions / attribution / aggregation per person bucket ----
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ncamp1 = ncamp + 1;
    let next_p = AtomicUsize::new(0);
    let accs: Vec<Acc> = par(nthreads, |_| {
        let mut acc = Acc {
            u_name: vec![], u_day: vec![], u_sessions: vec![], u_events: vec![], u_rev: vec![], u_cat: vec![],
            c_sessions: vec![0; ncamp1 * ndays], c_purch: vec![0; ncamp1 * ndays],
            c_rev: vec![0; ncamp1 * ndays], c_assist: vec![0; ncamp1 * ndays],
            cnt: vec![0; ncamp1], pos: vec![0; ncamp1], active: vec![],
        };
        loop {
            let b = next_p.fetch_add(1, Ordering::Relaxed);
            if b >= P {
                break;
            }
            let mut evs: Vec<Ev> = Vec::new();
            for t in &survivors {
                evs.extend_from_slice(&t[b]);
            }
            let mut cks: Vec<Ck> = Vec::new();
            for t in &parsed {
                cks.extend_from_slice(&t.1[b]);
            }
            evs.sort_unstable_by(|a, b| a.ph.cmp(&b.ph).then(a.ts.cmp(&b.ts)));
            cks.sort_unstable_by(|a, b| a.ph.cmp(&b.ph).then(a.ts.cmp(&b.ts)).then_with(|| a.cid.cmp(b.cid)));
            process_bucket(&evs, &cks, &mut acc, ncamp, ndays, day0, ncat_total);
        }
        acc
    });
    lap("sessions");

    // ---- output ----
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    par(accs.len(), |i| {
        let a = &accs[i];
        let cats: Vec<Option<B>> = a.u_cat.iter().map(|&c| if c == 255 { None } else { Some(cat_names[c as usize]) }).collect();
        write_file(
            &ud.join(format!("part-{:03}.parquet", i)),
            &[
                ("user_id", Column::Bytes(a.u_name.clone())),
                ("day", Column::Date(a.u_day.clone())),
                ("sessions", Column::I64(a.u_sessions.clone())),
                ("events", Column::I64(a.u_events.clone())),
                ("revenue_cents", Column::I64(a.u_rev.clone())),
                ("top_category", Column::OptBytes(cats)),
            ],
        )
        .unwrap();
    });
    // campaign_daily: sum thread tables
    let mut c_id: Vec<B> = vec![];
    let mut c_ch: Vec<B> = vec![];
    let mut c_day: Vec<i32> = vec![];
    let mut c_s: Vec<i64> = vec![];
    let mut c_p: Vec<i64> = vec![];
    let mut c_r: Vec<i64> = vec![];
    let mut c_a: Vec<i64> = vec![];
    for c in 0..ncamp1 {
        for d in 0..ndays {
            let k = c * ndays + d;
            let s: u64 = accs.iter().map(|a| a.c_sessions[k]).sum();
            let a_: u64 = accs.iter().map(|a| a.c_assist[k]).sum();
            if s == 0 && a_ == 0 {
                continue;
            }
            c_id.push(camp_ids[c]);
            c_ch.push(camp_ch[c]);
            c_day.push((day0 + d as i64) as i32);
            c_s.push(s as i64);
            c_p.push(accs.iter().map(|a| a.c_purch[k]).sum::<u64>() as i64);
            c_r.push(accs.iter().map(|a| a.c_rev[k]).sum::<u64>() as i64);
            c_a.push(a_ as i64);
        }
    }
    write_file(
        &cd.join("part-000.parquet"),
        &[
            ("campaign_id", Column::Bytes(c_id)),
            ("channel", Column::Bytes(c_ch)),
            ("day", Column::Date(c_day)),
            ("sessions", Column::I64(c_s)),
            ("purchasing_sessions", Column::I64(c_p)),
            ("revenue_cents", Column::I64(c_r)),
            ("assisted_sessions", Column::I64(c_a)),
        ],
    )
    .unwrap();
    lap("output");
    std::process::exit(0);
}

fn process_bucket(evs: &[Ev], cks: &[Ck], acc: &mut Acc, ncamp: usize, ndays: usize, day0: i64, ncat: usize) {
    let mut ci = 0usize; // pointer into cks (sorted by ph)
    let mut i = 0usize;
    while i < evs.len() {
        let ph = evs[i].ph;
        let mut j = i;
        while j < evs.len() && evs[j].ph == ph {
            j += 1;
        }
        let pe = &evs[i..j];
        while ci < cks.len() && cks[ci].ph < ph {
            ci += 1;
        }
        let mut cj = ci;
        while cj < cks.len() && cks[cj].ph == ph {
            cj += 1;
        }
        let pc = &cks[ci..cj];
        process_person(pe, pc, acc, ncamp, ndays, day0, ncat);
        i = j;
    }
}

fn add_click(acc: &mut Acc, c: u32) {
    let c = c as usize;
    if acc.cnt[c] == 0 {
        acc.pos[c] = acc.active.len() as u32;
        acc.active.push(c as u32);
    }
    acc.cnt[c] += 1;
}

fn rem_click(acc: &mut Acc, c: u32) {
    let c = c as usize;
    acc.cnt[c] -= 1;
    if acc.cnt[c] == 0 {
        let p = acc.pos[c] as usize;
        let last = acc.active.pop().unwrap();
        if p < acc.active.len() {
            acc.active[p] = last;
            acc.pos[last as usize] = p as u32;
        }
    }
}

struct Sess {
    start: i64,
    events: i64,
    rev: i64,
    purchases: i64,
    cat_rev: Vec<i64>,
}

fn process_person(pe: &[Ev], pc: &[Ck], acc: &mut Acc, ncamp: usize, ndays: usize, day0: i64, ncat: usize) {
    // sessionize
    let name = pe[0].name;
    let mut sessions: Vec<Sess> = Vec::new();
    let mut prev = i64::MIN;
    for e in pe {
        if sessions.is_empty() || e.ts - prev > 1_800_000 {
            sessions.push(Sess { start: e.ts, events: 0, rev: 0, purchases: 0, cat_rev: vec![0; ncat] });
        }
        prev = e.ts;
        let s = sessions.last_mut().unwrap();
        s.events += 1;
        if e.ty == 2 {
            s.rev += e.price as i64;
            s.purchases += 1;
            s.cat_rev[e.cat as usize] += e.price as i64;
        }
    }
    // attribution
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let mut d_sessions = 0i64;
    let mut d_events = 0i64;
    let mut d_rev = 0i64;
    let mut d_cat = vec![0i64; ncat];
    let flush = |acc: &mut Acc, day: i64, s: i64, ev: i64, rev: i64, cat: &[i64]| {
        acc.u_name.push(name);
        acc.u_day.push(day as i32);
        acc.u_sessions.push(s);
        acc.u_events.push(ev);
        acc.u_rev.push(rev);
        let mut top = 255u8;
        let mut bv = 0i64;
        for (k, &v) in cat.iter().enumerate() {
            if v > bv {
                bv = v;
                top = k as u8;
            }
        }
        acc.u_cat.push(top);
    };
    for s in &sessions {
        let st = s.start;
        while hi < pc.len() && pc[hi].ts <= st {
            add_click(acc, pc[hi].camp);
            hi += 1;
        }
        while lo < hi && pc[lo].ts < st - WEEK {
            rem_click(acc, pc[lo].camp);
            lo += 1;
        }
        let credit = if lo == hi {
            ncamp
        } else {
            let mut j = hi - 1;
            let t = pc[j].ts;
            while j > lo && pc[j - 1].ts == t {
                j -= 1;
            }
            pc[j].camp as usize
        };
        let day = st.div_euclid(DAY);
        let di = (day - day0) as usize;
        debug_assert!(di < ndays);
        let k = credit * ndays + di;
        acc.c_sessions[k] += 1;
        if s.purchases > 0 {
            acc.c_purch[k] += 1;
        }
        acc.c_rev[k] += s.rev as u64;
        for a in 0..acc.active.len() {
            let c = acc.active[a] as usize;
            acc.c_assist[c * ndays + di] += 1;
        }
        // user_daily accumulation
        if day != cur_day {
            if cur_day != i64::MIN {
                flush(acc, cur_day, d_sessions, d_events, d_rev, &d_cat);
            }
            cur_day = day;
            d_sessions = 0;
            d_events = 0;
            d_rev = 0;
            d_cat.iter_mut().for_each(|x| *x = 0);
        }
        d_sessions += 1;
        d_events += s.events;
        d_rev += s.rev;
        for (k, v) in s.cat_rev.iter().enumerate() {
            d_cat[k] += v;
        }
    }
    if cur_day != i64::MIN {
        flush(acc, cur_day, d_sessions, d_events, d_rev, &d_cat);
    }
    for t in lo..hi {
        rem_click(acc, pc[t].camp);
    }
}
