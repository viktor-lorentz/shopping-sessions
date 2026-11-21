//! Minimal Parquet writer: one row group, PLAIN encoding, uncompressed, one data page per column.

use std::io::Write;

pub enum Column<'a> {
    Bytes(Vec<&'a [u8]>),
    OptBytes(Vec<Option<&'a [u8]>>),
    Date(Vec<i32>),
    I64(Vec<i64>),
}

// ---- thrift compact protocol ----
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

fn zz(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

impl T {
    fn new() -> T {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if (1..=15).contains(&d) {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.varint(zz(id as i64));
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.varint(zz(v as i64));
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.varint(zz(v));
    }
    fn strf(&mut self, id: i16, s: &str) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s.as_bytes());
    }
    fn begin_struct_field(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn begin_struct_elem(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list_field(&mut self, id: i16, elem_ty: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem_ty);
        } else {
            self.b.push(0xF0 | elem_ty);
            self.varint(n as u64);
        }
    }
    fn list_i32_elem(&mut self, v: i32) {
        self.varint(zz(v as i64));
    }
    fn list_str_elem(&mut self, s: &str) {
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s.as_bytes());
    }
}

fn varint_vec(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

// returns (parquet type, converted type or -1, page bytes, num_values)
fn encode(col: &Column) -> (i32, i32, Vec<u8>, usize, bool) {
    match col {
        Column::Bytes(v) => {
            let mut out = Vec::with_capacity(v.iter().map(|s| s.len() + 4).sum());
            for s in v {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
            (6, 0, out, v.len(), false)
        }
        Column::OptBytes(v) => {
            let n = v.len();
            let groups = (n + 7) / 8;
            let mut levels = Vec::new();
            varint_vec(&mut levels, ((groups as u64) << 1) | 1);
            let mut packed = vec![0u8; groups];
            for (i, s) in v.iter().enumerate() {
                if s.is_some() {
                    packed[i / 8] |= 1 << (i % 8);
                }
            }
            levels.extend_from_slice(&packed);
            let mut out = Vec::new();
            out.extend_from_slice(&(levels.len() as u32).to_le_bytes());
            out.extend_from_slice(&levels);
            for s in v.iter().flatten() {
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s);
            }
            (6, 0, out, n, true)
        }
        Column::Date(v) => {
            let mut out = Vec::with_capacity(v.len() * 4);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            (1, 6, out, v.len(), false)
        }
        Column::I64(v) => {
            let mut out = Vec::with_capacity(v.len() * 8);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            (2, -1, out, v.len(), false)
        }
    }
}

pub fn write_file(path: &std::path::Path, cols: &[(&str, Column)]) -> std::io::Result<()> {
    let nrows = cols.first().map(|c| match &c.1 {
        Column::Bytes(v) => v.len(),
        Column::OptBytes(v) => v.len(),
        Column::Date(v) => v.len(),
        Column::I64(v) => v.len(),
    }).unwrap_or(0);
    let mut file: Vec<u8> = Vec::new();
    file.extend_from_slice(b"PAR1");
    struct Meta {
        ptype: i32,
        offset: i64,
        total: i64,
        n: i64,
    }
    let mut metas = Vec::new();
    let mut kinds = Vec::new();
    for (_, col) in cols {
        let (ptype, conv, data, n, optional) = encode(col);
        let mut ph = T::new();
        ph.i32f(1, 0); // DATA_PAGE
        ph.i32f(2, data.len() as i32);
        ph.i32f(3, data.len() as i32);
        ph.begin_struct_field(5);
        ph.i32f(1, n as i32);
        ph.i32f(2, 0); // PLAIN
        ph.i32f(3, 3); // RLE
        ph.i32f(4, 3); // RLE
        ph.end_struct();
        ph.end_struct_top();
        let offset = file.len() as i64;
        file.extend_from_slice(&ph.b);
        file.extend_from_slice(&data);
        metas.push(Meta { ptype, offset, total: (ph.b.len() + data.len()) as i64, n: n as i64 });
        kinds.push((conv, optional));
    }
    let mut m = T::new();
    m.i32f(1, 1);
    // schema: root + one element per column
    m.list_field(2, 12, cols.len() + 1);
    m.begin_struct_elem();
    m.strf(4, "schema");
    m.i32f(5, cols.len() as i32);
    m.end_struct();
    for (i, (name, _)) in cols.iter().enumerate() {
        m.begin_struct_elem();
        m.i32f(1, metas[i].ptype);
        m.i32f(3, if kinds[i].1 { 1 } else { 0 });
        m.strf(4, name);
        if kinds[i].0 >= 0 {
            m.i32f(6, kinds[i].0);
        }
        m.end_struct();
    }
    m.i64f(3, nrows as i64);
    m.list_field(4, 12, 1);
    m.begin_struct_elem();
    m.list_field(1, 12, cols.len());
    let mut total_bytes = 0;
    for (i, (name, _)) in cols.iter().enumerate() {
        let me = &metas[i];
        total_bytes += me.total;
        m.begin_struct_elem();
        m.i64f(2, me.offset);
        m.begin_struct_field(3);
        m.i32f(1, me.ptype);
        m.list_field(2, 5, 2);
        m.list_i32_elem(0);
        m.list_i32_elem(3);
        m.list_field(3, 8, 1);
        m.list_str_elem(name);
        m.i32f(4, 0);
        m.i64f(5, me.n);
        m.i64f(6, me.total);
        m.i64f(7, me.total);
        m.i64f(9, me.offset);
        m.end_struct();
        m.end_struct();
    }
    m.i64f(2, total_bytes);
    m.i64f(3, nrows as i64);
    m.end_struct();
    m.strf(6, "shopping");
    m.end_struct_top();
    file.extend_from_slice(&m.b);
    file.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path)?;
    f.write_all(&file)
}

impl T {
    fn end_struct_top(&mut self) {
        self.b.push(0);
    }
}
