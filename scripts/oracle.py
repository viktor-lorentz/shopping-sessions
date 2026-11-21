#!/usr/bin/env python3
"""Slow, literal reference implementation of rules.md. Usage: oracle.py DATA_DIR OUT_DIR
Writes OUT_DIR/user_daily.csv and OUT_DIR/campaign_daily.csv (compare.py reads these).
Strings are handled as latin-1 so str ordering equals byte-wise ordering."""
import calendar
import datetime
import glob
import json
import os
import re
import sys

D = "0123456789"
DAY = 86400000
EPOCH = datetime.date(1970, 1, 1).toordinal()


def read_lines(path):
    text = open(path, "rb").read().decode("latin-1")
    parts = text.split("\n")
    rest = parts[1:]
    if rest and rest[-1] == "":
        rest.pop()
    return rest


def fields(line, n):
    fs = line.split(",")
    if len(fs) != n:
        return None
    out = []
    for f in fs:
        if '"' in f:
            if len(f) >= 2 and f[0] == '"' and f[-1] == '"' and '"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out


def alldig(s):
    return len(s) > 0 and all(c in D for c in s)


F1 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$", re.ASCII)
F2 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$", re.ASCII)


def civil_ms(m):
    y, mo, d, h, mi, s = (int(x) for x in m[:6])
    if not (1 <= mo <= 12):
        return None
    if not (1 <= d <= calendar.monthrange(y, mo)[1]) if y >= 1 else not (1 <= d <= 28):
        return None
    if h > 23 or mi > 59 or s > 59:
        return None
    if y < 1:
        return -(10 ** 30)  # valid form, far outside any window
    days = datetime.date(y, mo, d).toordinal() - EPOCH
    return days * DAY + h * 3600000 + mi * 60000 + s * 1000


def parse_ts(s):
    if alldig(s):
        return int(s)
    m = F1.match(s)
    if m:
        return civil_ms(m.groups())
    m = F2.match(s)
    if m:
        g = m.groups()
        hh, mm = int(g[7]), int(g[8])
        if hh > 23 or mm > 59:
            return None
        base = civil_ms(g[:6])
        if base is None:
            return None
        off = (hh * 60 + mm) * 60000
        return base - off if g[6] == "+" else base + off
    return None


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws = parse_ts(man["window_start"])
    we = parse_ts(man["window_end"])

    cat_of = {}
    for ln in read_lines(os.path.join(data, "products.csv")):
        f = fields(ln, 2)
        if f:
            cat_of[f[0]] = f[1]
    chan = {}
    for ln in read_lines(os.path.join(data, "campaigns.csv")):
        f = fields(ln, 2)
        if f:
            chan[f[0]] = f[1]

    # links
    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for p in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
        for ln in read_lines(p):
            f = fields(ln, 2)
            if not f or f[0] == "" or f[1] == "":
                continue
            for x in f:
                parent.setdefault(x, x)
            ra, rb = find(f[0]), find(f[1])
            if ra != rb:
                parent[ra] = rb
    comp_min = {}
    for x in parent:
        r = find(x)
        if r not in comp_min or x < comp_min[r]:
            comp_min[r] = x

    def person(uid):
        return comp_min[find(uid)] if uid in parent else uid

    # events
    best = {}
    for p in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
        for ln in read_lines(p):
            f = fields(ln, 7)
            if not f or f[0] == "":
                continue
            ing = parse_ts(f[3])
            ing = ing if ing is not None else 10 ** 30
            cur = best.get(f[0])
            if cur is None or ing < cur[0]:
                best[f[0]] = (ing, f)
    events = {}
    for eid, (ing, f) in best.items():
        _, uid, tsb, _, ety, prod, price = f
        if uid == "" or prod == "":
            continue
        ts = parse_ts(tsb)
        if ts is None or not (ws <= ts < we):
            continue
        if ety not in ("view", "cart", "purchase"):
            continue
        pr = 0
        if ety == "purchase":
            if not alldig(price) or not (1 <= int(price) <= 10000000):
                continue
            pr = int(price)
        if ing - ts > DAY:
            continue
        events.setdefault(person(uid), []).append((ts, ety, prod, pr))

    # clicks
    clicks = {}
    seen = set()
    for p in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
        for ln in read_lines(p):
            f = fields(ln, 4)
            if not f or f[0] == "" or f[0] in seen:
                continue
            cid, uid, camp, tsb = f
            ts = parse_ts(tsb)
            if uid == "" or ts is None or not (ws - 7 * DAY <= ts < we) or camp not in chan:
                continue
            seen.add(cid)
            clicks.setdefault(person(uid), []).append((ts, cid, camp))

    user_rows = {}
    camp_rows = {}

    def crow(camp, day):
        return camp_rows.setdefault((camp, day), [0, 0, 0, 0])

    for pr_, evs in events.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > 1800000:
                sessions.append([e[0], []])
            sessions[-1][1].append(e)
            prev = e[0]
        cks = clicks.get(pr_, [])
        for start, es in sessions:
            day = datetime.date.fromordinal(start // DAY + EPOCH)
            q = [c for c in cks if c[0] <= start and start - c[0] <= 7 * DAY]
            if q:
                mx = max(c[0] for c in q)
                cid, camp = min((c[1], c[2]) for c in q if c[0] == mx)
            else:
                camp = "organic"
            rev = sum(e[3] for e in es if e[1] == "purchase")
            purch = any(e[1] == "purchase" for e in es)
            r = crow(camp, day)
            r[0] += 1
            r[1] += 1 if purch else 0
            r[2] += rev
            for a in {c[2] for c in q}:
                crow(a, day)[3] += 1
            u = user_rows.setdefault((pr_, day), [0, 0, 0, {}])
            u[0] += 1
            u[1] += len(es)
            u[2] += rev
            for e in es:
                if e[1] == "purchase":
                    c = cat_of.get(e[2], "unknown")
                    u[3][c] = u[3].get(c, 0) + e[3]

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "wb") as fh:
        fh.write(b"user_id,day,sessions,events,revenue_cents,top_category\n")
        for (u, day), (s, ev, rev, cr) in sorted(user_rows.items()):
            top = ""
            if cr:
                top = min(cr.items(), key=lambda kv: (-kv[1], kv[0]))[0]
            fh.write(f"{u},{day.isoformat()},{s},{ev},{rev},{top}\n".encode("latin-1"))
    with open(os.path.join(out, "campaign_daily.csv"), "wb") as fh:
        fh.write(b"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        for (c, day), (s, p, rev, a) in sorted(camp_rows.items()):
            ch = "organic" if c == "organic" else chan[c]
            fh.write(f"{c},{ch},{day.isoformat()},{s},{p},{rev},{a}\n".encode("latin-1"))


main()
