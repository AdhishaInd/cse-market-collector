"""Compare two builds of company_event (before / after a refresh) and classify every difference.

Change types (stored in company_event_change.change_type and listed in the daily report):
  NEW            event not in the previous build (a new announcement, notice, halt ...)
  MOVED          same event, different date (a 'further announcement', a rescheduled meeting, a re-estimated result)
  REMOVED        event in the previous build, gone now, still inside the window: withdrawn / cancelled / superseded
                 without a replacement - check these
  CHANGED        same event and date, but date_kind, amount or status changed (e.g. tentative -> confirmed,
                 UPCOMING -> OVERDUE)
  FILED          a RESULTS_DUE estimate disappeared because the filing it estimated arrived
  NEW_ESTIMATE   RESULTS_DUE row that entered the estimate window
  ESTIMATE_DROPPED  RESULTS_DUE row that left the window without a filing
  AGED_OUT       event older than the new window start (as_of - 365 days); counted, not listed
Pairing: rows match exactly on (company, share class, type, date); RESULTS_DUE rows match on their period instead of the
date. Unmatched old/new rows of the same company, class and type are paired as MOVED when the dates are within 120 days
(AGM/EGM) or 60 days (all others), nearest dates first.
"""
import collections, datetime as dt, json

MOVE_WINDOW = {"AGM": 120, "EGM": 120, "RIGHTS_EGM": 120, "SPLIT_EGM": 120}
DEFAULT_MOVE_WINDOW = 60
COLS = ("event_id", "company_id", "company_name", "symbol", "event_type", "event_date", "date_kind", "amount", "status",
        "detail", "source_endpoint", "source_id", "announced_on")


def load_rows(db):
    """company_event rows as dicts (with ticker), or [] when the table does not exist yet."""
    if not db.execute("select 1 from sqlite_master where name='company_event'").fetchone():
        return []
    q = ("select e." + ", e.".join(COLS) + ", c.ticker from company_event e left join company c using(company_id)")
    return [dict(zip(COLS + ("ticker",), r)) for r in db.execute(q)]


def ident(r):
    base = (r["company_id"], r["company_name"] if r["company_id"] is None else None, r["symbol"], r["event_type"])
    if r["event_type"] == "RESULTS_DUE":
        return base + (r["source_id"],)          # 'cid:kind:period_end' - the date is the estimate itself
    return base + (r["event_date"],)


def group(r):
    return (r["company_id"], r["company_name"] if r["company_id"] is None else None, r["symbol"], r["event_type"])


def _days(a, b):
    return abs((dt.date.fromisoformat(a) - dt.date.fromisoformat(b)).days)


def _amount_differs(a, b):
    if a is None or b is None:
        return (a is None) != (b is None)
    return abs(float(a) - float(b)) > 1e-9


def diff(old_rows, new_rows, new_since):
    """Returns a list of change dicts {change_type, row (new or old), old_date, new_date, what}."""
    new_since = str(new_since)
    old = {ident(r): r for r in old_rows}
    new = {ident(r): r for r in new_rows}
    out = []
    for k in old.keys() & new.keys():
        o, n = old[k], new[k]
        if o["event_date"] != n["event_date"]:          # RESULTS_DUE re-estimated
            out.append(dict(change_type="MOVED", row=n, old_date=o["event_date"], new_date=n["event_date"], what="date"))
        what = []
        if o["date_kind"] != n["date_kind"]:
            what.append(f"date_kind {o['date_kind']} -> {n['date_kind']}")
        if _amount_differs(o["amount"], n["amount"]):
            what.append(f"amount {o['amount']} -> {n['amount']}")
        if (o["status"] or "") != (n["status"] or ""):
            what.append(f"status {o['status']} -> {n['status']}")
        if what:
            out.append(dict(change_type="CHANGED", row=n, old_date=o["event_date"], new_date=n["event_date"], what="; ".join(what)))
    gone = [old[k] for k in old.keys() - new.keys()]
    came = [new[k] for k in new.keys() - old.keys()]
    # pair moves within the same company / class / type (not RESULTS_DUE: its identity already ignores the date)
    by_g = collections.defaultdict(lambda: ([], []))
    for r in gone:
        by_g[group(r)][0].append(r)
    for r in came:
        by_g[group(r)][1].append(r)
    paired_old, paired_new = set(), set()
    # same company, type and date, only the share-class symbol differs (e.g. the class came from a corporate_action row
    # that left the window): one CHANGED row, not REMOVED + NEW
    loose = collections.defaultdict(list)
    for r in came:
        loose[(r["company_id"], r["company_name"] if r["company_id"] is None else None, r["event_type"], r["event_date"])].append(r)
    for o in gone:
        if o["event_type"] == "RESULTS_DUE":
            continue
        for n in loose.get((o["company_id"], o["company_name"] if o["company_id"] is None else None, o["event_type"], o["event_date"]), []):
            if id(n) not in paired_new:
                paired_old.add(id(o)); paired_new.add(id(n))
                out.append(dict(change_type="CHANGED", row=n, old_date=o["event_date"], new_date=n["event_date"],
                                what=f"symbol {o['symbol']} -> {n['symbol']}"))
                break
    for g, (os_, ns) in by_g.items():
        os_ = [o for o in os_ if id(o) not in paired_old]
        ns = [n for n in ns if id(n) not in paired_new]
        if g[3] == "RESULTS_DUE" or not os_ or not ns:
            continue
        win = MOVE_WINDOW.get(g[3], DEFAULT_MOVE_WINDOW)
        cands = sorted((_days(o["event_date"], n["event_date"]), i, j) for i, o in enumerate(os_) for j, n in enumerate(ns)
                       if _days(o["event_date"], n["event_date"]) <= win)
        used_o, used_n = set(), set()
        for d, i, j in cands:
            if i in used_o or j in used_n:
                continue
            used_o.add(i); used_n.add(j)
            o, n = os_[i], ns[j]
            paired_old.add(id(o)); paired_new.add(id(n))
            out.append(dict(change_type="MOVED", row=n, old_date=o["event_date"], new_date=n["event_date"],
                            what=f"moved {d} days" + (f"; source {o['source_endpoint']}:{o['source_id']} -> {n['source_endpoint']}:{n['source_id']}"
                                                      if (o["source_endpoint"], o["source_id"]) != (n["source_endpoint"], n["source_id"]) else "")))
    filed = {(r["company_id"], r["detail"]) for r in new_rows if r["event_type"] == "RESULTS_FILED"}
    for r in gone:
        if id(r) in paired_old:
            continue
        if r["event_type"] == "RESULTS_DUE":
            # detail 'Q2 for period ending 2026-09-30' <-> RESULTS_FILED detail 'Q2 period ending 2026-09-30'
            fd = (r["detail"] or "").replace(" for period ending ", " period ending ")
            ct = "FILED" if (r["company_id"], fd) in filed else "ESTIMATE_DROPPED"
        elif r["event_date"] < new_since:
            ct = "AGED_OUT"
        else:
            ct = "REMOVED"
        out.append(dict(change_type=ct, row=r, old_date=r["event_date"], new_date=None, what=None))
    for r in came:
        if id(r) in paired_new:
            continue
        out.append(dict(change_type="NEW_ESTIMATE" if r["event_type"] == "RESULTS_DUE" else "NEW", row=r, old_date=None,
                        new_date=r["event_date"], what=None))
    out.sort(key=lambda c: (c["change_type"], c["new_date"] or c["old_date"], c["row"]["ticker"] or c["row"]["company_name"] or ""))
    return out


def counts(changes):
    return dict(sorted(collections.Counter(c["change_type"] for c in changes).items()))


def sanity(changes, old_rows, new_rows, as_of, max_removed=10, max_removed_share=0.2, min_kept=0.85):
    """Reasons to refuse the swap (empty list = fine). Estimated rows are excluded from the removal test: their window
    moves every day."""
    as_of = str(as_of)
    problems = []
    upcoming_old = [r for r in old_rows if r["event_date"] >= as_of and r["date_kind"] != "estimated"]
    removed_up = [c for c in changes if c["change_type"] == "REMOVED" and c["row"]["event_date"] >= as_of]
    limit = max(max_removed, int(max_removed_share * len(upcoming_old)))
    if len(removed_up) > limit:
        problems.append(f"{len(removed_up)} upcoming confirmed/tentative events would disappear (limit {limit})")
    if old_rows and len(new_rows) < min_kept * len(old_rows):
        problems.append(f"event count falls from {len(old_rows)} to {len(new_rows)} (< {min_kept:.0%})")
    return problems


CHANGE_TABLE = """
CREATE TABLE IF NOT EXISTS company_event_change (
    run_at       TEXT NOT NULL,      -- refresh run time (Colombo, ISO)
    as_of        TEXT NOT NULL,      -- event_load_meta.as_of of the new build
    change_type  TEXT NOT NULL CHECK (change_type IN ('NEW','MOVED','REMOVED','CHANGED','FILED','NEW_ESTIMATE','ESTIMATE_DROPPED','AGED_OUT')),
    company_id   INTEGER,
    ticker       TEXT,
    company_name TEXT,
    symbol       TEXT,
    event_type   TEXT NOT NULL,
    old_date     TEXT,
    new_date     TEXT,
    date_kind    TEXT,
    what         TEXT,               -- MOVED / CHANGED: what changed
    detail       TEXT,
    source       TEXT                -- endpoint:id of the row (new row when there is one)
);
CREATE INDEX IF NOT EXISTS ix_company_event_change_run ON company_event_change(as_of, change_type);
"""


def store(db, changes, run_at, as_of):
    """Append the changes (except AGED_OUT) to company_event_change. Caller commits."""
    db.executescript(CHANGE_TABLE)
    db.executemany("INSERT INTO company_event_change VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        (run_at, str(as_of), c["change_type"], c["row"]["company_id"], c["row"]["ticker"], c["row"]["company_name"],
         c["row"]["symbol"], c["row"]["event_type"], c["old_date"], c["new_date"], c["row"]["date_kind"], c["what"],
         c["row"]["detail"], f"{c['row']['source_endpoint']}:{c['row']['source_id']}")
        for c in changes if c["change_type"] != "AGED_OUT"])


def _name(r):
    return r["ticker"] or r["company_name"] or "?"


def _amt(r):
    return f" Rs.{float(r['amount']):g}" if r.get("amount") is not None else ""


def report(changes, as_of, extra=None):
    """Markdown daily report."""
    by = collections.defaultdict(list)
    for c in changes:
        by[c["change_type"]].append(c)
    L = [f"# Event calendar changes - {as_of}", ""]
    for k, v in (extra or {}).items():
        L.append(f"- {k}: {v}")
    L.append("- counts: " + ", ".join(f"{k} {v}" for k, v in counts(changes).items()) if changes else "- no changes")
    L.append("")

    def section(title, rows, fmt, limit=200):
        if not rows:
            return
        L.append(f"## {title} ({len(rows)})")
        L.append("")
        for c in rows[:limit]:
            L.append("- " + fmt(c))
        if len(rows) > limit:
            L.append(f"- ... {len(rows) - limit} more (see table company_event_change)")
        L.append("")

    up = lambda c: (c["new_date"] or c["old_date"]) >= str(as_of)
    new_ev = [c for c in by["NEW"] if c["row"]["event_type"] != "RESULTS_FILED"]
    section("New events, upcoming", [c for c in new_ev if up(c)],
            lambda c: f"{c['new_date']} {c['row']['event_type']} {_name(c['row'])} {c['row']['symbol'] or ''}{_amt(c['row'])} "
                      f"({c['row']['date_kind']}; announced {c['row']['announced_on'] or '?'})")
    section("New events, past dates (halts, filings of notices already held ...)", [c for c in new_ev if not up(c)],
            lambda c: f"{c['new_date']} {c['row']['event_type']} {_name(c['row'])}")
    section("Moved dates", [c for c in by["MOVED"] if c["row"]["event_type"] != "RESULTS_DUE"],
            lambda c: f"{c['row']['event_type']} {_name(c['row'])} {c['row']['symbol'] or ''}: {c['old_date']} -> {c['new_date']} ({c['what']})")
    section("Removed (withdrawn / cancelled / superseded without replacement) - check", by["REMOVED"],
            lambda c: f"{c['old_date']} {c['row']['event_type']} {_name(c['row'])} {c['row']['symbol'] or ''} "
                      f"(was {c['row']['date_kind']}, source {c['row']['source_endpoint']}:{c['row']['source_id']})")
    section("Changed", by["CHANGED"], lambda c: f"{c['new_date']} {c['row']['event_type']} {_name(c['row'])}: {c['what']}")
    section("Results filed", [c for c in by["NEW"] if c["row"]["event_type"] == "RESULTS_FILED"],
            lambda c: f"{c['new_date']} {_name(c['row'])} {c['row']['detail']} ({c['row']['source_endpoint']})")
    section("Results estimates moved", [c for c in by["MOVED"] if c["row"]["event_type"] == "RESULTS_DUE"],
            lambda c: f"{_name(c['row'])} {c['row']['detail']}: {c['old_date']} -> {c['new_date']}")
    section("Results estimates resolved by a filing", by["FILED"], lambda c: f"{_name(c['row'])} {c['row']['detail']} (was due {c['old_date']})", 60)
    section("New results estimates (window moved forward)", by["NEW_ESTIMATE"],
            lambda c: f"{c['new_date']} {_name(c['row'])} {c['row']['detail']}", 60)
    section("Results estimates dropped without a filing", by["ESTIMATE_DROPPED"],
            lambda c: f"{_name(c['row'])} {c['row']['detail']} (was {c['old_date']})", 60)
    if by["AGED_OUT"]:
        L.append(f"{len(by['AGED_OUT'])} events older than the window start left the calendar (normal).")
        L.append("")
    return "\n".join(L)


def to_json(changes):
    return json.dumps([{**{k: v for k, v in c.items() if k != "row"},
                        **{k: c["row"][k] for k in ("ticker", "company_name", "symbol", "event_type", "date_kind", "detail")}}
                       for c in changes if c["change_type"] != "AGED_OUT"], indent=0)
