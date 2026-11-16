"""One daily run of the corporate-action capture: fetch -> discover new notices -> details -> build on a copy -> check ->
swap -> report. Works in ACTIONS_ROOT (default actions/work): db/actions.db and raw_api/ built by `actions_sync.py pull`.

Usage:  python3 -I actions/pipeline/refresh_actions.py [--no-fetch] [--dry-run] [--max-requests 40] [--allow-large-change]

Steps
  1. Lock (logs/.lock); a second run exits 4.
  2. Fetch (unless --no-fetch): approvedAnnouncement (1 request), merged into today's snapshot
     raw_api/actions/approvedAnnouncement__day-<date>.json.
  3. Discover: every notice in this job's feed snapshots, the events job's feed snapshots and its corporate calendar
     (both pulled read-only from event_cache) that is not stored yet and was announced on or after the newest stored
     notice's date minus 14 days. Company: the calendar's symbol, else the exact normalised company name.
  4. Details, oldest notice first, only for action notices (and plain listing notices, which need their text to be
     classified): cached copies first (this job, the events job), else getAnnouncementById, then
     getGeneralAnnouncementById when that returned nothing. The record is used only when its id and symbol match.
     When the request budget is used up, the run stops at that notice; the rest wait for the next run.
  5. Build (build_actions.Builder) on a copy of db/actions.db, then check the copy: integrity, input tables unchanged,
     every link resolves, new ids above every known id, no two actions with one security/kind/ex-date/amount, and the
     size limits (<= 60 new actions, <= 400 new notices per run unless --allow-large-change).
  6. Swap the copy in; write logs/LAST_RUN.json (status, counts) and logs/<date>.md (details: private, goes to Turso
     only). Standard output carries counts only - it is the public Actions log.
Exit codes: 0 ok, 1 check failed, 2 CSE refused (429/403/WAF/non-JSON), 4 already running.
"""
import argparse, datetime as dt, fcntl, hashlib, json, os, pathlib, sqlite3, sys, traceback

CODE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(CODE))
import build_actions as B  # noqa: E402
import cse_client as C  # noqa: E402
import notices as N  # noqa: E402

ROOT = pathlib.Path(os.environ.get("ACTIONS_ROOT") or CODE.parent / "work")
REVIEWED = CODE.parent / "reviewed"
COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))
INPUTS = ["company", "security", "announcement", "corporate_action", "corporate_action_announcement"]
FEED = "approvedAnnouncement"
CURSOR_OVERLAP_DAYS = 14
MAX_NEW_ACTIONS, MAX_NEW_NOTICES = 60, 400


class Fail(Exception):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code


def log(*a):
    print(dt.datetime.now(COLO).strftime("%H:%M:%S"), *a, flush=True)


def paths(root):
    root = pathlib.Path(root)
    return root / "db" / "actions.db", root / "db" / "work_actions.db", root / "logs"


# ---- feed and discovery ----------------------------------------------------------------------------------------
def feed_rows(data):
    return list((data or {}).get("approvedAnnouncements") or [])


def fetch_feed(client, root, today):
    """Merge today's approvedAnnouncement into this job's snapshot for the day. Returns the number of records."""
    data = client._request(FEED, "POST", {})
    rows = feed_rows(data)
    if not rows:
        raise C.Stop(f"{FEED} returned no records (shape changed or blocked?)")
    f = pathlib.Path(root) / "raw_api" / "actions" / f"{FEED}__day-{today.isoformat()}.json"
    old = feed_rows(C.read_cached(f)[0]) if f.exists() else []
    merged = {r["announcementId"]: r for r in old}
    merged.update({r["announcementId"]: r for r in rows})
    tmp = f.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"_fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                               "data": {"approvedAnnouncements": sorted(merged.values(), key=lambda r: -r["announcementId"])}}))
    os.replace(tmp, f)
    return len(rows)


def discovered(root):
    """Every notice record the feeds and calendar know of, by announcement id: (record, found_in). The calendar copy
    (it carries the symbol and the current dates) is preferred; a feed copy fills what it lacks."""
    root = pathlib.Path(root)
    out = {}
    sources = [("feed", p) for p in sorted((root / "raw_api" / "actions").glob(f"{FEED}__day-*.json"))]
    sources += [("events_feed", p) for p in sorted((root / "raw_api" / "events").glob(f"{FEED}*.json"))]
    sources += [("calendar", p) for p in sorted((root / "raw_api" / "events").glob("corporateCalender__*.json"))]
    for tag, p in sources:
        data, _ = C.read_cached(p)
        rows = feed_rows(data) if tag != "calendar" else list((data or {}).get("approvedCoopCalAnnouncements") or [])
        for r in rows:
            aid = r.get("announcementId")
            if aid is None:
                continue
            if aid in out:
                prev, where = out[aid]
                base, other = (r, prev) if tag == "calendar" else (prev, r)
                merged = dict(other)
                merged.update({k: v for k, v in base.items() if v not in (None, "")})
                out[aid] = (merged, where if tag in where else where + "+" + tag)
            else:
                out[aid] = (dict(r), tag)
    return out


def company_of(rec, by_ticker, by_name):
    sym = (rec.get("symbol") or "").strip().split(".")[0]
    if sym and sym in by_ticker:
        return by_ticker[sym]
    return by_name.get(N.clean(rec.get("company")))


def ann_date(rec):
    return N.day(rec.get("dateOfAnnouncement")) or N.day(rec.get("createdDate"))


# ---- build and check -------------------------------------------------------------------------------------------------
def table_sha(db, t):
    h = hashlib.sha256()
    for row in db.execute(f'select * from "{t}" order by rowid'):
        h.update(repr(row).encode())
    return h.hexdigest()


def load_reviewed():
    p = REVIEWED / "notice_readings.json"
    r = json.loads(p.read_text()) if p.exists() else {}
    q = REVIEWED / "review_decisions.json"
    d = json.loads(q.read_text()) if q.exists() else {}
    return {int(k): v for k, v in r.items() if not k.startswith("_")}, {k: v for k, v in d.items() if not k.startswith("_")}


def build(root, run_at, today, client, allow_large, reviewed=None, decisions=None):
    main, work, _ = paths(root)
    work.unlink(missing_ok=True)
    src = sqlite3.connect(f"file:{main}?mode=ro", uri=True)
    db = sqlite3.connect(work)
    src.backup(db)
    src.close()
    before = {t: table_sha(db, t) for t in INPUTS}
    if reviewed is None:
        reviewed, decisions = load_reviewed()
    b = B.Builder(db, run_at, today, reviewed=reviewed, decisions=decisions)
    by_ticker = {t.split("#")[0]: cid for cid, t in b.tickers.items()}
    by_name = {}
    for cid, name in db.execute("select company_id, name from company"):
        by_name.setdefault(N.clean(name), set()).add(cid)
    by_name = {k: next(iter(v)) for k, v in by_name.items() if len(v) == 1}
    newest = max((n["announced_on"] for n in b.notes.values() if n["announced_on"] and n["cse_format"] == "NEW"), default=None)
    cursor = (dt.date.fromisoformat(newest) - dt.timedelta(days=CURSOR_OVERLAP_DAYS)).isoformat() if newest else "2000-01-01"
    recs = discovered(root)
    todo = []
    for aid, (r, where) in recs.items():
        if ("NEW", aid) in b.by_cse:
            continue
        on = ann_date(r)
        if not on or on < cursor:
            b.bump("older_than_cursor_not_stored")
            continue
        todo.append((on, aid, r, where))
    todo.sort(key=lambda x: (x[0], x[1]))
    b.bump("candidates", len(todo))
    stopped = None
    for on, aid, r, where in todo:
        cid = company_of(r, by_ticker, by_name)
        if cid is None:
            b.bump("company_unresolved")
            continue
        ticker = b.tickers[cid].split("#")[0]
        cat = (r.get("announcementCategory") or "").strip() or "(none)"
        catn = N.clean(cat) or "none"
        kinds, how = N.kinds_for(catn, b.kind_map, r.get("remarks") or "")
        needs_detail = bool(kinds & set(N.ACTIONS)) or catn in N.LISTING_CATEGORIES
        detail, status = None, "NOT_FETCHED"
        if needs_detail:
            try:
                detail, status, n = C.get_detail(root, "NEW", aid, ticker, cid, N.check_detail, client)
            except C.Budget:
                stopped = "budget"
                b.bump("left_for_next_run", len([t for t in todo if t[0] >= on]))
                break
            if status == "NOT_FETCHED":
                # no cached detail and no fetching allowed (--no-fetch): stop here, never store a notice without it
                stopped = "no_fetch"
                b.bump("left_for_next_run", len([t for t in todo if t[0] >= on]))
                break
        chk, base, dtype = N.check_detail("NEW", aid, detail, ticker, cid)
        docs = N.documents(detail)
        text = " ".join(x or "" for x in (cat, (base or {}).get("remarks"), (base or {}).get("title")))
        kinds, how = N.kinds_for(catn, b.kind_map, text)
        rec = dict(cse_format="NEW", cse_announcement_id=aid, company_id=cid, category_raw=cat, announced_on=on,
                   detail_status=status if status in ("FETCHED", "NO_DETAIL", "COMPANY_MISMATCH") else "NOT_FETCHED",
                   cse_detail_type=dtype, remarks=N.clean_text((base or {}).get("remarks")), file_url=docs[0] if docs else None,
                   kinds=kinds & set(N.ACTIONS), follow_up=N.follow_up_type(catn, (base or {}).get("remarks") or ""), documents=docs,
                   found_in=where)
        pk, _ = b.store_notice(rec)
        if status == "COMPANY_MISMATCH":
            b.bump("detail_refused_wrong_company")
            continue
        if kinds & set(N.ACTIONS):
            b.process(pk, detail if status == "FETCHED" else None, r)
    b.apply_late_readings(None)
    b.apply_decisions()
    db.commit()
    check(db, before, b, allow_large)
    db.close()
    return b, stopped


def check(db, before, b, allow_large):
    qc = db.execute("pragma quick_check").fetchone()[0]
    if qc != "ok":
        raise Fail(1, f"quick_check on the copy: {qc}")
    changed = [t for t, h in before.items() if table_sha(db, t) != h]
    if changed:
        raise Fail(1, f"input tables changed by the build: {changed}")
    q = lambda s: db.execute(s).fetchall()
    bad = q("""select count(*) from ac_link l where l.action_id not in (select action_id from corporate_action union select action_id from ac_action)
               or l.announcement_pk not in (select announcement_pk from announcement union select announcement_pk from ac_announcement)""")[0][0]
    if bad:
        raise Fail(1, f"{bad} links point to an unknown action or notice")
    clash = q("""select count(*) from ac_announcement c join announcement a using (cse_format, cse_announcement_id)
                 where a.announcement_pk != c.announcement_pk""")[0][0]
    clash += q("""select count(*) from ac_announcement c join announcement a using (announcement_pk)
                  where a.cse_format != c.cse_format or a.cse_announcement_id != c.cse_announcement_id""")[0][0]
    clash += q("""select count(*) from ac_action c join corporate_action a using (action_id)
                  where a.security_id != c.security_id or a.action_type != c.action_type""")[0][0]
    if clash:
        raise Fail(1, f"{clash} id clashes between the job's rows and the input tables (has the local machine created ids?)")
    new_a = q(f"select min(action_id) from ac_action where updated_at='{b.run}' and created_at='{b.run}'")[0][0]
    if new_a is not None and new_a <= b.input_action_max:
        raise Fail(1, "a new action id is not above the input table's ids")
    dup = 0
    seen = {}
    for a in b.actions.values():
        if a.get("ex_date") and not str(a.get("status", "")).startswith("SAME_AS"):
            k = (a["security_id"], a["action_type"], a["ex_date"], round(float(a["per_share"]), 6) if a.get("per_share") is not None else -1)
            dup += k in seen
            seen[k] = a["action_id"]
    if dup:
        raise Fail(1, f"{dup} actions share security, kind, ex-date and amount (would break main's unique index)")
    if not allow_large and (b.counts.get("actions_created", 0) > MAX_NEW_ACTIONS or b.counts.get("notices_stored", 0) > MAX_NEW_NOTICES):
        raise Fail(1, f"size limits: {b.counts.get('actions_created', 0)} new actions, {b.counts.get('notices_stored', 0)} new notices "
                      "(check the report; rerun with --allow-large-change if real)")


def report(db_path, run_at, counts):
    """Private report (Turso ac_run.report): what this run changed, with symbols. Never printed to the public log."""
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    sym = dict(db.execute("select security_id, symbol from security"))
    lines = [f"# Corporate-action capture - {run_at}", "", "Counts: " + json.dumps(counts, sort_keys=True), ""]
    acts = {r[0]: r for r in db.execute("select action_id, security_id, action_type, announced_on, ex_date, ratio_text, per_share, issue_price from corporate_action")}
    acts.update({r[0]: r for r in db.execute("select action_id, security_id, action_type, announced_on, ex_date, ratio_text, per_share, issue_price from ac_action")})
    lines.append("## New actions")
    for r in db.execute("select action_id, security_id, action_type, announced_on, ex_date, record_date, effective_date, ratio_text, per_share, issue_price, status from ac_action where created_at=?", (run_at,)):
        lines.append(f"- {r[0]} {sym.get(r[1])} {r[2]} announced {r[3]} ex {r[4]} record {r[5]} effective {r[6]} ratio {r[7]} per share {r[8]} price {r[9]} {r[10]}")
    lines.append("\n## Notices attached to an earlier action")
    for aid, pk, ev in db.execute("select l.action_id, l.announcement_pk, l.evidence from ac_link l where l.recorded_at=? and l.evidence != 'created'", (run_at,)):
        a = acts.get(aid)
        lines.append(f"- notice {pk} -> action {aid} {sym.get(a[1]) if a else '?'} {a[2] if a else ''} (evidence {ev})")
    lines.append("\n## Value changes")
    for aid, f, v, st, src, kind in db.execute("select action_id, field, value, status, source_pk, source_kind from ac_value where recorded_at=? order by value_id", (run_at,)):
        lines.append(f"- action {aid} {f} = {v} ({st}, from {kind} {src or ''})")
    lines.append("\n## Queued")
    for k, q, r in db.execute("select item_key, queue, reason from ac_queue where created_at=?", (run_at,)):
        lines.append(f"- {q}: {k} - {r}")
    return "\n".join(lines) + "\n"


def write_status(logdir, d):
    logdir.mkdir(parents=True, exist_ok=True)
    (logdir / ("LAST_DRYRUN.json" if d.get("dry_run") else "LAST_RUN.json")).write_text(json.dumps(d, indent=1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-fetch", action="store_true", help="no CSE requests: cached feeds and details only")
    ap.add_argument("--dry-run", action="store_true", help="everything except the swap")
    ap.add_argument("--max-requests", type=int, default=40)
    ap.add_argument("--allow-large-change", action="store_true")
    a = ap.parse_args()
    main_db, work, logdir = paths(ROOT)
    logdir.mkdir(parents=True, exist_ok=True)
    lock = open(logdir / ".lock", "w")  # noqa: SIM115
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("another run is active", file=sys.stderr)
        sys.exit(4)
    today = dt.datetime.now(COLO).date()
    run_at = dt.datetime.now(COLO).isoformat(timespec="seconds")
    status = {"started": run_at, "as_of": today.isoformat(), "dry_run": a.dry_run}
    client = None
    try:
        if not main_db.exists() or not (ROOT / "raw_api" / "events").exists():
            raise Fail(1, f"data directory {ROOT} is empty: pull it first (actions_sync.py pull)")
        if not a.no_fetch:
            client = C.Client(ROOT, max_requests=a.max_requests)
            n = fetch_feed(client, ROOT, today)
            log("feed records:", n)
        b, stopped = build(ROOT, run_at, today, client, a.allow_large_change)
        counts = dict(b.counts)
        counts["cse_requests"] = client.requests if client else 0
        if stopped:
            counts["stopped_by"] = stopped
        open_q = {}
        db = sqlite3.connect(work)
        for q, n in db.execute("select queue, count(*) from ac_queue where status='OPEN' group by 1"):
            open_q[q] = n
        db.close()
        text = report(work, run_at, counts)
        if a.dry_run:
            work.unlink(missing_ok=True)
        else:
            os.replace(work, main_db)
        stem = today.isoformat() + ("_dryrun" if a.dry_run else "")
        (logdir / f"{stem}.md").write_text(text)
        status.update(ok=True, exit=0, counts=counts, queues=open_q, report=str(logdir / f"{stem}.md"),
                      finished=dt.datetime.now(COLO).isoformat(timespec="seconds"))
        write_status(logdir, status)
        log("done:", json.dumps(counts, sort_keys=True))
        log("open queues:", json.dumps(open_q, sort_keys=True))
    except C.Stop as e:
        work.unlink(missing_ok=True)
        status.update(ok=False, exit=2, error="CSE refused: " + str(e), counts={"cse_requests": client.requests if client else 0},
                      finished=dt.datetime.now(COLO).isoformat(timespec="seconds"))
        write_status(logdir, status)
        log("STOPPED: the CSE refused a request; nothing changed (see README 'On failure')")
        sys.exit(2)
    except Fail as e:
        work.unlink(missing_ok=True)
        status.update(ok=False, exit=e.code, error=str(e), finished=dt.datetime.now(COLO).isoformat(timespec="seconds"))
        write_status(logdir, status)
        log("FAILED: check failed, exit", e.code, "(details in the run record)")
        sys.exit(e.code)
    except Exception as e:
        work.unlink(missing_ok=True)
        status.update(ok=False, exit=1, error=repr(e), trace=traceback.format_exc(), finished=dt.datetime.now(COLO).isoformat(timespec="seconds"))
        write_status(logdir, status)
        log("FAILED: unexpected error (details in the run record)")
        sys.exit(1)


if __name__ == "__main__":
    main()
