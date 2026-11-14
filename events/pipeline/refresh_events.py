"""One-command daily refresh of the event calendar: fetch (incremental) -> copy main DB -> load -> check -> swap -> report.

Usage:  python3 -I pipeline/refresh_events.py [--no-fetch] [--dry-run] [--allow-large-change] [--as-of YYYY-MM-DD]

Steps
  1. Lock (logs/events_daily/.lock): a second run waits for nothing and exits 4.
  2. fetch_events.py --daily, then fetch_event_details.py (only ids not cached). A 429/403/WAF page stops the run with
     exit 2 and nothing is rebuilt: as_of must not claim a day whose data was not fetched.
  3. Copy db/cse_financials.db with the SQLite backup API to db/work_events.db (main is only read).
  4. load_events.py --db db/work_events.db --as-of <today, Colombo>. The loader refuses (exit 1) on its own checks.
  5. Checks on the copy: PRAGMA quick_check, event_load_meta.as_of = today, row counts of every non-event table
     unchanged by the load, and the change sanity limits (event_diff.sanity: not more than max(10, 20%) upcoming
     confirmed events disappearing, total events not falling below 85%). Failure = exit 1, main untouched.
  6. Changes (event_diff) appended to company_event_change in the copy.
  7. Swap: main must be unchanged since step 3 (inode, size, mtime; no -journal/-wal file), else the copy is redone once
     and then the run gives up (exit 3) - another session may be writing to main. Main is kept as a hard link
     db/backup_events_<YYYYMMDD_HHMMSS>.db (the 7 newest are kept), then the copy is renamed over it (atomic).
  8. Report: logs/events_daily/<as_of>.md and .json; status in logs/events_daily/LAST_RUN.json.
Exit codes: 0 ok, 1 check failed, 2 fetch stopped (blocked / API change), 3 main changed during the run, 4 already running.
"""
import argparse, datetime as dt, fcntl, json, os, pathlib, sqlite3, subprocess, sys, time, traceback

ROOT = pathlib.Path(os.environ.get("EVENTS_ROOT") or pathlib.Path(__file__).resolve().parent.parent / "work")  # data dir: db/, raw_api/, logs/
CODE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(CODE))
import event_diff as ED  # noqa: E402

MAIN = ROOT / "db" / "cse_financials.db"
WORK = ROOT / "db" / "work_events.db"
LOGDIR = ROOT / "logs" / "events_daily"
COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))
EVENT_TABLES = {"company_event", "company_event_source", "result_estimate_backtest", "event_load_meta", "company_event_change"}
KEEP_BACKUPS = 7
PY = [sys.executable, "-I"]


class Fail(Exception):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code


def log(*a):
    print(dt.datetime.now(COLO).strftime("%H:%M:%S"), *a, flush=True)


def run(args, code_on_2=2):
    log("$", " ".join(str(x) for x in args[2:]))
    p = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    for line in (p.stdout + p.stderr).splitlines():
        print("   ", line, flush=True)
    if p.returncode == 2:
        raise Fail(code_on_2, f"{pathlib.Path(args[2]).name} stopped (exit 2): " + (p.stderr.strip().splitlines() or ["?"])[-1])
    if p.returncode != 0:
        raise Fail(1, f"{pathlib.Path(args[2]).name} failed (exit {p.returncode})")
    return p.stdout


def fingerprint(path):
    st = os.stat(path)
    side = [s for s in ("-journal", "-wal") if os.path.exists(str(path) + s) and os.path.getsize(str(path) + s) > 0]
    return (st.st_ino, st.st_size, st.st_mtime_ns, tuple(side))


def table_counts(db):
    names = [r[0] for r in db.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%'")]
    return {n: db.execute(f'select count(*) from "{n}"').fetchone()[0] for n in names if n not in EVENT_TABLES}


def copy_main():
    """Consistent copy of main (SQLite backup API). Returns main's fingerprint taken before the copy."""
    for _ in range(30):
        fp = fingerprint(MAIN)
        if not fp[3]:
            break
        log("main has an open journal (another writer); waiting 20 s")
        time.sleep(20)
    else:
        raise Fail(3, "main database stayed busy (journal present) for 10 minutes")
    WORK.unlink(missing_ok=True)
    src = sqlite3.connect(f"file:{MAIN}?mode=ro", uri=True)
    dst = sqlite3.connect(WORK)
    src.backup(dst)
    dst.close(); src.close()
    if fingerprint(MAIN) != fp:
        raise Fail(3, "main changed while it was being copied")
    return fp


def build(as_of, allow_large):
    fp = copy_main()
    db = sqlite3.connect(WORK)
    old_rows = ED.load_rows(db)
    before = table_counts(db)
    db.close()
    run(PY + [str(CODE / "load_events.py"), "--db", str(WORK), "--as-of", as_of.isoformat()], code_on_2=1)
    db = sqlite3.connect(WORK)
    qc = db.execute("PRAGMA quick_check").fetchone()[0]
    if qc != "ok":
        raise Fail(1, f"quick_check on the copy: {qc}")
    meta = dict(db.execute("select key, value from event_load_meta"))
    if meta.get("as_of") != as_of.isoformat():
        raise Fail(1, f"event_load_meta.as_of = {meta.get('as_of')}, expected {as_of}")
    after = table_counts(db)
    if after != before:
        bad = {k: (before.get(k), after.get(k)) for k in set(before) | set(after) if before.get(k) != after.get(k)}
        raise Fail(1, f"load changed non-event tables: {bad}")
    new_rows = ED.load_rows(db)
    changes = ED.diff(old_rows, new_rows, meta["since"])
    problems = ED.sanity(changes, old_rows, new_rows, as_of)
    if problems and not allow_large:
        raise Fail(1, "change sanity: " + "; ".join(problems) + " (rerun with --allow-large-change after checking "
                   "the report in logs/events_daily/ if the changes are real)")
    run_at = dt.datetime.now(COLO).isoformat(timespec="seconds")
    ED.store(db, changes, run_at, as_of)
    db.commit()
    db.close()
    return fp, old_rows, new_rows, changes, meta, problems


def swap(fp):
    if fingerprint(MAIN) != fp:
        return False
    backup = MAIN.parent / f"backup_events_{dt.datetime.now(COLO):%Y%m%d_%H%M%S}.db"   # same filesystem as main
    os.link(MAIN, backup)                 # keeps the old main (same inode, no copy)
    os.replace(WORK, MAIN)                # atomic rename
    dfd = os.open(MAIN.parent, os.O_RDONLY)
    os.fsync(dfd); os.close(dfd)
    olds = sorted(MAIN.parent.glob("backup_events_*.db"))
    for f in olds[:-KEEP_BACKUPS]:
        f.unlink()
    log("swapped; previous main kept as", backup.name)
    return True


def write_report(md, js, text, changes_json):
    """A later run on the same day appends to the day's report instead of replacing it."""
    run_at = dt.datetime.now(COLO).strftime("%H:%M")
    if md.exists():
        with md.open("a") as f:
            f.write(f"\n\n---\n\n<!-- run {run_at} -->\n" + text.replace("# Event calendar changes - ", f"# Later run {run_at} - ", 1))
    else:
        md.write_text(text)
    old = json.loads(js.read_text()) if js.exists() else []
    js.write_text(json.dumps(old + [{"run_at": run_at, **c} for c in json.loads(changes_json)], indent=0))


def write_status(d):
    LOGDIR.mkdir(parents=True, exist_ok=True)
    (LOGDIR / ("LAST_DRYRUN.json" if d.get("dry_run") else "LAST_RUN.json")).write_text(json.dumps(d, indent=1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-fetch", action="store_true", help="rebuild from the cache only (no CSE requests)")
    ap.add_argument("--dry-run", action="store_true", help="everything except the swap; report goes to *_dryrun.md")
    ap.add_argument("--allow-large-change", action="store_true", help="accept changes beyond the sanity limits")
    ap.add_argument("--as-of", help="default: today in Colombo")
    a = ap.parse_args()
    LOGDIR.mkdir(parents=True, exist_ok=True)
    lock = open(LOGDIR / ".lock", "w")  # noqa: SIM115 (held for the whole run)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("another refresh is running", file=sys.stderr)
        sys.exit(4)
    as_of = dt.date.fromisoformat(a.as_of) if a.as_of else dt.datetime.now(COLO).date()
    started = dt.datetime.now(COLO).isoformat(timespec="seconds")
    status = {"started": started, "as_of": as_of.isoformat(), "dry_run": a.dry_run}
    try:
        fetch_summary = None
        n_cache = sum(1 for _ in (ROOT / "raw_api" / "events").glob("*.json")) if (ROOT / "raw_api" / "events").exists() else 0
        if n_cache < 100 or not MAIN.exists():
            raise Fail(1, f"data directory {ROOT} has no database or only {n_cache} cached responses: pull it first "
                          "(events_sync.py pull) - an incremental run on an empty cache would refetch everything")
        if not a.no_fetch:
            out = run(PY + [str(CODE / "fetch_events.py"), "--daily"])
            fetch_summary = json.loads(out.strip().splitlines()[-1])
            out = run(PY + [str(CODE / "fetch_event_details.py"), "--max", "40"])
            status["detail_fetch"] = out.strip().splitlines()[-1]
            out = run(PY + [str(CODE / "letters.py")])
            status["letters"] = (out.strip().splitlines() or ["-"])[-1]
        status["fetch"] = fetch_summary
        for attempt in (1, 2):
            fp, old_rows, new_rows, changes, meta, problems = build(as_of, a.allow_large_change)
            if a.dry_run:
                WORK.unlink(missing_ok=True)
                break
            if swap(fp):
                break
            log("main changed during the build (another session writing?)" + ("; rebuilding once" if attempt == 1 else ""))
        else:
            raise Fail(3, "main changed during the build twice; not swapped (rerun later)")
        extra = {"events": f"{len(old_rows)} -> {len(new_rows)}", "window since": meta["since"],
                 "CSE requests": (fetch_summary or {}).get("requests", 0) if fetch_summary else "none (--no-fetch)",
                 "detail fetch": status.get("detail_fetch", "-")}
        if fetch_summary and fetch_summary.get("gaps"):
            extra["FEED GAP"] = "; ".join(f"{g['endpoint']} ids {g['newest_id_before']}..{g['oldest_id_now']} may be missing" for g in fetch_summary["gaps"])
        if problems:
            extra["sanity limits exceeded (accepted with --allow-large-change)"] = "; ".join(problems)
        stem = as_of.isoformat() + ("_dryrun" if a.dry_run else "")
        write_report(LOGDIR / f"{stem}.md", LOGDIR / f"{stem}.json", ED.report(changes, as_of, extra), ED.to_json(changes))
        status.update(ok=True, exit=0, finished=dt.datetime.now(COLO).isoformat(timespec="seconds"),
                      counts=ED.counts(changes), report=str(LOGDIR / f"{stem}.md"), gaps=(fetch_summary or {}).get("gaps", []))
        write_status(status)
        log("done:", json.dumps(ED.counts(changes)), "report", LOGDIR / f"{stem}.md")
        if status["gaps"]:
            log("WARNING: feed gap - see the report and docs/EVENTS_DAILY_REFRESH.md")
    except Fail as e:
        WORK.unlink(missing_ok=True)
        status.update(ok=False, exit=e.code, error=str(e), finished=dt.datetime.now(COLO).isoformat(timespec="seconds"))
        write_status(status)
        log("FAILED:", e)
        sys.exit(e.code)
    except Exception as e:
        WORK.unlink(missing_ok=True)
        status.update(ok=False, exit=1, error=repr(e), trace=traceback.format_exc(),
                      finished=dt.datetime.now(COLO).isoformat(timespec="seconds"))
        write_status(status)
        raise


if __name__ == "__main__":
    main()
