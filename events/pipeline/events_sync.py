"""Sync the event calendar between Turso (database cse-events) and a local SQLite file / data directory.

Turso holds what the daily job needs and produces - never the full financials database:
  input tables  (written only by push-inputs, from the local main database): sector, reporting_template, company,
                security, announcement, corporate_action, corporate_action_announcement, listing_event, report_document
  event tables  (written by push-results, from the daily job): company_event, company_event_source,
                result_estimate_backtest, event_load_meta, company_event_change
  event_cache   raw API responses and letter readings (path, sha256, gzip body) - the job's incremental cache
  event_run     one row per daily run: status, counts, report text
  sync_meta     per-table checksums of the pushed inputs

Commands (env TURSO_EVENTS_URL / TURSO_EVENTS_TOKEN):
  init --main DB [--root DIR]   create the schema; push inputs, the current event tables and the cache (first time)
  push-inputs --main DB         push changed input tables (after a local report / announcement / corporate-action refresh)
  push-cache --root DIR         push changed cache files
  pull --root DIR               build DIR/db/cse_financials.db (inputs + events) and DIR/raw_api/... for the daily job
  push-results --root DIR       after refresh_events.py: replace the event tables, append changes, push changed cache
                                files and the run record - one Turso transaction
  pull-events --main DB         copy the event tables from Turso into the local main database (copy -> check -> swap)
  needs-reading                 rights letters waiting for a person / agent (from the cache in Turso)
  done-today                    exit 0 if today's run already succeeded (used by the retry schedule)
Writes that replace a table run in one transaction (Hrana interactive stream), so readers never see half a table.
"""
import argparse, base64, datetime as dt, gzip, hashlib, json, os, pathlib, sqlite3, sys, time, urllib.error, urllib.request

CODE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(CODE))

# parents before children (foreign keys); sector / reporting_template only because company references them
INPUT_TABLES = ["sector", "reporting_template", "company", "security", "announcement", "corporate_action",
                "corporate_action_announcement", "listing_event", "report_document"]
EVENT_TABLES = ["company_event", "company_event_source", "result_estimate_backtest", "event_load_meta"]
CHANGE_TABLE = "company_event_change"
VIEWS = ["v_upcoming_events", "v_event_multi_source"]
OWN_SCHEMA = [
    "CREATE TABLE IF NOT EXISTS event_cache (path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, body BLOB NOT NULL, updated_at TEXT NOT NULL)",
    """CREATE TABLE IF NOT EXISTS event_run (run_at TEXT PRIMARY KEY, as_of TEXT, ok INTEGER NOT NULL, exit_code INTEGER,
       counts TEXT, error TEXT, report TEXT, status TEXT)""",
    "CREATE TABLE IF NOT EXISTS sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)",
]
CACHE_SKIP = ("raw_api/events/letters/pdf/", "raw_api/events/pdf_sample/", "raw_api/events/rules/")
COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))


class Fail(Exception):
    pass


# ---- Turso over HTTP (Hrana v2 pipeline) ------------------------------------------------------------------------
def _arg(v):
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool) or isinstance(v, int):
        return {"type": "integer", "value": str(int(v))}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"type": "blob", "base64": base64.b64encode(bytes(v)).decode()}
    return {"type": "text", "value": str(v)}


def _val(c):
    t = c["type"]
    if t == "null":
        return None
    if t == "integer":
        return int(c["value"])
    if t == "float":
        return float(c["value"])
    if t == "blob":
        b = c["base64"]
        return base64.b64decode(b + "=" * (-len(b) % 4))      # Turso omits the padding
    return c["value"]


class Turso:
    def __init__(self, url=None, token=None):
        url = url or os.environ.get("TURSO_EVENTS_URL", "")
        self.token = token or os.environ.get("TURSO_EVENTS_TOKEN", "")
        if not url or not self.token:
            raise Fail("TURSO_EVENTS_URL / TURSO_EVENTS_TOKEN not set")
        self.base = url.replace("libsql://", "https://").rstrip("/")
        self.baton, self.url = None, self.base

    def _post(self, requests):
        body = json.dumps({"baton": self.baton, "requests": requests}).encode()
        err = None
        for attempt in range(4):
            try:
                r = urllib.request.Request(self.url + "/v2/pipeline", data=body, method="POST",
                                           headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"})
                with urllib.request.urlopen(r, timeout=180) as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                err = f"HTTP {e.code}: {e.read()[:300]!r}"
                if self.baton or e.code < 500:      # inside a transaction a retry could apply statements twice
                    break
            except Exception as e:
                err = repr(e)
                if self.baton:
                    break
            time.sleep(5 * (attempt + 1))
        raise Fail(f"Turso request failed: {err}")

    def run(self, statements, keep_open=False):
        """[(sql, args)] -> list of results ({cols, rows}); raises Fail on the first error."""
        reqs = [{"type": "execute", "stmt": {"sql": s, "args": [_arg(a) for a in args]}} for s, args in statements]
        if not keep_open:
            reqs.append({"type": "close"})
        out = self._post(reqs)
        self.baton = out.get("baton") if keep_open else None
        if keep_open and out.get("base_url"):
            self.url = out["base_url"].rstrip("/")
        if not keep_open:
            self.url = self.base
        res = []
        for x in out["results"]:
            if x.get("type") == "error":
                if keep_open:
                    self.abort()
                raise Fail(f"Turso: {x.get('error', {}).get('message')}")
            if x.get("type") == "ok" and x["response"]["type"] == "execute":
                res.append(x["response"]["result"])
        return res

    def query(self, sql, args=()):
        if self.baton:
            raise Fail("query while a transaction stream is open would close it (read before starting the Tx)")
        r = self.run([(sql, list(args))])[0]
        return [tuple(_val(c) for c in row) for row in r["rows"]]

    def abort(self):
        if self.baton:
            try:
                self._post([{"type": "execute", "stmt": {"sql": "ROLLBACK"}}, {"type": "close"}])
            except Fail:
                pass
        self.baton, self.url = None, self.base


class Tx:
    """One transaction across several HTTP requests (Hrana stream + baton). Each request is a Hrana *batch* whose steps
    each run only if the previous step succeeded: after a failed statement nothing else in the request runs. (In a plain
    pipeline the server rolls the transaction back on an error and then runs the remaining statements in autocommit -
    seen 2026-11-14 - which would commit half a write.) The first error is raised; the transaction is rolled back."""
    def __init__(self, t, chunk_bytes=2_000_000):
        self.t, self.buf, self.size, self.chunk, self.first = t, [], 0, chunk_bytes, True

    def add(self, sql, args=()):
        self.buf.append((sql, list(args)))
        self.size += len(sql) + sum(len(str(a)) + 16 for a in args)
        if self.size > self.chunk:
            self.flush()

    def _batch(self, stmts, close):
        steps = []
        for i, (s, args) in enumerate(stmts):
            st = {"stmt": {"sql": s, "args": [_arg(a) for a in args]}}
            if i:
                st["condition"] = {"type": "ok", "step": i - 1}
            steps.append(st)
        reqs = [{"type": "batch", "batch": {"steps": steps}}] + ([{"type": "close"}] if close else [])
        out = self.t._post(reqs)
        if not close:
            self.t.baton = out.get("baton")
            if out.get("base_url"):
                self.t.url = out["base_url"].rstrip("/")
        res = out["results"][0]
        if res.get("type") == "error":
            raise Fail(f"Turso: {res.get('error', {}).get('message')}")
        br = res["response"]["result"]
        for i, e in enumerate(br.get("step_errors") or []):
            if e:
                raise Fail(f"Turso: {e.get('message')} (statement: {stmts[i][0][:80]})")
        if any(r is None for r in br["step_results"]):
            raise Fail("Turso: a batch step did not run")

    def flush(self, close=False):
        stmts = ([("BEGIN", [])] if self.first else []) + self.buf
        self.buf, self.size = [], 0
        if not stmts and not close:
            return
        try:
            self._batch(stmts, close)
        except Exception:
            self.rollback()
            raise
        self.first = False

    def commit(self):
        self.buf.append(("COMMIT", []))
        self.flush(close=True)
        self.t.baton, self.t.url = None, self.t.base

    def rollback(self):
        self.buf = []
        self.t.abort()


# ---- helpers --------------------------------------------------------------------------------------------------
def schema_sql(db, names):
    """CREATE statements for the named tables (IF NOT EXISTS), their indexes, and the views, from a local db."""
    out = []
    for n in names:
        r = db.execute("select sql from sqlite_master where type='table' and name=?", (n,)).fetchone()
        if not r:
            raise Fail(f"table {n} missing locally")
        out.append(r[0].replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1))
        for (s,) in db.execute("select sql from sqlite_master where type='index' and tbl_name=? and sql is not null", (n,)):
            out.append(s.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1).replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1))
    return out


def columns(db, t):
    return [r[1] for r in db.execute(f'pragma table_info("{t}")')]


def table_sha(db, t):
    h = hashlib.sha256()
    for row in db.execute(f'select * from "{t}" order by rowid'):
        h.update(repr(row).encode())
    return h.hexdigest()


def insert_rows(tx, t, cols, rows, max_vars=30000):
    per = max(1, min(500, max_vars // max(1, len(cols))))
    ph = "(" + ",".join("?" * len(cols)) + ")"
    collist = ",".join(f'"{c}"' for c in cols)
    for i in range(0, len(rows), per):
        part = rows[i:i + per]
        tx.add(f'INSERT INTO "{t}" ({collist}) VALUES ' + ",".join([ph] * len(part)), [v for r in part for v in r])


def replace_tables(tx, db, tables):
    """Replace whole tables (given parent-first): deletes child-first, then inserts parent-first. Returns {table: rows}."""
    for t in reversed(tables):
        tx.add(f'DELETE FROM "{t}"')
    out = {}
    for t in tables:
        cols = columns(db, t)
        rows = db.execute(f'select {",".join(chr(34) + c + chr(34) for c in cols)} from "{t}" order by rowid').fetchall()
        insert_rows(tx, t, cols, rows)
        out[t] = len(rows)
    return out


def fetch_table(t, name, page=4000):
    """All rows of a Turso table, by rowid pages. Returns (cols, rows)."""
    cols, rows, last = None, [], -1 << 62
    while True:
        r = t.run([(f'select rowid as __rid, * from "{name}" where rowid > ? order by rowid limit {page}', [last])])[0]
        if cols is None:
            cols = [c["name"] for c in r["cols"]][1:]
        batch = [[_val(c) for c in row] for row in r["rows"]]
        rows += [b[1:] for b in batch]
        if len(batch) < page:
            return cols, rows
        last = batch[-1][0]


def cache_files(root):
    root = pathlib.Path(root)
    out = {}
    for p in sorted((root / "raw_api" / "events").rglob("*")):
        rel = p.relative_to(root).as_posix()
        if p.is_file() and not rel.startswith(CACHE_SKIP) and not rel.endswith((".tmp", ".pdf", ".png")):
            out[rel] = p
    # legacy announcement details that the calendar refers to
    cal_ids = set()
    for f in (root / "raw_api" / "events").glob("corporateCalender__*.json"):
        d = json.loads(f.read_text()).get("data") or {}
        cal_ids |= {r["announcementId"] for r in d.get("approvedCoopCalAnnouncements") or []}
    for a in sorted(cal_ids):
        p = root / "raw_api" / "ann_detail" / f"new_{a}.json"
        if p.exists():
            out[p.relative_to(root).as_posix()] = p
    return out


def sha(b):
    return hashlib.sha256(b).hexdigest()


def now():
    return dt.datetime.now(COLO).isoformat(timespec="seconds")


def remote_cache_index(t):
    return dict(t.query("select path, sha256 from event_cache"))


def push_cache(t, root, tx=None, known=None):
    """Upload cache files whose content differs from Turso. Uses tx when given (caller commits)."""
    known = remote_cache_index(t) if known is None else known
    own = tx is None
    tx = tx or Tx(t)
    n = 0
    try:
        for rel, p in cache_files(root).items():
            b = p.read_bytes()
            h = sha(b)
            if known.get(rel) != h:
                tx.add("INSERT INTO event_cache(path, sha256, body, updated_at) VALUES (?,?,?,?) ON CONFLICT(path) DO UPDATE SET "
                       "sha256=excluded.sha256, body=excluded.body, updated_at=excluded.updated_at", [rel, h, gzip.compress(b), now()])
                n += 1
        if own:
            tx.commit()
    except Exception:
        if own:
            tx.rollback()
        raise
    return n


# ---- commands -------------------------------------------------------------------------------------------------
def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def cmd_init(t, a):
    db = ro(a.main)
    stmts = schema_sql(db, INPUT_TABLES + EVENT_TABLES + [CHANGE_TABLE]) + OWN_SCHEMA
    for v in VIEWS:
        r = db.execute("select sql from sqlite_master where type='view' and name=?", (v,)).fetchone()
        if r:
            stmts += [f'DROP VIEW IF EXISTS "{v}"', r[0]]
    t.run([(s, []) for s in stmts])
    print("schema ready")
    cmd_push_inputs(t, a)
    if not t.query("select count(*) from company_event")[0][0]:
        tx = Tx(t)
        try:
            print(replace_tables(tx, db, EVENT_TABLES + [CHANGE_TABLE]))
            tx.commit()
        except Exception:
            tx.rollback(); raise
    if a.root:
        print("cache files pushed:", push_cache(t, a.root))


def cmd_push_inputs(t, a):
    db = ro(a.main)
    have = dict(t.query("select key, value from sync_meta where key like 'sha:%'"))
    todo = [(tb, table_sha(db, tb)) for tb in INPUT_TABLES]
    todo = [(tb, h) for tb, h in todo if have.get(f"sha:{tb}") != h]
    if not todo:
        print("inputs unchanged")
        return
    tx = Tx(t)
    try:
        order = [tb for tb in INPUT_TABLES if tb in dict(todo)]
        print("pushed rows:", replace_tables(tx, db, order))
        for tb, h in todo:
            tx.add("INSERT INTO sync_meta VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                   [f"sha:{tb}", h, now()])
        tx.add("INSERT INTO sync_meta VALUES ('inputs_pushed_at',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
               [now(), now()])
        tx.commit()
    except Exception:
        tx.rollback()
        raise


def cmd_push_cache(t, a):
    print("cache files pushed:", push_cache(t, a.root))


def cmd_pull(t, a):
    root = pathlib.Path(a.root)
    (root / "db").mkdir(parents=True, exist_ok=True)
    dbp = root / "db" / "cse_financials.db"
    tmp = dbp.with_suffix(".pulling")
    tmp.unlink(missing_ok=True)
    db = sqlite3.connect(tmp)
    names = INPUT_TABLES + EVENT_TABLES + [CHANGE_TABLE]
    sqls = dict(t.query("select name, sql from sqlite_master where type='table'"))
    idx = t.query("select tbl_name, sql from sqlite_master where type='index' and sql is not null")
    for n in names:
        if n not in sqls:
            raise Fail(f"Turso has no table {n} (run init)")
        db.execute(sqls[n])
        cols, rows = fetch_table(t, n)
        if rows:
            db.executemany(f'insert into "{n}" ({",".join(chr(34) + c + chr(34) for c in cols)}) values ({",".join("?" * len(cols))})', rows)
        for tb, s in idx:
            if tb == n:
                db.execute(s)
        print("pulled", n, len(rows))
    db.commit()
    if db.execute("pragma foreign_key_check").fetchall():
        raise Fail("pulled data fails the foreign-key check")
    db.close()
    manifest = {}
    _, rows = fetch_table(t, "event_cache", page=300)
    if len(rows) < 100:
        raise Fail(f"event_cache has only {len(rows)} files")
    for path, h, body, _ in rows:
        p = root / path
        p.parent.mkdir(parents=True, exist_ok=True)
        b = gzip.decompress(body)
        if sha(b) != h:
            raise Fail(f"cache file {path}: checksum mismatch")
        p.write_bytes(b)
        manifest[path] = h
    (root / ".sync_manifest.json").write_text(json.dumps(manifest))
    os.replace(tmp, dbp)                 # the database appears only after the cache is complete
    print("cache files", len(manifest))


def cmd_push_results(t, a):
    root = pathlib.Path(a.root)
    status = json.loads((root / "logs" / "events_daily" / "LAST_RUN.json").read_text())
    report = None
    if status.get("report") and pathlib.Path(status["report"]).exists():
        report = pathlib.Path(status["report"]).read_text()
    known = json.loads((root / ".sync_manifest.json").read_text())
    db = ro(root / "db" / "cse_financials.db")
    last = t.query(f"select max(run_at) from {CHANGE_TABLE}")[0][0] or ""      # read before the transaction
    tx = Tx(t)
    try:
        if status.get("ok"):
            print(replace_tables(tx, db, EVENT_TABLES))
            cols = columns(db, CHANGE_TABLE)
            new = db.execute(f"select * from {CHANGE_TABLE} where run_at > ? order by rowid", (last,)).fetchall()
            insert_rows(tx, CHANGE_TABLE, cols, new)
            print(CHANGE_TABLE, "appended", len(new))
        n = push_cache(t, root, tx, known)
        print("cache files pushed:", n)
        tx.add("INSERT OR REPLACE INTO event_run VALUES (?,?,?,?,?,?,?,?)",
               [status.get("started"), status.get("as_of"), 1 if status.get("ok") else 0, status.get("exit"),
                json.dumps(status.get("counts")), status.get("error"), report, json.dumps(status)])
        tx.commit()
    except Exception:
        tx.rollback()
        raise


def cmd_pull_events(t, a):
    import refresh_events as R
    main = pathlib.Path(a.main).resolve()
    R.MAIN, R.WORK = main, main.with_name("work_events_pull.db")
    fp = R.copy_main()
    db = sqlite3.connect(R.WORK)
    before = R.table_counts(db)
    remote_asof = t.query("select value from event_load_meta where key='as_of'")
    local_asof = db.execute("select value from event_load_meta where key='as_of'").fetchone() if \
        db.execute("select 1 from sqlite_master where name='event_load_meta'").fetchone() else None
    if not remote_asof:
        raise Fail("Turso has no event_load_meta.as_of")
    if local_asof and local_asof[0] > remote_asof[0][0] and not a.force:
        raise Fail(f"local as_of {local_asof[0]} is newer than Turso's {remote_asof[0][0]} (use --force)")
    sqls = dict(t.query("select name, sql from sqlite_master where type='table'"))
    for n in EVENT_TABLES + [CHANGE_TABLE]:
        cols, rows = fetch_table(t, n)
        if not db.execute("select 1 from sqlite_master where name=?", (n,)).fetchone():
            db.execute(sqls[n])
        db.execute(f'delete from "{n}"')
        if rows:
            db.executemany(f'insert into "{n}" ({",".join(chr(34) + c + chr(34) for c in cols)}) values ({",".join("?" * len(cols))})', rows)
        print("events", n, len(rows))
    db.commit()
    if db.execute("pragma quick_check").fetchone()[0] != "ok" or db.execute("pragma foreign_key_check").fetchall():
        raise Fail("copy failed quick_check / foreign_key_check")
    if R.table_counts(db) != before:
        raise Fail("pull changed non-event tables")
    db.close()
    try:
        swapped = R.swap(fp)
    finally:
        if R.WORK.exists():
            R.WORK.unlink()
    if not swapped:
        raise Fail("main changed during the pull; nothing swapped (rerun)")
    print("local main now at as_of", remote_asof[0][0])


def cmd_done_today(t, a):
    """Exit 0 when a successful run for today's Colombo date is already recorded (the retry schedule then does nothing)."""
    today = dt.datetime.now(COLO).date().isoformat()
    n = t.query("select count(*) from event_run where ok = 1 and as_of = ?", [today])[0][0]
    print("done today" if n else "not done today")
    sys.exit(0 if n else 3)


def cmd_needs_reading(t, a):
    rv = __import__("letters").load_reviewed()
    for path, body in t.query("select path, body from event_cache where path like 'raw_api/events/letters/%.json'"):
        r = json.loads(gzip.decompress(body))
        if r.get("status") == "NEEDS_READING" and int(r["announcementId"]) not in rv:
            print(json.dumps({k: r.get(k) for k in ("announcementId", "symbol", "company", "reason", "documents")}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["init", "push-inputs", "push-cache", "pull", "push-results", "pull-events", "needs-reading", "done-today"])
    ap.add_argument("--main", help="local main database (init, push-inputs, pull-events)")
    ap.add_argument("--root", help="data directory (db/, raw_api/, logs/)")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()
    try:
        t = Turso()
        {"init": cmd_init, "push-inputs": cmd_push_inputs, "push-cache": cmd_push_cache, "pull": cmd_pull,
         "push-results": cmd_push_results, "pull-events": cmd_pull_events, "needs-reading": cmd_needs_reading,
         "done-today": cmd_done_today}[a.cmd](t, a)
    except Fail as e:
        print("FAILED:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
