"""Sync the corporate-action capture between Turso (cse-main, the single main database) and a local SQLite file / data directory.

Turso tables used (the event-calendar tables and main's own tables are only READ, never written):
  read   company, security, announcement, corporate_action, corporate_action_announcement (main's own tables),
         sync_meta (when corporate_action last changed),
         event_cache (the events job's feed snapshots, corporate calendar and announcement details)
  write  ac_announcement, ac_action, ac_link, ac_value, ac_queue, ac_meta  (this job's rows; see build_actions.py)
         ac_run (one row per run), ac_cache (this job's raw responses)

Commands (env TURSO_EVENTS_URL / TURSO_EVENTS_TOKEN):
  init --main DB            create the ac_ tables; set the id floor from the local main db (ids the job allocates are
                            above every id main has, even when main is ahead of the Turso input copy)
  pull --root DIR           DIR/db/actions.db (inputs + ac_ tables) and DIR/raw_api/ (caches) for a run
  push-results --root DIR   after refresh_actions.py: this run's ac_ rows, changed cache files and the run record,
                            in one Turso transaction (the cache and run record also when the run failed)
  pull-actions --main DB [--dry-run]
                            new notices, actions, links and value changes into the local main db: copy -> check ->
                            swap (backup next to main); then marks the rows pulled in Turso
  needs-reading | needs-review   open queue items (local use: prints symbols; never run it in the public workflow)
  queue-counts              open items per queue (counts only; used by the workflow)
  done-today                exit 0 if today's run already succeeded (the retry schedule then does nothing)
"""
import argparse, datetime as dt, gzip, hashlib, json, os, pathlib, sqlite3, sys, time

CODE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(CODE))
sys.path.insert(1, str(CODE.parent.parent / "events" / "pipeline"))
import build_actions as B  # noqa: E402
import notices as N  # noqa: E402
from events_sync import Turso, Tx, Fail, fetch_table, insert_rows, columns, sha  # noqa: E402

COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))
INPUTS = ["company", "security", "announcement", "corporate_action", "corporate_action_announcement"]
AC = ["ac_announcement", "ac_action", "ac_link", "ac_value", "ac_queue", "ac_meta"]
PK = {"ac_announcement": ["announcement_pk"], "ac_action": ["action_id"], "ac_link": ["action_id", "announcement_pk"],
      "ac_value": ["value_id"], "ac_queue": ["item_key"], "ac_meta": ["key"]}
OWN_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS ac_run (run_at TEXT PRIMARY KEY, as_of TEXT, ok INTEGER NOT NULL, exit_code INTEGER,
       counts TEXT, error TEXT, report TEXT, status TEXT)""",
    "CREATE TABLE IF NOT EXISTS ac_cache (path TEXT PRIMARY KEY, sha256 TEXT NOT NULL, body BLOB NOT NULL, updated_at TEXT NOT NULL)",
]
EVENT_CACHE_USED = ("raw_api/events/approvedAnnouncement", "raw_api/events/corporateCalender__",
                    "raw_api/events/getAnnouncementById__", "raw_api/events/getGeneralAnnouncementById__")
KEEP_BACKUPS = 7


def now():
    return dt.datetime.now(COLO).isoformat(timespec="seconds")


def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def schema_statements():
    return B.SCHEMA + OWN_SCHEMA


# ---- init -----------------------------------------------------------------------------------------------------------
def cmd_init(t, a):
    t.run([(s, []) for s in schema_statements()])
    db = ro(a.main)
    ma = db.execute("select max(action_id) from corporate_action").fetchone()[0] or 0
    mn = db.execute("select max(announcement_pk) from announcement").fetchone()[0] or 0
    have = dict(t.query("select key, value from ac_meta"))
    ta = t.query("select max(action_id) from corporate_action")[0][0] or 0
    tn = t.query("select max(announcement_pk) from announcement")[0][0] or 0
    stmts = []
    for key, v in (("floor_action_id", ma), ("floor_announcement_pk", mn)):
        if int(have.get(key, 0)) < v:       # the floor only ever rises
            stmts.append(("INSERT INTO ac_meta VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                          [key, str(v), now()]))
    if stmts:
        t.run(stmts)
    print(f"schema ready; id floor: action {max(ma, int(have.get('floor_action_id', 0)))}, notice {max(mn, int(have.get('floor_announcement_pk', 0)))}")
    seed_main_rows(t, db)
    if (ma, mn) != (ta, tn):
        print(f"note: main is ahead of the Turso input copy (actions {ma} vs {ta}, notices {mn} vs {tn}); its extra rows were "
              "seeded into the ac_ tables (marked pulled). Sync the local working copy to cse-main if it is ahead.")


def seed_main_rows(t, db):
    """Rows main has but the Turso input copy lacks (main was refreshed locally after the last push-inputs): copied into
    the ac_ tables with main's own ids and marked pulled, so the job neither captures those notices again under new
    ids nor misses their links. Never changes an input table."""
    have_n = {r[0] for r in t.query("select announcement_pk from announcement")} | {r[0] for r in t.query("select announcement_pk from ac_announcement")}
    have_a = {r[0] for r in t.query("select action_id from corporate_action")} | {r[0] for r in t.query("select action_id from ac_action")}
    have_l = set(t.query("select action_id, announcement_pk from corporate_action_announcement")) | set(t.query("select action_id, announcement_pk from ac_link"))
    stamp = now()
    tx = Tx(t)
    n = {"notices": 0, "actions": 0, "links": 0}
    try:
        for r in db.execute(f"select {','.join(B.ANN_COLS)} from announcement order by 1"):
            if r[0] in have_n:
                continue
            tx.add("INSERT INTO ac_announcement VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", list(r) + ["[]", None, "[]", "main", stamp, stamp, stamp])
            n["notices"] += 1
        cols = B.ACTION_COLS
        for r in db.execute(f"select {','.join(cols)} from corporate_action order by 1"):
            if r[0] in have_a:
                continue
            first = db.execute("select min(announcement_pk) from corporate_action_announcement where action_id=?", (r[0],)).fetchone()[0]
            tx.add(f"INSERT INTO ac_action ({','.join(cols)}, status, created_by_pk, created_at, updated_at, pulled_at) VALUES "
                   f"({','.join('?' * (len(cols) + 5))})", list(r) + ["ACTIVE", first or 0, stamp, stamp, stamp])
            n["actions"] += 1
        for aid, pk in db.execute("select action_id, announcement_pk from corporate_action_announcement"):
            if (aid, pk) in have_l:
                continue
            tx.add("INSERT INTO ac_link VALUES (?,?,?,?,?,?)", [aid, pk, "main", stamp, stamp, stamp])
            n["links"] += 1
        tx.commit()
    except Exception:
        tx.rollback()
        raise
    print("seeded from main (rows the Turso input copy lacks):", json.dumps(n))


# ---- pull (for a run) ---------------------------------------------------------------------------------------------
def cmd_pull(t, a):
    root = pathlib.Path(a.root)
    (root / "db").mkdir(parents=True, exist_ok=True)
    dbp = root / "db" / "actions.db"
    tmp = dbp.with_suffix(".pulling")
    tmp.unlink(missing_ok=True)
    db = sqlite3.connect(tmp)
    sqls = dict(t.query("select name, sql from sqlite_master where type='table'"))
    idx = t.query("select tbl_name, sql from sqlite_master where type='index' and sql is not null")
    for n in INPUTS + AC:
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
    pushed = dict(t.query("select key, updated_at from sync_meta where key='sha:corporate_action'"))
    db.execute("INSERT OR REPLACE INTO ac_meta VALUES ('local:inputs_corporate_action_pushed_at', ?, ?)",
               (pushed.get("sha:corporate_action", ""), now()))
    db.commit()
    db.close()
    manifest = {}
    _, rows = fetch_table(t, "ac_cache", page=300)
    for path, h, body, _ in rows:
        write_cache(root, path, h, body)
        manifest[path] = h
    n_ev = 0
    for prefix in EVENT_CACHE_USED:
        for path, h, body in t.query("select path, sha256, body from event_cache where path like ? || '%' and path not like 'raw_api/events/history/%'", [prefix]):
            write_cache(root, path, h, body)
            n_ev += 1
    if n_ev < 50:
        raise Fail(f"event_cache gave only {n_ev} feed/calendar/detail files (is the events job running?)")
    (root / ".sync_manifest.json").write_text(json.dumps(manifest))
    os.replace(tmp, dbp)
    print("cache files", len(manifest), "events cache files (read only)", n_ev)


def write_cache(root, path, h, body):
    p = pathlib.Path(root) / path
    p.parent.mkdir(parents=True, exist_ok=True)
    b = gzip.decompress(body)
    if sha(b) != h:
        raise Fail(f"cache file checksum mismatch ({path.split('/')[-1][:40]})")
    p.write_bytes(b)


# ---- push the results of a run --------------------------------------------------------------------------------------
def cache_files(root):
    base = pathlib.Path(root) / "raw_api" / "actions"
    return {p.relative_to(root).as_posix(): p for p in sorted(base.glob("*.json"))} if base.exists() else {}


def upsert_sql(table, cols):
    keys = PK[table]
    upd = ",".join(f'"{c}"=excluded."{c}"' for c in cols if c not in keys)
    return (f'INSERT INTO "{table}" ({",".join(chr(34) + c + chr(34) for c in cols)}) VALUES ({",".join("?" * len(cols))}) '
            f'ON CONFLICT({",".join(keys)}) DO UPDATE SET {upd}')


CREATED_COL = {"ac_announcement": "captured_at", "ac_action": "created_at", "ac_link": "recorded_at", "ac_value": "recorded_at",
               "ac_queue": "created_at"}


def push_statements(db, run_at):
    """(sql, args) for this run's rows. A row the run CREATED (new notice pk, action id, value id, link, queue item) is
    a plain INSERT: if another run (a local run overlapping the cloud job, or a run from a stale copy) already wrote
    that key, the transaction fails and nothing is written, instead of one action/notice overwriting another under
    the same id. Rows the run only updated are upserts."""
    out = []
    for tb, (cols, rows) in run_rows(db, run_at).items():
        cc = cols.index(CREATED_COL[tb]) if tb in CREATED_COL else None
        ins = f'INSERT INTO "{tb}" ({",".join(chr(34) + c + chr(34) for c in cols)}) VALUES ({",".join("?" * len(cols))})'
        for r in rows:
            out.append((ins if cc is not None and r[cc] == run_at else upsert_sql(tb, cols), list(r)))
    return out


def run_rows(db, run_at):
    """Rows of the ac_ tables this run wrote (updated_at = run_at), per table."""
    out = {}
    for tb in AC:
        cols = columns(db, tb)
        if "updated_at" not in cols:
            continue
        rows = db.execute(f'select * from "{tb}" where updated_at = ? and ({"key not like ?" if tb == "ac_meta" else "1 or ?"}) order by rowid',
                          (run_at, "local:%")).fetchall()
        out[tb] = (cols, rows)
    return out


def cmd_push_results(t, a):
    root = pathlib.Path(a.root)
    status = json.loads((root / "logs" / "LAST_RUN.json").read_text())
    report = None
    if status.get("report") and pathlib.Path(status["report"]).exists():
        report = pathlib.Path(status["report"]).read_text()
    known = json.loads((root / ".sync_manifest.json").read_text())
    db = ro(root / "db" / "actions.db")
    tx = Tx(t)
    try:
        if status.get("ok"):
            stmts = push_statements(db, status["started"])
            for sql, args in stmts:
                tx.add(sql, args)
            print("rows", len(stmts))
        n = 0
        for rel, p in cache_files(root).items():
            b = p.read_bytes()
            h = sha(b)
            if known.get(rel) != h:
                tx.add("INSERT INTO ac_cache VALUES (?,?,?,?) ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256, body=excluded.body, updated_at=excluded.updated_at",
                       [rel, h, gzip.compress(b), now()])
                n += 1
        print("cache files pushed:", n)
        tx.add("INSERT OR REPLACE INTO ac_run VALUES (?,?,?,?,?,?,?,?)",
               [status.get("started"), status.get("as_of"), 1 if status.get("ok") else 0, status.get("exit"),
                json.dumps(status.get("counts")), status.get("error"), report, json.dumps(status)])
        tx.commit()
    except Exception:
        tx.rollback()
        raise


# ---- local: pull the captured actions into main ---------------------------------------------------------------------
def fingerprint(path):
    st = os.stat(path)
    side = [s for s in ("-journal", "-wal") if os.path.exists(str(path) + s) and os.path.getsize(str(path) + s) > 0]
    return (st.st_ino, st.st_size, st.st_mtime_ns, tuple(side))


def copy_db(main, work):
    fp = fingerprint(main)
    if fp[3]:
        raise Fail("main has an open journal (another writer?); try again later")
    work.unlink(missing_ok=True)
    src, dst = ro(main), sqlite3.connect(work)
    src.backup(dst)
    dst.close()
    src.close()
    if fingerprint(main) != fp:
        raise Fail("main changed while it was being copied")
    return fp


def swap(main, work, fp):
    if fingerprint(main) != fp:
        return None
    backup = main.parent / f"backup_actions_{dt.datetime.now(COLO):%Y%m%d_%H%M%S_%f}.db"
    os.link(main, backup)
    os.replace(work, main)
    dfd = os.open(main.parent, os.O_RDONLY)
    os.fsync(dfd)
    os.close(dfd)
    for f in sorted(main.parent.glob("backup_actions_*.db"))[:-KEEP_BACKUPS]:
        f.unlink()
    return backup


def counts(db):
    names = [r[0] for r in db.execute("select name from sqlite_master where type='table' and name not like 'sqlite_%'")]
    return {n: db.execute(f'select count(*) from "{n}"').fetchone()[0] for n in names}


HISTORY_DDL = [
    """CREATE TABLE IF NOT EXISTS action_value_history (value_id INTEGER PRIMARY KEY, action_id INTEGER NOT NULL, field TEXT NOT NULL,
       value TEXT, status TEXT NOT NULL, source_pk INTEGER, source_kind TEXT NOT NULL, recorded_at TEXT NOT NULL, superseded_by INTEGER)""",
    """CREATE TABLE IF NOT EXISTS action_capture_queue (item_key TEXT PRIMARY KEY, queue TEXT NOT NULL, announcement_pk INTEGER,
       action_id INTEGER, reason TEXT NOT NULL, documents TEXT, status TEXT NOT NULL, created_at TEXT NOT NULL, resolved_at TEXT, resolution TEXT)""",
    """CREATE TABLE IF NOT EXISTS action_capture_link (action_id INTEGER NOT NULL, announcement_pk INTEGER NOT NULL, evidence TEXT NOT NULL,
       recorded_at TEXT NOT NULL, PRIMARY KEY (action_id, announcement_pk))""",
]
OWN_MAIN_TABLES = {"action_value_history", "action_capture_queue", "action_capture_link"}


def held_actions(rows):
    """New actions waiting for review (an OPEN needs-review item names the action and the notice that created it):
    not pulled into main until the item is resolved, so a possible duplicate never reaches the price adjustments."""
    created = {r["action_id"]: r["created_by_pk"] for r in rows["ac_action"]}
    return {q["action_id"] for q in rows["ac_queue"] if q["queue"] == "needs-review" and q["status"] == "OPEN"
            and q["action_id"] in created and created[q["action_id"]] == q["announcement_pk"]}


def apply_to_main(db, rows, include_unreviewed=False):
    """Apply the captured rows (dict table -> list of dicts) to a copy of main. Returns a report dict. Raises Fail on an
    id clash. Idempotent: rows already in main are skipped."""
    out = {"notices_added": 0, "actions_added": 0, "links_added": 0, "values_applied": 0, "value_conflicts": [], "skipped_same_as": 0,
           "held_for_review": 0}
    held = set() if include_unreviewed else held_actions(rows)
    for s in HISTORY_DDL:
        db.execute(s)
    ann_cols = [c for c in B.ANN_COLS]
    for r in rows["ac_announcement"]:
        have = db.execute("select cse_format, cse_announcement_id from announcement where announcement_pk=?", (r["announcement_pk"],)).fetchone()
        if have:
            if tuple(have) != (r["cse_format"], r["cse_announcement_id"]):
                raise Fail(f"announcement_pk {r['announcement_pk']} is another notice in main (local ids created since go-live?)")
            continue
        if db.execute("select 1 from announcement where cse_format=? and cse_announcement_id=?", (r["cse_format"], r["cse_announcement_id"])).fetchone():
            raise Fail(f"notice {r['cse_format']} {r['cse_announcement_id']} is in main under another pk (loaded locally?)")
        db.execute(f"insert into announcement ({','.join(ann_cols)}) values ({','.join('?' * len(ann_cols))})", [r[c] for c in ann_cols])
        out["notices_added"] += 1
    values = sorted(rows["ac_value"], key=lambda v: v["value_id"])
    by_id = {v["value_id"]: v for v in values}
    prev = {v["superseded_by"]: v for v in values if v["superseded_by"]}
    act_cols = list(B.ACTION_COLS)
    for r in rows["ac_action"]:
        have = db.execute("select security_id, action_type from corporate_action where action_id=?", (r["action_id"],)).fetchone()
        if str(r["status"]).startswith("SAME_AS"):
            out["skipped_same_as"] += 1
            continue
        if have:
            if tuple(have) != (r["security_id"], r["action_type"]):
                raise Fail(f"action_id {r['action_id']} is another action in main (local ids created since go-live?)")
            continue
        if r["action_id"] in held:
            out["held_for_review"] += 1
            continue
        db.execute(f"insert into corporate_action ({','.join(act_cols)}) values ({','.join('?' * len(act_cols))})", [r[c] for c in act_cols])
        out["actions_added"] += 1
    for r in rows["ac_link"]:
        if not db.execute("select 1 from corporate_action where action_id=?", (r["action_id"],)).fetchone():
            continue            # a SAME_AS action that never reached main, or one held for review
        c = db.execute("insert or ignore into corporate_action_announcement values (?,?)", (r["action_id"], r["announcement_pk"]))
        out["links_added"] += c.rowcount
    # value changes on actions that were already in main (the job's own new actions were inserted complete above)
    for v in values:
        if v["status"] != "CURRENT" or v["field"] not in N.FIELDS + ("detail_level",):
            continue
        cur = db.execute(f'select "{v["field"]}" from corporate_action where action_id=?', (v["action_id"],)).fetchone()
        if cur is None:
            continue
        new = B.from_text(v["field"], v["value"])
        if B.same(v["field"], cur[0], new):
            continue
        old = prev.get(v["value_id"])
        old_val = B.from_text(v["field"], old["value"]) if old else None
        if cur[0] is None or (old is not None and B.same(v["field"], cur[0], old_val)):
            db.execute(f'update corporate_action set "{v["field"]}"=? where action_id=?', (new, v["action_id"]))
            out["values_applied"] += 1
        else:
            out["value_conflicts"].append((v["action_id"], v["field"], cur[0], v["value"]))
    db.execute("delete from action_value_history")
    for v in values:
        db.execute("insert into action_value_history values (?,?,?,?,?,?,?,?,?)",
                   (v["value_id"], v["action_id"], v["field"], v["value"], v["status"], v["source_pk"], v["source_kind"], v["recorded_at"], v["superseded_by"]))
    db.execute("delete from action_capture_queue")
    for q in rows["ac_queue"]:
        db.execute("insert into action_capture_queue values (?,?,?,?,?,?,?,?,?,?)",
                   tuple(q[c] for c in ("item_key", "queue", "announcement_pk", "action_id", "reason", "documents", "status", "created_at", "resolved_at", "resolution")))
    db.execute("delete from action_capture_link")
    for r in rows["ac_link"]:
        db.execute("insert into action_capture_link values (?,?,?,?)", (r["action_id"], r["announcement_pk"], r["evidence"], r["recorded_at"]))
    return out


def read_rows(t):
    rows = {}
    for tb in AC[:-1]:
        cols, rr = fetch_table(t, tb)
        rows[tb] = [dict(zip(cols, r)) for r in rr]
    return rows


def pull_into(main, rows, dry_run=False, include_unreviewed=False):
    """copy -> apply -> check -> swap. Returns (report, backup path or None)."""
    main = pathlib.Path(main).resolve()
    work = main.with_name("work_actions_pull.db")
    fp = copy_db(main, work)
    try:
        db = sqlite3.connect(work)
        db.execute("pragma foreign_keys=on")
        before = counts(db)
        rep = apply_to_main(db, rows, include_unreviewed)
        db.commit()
        if db.execute("pragma quick_check").fetchone()[0] != "ok" or db.execute("pragma foreign_key_check").fetchall():
            raise Fail("copy failed quick_check / foreign_key_check")
        after = counts(db)
        expect = {"announcement": rep["notices_added"], "corporate_action": rep["actions_added"], "corporate_action_announcement": rep["links_added"]}
        for tb, n in after.items():
            if tb in OWN_MAIN_TABLES:
                continue
            if n - before.get(tb, 0) != expect.get(tb, 0):
                raise Fail(f"table {tb} changed unexpectedly ({before.get(tb)} -> {n})")
        held = set() if include_unreviewed else held_actions(rows)
        missing = [r["action_id"] for r in rows["ac_action"] if not str(r["status"]).startswith("SAME_AS") and r["action_id"] not in held
                   and not db.execute("select 1 from corporate_action where action_id=?", (r["action_id"],)).fetchone()]
        if missing:
            raise Fail(f"{len(missing)} captured actions missing after the pull")
        db.close()
        if dry_run:
            return rep, None
        backup = swap(main, work, fp)
        if backup is None:
            raise Fail("main changed during the pull; nothing swapped (rerun)")
        return rep, backup
    finally:
        work.unlink(missing_ok=True)


def mark_pulled_statements(stamp, held, at):
    """UPDATE statements that mark the pulled rows in Turso. Actions held back for review (and their links and values)
    never reached main, so they stay unmarked: `apply_decisions` refuses `same_as` for a pulled action."""
    held = sorted(held)
    out = []
    for tb in ("ac_announcement", "ac_action", "ac_link", "ac_value"):
        sql, args = f"UPDATE {tb} SET pulled_at=? WHERE pulled_at IS NULL AND updated_at <= ?", [at, stamp]
        if held and tb != "ac_announcement":
            sql += f" AND action_id NOT IN ({','.join('?' * len(held))})"
            args += held
        out.append((sql, args))
    return out


def cmd_pull_actions(t, a):
    stamp = t.query("select max(updated_at) from (select updated_at from ac_announcement union all select updated_at from ac_action "
                    "union all select updated_at from ac_link union all select updated_at from ac_value)")[0][0] or ""
    rows = read_rows(t)
    held = set() if a.include_unreviewed else held_actions(rows)
    rep, backup = pull_into(a.main, rows, a.dry_run, a.include_unreviewed)
    conflicts = rep.pop("value_conflicts")
    print("pull:", json.dumps(rep))
    for c in conflicts:
        print("CONFLICT (main kept):", c)
    if a.dry_run:
        print("dry run: main untouched")
        return
    print("swapped; previous main kept as", backup.name)
    tx = Tx(t)
    try:
        for sql, args in mark_pulled_statements(stamp, held, now()):
            tx.add(sql, args)
        tx.commit()
    except Exception:
        tx.rollback()
        print("WARNING: rows not marked pulled in Turso (harmless: the next pull skips what main has)")
        raise
    print("next: the local apply (see actions/README.md), then sync the working copy to cse-main")


# ---- queues / status ------------------------------------------------------------------------------------------------
def cmd_queue(t, a):
    q = {"needs-reading": "needs-reading", "needs-review": "needs-review"}[a.cmd]
    sym = dict(t.query("select security_id, symbol from security"))
    acts = dict(t.query("select action_id, security_id from corporate_action"))
    acts.update(dict(t.query("select action_id, security_id from ac_action")))
    notes = {r[0]: r[1:] for r in t.query("select announcement_pk, cse_announcement_id, category_raw, announced_on from ac_announcement")}
    for key, pk, aid, reason, docs in t.query("select item_key, announcement_pk, action_id, reason, documents from ac_queue where queue=? and status='OPEN' order by created_at", [q]):
        n = notes.get(pk, (None, None, None))
        print(json.dumps({"item": key, "announcementId": n[0], "category": n[1], "announced_on": n[2], "action_id": aid,
                          "symbol": sym.get(acts.get(aid)), "reason": reason, "documents": json.loads(docs or "[]")}))


def cmd_queue_counts(t, a):
    c = dict(t.query("select queue, count(*) from ac_queue where status='OPEN' group by 1"))
    print(json.dumps({"needs-reading": c.get("needs-reading", 0), "needs-review": c.get("needs-review", 0)}))


def cmd_done_today(t, a):
    """Exit 0 (the retry schedule does nothing) when today's run succeeded, or when the CSE refused a request today
    (exit 2: no retry on the same day; the next daily run is soon enough)."""
    today = dt.datetime.now(COLO).date().isoformat()
    ok, refused = t.query("select coalesce(sum(ok = 1), 0), coalesce(sum(exit_code = 2), 0) from ac_run where as_of = ?", [today])[0]
    print("done today" if ok else "the CSE refused a request today: no retry" if refused else "not done today")
    sys.exit(0 if ok or refused else 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["init", "pull", "push-results", "pull-actions", "needs-reading", "needs-review", "queue-counts", "done-today"])
    ap.add_argument("--main", help="local main database (init, pull-actions)")
    ap.add_argument("--root", help="data directory (db/, raw_api/, logs/)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--include-unreviewed", action="store_true", help="pull-actions: also pull new actions still waiting for review")
    a = ap.parse_args()
    try:
        t = Turso()
        {"init": cmd_init, "pull": cmd_pull, "push-results": cmd_push_results, "pull-actions": cmd_pull_actions,
         "needs-reading": cmd_queue, "needs-review": cmd_queue, "queue-counts": cmd_queue_counts, "done-today": cmd_done_today}[a.cmd](t, a)
    except Fail as e:
        print("FAILED:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
