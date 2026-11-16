"""Incremental corporate-action builder. Works on the job's SQLite copy: the input tables pushed from the local main
database (company, security, announcement, corporate_action, corporate_action_announcement) plus the job's own `ac_`
tables. It never rewrites an input row and never renumbers anything:

  * existing action ids are immutable; a new action gets max(known ids, floor) + 1;
  * a follow-up notice is ATTACHED to its action when the evidence says so (in this order):
      text        the notice names the date of an announcement already linked to the action ("further to the
                  announcement dated 23rd July 2026", "addendum ...");
      structured  CSE's detail links it (cash dividends: dateOfInitialAnnouncement);
      same_event  same security, kind, ex-date and terms (a restatement);
      only_open   a dates / amended / listing / subscription / further notice, and exactly one open (undated) action of
                  the same security and kind announced in the 500 days before, with terms that do not disagree;
  * a changed value is recorded with the notice as its source; the old value is kept as SUPERSEDED (ac_value);
  * a postponement removes the dates (kept as SUPERSEDED) until a new dates notice arrives;
  * nothing links it -> a new action; a follow-up that links to nothing, or an initial notice while an open action
    of the same kind exists, goes to the needs-review queue instead of being guessed;
  * a dates notice whose dates are only in its PDF letter goes to the needs-reading queue; reviewed readings
    (actions/reviewed/notice_readings.json) are applied with source_kind 'reviewed'.
"""
import datetime as dt, json, pathlib, re, sqlite3

import notices as N

ACTION_COLS = ("action_id", "company_id", "security_id", "action_type", "announced_on") + N.FIELDS + ("adjustment_factor", "detail_level")
ANN_COLS = ("announcement_pk", "cse_format", "cse_announcement_id", "company_id", "category_raw", "normalized_category",
            "announced_on", "detail_status", "cse_detail_type", "remarks", "file_url")
OWN_TABLES = ["ac_announcement", "ac_action", "ac_link", "ac_value", "ac_queue", "ac_meta"]
SCHEMA = [
    f"""CREATE TABLE IF NOT EXISTS ac_announcement (announcement_pk INTEGER PRIMARY KEY, cse_format TEXT NOT NULL,
        cse_announcement_id INTEGER NOT NULL, company_id INTEGER NOT NULL, category_raw TEXT NOT NULL, normalized_category TEXT NOT NULL,
        announced_on TEXT, detail_status TEXT NOT NULL, cse_detail_type TEXT, remarks TEXT, file_url TEXT,
        kinds TEXT, follow_up TEXT, documents TEXT, found_in TEXT, captured_at TEXT NOT NULL, updated_at TEXT NOT NULL, pulled_at TEXT,
        UNIQUE (cse_format, cse_announcement_id))""",
    f"""CREATE TABLE IF NOT EXISTS ac_action (action_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL, security_id INTEGER NOT NULL,
        action_type TEXT NOT NULL, announced_on TEXT, {", ".join(f + " " + ("NUMERIC" if f in ("per_share", "issue_price") else "INTEGER" if f.startswith("shares") else "TEXT") for f in N.FIELDS)},
        adjustment_factor NUMERIC, detail_level TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ACTIVE', created_by_pk INTEGER NOT NULL,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, pulled_at TEXT)""",
    """CREATE TABLE IF NOT EXISTS ac_link (action_id INTEGER NOT NULL, announcement_pk INTEGER NOT NULL, evidence TEXT NOT NULL,
        recorded_at TEXT NOT NULL, updated_at TEXT NOT NULL, pulled_at TEXT, PRIMARY KEY (action_id, announcement_pk))""",
    """CREATE TABLE IF NOT EXISTS ac_value (value_id INTEGER PRIMARY KEY, action_id INTEGER NOT NULL, field TEXT NOT NULL, value TEXT,
        status TEXT NOT NULL CHECK (status IN ('CURRENT','SUPERSEDED')), source_pk INTEGER, source_kind TEXT NOT NULL,
        recorded_at TEXT NOT NULL, superseded_by INTEGER, updated_at TEXT NOT NULL, pulled_at TEXT)""",
    "CREATE INDEX IF NOT EXISTS ix_ac_value_action ON ac_value(action_id, field, status)",
    """CREATE TABLE IF NOT EXISTS ac_queue (item_key TEXT PRIMARY KEY, queue TEXT NOT NULL CHECK (queue IN ('needs-review','needs-reading')),
        announcement_pk INTEGER, action_id INTEGER, reason TEXT NOT NULL, documents TEXT, status TEXT NOT NULL DEFAULT 'OPEN',
        created_at TEXT NOT NULL, resolved_at TEXT, resolution TEXT, updated_at TEXT NOT NULL)""",
    "CREATE TABLE IF NOT EXISTS ac_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)",
]
OPEN_WINDOW_DAYS = 500
DUP_WINDOW_DAYS = 365
RECENT_WINDOW_DAYS = 180
TYPED = {"per_share": float, "issue_price": float, "shares_before": int, "shares_after": int, "shares_issued": int}


def ensure_schema(db):
    for s in SCHEMA:
        db.execute(s)


def to_text(v):
    if v is None:
        return None
    return repr(v) if isinstance(v, float) else str(v)


def from_text(field, v):
    if v is None:
        return None
    f = TYPED.get(field)
    if f is int:
        return int(float(v))
    if f is float:
        return float(v)
    return v


def same(field, a, b):
    if a is None or b is None:
        return a is None and b is None
    if field in TYPED:
        return abs(float(a) - float(b)) < 1e-9
    return str(a) == str(b)


def changes_set_dates(a, c):
    """True when notice values `a` would replace a date that action `c` already holds (terms are checked by
    notices.terms_disagree)."""
    return any(a.get(f) is not None and c.get(f) is not None and not same(f, c[f], a[f]) for f in N.DATE_FIELDS)


class Builder:
    def __init__(self, db, run_at, today=None, kind_map=None, reviewed=None, decisions=None):
        self.db, self.run = db, run_at
        self.today = today or dt.date.fromisoformat(run_at[:10])
        self.kind_map = kind_map if kind_map is not None else N.load_kind_map()
        self.reviewed = reviewed or {}
        self.decisions = decisions or {}
        self.counts = {}
        ensure_schema(db)
        self.load()

    # ---- state ------------------------------------------------------------------------------------------------
    def bump(self, k, n=1):
        self.counts[k] = self.counts.get(k, 0) + n

    def meta(self, key, default=None):
        r = self.db.execute("select value from ac_meta where key=?", (key,)).fetchone()
        return r[0] if r else default

    def load(self):
        db = self.db
        db.row_factory = None
        self.ca_pushed_at = self.meta("local:inputs_corporate_action_pushed_at", "")
        self.tickers = {cid: t for cid, t in db.execute("select company_id, ticker from company")}
        self.secs = {}
        for sid, cid, cls, status in db.execute("select security_id, company_id, share_class, listing_status from security order by listing_status desc, security_id"):
            self.secs.setdefault(cid, {}).setdefault(cls, sid)
        self.sec_class = {sid: cls for sid, cid, cls in db.execute("select security_id, company_id, share_class from security")}
        self.notes = {}
        for r in db.execute(f"select {','.join(ANN_COLS)} from announcement"):
            self.notes[r[0]] = dict(zip(ANN_COLS, r))
        self.input_ann_max = max(self.notes, default=0)
        for r in db.execute(f"select {','.join(ANN_COLS)} from ac_announcement"):
            self.notes.setdefault(r[0], dict(zip(ANN_COLS, r)))
        self.by_cse = {(n["cse_format"], n["cse_announcement_id"]): pk for pk, n in self.notes.items()}
        self.actions = {}
        for r in db.execute(f"select {','.join(ACTION_COLS)} from corporate_action"):
            a = dict(zip(ACTION_COLS, r))
            a["status"], a["origin"] = "ACTIVE", "main"
            self.actions[a["action_id"]] = a
        self.input_action_max = max(self.actions, default=0)
        for r in db.execute(f"select {','.join(ACTION_COLS)}, status from ac_action"):
            a = dict(zip(ACTION_COLS + ("status",), r))
            if a["action_id"] not in self.actions:
                a["origin"] = "capture"
                self.actions[a["action_id"]] = a
        # value overlay: a value recorded by the job counts unless main already had it when its corporate_action table
        # was last pushed (pulled before that push: main, including any hand review since, is the truth)
        for aid, field, value, pulled in db.execute(
                "select action_id, field, value, pulled_at from ac_value where status='CURRENT' order by value_id"):
            if aid in self.actions and (pulled is None or not self.ca_pushed_at or pulled > self.ca_pushed_at):
                if field == "status":
                    self.actions[aid]["status"] = value
                elif field in N.FIELDS or field == "detail_level":
                    self.actions[aid][field] = from_text(field, value)
        self.links = {}
        for aid, pk in db.execute("select action_id, announcement_pk from corporate_action_announcement"):
            self.links.setdefault(aid, set()).add(pk)
        for aid, pk in db.execute("select action_id, announcement_pk from ac_link"):
            self.links.setdefault(aid, set()).add(pk)
        floor_a = int(self.meta("floor_action_id", "0"))
        floor_n = int(self.meta("floor_announcement_pk", "0"))
        self.next_action = max([floor_a] + list(self.actions)) + 1
        self.next_pk = max([floor_n] + list(self.notes)) + 1
        self.next_value = (db.execute("select max(value_id) from ac_value").fetchone()[0] or 0) + 1

    def security_for(self, cid, cls):
        by = self.secs.get(cid, {})
        return by.get(cls) or (next(iter(by.values())) if cls == "VOTING" and by else None)

    # ---- writes ------------------------------------------------------------------------------------------------
    def store_notice(self, rec):
        """rec: cse_format, cse_announcement_id, company_id, category_raw, announced_on, detail_status, cse_detail_type,
        remarks, file_url, kinds, follow_up, documents, found_in. Returns the new pk (existing pk if stored already)."""
        key = (rec["cse_format"], rec["cse_announcement_id"])
        if key in self.by_cse:
            return self.by_cse[key], False
        pk = self.next_pk
        self.next_pk += 1
        n = dict(announcement_pk=pk, normalized_category=N.clean(rec["category_raw"]) or "none", **{k: rec.get(k) for k in ANN_COLS if k not in ("announcement_pk", "normalized_category")})
        self.db.execute("insert into ac_announcement values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        [n[k] for k in ANN_COLS] + [json.dumps(sorted(rec.get("kinds") or [])), rec.get("follow_up"),
                                                    json.dumps(rec.get("documents") or []), rec.get("found_in"), self.run, self.run, None])
        self.notes[pk] = n
        self.by_cse[key] = pk
        self.bump("notices_stored")
        return pk, True

    def record_value(self, aid, field, value, source_pk, source_kind):
        """Make `value` the current value of field; the previous value (job's or main's) is kept as SUPERSEDED."""
        a = self.actions[aid]
        old = a.get(field) if field != "status" else a.get("status")
        if same(field, old, value):
            return False
        cur = self.db.execute("select value_id from ac_value where action_id=? and field=? and status='CURRENT'", (aid, field)).fetchone()
        new_id = self.next_value
        self.next_value += 1
        if cur:
            self.db.execute("update ac_value set status='SUPERSEDED', superseded_by=?, updated_at=?, pulled_at=NULL where value_id=?", (new_id, self.run, cur[0]))
        elif old is not None and a["origin"] == "main":
            # main's value, recorded once so the history shows what it replaced
            self.db.execute("insert into ac_value values (?,?,?,?,?,?,?,?,?,?,?)",
                            (new_id, aid, field, to_text(old), "SUPERSEDED", None, "main", self.run, new_id + 1, self.run, None))
            new_id = self.next_value
            self.next_value += 1
        self.db.execute("insert into ac_value values (?,?,?,?,?,?,?,?,?,?,?)",
                        (new_id, aid, field, to_text(value), "CURRENT", source_pk, source_kind, self.run, None, self.run, None))
        if field == "status":
            a["status"] = value
        else:
            a[field] = value
        if a["origin"] == "capture":
            self.sync_action_row(aid)
        return True

    def sync_action_row(self, aid):
        a = self.actions[aid]
        sets = ",".join(f"{c}=?" for c in ACTION_COLS[1:]) + ", status=?, updated_at=?, pulled_at=NULL"
        self.db.execute(f"update ac_action set {sets} where action_id=?", [a.get(c) for c in ACTION_COLS[1:]] + [a.get("status") or "ACTIVE", self.run, aid])

    def link(self, aid, pk, evidence):
        if pk in self.links.get(aid, set()):
            return False
        self.db.execute("insert or ignore into ac_link values (?,?,?,?,?,?)", (aid, pk, evidence, self.run, self.run, None))
        self.links.setdefault(aid, set()).add(pk)
        return True

    def queue(self, q, key, pk, aid, reason, docs=None):
        if self.db.execute("select 1 from ac_queue where item_key=?", (key,)).fetchone():
            return
        self.db.execute("insert into ac_queue values (?,?,?,?,?,?,?,?,?,?,?)",
                        (key, q, pk, aid, reason, json.dumps(docs or []), "OPEN", self.run, None, None, self.run))
        self.bump(q.replace("-", "_") + "_queued")

    def resolve(self, key, resolution):
        self.db.execute("update ac_queue set status='RESOLVED', resolved_at=?, resolution=?, updated_at=? where item_key=? and status='OPEN'",
                        (self.run, resolution, self.run, key))

    def new_action(self, cid, sid, kind, n, a, pk, source_kind):
        aid = self.next_action
        self.next_action += 1
        row = dict(action_id=aid, company_id=cid, security_id=sid, action_type=kind, announced_on=n["announced_on"],
                   adjustment_factor=None, detail_level=a.get("level", "TITLE_ONLY"), status="ACTIVE", origin="capture")
        for f in N.FIELDS:
            row[f] = None
        self.actions[aid] = row
        self.db.execute(f"insert into ac_action ({','.join(ACTION_COLS)}, status, created_by_pk, created_at, updated_at) values "
                        f"({','.join('?' * (len(ACTION_COLS) + 4))})", [row.get(c) for c in ACTION_COLS] + ["ACTIVE", pk, self.run, self.run])
        for f in N.FIELDS:
            if a.get(f) is not None:
                self.record_value(aid, f, a[f], pk, source_kind)
        self.link(aid, pk, "created")
        self.bump("actions_created")
        return aid

    # ---- linking ------------------------------------------------------------------------------------------------
    def note_dates(self, aid):
        return {self.notes[p]["announced_on"] for p in self.links.get(aid, ()) if p in self.notes and self.notes[p]["announced_on"]}

    def candidates(self, sid, kind, on, pk):
        out = []
        for a in self.actions.values():
            if a["security_id"] != sid or a["action_type"] != kind or a.get("status", "ACTIVE").startswith("SAME_AS"):
                continue
            if pk in self.links.get(a["action_id"], ()):
                continue
            first = a["announced_on"] or min(self.note_dates(a["action_id"]) or {None}, key=lambda x: x or "")
            if on and first and first > on:
                continue
            out.append(a)
        return out

    def find(self, kind, sid, n, pk, a, base, follow_up, text):
        """(action_id, evidence) or (None, why) for one value row of a notice."""
        on = n["announced_on"]
        cands = self.candidates(sid, kind, on, pk)
        recent = [c for c in cands if not on or not c["announced_on"] or (dt.date.fromisoformat(on) - dt.date.fromisoformat(c["announced_on"])).days <= OPEN_WINDOW_DAYS]
        refs = N.referenced_dates(text) - {on}
        if refs:
            hit = [c for c in cands if self.note_dates(c["action_id"]) & refs]
            if len(hit) == 1:
                return hit[0]["action_id"], "text"
            if len(hit) > 1:
                open_hit = [c for c in hit if not N.is_dated(c)]
                if len(open_hit) == 1:
                    return open_hit[0]["action_id"], "text"
                return None, "text names a date shared by several actions"
        init = N.day((base or {}).get("dateOfInitialAnnouncement"))
        if init and init != on:
            hit = [c for c in cands if init in self.note_dates(c["action_id"])]
            if len(hit) == 1:
                return hit[0]["action_id"], "structured"
        if a.get("ex_date"):
            hit = [c for c in cands if c.get("ex_date") == a["ex_date"] and not N.terms_disagree(kind, a, c)]
            if len(hit) == 1:
                return hit[0]["action_id"], "same_event"
        if follow_up:
            open_ = [c for c in recent if not N.is_dated(c) and not N.terms_disagree(kind, a, c)]
            if len(open_) == 1:
                o = open_[0]
                if follow_up in ("listing", "subscription", "further") and any(
                        c is not o and c["announced_on"] and o["announced_on"] and c["announced_on"] > o["announced_on"] for c in recent):
                    # these notices follow the latest issue, which is usually dated by now: an older undated action
                    # (a proposal that never got dates) is not evidence enough
                    return None, "an undated action and a later action of this kind could match"
                return o["action_id"], "only_open"
            if len(open_) > 1:
                return None, f"{len(open_)} open actions of this kind could match"
            if follow_up in ("listing", "subscription", "further"):
                # these follow the event itself (listing of the new shares, level of subscription, letters of
                # allotment, offer document): the action is usually dated by now
                near = [c for c in cands if on and c["announced_on"] and (dt.date.fromisoformat(on) - dt.date.fromisoformat(c["announced_on"])).days <= RECENT_WINDOW_DAYS
                        and not N.terms_disagree(kind, a, c)]
                if len(near) == 1:
                    if changes_set_dates(a, near[0]):
                        # weak evidence never replaces a date the action already holds
                        return None, "the only recent action of this kind holds other dates"
                    return near[0]["action_id"], "only_recent"
                if len(near) > 1:
                    return None, f"{len(near)} recent actions of this kind could match"
        near = [c for c in cands if on and c["announced_on"] and (dt.date.fromisoformat(on) - dt.date.fromisoformat(c["announced_on"])).days <= DUP_WINDOW_DAYS
                and N.same_terms(kind, a, c)]
        if len(near) == 1:
            return near[0]["action_id"], "same_terms"
        if follow_up:
            return None, "no open action of this kind"
        return None, None

    # ---- one notice -----------------------------------------------------------------------------------------------
    def process(self, pk, detail=None, z=None, body=None):
        """Turn one stored notice into action links / new actions. detail: the raw detail response (already identity
        checked by the caller; status in the notice row); z: the notice's own feed/calendar row; body: old-format text."""
        n = self.notes[pk]
        cid = n["company_id"]
        status, base, dtype = N.check_detail(n["cse_format"], n["cse_announcement_id"], detail, self.tickers.get(cid), cid)
        if n["detail_status"] == "COMPANY_MISMATCH" or status == "COMPANY_MISMATCH":
            self.bump("detail_refused_wrong_company")
            return
        text = " ".join(x or "" for x in (n["category_raw"], n["remarks"], (base or {}).get("title"), (base or {}).get("otherRemark"), body))
        kinds, how = N.kinds_for(n["normalized_category"], self.kind_map, text)
        kinds &= set(N.ACTIONS)
        if not kinds:
            return
        if how == "rule":
            self.bump("category_by_rule")
        follow = N.follow_up_type(n["normalized_category"], " ".join(x or "" for x in (n["remarks"], (base or {}).get("title"))))
        if how == "listing" and not follow:
            follow = "listing"
        weak = False
        if not follow and dtype == "General" and N.prints_dates(base):
            follow, weak = "dates", True       # a free-form notice that prints the dates acts as the dates notice
        both = N.names_both_classes(n["category_raw"], n["remarks"], (base or {}).get("title"))
        docs = N.documents(detail)
        reading = self.reviewed.get(int(n["cse_announcement_id"])) if n["cse_format"] == "NEW" else None
        self.bump("action_notices")
        for kind in sorted(kinds):
            rows = N.values(kind, base if status == "FETCHED" else None, dtype, z, body if n["cse_format"] == "OLD" else None)
            if reading and reading.get("kind", kind) == kind:
                rv = {f: reading["values"][f] for f in N.FIELDS if reading.get("values", {}).get(f) is not None}
                for r in rows:
                    r.update(rv)
                    r["level"], r["reviewed"] = "SCAN_REVIEWED", True
            classes = {r["cls"] for r in rows}
            if both and "NON_VOTING" not in classes and self.secs.get(cid, {}).get("NON_VOTING"):
                for r in [r for r in rows if r["cls"] == "VOTING"]:
                    if (base or {}).get("dType") in (None, "General") or not any(r.get(f) is not None for f in N.TERM_FIELDS):
                        rows.append(dict(r, cls="NON_VOTING"))
                        self.bump("both_classes_by_text")
            for a in rows:
                sid = self.security_for(cid, a["cls"])
                if sid is None:
                    self.bump("no_matching_share_class")
                    continue
                if a.get("issue_price") and N.consideration_is_total(a):
                    self.queue("needs-review", f"total-consideration:{pk}:{sid}", pk, None,
                               "printed consideration looks like a total, not a price per share: price not stored")
                    a.pop("issue_price")
                elif a.get("issue_price") and N.price_inconsistent(a):
                    self.queue("needs-review", f"price-inconsistent:{pk}:{sid}", pk, None,
                               "printed consideration x new shares does not match the amount capitalised: price not stored")
                    a.pop("issue_price")
                a.pop("_total", None)
                self.apply_row(kind, cid, sid, n, pk, a, base, follow, text, docs, weak)

    def apply_row(self, kind, cid, sid, n, pk, a, base, follow, text, docs, weak=False):
        src = "reviewed" if a.get("reviewed") else ("structured" if a.get("level") == "STRUCTURED" else "body" if a.get("level") == "BODY_TEXT" else "title")
        aid, why = self.find(kind, sid, n, pk, a, base, follow, text)
        if aid is None:
            if follow == "postponed":
                self.queue("needs-review", f"unlinked:{pk}:{sid}:{kind}", pk, None, "postponement names no action that could be linked")
                self.bump("unlinked_postponement")
                return
            aid = self.new_action(cid, sid, kind, n, a, pk, src)
            if follow and not weak:
                self.queue("needs-review", f"unlinked:{pk}:{sid}:{kind}", pk, aid,
                           f"{follow} notice links to no earlier action ({why}); a new action was created")
            else:
                age = lambda c: (dt.date.fromisoformat(n["announced_on"]) - dt.date.fromisoformat(c["announced_on"])).days
                near = [c for c in self.candidates(sid, kind, n["announced_on"], pk)
                        if c["action_id"] != aid and n["announced_on"] and c["announced_on"]
                        and ((not N.is_dated(c) and age(c) <= DUP_WINDOW_DAYS) or (kind != "CASH_DIVIDEND" and age(c) <= RECENT_WINDOW_DAYS))]
                if near:
                    self.queue("needs-review", f"possible-duplicate:{pk}:{sid}:{kind}", pk, aid,
                               "new action while action(s) " + ",".join(str(c["action_id"]) for c in near) + " of the same kind are open or recent")
            self.check_reading(kind, aid, pk, n, a, follow, docs)
            return
        self.link(aid, pk, why)
        self.bump("notices_attached")
        self.bump("attached_by_" + why)
        act = self.actions[aid]
        if follow == "postponed" and not any(a.get(f) for f in N.DATE_FIELDS):
            for f in N.DATE_FIELDS:
                if act.get(f) is not None:
                    self.record_value(aid, f, None, pk, "postponement")
            self.record_value(aid, "status", "POSTPONED", pk, "postponement")
            self.bump("postponed")
            if re.search(r"cancel|withdraw", text, re.I):
                self.queue("needs-review", f"cancelled:{pk}:{aid}", pk, aid, "notice may cancel or withdraw the action")
            return
        changed_dates = changed_terms = False
        for f in N.FIELDS:
            v = a.get(f)
            if v is None:
                continue
            old = act.get(f)
            if old is not None and same(f, old, v):
                continue
            improves = False
            if f == "ratio_text" and old is not None:
                ro, rn = N.ratio(old), N.ratio(v)
                if rn is None or (ro and abs(ro[0] / ro[1] - rn[0] / rn[1]) < 1e-9):
                    continue      # same ratio in other words, or the new text is not a clean ratio: keep the first
                improves = ro is None
            self.record_value(aid, f, v, pk, src)
            if old is not None and not improves:
                if f in N.DATE_FIELDS:
                    changed_dates = True
                elif f in N.TERM_FIELDS:
                    changed_terms = True
            else:
                self.bump("values_filled")
        if act.get("status") == "POSTPONED" and N.is_dated(act):
            self.record_value(aid, "status", "ACTIVE", pk, src)
        if N.RANK.get(a.get("level"), 0) > N.RANK.get(act.get("detail_level"), 0):
            self.record_value(aid, "detail_level", a["level"], pk, src)
        if changed_dates:
            self.bump("dates_superseded")
        if changed_terms:
            self.bump("terms_superseded")
            self.queue("needs-review", f"terms-revised:{pk}:{aid}", pk, aid, "a linked notice prints different terms; the new terms are current")
        self.check_reading(kind, aid, pk, n, a, follow, docs)

    def check_reading(self, kind, aid, pk, n, a, follow, docs):
        """A dates notice of a structural action that prints no date in any structured field: the dates are in its
        letter. Queue it for reading unless a reviewed reading was applied."""
        if a.get("reviewed"):
            self.resolve(f"reading:{pk}", "applied from actions/reviewed/notice_readings.json")
            return
        if kind not in N.STRUCTURAL or follow not in ("dates", "amended"):
            return
        if any(a.get(f) for f in ("ex_date", "record_date", "effective_date")):
            return
        if not docs:
            self.queue("needs-review", f"no-document:{pk}", pk, aid, "dates notice prints no date and has no attached document")
            return
        self.queue("needs-reading", f"reading:{pk}", pk, aid, f"{follow} notice: its values are only in the attached letter", docs)

    # ---- reviewed readings / decisions arriving after the notice ---------------------------------------------------
    def apply_late_readings(self, detail_of):
        """Readings recorded after their notice was processed: apply them to the action(s) the notice is linked to."""
        for key, q_pk, q_aid in self.db.execute("select item_key, announcement_pk, action_id from ac_queue where queue='needs-reading' and status='OPEN'").fetchall():
            n = self.notes.get(q_pk)
            if not n or int(n["cse_announcement_id"]) not in self.reviewed:
                continue
            r = self.reviewed[int(n["cse_announcement_id"])]
            vals = {f: v for f, v in (r.get("values") or {}).items() if f in N.FIELDS and v is not None}
            targets = [aid for aid, pks in self.links.items() if q_pk in pks and (not r.get("kind") or self.actions[aid]["action_type"] == r["kind"])]
            for aid in targets:
                act = self.actions[aid]
                for f, v in vals.items():
                    if act.get(f) is None or not same(f, act.get(f), v):
                        self.record_value(aid, f, from_text(f, v), q_pk, "reviewed")
                if N.RANK["SCAN_REVIEWED"] > N.RANK.get(act.get("detail_level"), 0) and vals:
                    self.record_value(aid, "detail_level", "SCAN_REVIEWED", q_pk, "reviewed")
                if act.get("status") == "POSTPONED" and N.is_dated(act):
                    self.record_value(aid, "status", "ACTIVE", q_pk, "reviewed")
            self.resolve(key, "applied from actions/reviewed/notice_readings.json" if vals else "reviewed: the letter prints no values")
            self.bump("readings_applied")

    def apply_decisions(self):
        """actions/reviewed/review_decisions.json: {"<item_key>": {"decision": "keep" | "same_as", "action_id": N, ...}}.
        same_as: the new action is the same event as action N; allowed only while the new action has not been pulled
        into main. Its links and values move to N; it is kept as a row with status SAME_AS:N (its id is never reused)."""
        for key, d in self.decisions.items():
            row = self.db.execute("select announcement_pk, action_id, status from ac_queue where item_key=?", (key,)).fetchone()
            if not row or row[2] != "OPEN":
                continue
            pk, new_aid, _ = row
            if d.get("decision") == "keep":
                self.resolve(key, "kept as a separate action: " + d.get("note", ""))
                self.bump("decisions_applied")
            elif d.get("decision") == "same_as" and new_aid in self.actions and d.get("action_id") in self.actions:
                tgt = int(d["action_id"])
                pulled = self.db.execute("select pulled_at from ac_action where action_id=?", (new_aid,)).fetchone()
                if not pulled or pulled[0]:
                    self.bump("decisions_refused_already_pulled")
                    continue
                for p in sorted(self.links.get(new_aid, ())):
                    self.link(tgt, p, "review:" + key)
                src = self.actions[new_aid]
                for f in N.FIELDS:
                    if src.get(f) is not None and self.actions[tgt].get(f) is None:
                        self.record_value(tgt, f, src[f], pk, "review")
                self.record_value(new_aid, "status", f"SAME_AS:{tgt}", pk, "review")
                self.resolve(key, f"same event as action {tgt}: " + d.get("note", ""))
                self.bump("decisions_applied")
