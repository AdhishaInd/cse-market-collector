"""Build the company event calendar (dividend/rights/scrip/split key dates, AGM/EGM, estimated results dates,
halts/watch-list/suspensions, new-listing notices) from the cached CSE API responses in raw_api/events/ and
read-only inputs already in the database (corporate_action, announcement, listing_event, report_document).

Usage:  python -I pipeline/load_events.py [--db db/staging_events.db] [--as-of YYYY-MM-DD] [--since YYYY-MM-DD]
                                         [--dry-run]

* Idempotent: creates the tables if missing, then replaces ALL rows of company_event / company_event_source /
  result_estimate_backtest / event_load_meta in ONE transaction. Rerunning with the same cache gives identical rows.
* Writes only the four tables above (+ two views). Never touches corporate_action or any other table.
* Refuses (rolls back, exit 1) when a check fails. Values that cannot be read unambiguously are dropped and counted
  by reason (printed and stored in event_load_meta key 'refusals').
* date_kind: confirmed        = date stated in an official CSE announcement / filing / meeting notice
             announced_tentative = officially announced but conditional (dividend needing shareholder approval at an
                               AGM not yet held, rights/split before the approving EGM) - the date can still move
             estimated        = computed here (results due dates); `method` says how; never mixed with confirmed.
"""
import os, argparse, calendar, collections, datetime as dt, json, pathlib, re, sqlite3, statistics as st, sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import letters as LT  # noqa: E402  (stdlib only at import time)
ROOT = pathlib.Path(os.environ.get("EVENTS_ROOT") or pathlib.Path(__file__).resolve().parent.parent / "work")  # data dir: db/, raw_api/, logs/
RAW = ROOT / "raw_api" / "events"
ANN_DETAIL = ROOT / "raw_api" / "ann_detail"
COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))

EVENT_TYPES = (
    "CASH_DIV_XD", "CASH_DIV_RECORD", "CASH_DIV_PAYMENT",
    "SCRIP_DIV_XD", "SCRIP_DIV_RECORD", "SCRIP_DIV_PAYMENT",
    "RIGHTS_XR", "RIGHTS_RECORD", "RIGHTS_LETTERS_DISPATCH", "RIGHTS_TRADING_START", "RIGHTS_EGM",
    "RIGHTS_RENUNCIATION_LAST", "RIGHTS_ACCEPTANCE_LAST",
    "SPLIT_TRADING_SUSPENDED", "SPLIT_RECORD", "SPLIT_SUSPENSION_LAST_DAY", "SPLIT_TRADING_START", "SPLIT_EGM",
    "REPURCHASE_OFFER_OPEN", "REPURCHASE_OFFER_CLOSE",
    "AGM", "EGM",
    "RESULTS_FILED", "RESULTS_DUE",
    "TRADING_HALT", "TRADING_HALT_LIFTED", "WATCH_LIST_IN", "WATCH_LIST_OUT", "SUSPENDED", "SUSPENSION_LIFTED",
    "DELISTED", "BOARD_TRANSFER",
    "NEW_LISTING_NOTICE",
)
DATE_KINDS = ("confirmed", "announced_tentative", "estimated")

SCHEMA = """
CREATE TABLE IF NOT EXISTS company_event (
    event_id        INTEGER PRIMARY KEY,
    company_id      INTEGER REFERENCES company(company_id),   -- NULL only for NEW_LISTING_NOTICE (not yet a company)
    company_name    TEXT,
    symbol          TEXT,      -- security symbol (e.g. JKH.N0000) when the event is specific to one share class, else NULL
    event_type      TEXT NOT NULL,
    event_date      TEXT NOT NULL CHECK (date(event_date) IS event_date),
    date_kind       TEXT NOT NULL CHECK (date_kind IN ('confirmed','announced_tentative','estimated')),
    amount          NUMERIC,   -- per share, Rs. (dividend per share; rights issue price)
    amount_kind     TEXT CHECK (amount_kind IS NULL OR amount_kind IN ('DIVIDEND_PER_SHARE','ISSUE_PRICE')),
    detail          TEXT,
    method          TEXT,      -- estimated rows only: how the date was computed
    rule_deadline   TEXT,      -- RESULTS_DUE only: CSE listing-rule deadline for that filing
    status          TEXT,      -- RESULTS_DUE only: UPCOMING / OVERDUE (past rule deadline, not in the report index)
    announced_on    TEXT,      -- date the notice was published (for lead-time measurement)
    source_endpoint TEXT NOT NULL,   -- primary source; all sources are in company_event_source
    source_id       TEXT,
    fetched_at      TEXT       -- when the primary source response was fetched (NULL for rows read from the database)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_company_event ON company_event(COALESCE(company_id, -1), COALESCE(symbol, ''), event_type,
    event_date, CASE WHEN company_id IS NULL THEN company_name ELSE '' END);
CREATE INDEX IF NOT EXISTS ix_company_event_date ON company_event(event_date);
CREATE INDEX IF NOT EXISTS ix_company_event_company ON company_event(company_id, event_date);
CREATE TABLE IF NOT EXISTS company_event_source (
    event_id        INTEGER NOT NULL REFERENCES company_event(event_id),
    source_endpoint TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    PRIMARY KEY (event_id, source_endpoint, source_id)
);
CREATE TABLE IF NOT EXISTS result_estimate_backtest (
    company_id   INTEGER NOT NULL REFERENCES company(company_id),
    report_kind  TEXT NOT NULL,          -- Q1/Q2/Q3 interim, Q4 year-end quarter (interim), AR annual report, AFS audited FS
    period_end   TEXT NOT NULL,
    filed_on     TEXT NOT NULL,          -- actual: first upload date in report_document
    rule_deadline TEXT NOT NULL,
    est_rule     TEXT NOT NULL,          -- baseline: the rule deadline itself
    est_model    TEXT NOT NULL,          -- the method used for RESULTS_DUE, using only filings made before period_end
    model_basis  TEXT NOT NULL,          -- 'history' or 'rule_fallback'
    err_rule_days  INTEGER NOT NULL,     -- estimate minus actual (positive = estimate later than actual)
    err_model_days INTEGER NOT NULL,
    PRIMARY KEY (company_id, report_kind, period_end)
);
CREATE TABLE IF NOT EXISTS event_load_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
DROP VIEW IF EXISTS v_upcoming_events;
CREATE VIEW v_upcoming_events AS
SELECT e.event_date, e.event_type, e.date_kind, c.ticker, e.symbol, e.company_name, e.amount, e.detail, e.method,
       e.rule_deadline, e.status, e.announced_on
FROM company_event e LEFT JOIN company c USING (company_id)
WHERE e.event_date >= (SELECT value FROM event_load_meta WHERE key = 'as_of')
ORDER BY e.event_date, c.ticker;
DROP VIEW IF EXISTS v_event_multi_source;
CREATE VIEW v_event_multi_source AS
SELECT event_id, COUNT(*) AS n_sources, GROUP_CONCAT(source_endpoint || ':' || source_id, ' ') AS sources
FROM company_event_source GROUP BY event_id HAVING COUNT(*) > 1;
"""

# CSE listing-rule filing deadlines. Verified 2026-11-12 against Listing Rules Section 7 (PDF modified 2023-09-30; the 2026-11-05
# revision could not be downloaded): Rule 7.4 interim 45 days (Q1-Q3), 2 months (Q4); Rule 7.5 annual report/audited FS 5 months.
RULE = {
    "Q1": ("45 days after quarter end", lambda p: p + dt.timedelta(45)),
    "Q2": ("45 days after quarter end", lambda p: p + dt.timedelta(45)),
    "Q3": ("45 days after quarter end", lambda p: p + dt.timedelta(45)),
    "Q4": ("2 months after financial year end (unaudited year-end quarter)", lambda p: add_months(p, 2)),
    "AR": ("5 months after financial year end (annual report)", lambda p: add_months(p, 5)),
    "AFS": ("5 months after financial year end (audited financial statements, Rule 7.5)", lambda p: add_months(p, 5)),
}


def add_months(d, n):
    y, m = d.year + (d.month - 1 + n) // 12, (d.month - 1 + n) % 12 + 1
    return dt.date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def month_end(y, m):
    return dt.date(y, m, calendar.monthrange(y, m)[1])


def D(s):
    return dt.date.fromisoformat(str(s)[:10]) if s else None


def ms_to_date(ms):
    if ms in (None, "", 0):
        return None
    return dt.datetime.fromtimestamp(ms / 1000, COLO).date()


MON = {m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def dmy(s):
    """'30 Mar 2027' -> date, else None."""
    m = re.fullmatch(r"\s*(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})\s*", s or "")
    if not m or m.group(2).title() not in MON:
        return None
    try:
        return dt.date(int(m.group(3)), MON[m.group(2).title()], int(m.group(1)))
    except ValueError:
        return None


def any_date(v):
    return ms_to_date(v) if isinstance(v, (int, float)) else dmy(v) if isinstance(v, str) else None


def read_cache(path):
    j = json.loads(path.read_text())
    if isinstance(j, dict) and "_fetched_at" in j:
        return j["data"], j["_fetched_at"]
    return j, None


def feed_records(endpoint, field, idf, raw=None):
    """Records of a rolling feed merged over every stored snapshot (legacy `<endpoint>.json` and the daily
    `<endpoint>__day-YYYY-MM-DD.json` files written by fetch_events.py --daily). One (record, fetched_at) per id, the
    most recently fetched copy winning; newest id first."""
    raw = raw or RAW
    out = {}
    for f in sorted(raw.glob(f"{endpoint}.json")) + sorted(raw.glob(f"{endpoint}__day-*.json")):
        data, ts = read_cache(f)
        for r in (data or {}).get(field) or []:
            if r[idf] not in out or (ts or "") >= (out[r[idf]][1] or ""):
                out[r[idf]] = (r, ts)
    return [out[k] for k in sorted(out, reverse=True)]


INTERIM_TITLE = re.compile(r"Interim Financial Statements for the (?:Quarter|Period|Three Months|Six Months|Nine Months|Twelve Months|Year)s? ended\s+"
                           r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,9})\s+(\d{4})\s*", re.I)
ANNUAL_TITLE = re.compile(r"Annual Report\s+(20\d{2})(?:\s*[-/]\s*(20\d{2}|\d{2}))?", re.I)


def feed_filing(title, fye_month):
    """(category, period_end) for a getFinancialAnnouncement title, or None. Accuracy over coverage: only the standard
    title forms are read - 'Interim Financial Statements for the Quarter ended 30th September 2026' and
    'Annual Report 2025/26' / 'Annual Report 2025' (period end = the company's fiscal year end in the last year named,
    the convention of scripts/load_master.py annual_end_from_title). Errata, audited statements, prospectuses and
    anything else are ignored (they reach the calendar later through report_document)."""
    t = " ".join((title or "").split())
    m = INTERIM_TITLE.fullmatch(t)
    if m:
        mon = MON.get(m.group(2)[:3].title())
        try:
            return ("INTERIM", dt.date(int(m.group(3)), mon, int(m.group(1)))) if mon else None
        except ValueError:
            return None
    m = ANNUAL_TITLE.fullmatch(t)
    if m and fye_month:
        y1, y2 = int(m.group(1)), m.group(2)
        y = y1 if y2 is None else int(y2) if len(y2) == 4 else int(m.group(1)[:2] + y2)
        if y not in (y1, y1 + 1):
            return None
        return "ANNUAL", month_end(y, fye_month)
    return None


def ts_to_colombo_date(s):
    """'11 Nov 2026 04:44:14 PM' (Colombo time, as the feed prints it) -> date."""
    try:
        return dt.datetime.strptime(" ".join((s or "").split()), "%d %b %Y %I:%M:%S %p").date()
    except ValueError:
        return None


# --------------------------------------------------------------------------------------------------------------
class Events:
    def __init__(self, as_of, since, refusals):
        self.rows, self.as_of, self.since, self.ref = {}, as_of, since, refusals
        self.by_ann = collections.defaultdict(list)       # announcementId -> [row keys]
        self.conflicts = []
        self.meetings = []          # AGM/EGM candidates, resolved in resolve_meetings()

    def linked(self, aid):
        return [k for k in self.by_ann.get(aid, []) if k in self.rows]

    def add(self, company_id, name, symbol, etype, date, kind, src, sid, fetched=None, amount=None, amount_kind=None,
            detail=None, method=None, rule_deadline=None, status=None, announced_on=None, ann_id=None):
        assert etype in EVENT_TYPES and kind in DATE_KINDS, (etype, kind)
        if date is None:
            return None
        if not (dt.date(2000, 1, 1) <= date <= dt.date(2032, 12, 31)):
            self.ref["date_out_of_range"] += 1
            return None
        key = (company_id, symbol, etype, date.isoformat(), None if company_id is not None else name)
        r = self.rows.get(key)
        if r is None:
            r = self.rows[key] = dict(company_id=company_id, company_name=name, symbol=symbol, event_type=etype,
                                      event_date=date.isoformat(), date_kind=kind, amount=amount,
                                      amount_kind=amount_kind, detail=detail, method=method,
                                      rule_deadline=rule_deadline, status=status, announced_on=announced_on,
                                      sources=[], primary=(src, str(sid), fetched))
        else:
            if amount is not None:
                if r["amount"] is None:
                    r["amount"], r["amount_kind"] = amount, amount_kind
                elif abs(float(r["amount"]) - float(amount)) > 1e-9:
                    self.conflicts.append(("amount", key, r["amount"], amount, src))
            if r["detail"] is None and detail:
                r["detail"] = detail
            if r["announced_on"] is None:
                r["announced_on"] = announced_on
            if r["primary"][0] == "corporate_action" and src != "corporate_action":
                r["primary"] = (src, str(sid), fetched)      # prefer the API source as primary
            if kind == "announced_tentative" and r["date_kind"] == "confirmed" and etype not in ("RESULTS_FILED",):
                r["date_kind"] = kind
        if (src, str(sid)) not in r["sources"]:
            r["sources"].append((src, str(sid)))
        for a in (ann_id if isinstance(ann_id, (list, tuple)) else [ann_id] if ann_id is not None else []):
            if key not in self.by_ann[a]:
                self.by_ann[a].append(key)
        return key


# --------------------------------------------------------------------------------------------------------------
def load_inputs(db):
    ticker, comp_name, sec_by_class = {}, {}, collections.defaultdict(dict)
    for cid, t, n, ls in db.execute("select company_id, ticker, name, listing_status from company"):
        ticker[t] = cid
        comp_name[cid] = n
    sec_sym = {}
    for sid, cid, sym, sc in db.execute("select security_id, company_id, symbol, share_class from security"):
        sec_sym[sid] = sym
        sec_by_class[cid][sc] = sym
    names = {}
    for cid, n in comp_name.items():
        names.setdefault(" ".join(n.upper().split()), []).append(cid)
    return ticker, comp_name, sec_sym, sec_by_class, names


def detail_for(ann_id, symbol=None):
    """(detail, fetched_at) - see fetch_events.detail_record (null getAnnouncementById -> checked general record)."""
    d = LT.F.detail_record(ann_id, symbol, ROOT)
    f = RAW / f"getAnnouncementById__announcementId-{ann_id}.json"
    return (d or {}), (read_cache(f)[1] if f.exists() else None)


def approval_pending(base, agm_date, as_of, event_date):
    """True when the announcement says shareholder approval is required (R) and the approving AGM has not been held."""
    if event_date < as_of:
        return False
    if (base or {}).get("shrHolderApproval") == "R":
        return agm_date is None or agm_date >= as_of
    return False


def resolve_meetings(E, since, refusals):
    """One AGM per company and financial year: candidates within 120 days of each other are the same meeting; keep the date of
    the highest-level source (3 = 'resolutions' notice after the meeting was held, 2 = meeting notice / CSE meetings calendar,
    1 = AGM date quoted in a dividend announcement), latest announcement winning ties. A later EGM notice that says
    adjournment/postponement/rescheduling replaces earlier EGM notices of the same company within 90 days."""
    by = collections.defaultdict(list)
    for m in E.meetings:
        by[(m["cid"], m["type"])].append(m)
    for (cid, typ), ms in by.items():
        ms.sort(key=lambda m: m["date"])
        clusters, cur = [], []
        for m in ms:
            if cur and (m["date"] - cur[-1]["date"]).days > (120 if typ == "AGM" else 0):
                clusters.append(cur)
                cur = []
            cur.append(m)
        clusters.append(cur)
        for cl in clusters:
            if typ == "AGM":
                best = max(cl, key=lambda m: (m["level"], m["announced_on"] or dt.date.min))
                keep = [m for m in cl if m["date"] == best["date"]]
                refusals["meeting_date_superseded"] += len(cl) - len(keep)
            else:
                keep = cl
            if keep[0]["date"] < since:
                continue
            first = keep[0]
            kinds = max(keep, key=lambda m: m["level"])
            k = E.add(cid, first["name"], None, typ, first["date"], "confirmed", kinds["src"], kinds["sid"], kinds["ts"],
                      detail=kinds["detail"], announced_on=min((m["announced_on"] for m in keep if m["announced_on"]), default=None) and
                      min(m["announced_on"] for m in keep if m["announced_on"]).isoformat())      # first notice of this date
            for m in keep:
                if (m["src"], str(m["sid"])) not in E.rows[k]["sources"]:
                    E.rows[k]["sources"].append((m["src"], str(m["sid"])))
    # EGM adjournments
    egm_rows = collections.defaultdict(list)
    for m in E.meetings:
        if m["type"] == "EGM" and m.get("remarks") and m["announced_on"]:
            egm_rows[m["cid"]].append(m)
    for cid, ms in egm_rows.items():
        for m in ms:
            if any(w in m["remarks"] for w in ("ADJOURN", "POSTPON", "RESCHEDUL")):
                for o in list(ms):
                    if o is not m and o["announced_on"] < m["announced_on"] and o["date"] != m["date"] and abs((o["date"] - m["date"]).days) <= 90:
                        key = (cid, None, "EGM", o["date"].isoformat(), None)
                        if key in E.rows and all(s[0] == "getAnnouncementById" for s in E.rows[key]["sources"]):
                            del E.rows[key]
                            refusals["egm_superseded_by_adjournment"] += 1


def build(db, as_of, since, refusals):
    ticker, comp_name, sec_sym, sec_by_class, names = load_inputs(db)
    reviewed = LT.load_reviewed()
    E = Events(as_of, since, refusals)
    stats = collections.Counter()

    # ---- 1. corporate_action (read only) -----------------------------------------------------------------
    link = collections.defaultdict(list)
    for aid, cse_id in db.execute(
            "select l.action_id, a.cse_announcement_id from corporate_action_announcement l "
            "join announcement a using(announcement_pk) where a.cse_format='NEW'"):
        link[aid].append(cse_id)
    TYPEMAP = {"CASH_DIVIDEND": ("CASH_DIV_XD", "CASH_DIV_RECORD", "CASH_DIV_PAYMENT", None),
               "SCRIP_DIVIDEND": ("SCRIP_DIV_XD", "SCRIP_DIV_RECORD", "SCRIP_DIV_PAYMENT", None),
               "RIGHTS_ISSUE": ("RIGHTS_XR", "RIGHTS_RECORD", None, "RIGHTS_TRADING_START")}
    q = ("select action_id, company_id, security_id, action_type, announced_on, dividend_stage, financial_year, per_share, "
         "ratio_text, issue_price, record_date, ex_date, effective_date, payment_date from corporate_action "
         "where coalesce(ex_date, record_date, payment_date, effective_date) >= ?")
    for (aid, cid, sid, at, ann_on, stage, fy, ps, ratio, ip, rec, ex, eff, pay) in db.execute(q, (since.isoformat(),)):
        sym, name = sec_sym.get(sid), comp_name[cid]
        anns = link.get(aid, [])
        det = " ".join(x for x in (stage, f"FY {fy}" if fy else None,
                                   ratio if at in ("RIGHTS_ISSUE", "SCRIP_DIVIDEND", "SUB_DIVISION", "CAPITALISATION", "CONSOLIDATION") and ratio and re.search("[A-Za-z:]", ratio) else None) if x)
        stats["corporate_action_rows"] += 1
        if at in TYPEMAP:
            xd, rc, pm, ef = TYPEMAP[at]
            amt, ak = (ps, "DIVIDEND_PER_SHARE") if at == "CASH_DIVIDEND" else (ip, "ISSUE_PRICE") if at == "RIGHTS_ISSUE" else (None, None)
            for t, d in ((xd, ex), (rc, rec), (pm, pay), (ef, eff)):
                if t and d:
                    k = E.add(cid, name, sym, t, D(d), "confirmed", "corporate_action", aid, None, amount=amt,
                              amount_kind=ak if amt is not None else None, announced_on=ann_on, detail=det or None, ann_id=anns)
                    for a in anns:
                        if ("announcement", str(a)) not in E.rows[k]["sources"]:
                            E.rows[k]["sources"].append(("announcement", str(a)))
        elif at in ("SUB_DIVISION", "CONSOLIDATION", "CAPITALISATION"):
            for t, d in (("SPLIT_RECORD", rec), ("SPLIT_TRADING_START", eff)):
                if d:
                    k = E.add(cid, name, sym, t, D(d), "confirmed", "corporate_action", aid, None,
                              detail=f"{at} {ratio or ''}".strip(), announced_on=ann_on, ann_id=anns)
                    for a in anns:
                        if ("announcement", str(a)) not in E.rows[k]["sources"]:
                            E.rows[k]["sources"].append(("announcement", str(a)))
        # REPURCHASE: dates come from the announcement detail (offer opening/closing) in section 2

    # ---- 2. corporateCalender ---------------------------------------------------------------------------
    cal = {}
    for f in sorted(RAW.glob("corporateCalender__*.json")):
        data, ts = read_cache(f)
        for r in (data or {}).get("approvedCoopCalAnnouncements") or []:
            if r["announcementId"] not in cal or (ts or "") > (cal[r["announcementId"]][1] or ""):
                cal[r["announcementId"]] = (r, ts)
    stats["calendar_announcements"] = len(cal)
    CALMAP = {
        "CASH DIVIDEND": [("xd", "CASH_DIV_XD"), ("recordDate", "CASH_DIV_RECORD"), ("paymentDate", "CASH_DIV_PAYMENT")],
        "SCRIP DIVIDEND (DATES)": [("xd", "SCRIP_DIV_XD"), ("recordDate", "SCRIP_DIV_RECORD"), ("paymentDate", "SCRIP_DIV_PAYMENT")],
        "RIGHTS ISSUE (DATES)": [("xr", "RIGHTS_XR"), ("recordDate", "RIGHTS_RECORD"), ("allotment", "RIGHTS_LETTERS_DISPATCH"),
                                 ("tradingCommencement", "RIGHTS_TRADING_START")],
        "SUB-DIVISION OF SHARES (DATES)": [("tradingSuspended", "SPLIT_TRADING_SUSPENDED"),
                                           ("tradingCommencement", "SPLIT_TRADING_START")],
    }
    # a company can re-announce the same dividend/rights dates (a 'further announcement' moves a date): the latest
    # announcement of the same corporate_action (or, when not yet linked, of the same company/category with first
    # dates within 40 days) supersedes the earlier ones
    ann_actions = collections.defaultdict(set)
    for act, anns in link.items():
        for x in anns:
            ann_actions[x].add(act)

    def first_date(r):
        ds = [r[k] for k in ("xd", "xr", "recordDate", "tradingCommencement", "tradingSuspended") if r.get(k)]
        return min(ds) / 86400000 if ds else None
    groups = collections.defaultdict(list)
    for aid, (r, ts) in cal.items():
        groups[(r["symbol"], r["announcementCategory"])].append(aid)
    superseded = set()
    for (k_sym, k_cat), ids in groups.items():
        ids.sort()
        for i, a in enumerate(ids):
            if k_cat == "REPURCHASE OF SHARES":
                continue            # repurchase offer dates sit in the earlier announcements; later ones are follow-ups
            for b in ids[i + 1:]:
                if first_date(cal[b][0]) is None:
                    continue        # a later notice without dates never replaces dated ones
                if (ann_actions[a] & ann_actions[b]) or (not ann_actions[a] and not ann_actions[b] and first_date(cal[a][0]) and first_date(cal[b][0])
                                                         and abs(first_date(cal[a][0]) - first_date(cal[b][0])) <= 40):
                    superseded.add(a)
                    break
    stats["calendar_superseded_rows"] = len(superseded)
    for aid, (r, ts) in sorted(cal.items()):
        if aid in superseded:
            continue
        cat = r["announcementCategory"]
        cid = ticker.get(r["symbol"])
        if cid is None:
            refusals["calendar_symbol_not_in_company"] += 1
            continue
        name = comp_name[cid]
        ann_on = dmy(r["dateOfAnnouncement"])
        base, dts = detail_for(aid, r["symbol"])
        base = (base or {}).get("reqBaseAnnouncement") or {}
        agm = ms_to_date(r.get("agmDate")) or dmy(base.get("agm"))
        egm = any_date(base.get("egm"))
        if cat == "REPURCHASE OF SHARES":
            for k, t in (("offerOpeningDateRepurchase", "REPURCHASE_OFFER_OPEN"), ("offerClosingDateRepurchase", "REPURCHASE_OFFER_CLOSE")):
                d = any_date(base.get(k))
                if d and d >= since:
                    E.add(cid, name, None, t, d, "confirmed", "corporateCalender", aid, ts, announced_on=ann_on and ann_on.isoformat(),
                          ann_id=aid, detail=base.get("remarks"))
            continue
        spec = CALMAP.get(cat)
        if spec is None:
            refusals["calendar_unknown_category"] += 1
            continue
        # date-order sanity for dividends
        if cat in ("CASH DIVIDEND", "SCRIP DIVIDEND (DATES)"):
            xd, rc, pm = ms_to_date(r.get("xd")), ms_to_date(r.get("recordDate")), ms_to_date(r.get("paymentDate"))
            if (xd and rc and not (xd <= rc <= xd + dt.timedelta(10))) or (rc and pm and pm < rc):
                refusals["dividend_date_order"] += 1
                continue
        # share classes and amounts from the detail record
        per_class = []
        if cat == "CASH DIVIDEND" and base:
            if base.get("votingShare") and sec_by_class[cid].get("VOTING"):
                per_class.append((sec_by_class[cid]["VOTING"], base.get("votingDivPerShare")))
            if base.get("noneVotingShare") and sec_by_class[cid].get("NON_VOTING"):
                per_class.append((sec_by_class[cid]["NON_VOTING"], base.get("nonVotingDivPerShare")))
        dets = None
        if cat == "CASH DIVIDEND":
            dets = " ".join(x for x in ((base.get("remarks") or "").strip().title() or None,) if x) or None
        ao = ann_on.isoformat() if ann_on else None
        linked_info = {(E.rows[k]["symbol"], E.rows[k]["amount"], E.rows[k]["amount_kind"]) for k in E.linked(aid)}
        rights_text = " ".join(x for x in (base.get("votingProportion"), base.get("nonVotingProportion")) if x) or None
        for fld, t in spec:
            d = ms_to_date(r.get(fld))
            if d is None:
                continue
            if d < since:
                continue
            kind = "announced_tentative" if (cat in ("CASH DIVIDEND", "SCRIP DIVIDEND (DATES)") and approval_pending(base, agm, as_of, d)) else "confirmed"
            existing = [k for k in E.linked(aid) if E.rows[k]["event_type"] == t]
            if existing:
                for k in existing:
                    row = E.rows[k]
                    if row["event_date"] != d.isoformat():
                        E.conflicts.append(("date", k, row["event_date"], d.isoformat(), "corporateCalender"))
                        del E.rows[k]
                        E.add(row["company_id"], name, row["symbol"], t, d, kind, "corporateCalender", aid, ts,
                              amount=row["amount"], amount_kind=row["amount_kind"], detail=row["detail"],
                              announced_on=ao, ann_id=aid)
                        for s in row["sources"]:
                            E.rows[(row["company_id"], row["symbol"], t, d.isoformat(), None)]["sources"].append(s)
                    else:
                        E.add(row["company_id"], name, row["symbol"], t, d, kind, "corporateCalender", aid, ts,
                              announced_on=ao, ann_id=aid)
                if kind == "announced_tentative":
                    for k in E.linked(aid):
                        if E.rows[k]["event_type"] == t:
                            E.rows[k]["date_kind"] = kind
            elif linked_info:
                for sym, amt, ak in linked_info:
                    E.add(cid, name, sym, t, d, kind, "corporateCalender", aid, ts, amount=amt, amount_kind=ak,
                          detail=dets, announced_on=ao, ann_id=aid)
            elif per_class and t.startswith("CASH_DIV"):
                for sym, amt in per_class:
                    E.add(cid, name, sym, t, d, kind, "corporateCalender", aid, ts, amount=amt if amt else None,
                          amount_kind="DIVIDEND_PER_SHARE" if amt else None, detail=dets, announced_on=ao, ann_id=aid)
            else:
                E.add(cid, name, None, t, d, kind, "corporateCalender", aid, ts,
                      detail=dets or rights_text or base.get("remarks"), announced_on=ao, ann_id=aid)
        # SPLIT record / suspension last day / EGM dates only exist in the detail record
        if cat == "SUB-DIVISION OF SHARES (DATES)":
            for k, t in (("subDivisionBasedOnShareholdingsAsAt", "SPLIT_RECORD"), ("lastTradingSuspended", "SPLIT_SUSPENSION_LAST_DAY")):
                d = any_date(base.get(k))
                if d and d >= since:
                    E.add(cid, name, None, t, d, "announced_tentative" if egm and egm >= as_of else "confirmed",
                          "corporateCalender", aid, ts, announced_on=ao, ann_id=aid)
        # last dates of renunciation / acceptance and payment: only in the PDF letter (letters.py; reviewed readings win)
        if cat == "RIGHTS ISSUE (DATES)":
            ld = LT.letter_dates(aid, RAW, reviewed)
            if ld is None:
                stats["rights_letters_unread"] += 1
            else:
                stats["rights_letters_" + ld[2]] += 1
                syms = sorted({E.rows[k]["symbol"] for k in E.linked(aid) if E.rows[k]["event_type"].startswith("RIGHTS_")} - {None}) or [None]
                for d, t in ((ld[0], "RIGHTS_RENUNCIATION_LAST"), (ld[1], "RIGHTS_ACCEPTANCE_LAST")):
                    if d >= since:
                        for sym in syms:
                            E.add(cid, name, sym, t, d, "confirmed", ld[2], aid, ts, announced_on=ao, ann_id=aid,
                                  detail="date from the rights issue letter" + (" (read from the page image, reviewed)" if ld[2].endswith("Reviewed") else ""))
        if egm and egm >= since and cat in ("RIGHTS ISSUE (DATES)", "SUB-DIVISION OF SHARES (DATES)"):
            E.add(cid, name, None, "RIGHTS_EGM" if cat.startswith("RIGHTS") else "SPLIT_EGM", egm, "confirmed",
                  "corporateCalender", aid, ts, announced_on=ao, ann_id=aid,
                  detail="EGM to approve " + ("rights issue" if cat.startswith("RIGHTS") else "sub-division"))
            if egm >= as_of:   # the dated rights/split events depend on this EGM
                for k in E.linked(aid):
                    if E.rows[k]["event_type"] not in ("RIGHTS_EGM", "SPLIT_EGM"):
                        E.rows[k]["date_kind"] = "announced_tentative"
        if agm and agm >= since:
            E.meetings.append(dict(cid=cid, name=name, type="AGM", date=agm, level=1, src="corporateCalender", sid=aid, ts=ts,
                                   announced_on=ann_on, detail="AGM date stated in the dividend announcement"))
    # ---- 3. AGM / EGM: getAgmEgmEvents (CSE meetings calendar) and notice announcements ------------------
    for f in sorted(RAW.glob("getAgmEgmEvents__*.json")):
        data, ts = read_cache(f)
        for r in data or []:
            cid = ticker.get(r["company"])
            d = ms_to_date(r["date"])
            if cid is None:
                refusals["agm_calendar_unmapped_company"] += 1
                continue
            if d is None or d < since:
                continue
            detail = "; ".join(x for x in (r.get("reason"), r.get("time"), ", ".join(filter(None, (r.get("addressLine1"), r.get("addressLine2"), r.get("addressLine3"))))) if x)
            E.meetings.append(dict(cid=cid, name=comp_name[cid], type="AGM" if r["agmEgmCategory"] == "AGM" else "EGM", date=d,
                                   level=2, src="getAgmEgmEvents", sid=r["id"], ts=ts, announced_on=None, detail=detail or None))
    agm_notice_dates = collections.defaultdict(list)    # (cid, 'AGM'/'EGM') -> [(announced_on, event_date, dType)]
    for f in sorted(RAW.glob("getAnnouncementById__*.json")):
        data, ts = read_cache(f)
        b = (data or {}).get("reqBaseAnnouncement") or {}
        dtp = b.get("dType") or ""
        if not (dtp.startswith("Agm") or "GeneralMeeting" in dtp):
            continue
        cid = ticker.get(b.get("symbol"))
        isagm = dtp.startswith("Agm")
        d = any_date(b.get("dateOfAgm") if isagm else b.get("dateOfEgm"))
        if cid is None or d is None:
            refusals["meeting_notice_unmapped_or_undated"] += 1
            continue
        ao = dmy(b.get("dateOfAnnouncement"))
        agm_notice_dates[(cid, "AGM" if isagm else "EGM")].append((ao, d, dtp))
        if d < since:
            continue
        held = dtp.endswith("Approved")
        det = (f"meeting held; resolutions approved={b.get('resolutionApproved')}" if held else
               "; ".join(x for x in (b.get("remarks"), b.get("time"), b.get("venue")) if x))
        E.meetings.append(dict(cid=cid, name=comp_name[cid], type="AGM" if isagm else "EGM", date=d, level=3 if held else 2,
                               src="getAnnouncementById", sid=b["id"], ts=ts, announced_on=ao, detail=det or None,
                               remarks=(b.get("remarks") or "").upper()))
    resolve_meetings(E, since, refusals)
    # ---- 4. halts, watch list, suspensions, delistings (confirmed, past) ----------------------------------
    for cid, cat, ao, pk, cid_a in db.execute(
            "select company_id, normalized_category, announced_on, announcement_pk, cse_announcement_id from announcement "
            "where cse_format='NEW' and normalized_category in ('trading halted','trading halt lifted') and announced_on >= ?",
            (since.isoformat(),)):
        if cid is None:
            refusals["halt_no_company"] += 1
            continue
        E.add(cid, comp_name[cid], None, "TRADING_HALT" if cat == "trading halted" else "TRADING_HALT_LIFTED", D(ao),
              "confirmed", "announcement", cid_a, None, announced_on=ao, detail="date = announcement date")
    for cid, et, d, basis, reason, apk in db.execute(
            "select company_id, event_type, effective_date, date_basis, reason, announcement_pk from listing_event "
            "where effective_date >= ?", (since.isoformat(),)):
        E.add(cid, comp_name[cid], None, et, D(d), "confirmed", "listing_event", apk,
              detail=((reason or "")[:300] + ("" if basis == "STATED" else " [date = announcement date; effective date not stated]")).strip() or None)
    # new announcements since the database snapshot: approvedAnnouncement (company name only)
    known = {r[0] for r in db.execute("select cse_announcement_id from announcement where cse_format='NEW'")}
    for r, ts in feed_records("approvedAnnouncement", "approvedAnnouncements", "announcementId"):
        if r["announcementId"] in known:
            continue
        t = {"TRADING HALTED": "TRADING_HALT", "TRADING HALT LIFTED": "TRADING_HALT_LIFTED"}.get(r["announcementCategory"])
        if not t:
            continue
        cids = names.get(" ".join((r["company"] or "").upper().split()), [])
        d = dmy(r["dateOfAnnouncement"])
        if len(cids) != 1 or d is None:
            refusals["approvedAnnouncement_halt_unmatched_name"] += 1
            continue
        E.add(cids[0], comp_name[cids[0]], None, t, d, "confirmed", "approvedAnnouncement", r["announcementId"], ts,
              announced_on=d.isoformat(), detail="date = announcement date")
    # ---- 5. new listing notices ----------------------------------------------------------------------------
    for t in "PIS":
        f = RAW / f"upcomingListings__newListingType-{t}.json"
        if not f.exists():
            continue
        data, ts = read_cache(f)
        for r in (data or {}).get("content") or []:
            d = D(r["publishedDate"])
            if d and d >= since and r.get("status") == "A":
                E.add(None, r["title"], None, "NEW_LISTING_NOTICE", d, "confirmed", "upcomingListings", r["id"], ts,
                      detail=f"{r['category']} (newListingType {t}); event_date = notice published date, the offer/listing date is only in the attached documents",
                      announced_on=d.isoformat())
    return E, agm_notice_dates, stats, dict(ticker=ticker, comp_name=comp_name)


# --------------------------------------------------------------------------------------------------------------
def filings(db, feed=None):
    """(company_id, kind, period_end) -> first filing date, plus the company's fiscal-year-end month."""
    fye = collections.defaultdict(collections.Counter)
    for cid, pe in db.execute("select company_id, period_end_date from report_document where category='ANNUAL' and period_end_date >= '2019-01-01'"):
        fye[cid][int(pe[5:7])] += 1
    fy = {c: v.most_common(1)[0][0] for c, v in fye.items()}
    F = collections.defaultdict(dict)
    for cid, cat, title, pe, up, did in db.execute(
            "select company_id, category, title, period_end_date, uploaded_at, document_id from report_document "
            "where period_end_date >= '2016-01-01' and category in ('INTERIM','ANNUAL') and uploaded_at is not null"):
        if cid not in fy:
            continue
        m = int(pe[5:7])
        q = ((m - fy[cid]) % 12) // 3
        q = 4 if q == 0 else q
        tl = (title or "").lower()
        if cat == "INTERIM":
            kind = f"Q{q}"
        else:
            kind = "AR" if "annual report" in tl else "AFS" if "audited" in tl else None
        if kind is None:
            continue
        p, f = D(pe), D(up)
        k = (kind, p)
        if k not in F[cid] or f < F[cid][k][0]:
            F[cid][k] = (f, did)
    # filings newer than the report index, from the stored getFinancialAnnouncement snapshots (ids not yet in
    # report_document). Source id 'gfa:<report id>'; only standard titles are read (feed_filing).
    if feed is not None:
        ticker = dict(db.execute("select ticker, company_id from company"))
        indexed = {r[0] for r in db.execute("select document_id from report_document")}
        for r, ts in feed:
            cid = ticker.get(r.get("symbol"))
            if r["id"] in indexed or cid not in fy:
                continue
            got = feed_filing(r.get("fileText"), fy[cid])
            f = ts_to_colombo_date(r.get("uploadedDate"))
            if not got or not f:
                continue
            cat, p = got
            if cat == "INTERIM":
                q = ((p.month - fy[cid]) % 12) // 3
                kind = f"Q{4 if q == 0 else q}"
            else:
                kind = "AR"
            k = (kind, p)
            if k not in F[cid] or f < F[cid][k][0]:
                F[cid][k] = (f, f"gfa:{r['id']}")
    return F, fy


def snap_weekday(d):
    while d.weekday() > 4:
        d -= dt.timedelta(1)
    return d


def predict(F, cid, kind, p):
    """Estimate the filing date of (kind, period_end p) using only filings of this company made before p.
    Q1-Q3/Q4: median of the last 4 filings' lags of the same class (Q1-Q3 pooled; Q4 alone), >= 2 needed.
    AR/AFS: lag of the previous year's filing (1 needed). Capped at the rule deadline; weekend -> previous Friday.
    Fallback (not enough history): the rule deadline itself."""
    rule = RULE[kind][1](p)
    cls = (lambda k: "Q13" if k in ("Q1", "Q2", "Q3") else k)
    hist = sorted([(pp, (f - pp).days) for (kk, pp), (f, _) in F[cid].items() if f < p and pp < p and cls(kk) == cls(kind)], reverse=True)
    n_need, n_use = (1, 1) if kind in ("AR", "AFS") else (2, 4)
    lags = [l for _, l in hist[:n_use]]
    if len(lags) < n_need:
        return snap_weekday(rule), rule, "rule_fallback", f"no/insufficient own filing history; CSE rule deadline ({RULE[kind][0]})"
    lag = int(round(st.median(lags)))
    est = snap_weekday(min(p + dt.timedelta(lag), rule))
    how = (f"last year's filing lag {lag}d" if n_use == 1 else f"median lag {lag}d of last {len(lags)} filings ({kind if kind == 'Q4' else 'Q1-Q3'})")
    return est, rule, "history", f"{how}; capped at rule deadline ({RULE[kind][0]}); weekend -> previous Friday"


def results_events(db, E, as_of, since, comp_name, feed=None):
    F, fy = filings(db, feed)
    status = dict(db.execute("select company_id, listing_status from company"))
    n_est = n_filed = 0
    # confirmed: filings made in the window
    for cid, d in F.items():
        for (kind, p), (f, did) in d.items():
            if f >= since:
                src, sid = ("getFinancialAnnouncement", str(did)[4:]) if str(did).startswith("gfa:") else ("report_document", did)
                E.add(cid, comp_name[cid], None, "RESULTS_FILED", f, "confirmed", src, sid, None,
                      detail=f"{kind} period ending {p.isoformat()}")
                n_filed += 1
    # estimated: next unfiled periods per listed company with recent filings
    for cid, d in F.items():
        if status.get(cid) != "LISTED" or not d or max(f for f, _ in d.values()) < as_of - dt.timedelta(800):
            continue
        f0 = fy[cid]
        ends = []
        y = as_of.year - 1
        for yy in (y, y + 1, y + 2):
            for i in range(1, 5):
                m = (f0 + 3 * i - 1) % 12 + 1
                yr = yy + ((f0 + 3 * i - 1) // 12)
                ends.append((month_end(yr, m), f"Q{i}" if i < 4 else "Q4"))
        for p, qk in ends:
            kinds = [qk] + (["AR"] if qk == "Q4" else [])
            if qk == "Q4" and sum(1 for (k, _) in d if k == "AFS") >= 2:
                kinds.append("AFS")
            for kind in kinds:
                if (kind, p) in d:
                    continue
                est, rule, basis, method = predict(F, cid, kind, p)
                if not (as_of - dt.timedelta(45) <= est <= as_of + dt.timedelta(125)) and not (est < as_of and rule >= as_of - dt.timedelta(45)):
                    continue
                if kind == "AR" and not any(k == "AR" for (k, _) in d):
                    continue
                overdue = rule < as_of
                E.add(cid, comp_name[cid], None, "RESULTS_DUE", est, "estimated", "report_document", f"{cid}:{kind}:{p.isoformat()}", None,
                      detail=f"{kind} for period ending {p.isoformat()}", method=method, rule_deadline=rule.isoformat(),
                      status="OVERDUE" if overdue else "UPCOMING")
                n_est += 1
    # backtest: filings made in [as_of-365d, as_of]
    bt = []
    for cid, d in F.items():
        for (kind, p), (f, did) in d.items():
            if as_of - dt.timedelta(365) <= f <= as_of:
                est, rule, basis, method = predict(F, cid, kind, p)
                bt.append((cid, kind, p.isoformat(), f.isoformat(), rule.isoformat(), snap_weekday(rule).isoformat(), est.isoformat(), basis,
                           (snap_weekday(rule) - f).days, (est - f).days))
    return n_est, n_filed, bt


# --------------------------------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(ROOT / "db" / "staging_events.db"))
    ap.add_argument("--as-of")
    ap.add_argument("--since")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    stamps = []
    for f in RAW.glob("*.json"):
        try:
            stamps.append(json.loads(f.read_text()).get("_fetched_at", ""))
        except Exception:
            pass
    stamps = [s for s in stamps if s]
    if not stamps:
        sys.exit("no cached API responses in raw_api/events/ - run pipeline/fetch_events.py first")
    as_of = D(a.as_of) if a.as_of else dt.datetime.fromisoformat(max(stamps)).astimezone(COLO).date()
    since = D(a.since) if a.since else as_of - dt.timedelta(days=365)
    db = sqlite3.connect(a.db)
    db.execute("PRAGMA foreign_keys=ON")
    for t in ("company", "security", "corporate_action", "corporate_action_announcement", "announcement", "listing_event", "report_document"):
        if not db.execute("select 1 from sqlite_master where type='table' and name=?", (t,)).fetchone():
            sys.exit(f"refusing: table {t} missing in {a.db}")
    refusals = collections.Counter()
    E, agm_notices, stats, maps = build(db, as_of, since, refusals)
    feed = feed_records("getFinancialAnnouncement", "reqFinancialAnnouncemnets", "id")
    n_est, n_filed, bt = results_events(db, E, as_of, since, maps["comp_name"], feed)
    # ---- merge rows that differ only by symbol NULL vs symbol set (same company, type, date) ----------------
    by = collections.defaultdict(list)
    for k, r in E.rows.items():
        by[(r["company_id"], r["event_type"], r["event_date"])].append(k)
    merged = 0
    for g, keys in by.items():
        withsym = [k for k in keys if E.rows[k]["symbol"]]
        nosym = [k for k in keys if not E.rows[k]["symbol"]]
        if withsym and nosym and g[0] is not None and g[1] not in ("AGM", "EGM"):
            for k in nosym:
                r = E.rows.pop(k)
                for k2 in withsym:
                    for s in r["sources"]:
                        if s not in E.rows[k2]["sources"]:
                            E.rows[k2]["sources"].append(s)
                merged += 1
    # ---- checks ---------------------------------------------------------------------------------------------
    problems = []
    rows = list(E.rows.values())
    if stats["calendar_announcements"] < 100:
        problems.append("corporateCalender cache has < 100 announcements")
    if len(rows) < 1000:
        problems.append(f"only {len(rows)} events built")
    dateconf = [c for c in E.conflicts if c[0] == "date"]
    if dateconf and len(dateconf) > 0.05 * max(1, stats["corporate_action_rows"]):
        problems.append(f"{len(dateconf)} calendar/corporate_action date conflicts (> 5%)")
    for r in rows:
        if r["event_type"] == "RESULTS_DUE" and not (r["method"] and r["rule_deadline"]):
            problems.append("RESULTS_DUE without method/rule_deadline")
            break
        if r["date_kind"] == "estimated" and r["event_type"] != "RESULTS_DUE":
            problems.append("estimated kind on non-results event")
            break
    if problems:
        print("REFUSING:", *problems, sep="\n  ")
        sys.exit(1)
    counts = collections.Counter((r["event_type"], r["date_kind"]) for r in rows)
    print(f"as_of {as_of}  window since {since}  events {len(rows)}  (symbol-merge {merged}, conflicts {len(E.conflicts)})")
    for (t, k), n in sorted(counts.items()):
        print(f"  {t:28s} {k:20s} {n:5d}")
    print("refusals/drops:", dict(refusals))
    print("stats:", dict(stats))
    if a.dry_run:
        print("dry run: nothing written")
        return
    # ---- write (one transaction) ----------------------------------------------------------------------------
    db.executescript("")      # no implicit commit games
    db.isolation_level = None
    db.execute("BEGIN")
    try:
        for stmt in SCHEMA.split(";\n"):
            if stmt.strip():
                db.execute(stmt)
        for t in ("company_event_source", "company_event", "result_estimate_backtest", "event_load_meta"):
            db.execute(f"DELETE FROM {t}")
        rows.sort(key=lambda r: (r["event_date"], r["company_id"] or 0, r["event_type"], r["symbol"] or "", r["company_name"] or ""))
        for r in rows:
            src, sid, fetched = r["primary"]
            cur = db.execute(
                "INSERT INTO company_event(company_id,company_name,symbol,event_type,event_date,date_kind,amount,amount_kind,detail,"
                "method,rule_deadline,status,announced_on,source_endpoint,source_id,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (r["company_id"], r["company_name"], r["symbol"], r["event_type"], r["event_date"], r["date_kind"], r["amount"],
                 r["amount_kind"], r["detail"], r["method"], r["rule_deadline"], r["status"], r["announced_on"], src, sid, fetched))
            eid = cur.lastrowid
            for s in dict.fromkeys(r["sources"] + [(src, sid)]):
                db.execute("INSERT OR IGNORE INTO company_event_source VALUES (?,?,?)", (eid, s[0], s[1]))
        db.executemany("INSERT INTO result_estimate_backtest VALUES (?,?,?,?,?,?,?,?,?,?)", bt)
        meta = {"as_of": as_of.isoformat(), "since": since.isoformat(), "events": len(rows),
                "refusals": json.dumps(dict(refusals)), "conflicts": json.dumps([list(map(str, c)) for c in E.conflicts[:50]]),
                "event_counts": json.dumps({f"{t}|{k}": n for (t, k), n in sorted(counts.items())})}
        db.executemany("INSERT INTO event_load_meta VALUES (?,?)", meta.items())
        bad = db.execute("PRAGMA foreign_key_check").fetchall()
        if bad:
            raise RuntimeError(f"foreign key violations: {bad[:3]}")
        n = db.execute("select count(*) from company_event").fetchone()[0]
        if n != len(rows):
            raise RuntimeError("row count mismatch")
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    print(f"loaded {len(rows)} company_event rows, {db.execute('select count(*) from company_event_source').fetchone()[0]} source links, "
          f"{len(bt)} backtest rows into {a.db}")


if __name__ == "__main__":
    main()
