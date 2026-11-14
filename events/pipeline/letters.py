"""Rights issue letters: read the last date of renunciation and the last date of acceptance and payment, which exist
only in the PDF letter attached to a 'RIGHTS ISSUE (DATES)' announcement (no JSON field carries them).

Usage:
  python3 -I letters.py                  # read letters of rights announcements not read yet (downloads their PDFs)
  python3 -I letters.py needs-reading    # list letters the automatic reader refused (for a person / agent)
  python3 -I letters.py check-reviewed   # validate reviewed/letter_readings.json against the calendar

Accuracy over coverage. A letter is READ only when all of these hold:
  * the PDF has a real text layer and each label ("last date of renunciation", "last date of acceptance and payment")
    is followed, before the next label, by exactly one cleanly written date (same date wherever the label repeats);
  * the letter's own trading-commencement / record / XR / dispatch dates, read the same way, match the calendar
    (at least one match, no mismatch) - this proves the text layer is not OCR junk and the layout was read in order;
  * order and spacing: trading start <= renunciation <= acceptance, renunciation >= record date, acceptance within
    35 days of trading start and within 21 days of renunciation.
Anything else is NEEDS_READING with the reason; nothing is guessed. A person or agent reads those letters from the page
image and records them in events/reviewed/letter_readings.json (see the doc), which overrides the automatic reading.
Readings are cached in raw_api/events/letters/<announcementId>.json; PDFs in raw_api/events/letters/pdf/ (not kept
in Turso). PDF URLs come from the announcement detail (baseUrl + fileUrl); nothing is guessed.
"""
import datetime as dt, json, os, pathlib, re, sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import fetch_events as F  # noqa: E402

ROOT = F.ROOT
LETTERS = ROOT / "raw_api" / "events" / "letters"
REVIEWED = pathlib.Path(__file__).resolve().parent.parent / "reviewed" / "letter_readings.json"
COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))
CATEGORY = "RIGHTS ISSUE (DATES)"

MONTHS = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6, "july": 7, "august": 8,
          "september": 9, "october": 10, "november": 11, "december": 12,
          "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12}
_M = "|".join(sorted(MONTHS, key=len, reverse=True))
DATE_RE = re.compile(
    rf"(?<![\w])(?:(?P<d1>\d{{1,2}})\s*(?:st|nd|rd|th)?\s*(?:of\s+)?(?P<m1>{_M})\.?\s*,?\s*(?P<y1>\d{{4}})"
    rf"|(?P<m2>{_M})\.?\s+(?P<d2>\d{{1,2}})\s*(?:st|nd|rd|th)?\s*,?\s*(?P<y2>\d{{4}})"
    rf"|(?P<d3>\d{{1,2}})[./-](?P<mo3>\d{{1,2}})[./-](?P<y3>\d{{4}}))(?![\w])", re.I)

TARGET = {"renunciation": re.compile(r"last\s+date\s+(?:of|for)\s+(?:the\s+)?renunciation", re.I),
          "acceptance": re.compile(r"last\s+date\s+(?:of|for)\s+(?:the\s+)?acceptance\s*(?:and|&|for)\s*payments?", re.I)}
# labels used only to validate the letter against the calendar (key = calendar field)
CHECK = {"tradingCommencement": re.compile(r"commencement\s+of\s+trading|trad(?:e|ing)\s+commencement|trading\s+(?:of\s+(?:the\s+)?rights\s+)?(?:will\s+)?commence", re.I),
         "recordDate": re.compile(r"record\s+date|date\s+of\s+record", re.I),
         "xr": re.compile(r"\bxr\b|ex[-\s]?rights?\s+date|quoted\s+ex[-\s]?rights?", re.I),
         "allotment": re.compile(r"dispatch\s+of\s+(?:the\s+)?provisional\s+letters?", re.I)}
# any other "last date of/for ..." label ends the window too (e.g. last date of trading of rights)
OTHER_LABELS = re.compile(r"last\s+date\s+(?:of|for)|date\s+of\s+(?:dispatch|commencement)", re.I)
WINDOW = 160


def norm(text):
    return " ".join((text or "").replace(" ", " ").split())


def to_date(m):
    try:
        if m.group("y1"):
            return dt.date(int(m.group("y1")), MONTHS[m.group("m1").lower()], int(m.group("d1")))
        if m.group("y2"):
            return dt.date(int(m.group("y2")), MONTHS[m.group("m2").lower()], int(m.group("d2")))
        return dt.date(int(m.group("y3")), int(m.group("mo3")), int(m.group("d3")))
    except (ValueError, KeyError):
        return None


def label_dates(text, label_re):
    """Every date that directly follows an occurrence of the label: the first date inside the next WINDOW characters,
    provided no other label starts before it. Returns (set of dates, number of label occurrences)."""
    labels = [m.start() for r in list(TARGET.values()) + list(CHECK.values()) + [OTHER_LABELS] for m in r.finditer(text)]
    out, n = set(), 0
    for m in label_re.finditer(text):
        n += 1
        end = m.end()
        nxt = min([p for p in labels if p > m.start() + 1] + [len(text)])
        win = text[end:min(end + WINDOW, nxt)]
        dm = DATE_RE.search(win)
        if dm:
            d = to_date(dm)
            if d:
                out.add(d)
    return out, n


def read_text(text, cal):
    """cal: the calendar row (ms timestamps). Returns dict(status, renunciation, acceptance, reason, checks)."""
    t = norm(text)
    if len(t) < 200:
        return dict(status="NEEDS_READING", reason="no usable text layer (scanned letter)")
    got = {}
    for k, r in TARGET.items():
        ds, n = label_dates(t, r)
        if n == 0:
            return dict(status="NEEDS_READING", reason=f"label for {k} not found in the text")
        if len(ds) != 1:
            return dict(status="NEEDS_READING", reason=f"{k}: {len(ds)} clean dates after the label ({sorted(map(str, ds))})")
        got[k] = ds.pop()
    checks, bad = [], []
    for fld, r in CHECK.items():
        cv = cal.get(fld)
        if not cv:
            continue
        cd = dt.datetime.fromtimestamp(cv / 1000, COLO).date()
        ds, _ = label_dates(t, r)
        if len(ds) == 1:
            d = next(iter(ds))
            (checks if d == cd else bad).append(f"{fld} letter {d} calendar {cd}")
    if bad:
        return dict(status="NEEDS_READING", reason="letter disagrees with the calendar: " + "; ".join(bad), **{k: str(v) for k, v in got.items()})
    if not checks:
        return dict(status="NEEDS_READING", reason="no calendar date could be confirmed in the letter text (text layer unverified)",
                    **{k: str(v) for k, v in got.items()})
    prob = order_problems(got["renunciation"], got["acceptance"], cal)
    if prob:
        return dict(status="NEEDS_READING", reason=prob, **{k: str(v) for k, v in got.items()})
    return dict(status="READ", renunciation=got["renunciation"].isoformat(), acceptance=got["acceptance"].isoformat(),
                checks=checks)


def order_problems(ren, acc, cal):
    ms = lambda k: dt.datetime.fromtimestamp(cal[k] / 1000, COLO).date() if cal.get(k) else None
    tc, rec = ms("tradingCommencement"), ms("recordDate")
    if acc < ren:
        return f"acceptance {acc} before renunciation {ren}"
    if (acc - ren).days > 21:
        return f"acceptance {acc} more than 21 days after renunciation {ren}"
    if tc and ren < tc:
        return f"renunciation {ren} before trading start {tc}"
    if tc and (acc - tc).days > 35:
        return f"acceptance {acc} more than 35 days after trading start {tc}"
    if rec and ren < rec:
        return f"renunciation {ren} before record date {rec}"
    return None


def pdf_text(path):
    try:
        import pymupdf as fitz
    except ImportError:
        import fitz
    with fitz.open(path) as d:
        return "\n".join(p.get_text() for p in d)


# ---- calendar / details ------------------------------------------------------------------------------------
def rights_announcements():
    """Current calendar rows of category RIGHTS ISSUE (DATES), newest fetch winning, by announcementId."""
    cal = {}
    for f in sorted(F.CACHE.glob("corporateCalender__*.json")):
        data, ts = F.read_cache(f)
        for r in (data or {}).get("approvedCoopCalAnnouncements") or []:
            if r.get("announcementCategory") == CATEGORY and (r["announcementId"] not in cal or (ts or "") >= cal[r["announcementId"]][1]):
                cal[r["announcementId"]] = (r, ts or "")
    return {a: r for a, (r, _) in cal.items()}


def detail(aid, symbol=None):
    return F.detail_record(aid, symbol)


def docs(det):
    out = []
    for d in (det or {}).get("reqAnnouncementDocs") or []:
        if d.get("fileUrl") and (d.get("contentType") or "").endswith("pdf"):
            out.append((d.get("id"), (d.get("baseUrl") or "https://cdn.cse.lk/").rstrip("/") + "/" + d["fileUrl"].lstrip("/")))
    return out


def reading_path(aid):
    return LETTERS / f"{aid}.json"


def read_one(aid, cal_row):
    det = detail(aid, cal_row.get("symbol"))
    if det is None:
        return dict(status="NO_DETAIL", reason="announcement detail not fetched yet")
    ds = docs(det)
    if not ds:
        return dict(status="NO_DOCUMENT", reason="no PDF attached to the announcement")
    results, texts = [], []
    for did, url in ds:
        pdf = LETTERS / "pdf" / f"{aid}_{did}.pdf"
        F.fetch_file(url, pdf)
        try:
            txt = pdf_text(pdf)
        except Exception as e:
            results.append(dict(status="NEEDS_READING", reason=f"PDF unreadable: {e!r}"))
            continue
        texts.append(norm(txt)[:4000])
        results.append(read_text(txt, cal_row))
    ok = [r for r in results if r["status"] == "READ"]
    out = dict(results[0]) if not ok else dict(ok[0])
    if ok and len({(r["renunciation"], r["acceptance"]) for r in ok}) > 1:
        out = dict(status="NEEDS_READING", reason="the attached documents disagree")
    out.update(documents=[u for _, u in ds], text_excerpt=texts[0] if texts else None)
    return out


def write_reading(aid, rec, cal_row):
    LETTERS.mkdir(parents=True, exist_ok=True)
    rec = dict(rec, announcementId=aid, symbol=cal_row.get("symbol"), company=cal_row.get("company"),
               read_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"))
    F.write_json(reading_path(aid), rec)


def process(max_n=40, since=None):
    """Read every rights letter that has no reading yet (or whose detail was missing before). Returns counts."""
    since = since or (dt.date.today() - dt.timedelta(days=400))
    counts = {}
    n = 0
    for aid, row in sorted(rights_announcements().items(), reverse=True):
        p = reading_path(aid)
        if p.exists() and json.loads(p.read_text()).get("status") != "NO_DETAIL":
            continue
        tc = row.get("tradingCommencement") or row.get("xr") or row.get("recordDate")
        if tc and dt.datetime.fromtimestamp(tc / 1000, COLO).date() < since:
            continue
        if n >= max_n:
            break
        rec = read_one(aid, row)
        write_reading(aid, rec, row)
        counts[rec["status"]] = counts.get(rec["status"], 0) + 1
        n += 1
        print(aid, row.get("symbol"), rec["status"], rec.get("reason") or f"{rec.get('renunciation')} / {rec.get('acceptance')}", flush=True)
    return counts


# ---- reviewed readings (by a person / agent from the page image) --------------------------------------------
def load_reviewed(path=None):
    p = pathlib.Path(path or REVIEWED)
    return {int(k): v for k, v in json.loads(p.read_text()).items() if not k.startswith("_")} if p.exists() else {}


def letter_dates(aid, raw=None, reviewed=None):
    """(renunciation, acceptance, source) for the loader: a reviewed reading first, then an automatic READ, else None."""
    rv = (reviewed if reviewed is not None else load_reviewed()).get(int(aid))
    if rv and rv.get("renunciation") and rv.get("acceptance"):
        return dt.date.fromisoformat(rv["renunciation"]), dt.date.fromisoformat(rv["acceptance"]), "rightsLetterReviewed"
    p = (pathlib.Path(raw) / "letters" if raw else LETTERS) / f"{aid}.json"
    if p.exists():
        r = json.loads(p.read_text())
        if r.get("status") == "READ":
            return dt.date.fromisoformat(r["renunciation"]), dt.date.fromisoformat(r["acceptance"]), "rightsLetter"
    return None


def needs_reading():
    rv = load_reviewed()
    out = []
    for p in sorted(LETTERS.glob("*.json")):
        r = json.loads(p.read_text())
        if r.get("status") in ("NEEDS_READING",) and int(r["announcementId"]) not in rv:
            out.append(r)
    return out


def check_reviewed():
    cal = rights_announcements()
    problems = []
    for aid, v in load_reviewed().items():
        try:
            ren, acc = dt.date.fromisoformat(v["renunciation"]), dt.date.fromisoformat(v["acceptance"])
        except Exception as e:
            problems.append(f"{aid}: bad dates ({e})")
            continue
        if not v.get("read_from"):
            problems.append(f"{aid}: read_from (document + page) missing")
        if aid not in cal:
            problems.append(f"{aid}: not a {CATEGORY} announcement in the calendar cache")
            continue
        p = order_problems(ren, acc, cal[aid])
        if p:
            problems.append(f"{aid}: {p}")
    return problems


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "read"
    if cmd == "needs-reading":
        for r in needs_reading():
            print(json.dumps({k: r.get(k) for k in ("announcementId", "symbol", "company", "reason", "documents", "renunciation", "acceptance")}))
    elif cmd == "check-reviewed":
        probs = check_reviewed()
        print("\n".join(probs) or "reviewed readings OK")
        sys.exit(1 if probs else 0)
    else:
        try:
            import pymupdf  # noqa: F401
        except ImportError:
            print("pymupdf not installed: letters skipped", file=sys.stderr)
            return
        try:
            print(json.dumps(process()))
        except F.Stop as e:
            print("STOP:", e, file=sys.stderr)
            sys.exit(2)


if __name__ == "__main__":
    main()
