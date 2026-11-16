"""Reading one CSE announcement: its category, the corporate-action kinds it concerns, the values it prints, and the
evidence it gives for linking to an earlier announcement. Pure functions, no network, no database writes.

Value extraction is a port of the local loader (cse_financials scripts/load_actions.py, `values`), so a notice read here
gives the same values as main got from it. Values come only from:
  * CSE's structured detail (getAnnouncementById / getGeneralAnnouncementById / announcementById),
  * the announcement's own row in CSE's feed or corporate calendar (same announcement id and symbol),
  * labelled body text of old-format notices,
  * a reviewed reading of the attached letter (actions/reviewed/notice_readings.json).
Nothing is estimated: a missing ratio, price or date stays NULL.
"""
import datetime as dt, json, pathlib, re, unicodedata

HERE = pathlib.Path(__file__).resolve().parent
COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))
ACTIONS = ("CASH_DIVIDEND", "SCRIP_DIVIDEND", "SUB_DIVISION", "CONSOLIDATION", "CAPITALISATION", "RIGHTS_ISSUE",
           "REPURCHASE", "CAPITAL_REDUCTION", "PRIVATE_PLACEMENT")
STRUCTURAL = ("SCRIP_DIVIDEND", "SUB_DIVISION", "CONSOLIDATION", "CAPITALISATION", "RIGHTS_ISSUE")
FIELDS = ("dividend_stage", "financial_year", "per_share", "ratio_text", "shares_before", "shares_after", "shares_issued",
          "issue_price", "record_date", "ex_date", "effective_date", "payment_date")
DATE_FIELDS = ("record_date", "ex_date", "effective_date", "payment_date")
TERM_FIELDS = ("per_share", "ratio_text", "issue_price")
RANK = {"TITLE_ONLY": 0, "INFERRED": 0, "BODY_TEXT": 1, "STRUCTURED": 2, "SCAN_REVIEWED": 3}


# ---- text ---------------------------------------------------------------------------------------------------------
def clean(text):
    """Same as cse_financials scripts/normalize.clean: lowercase letters, digits and single spaces only."""
    s = unicodedata.normalize("NFKC", text or "").lower().replace("&", " and ")
    s = re.sub(r"['’‘`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\bnoncurrent\b", "non current", s)
    return " ".join(s.split())


def clean_text(s):
    return " ".join(re.sub(r"<[^>]+>", " ", s or "").replace("&nbsp;", " ").split()) or None


def day(text):
    """The date formats CSE uses (ms timestamps or text) -> ISO date, or None."""
    if text in (None, ""):
        return None
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        try:
            d = dt.datetime.fromtimestamp(text / 1000, COLO).date()
        except (OverflowError, OSError, ValueError):
            return None
        return d.isoformat() if 2000 <= d.year <= 2035 else None
    s = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", str(text).strip(), flags=re.I)
    for f in ("%d %b %Y", "%d.%b.%Y", "%d %B %Y", "%B %d %Y", "%B %d, %Y", "%d-%b-%Y", "%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d"):
        try:
            d = dt.datetime.strptime(s, f).date()
            return d.isoformat() if 2000 <= d.year <= 2035 else None
        except ValueError:
            pass
    return None


def number(v):
    if v in (None, "", "N/A"):
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except ValueError:
        return None


def positive(v):
    v = number(v)
    return v if v and v > 0 else None


def ratio(text):
    """(new, per existing) from the forms CSE uses; None when not clean (port of load_actions.ratio)."""
    t = str(text or "").strip()
    if re.fullmatch(r"\d+(?:\.\d+)?", t):
        v = float(t)
        if v in (0, 1):
            return None
        return (v, 1.0) if v < 1 else (1.0, v)
    nums = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", t)]
    if len(nums) == 2 and (":" in t or re.search(r"for|\beach\b|\bevery\b|\binto\b|\bto\b", t, re.I)):
        return (nums[0], nums[1]) if nums[0] > 0 and nums[1] > 0 else None
    if len(nums) == 1 and nums[0] < 1 and re.search(r"\bevery one\b|\beach\b", t, re.I):
        return (nums[0], 1.0)
    if len(nums) == 1 and re.match(r"\s*(in the proportion of\s+)?one\b", t, re.I) and re.search(r"\bevery\b|\beach\b", t, re.I):
        return (1.0, nums[0]) if nums[0] > 0 else None
    return None


MONTHS = {m: i + 1 for i, m in enumerate("january february march april may june july august september october november december".split())}
MONTHS.update({m[:3]: i for m, i in list(MONTHS.items())})
MONTHS["sept"] = 9
_M = "|".join(sorted(MONTHS, key=len, reverse=True))
DATE_RE = re.compile(
    rf"(?<![\w])(?:(?P<d1>\d{{1,2}})\s*(?:st|nd|rd|th)?\s*(?:of\s+)?(?P<m1>{_M})\.?\s*,?\s*(?P<y1>\d{{4}})"
    rf"|(?P<m2>{_M})\.?\s+(?P<d2>\d{{1,2}})\s*(?:st|nd|rd|th)?\s*,?\s*(?P<y2>\d{{4}})"
    rf"|(?P<d3>\d{{1,2}})[./-](?P<mo3>\d{{1,2}})[./-](?P<y3>\d{{4}}))(?![\w])", re.I)
REFERENCE_RE = re.compile(r"further\s+to|with\s+reference\s+to|in\s+continuation|pursuant\s+to|addendum|"
                          r"announcement\s+(?:dated|made|of|on|published)|dated|initial\s+announcement|previous\s+announcement", re.I)


def _to_date(m):
    try:
        if m.group("y1"):
            return dt.date(int(m.group("y1")), MONTHS[m.group("m1").lower()], int(m.group("d1")))
        if m.group("y2"):
            return dt.date(int(m.group("y2")), MONTHS[m.group("m2").lower()], int(m.group("d2")))
        return dt.date(int(m.group("y3")), int(m.group("mo3")), int(m.group("d3")))
    except (ValueError, KeyError):
        return None


def referenced_dates(text):
    """Dates the text names as an earlier announcement's date: a date within 80 characters after 'further to',
    'announcement dated', 'addendum', ... Returns a set of ISO dates."""
    t = " ".join((text or "").split())
    out = set()
    for m in REFERENCE_RE.finditer(t):
        for dm in DATE_RE.finditer(t[m.end():m.end() + 80]):
            d = _to_date(dm)
            if d:
                out.add(d.isoformat())
            break
    return out


# ---- categories -----------------------------------------------------------------------------------------------------
def load_kind_map(path=None):
    p = pathlib.Path(path or HERE.parent / "kind_map.json")
    return {k: set(v) for k, v in json.loads(p.read_text())["map"].items()}


RULES = [(r"\bscrip\b", "SCRIP_DIVIDEND"), (r"\bsub ?division\b|\bsplit\b", "SUB_DIVISION"), (r"\bconsolidation\b", "CONSOLIDATION"),
         (r"\bcapitali[sz]ation\b|\bbonus issue\b", "CAPITALISATION"), (r"\brights? (?:issue|isssue)\b", "RIGHTS_ISSUE"),
         (r"\brepurchase\b|\bbuy ?back\b", "REPURCHASE"), (r"\breduction of (?:stated )?capital\b", "CAPITAL_REDUCTION"),
         (r"\bprivate placement\b", "PRIVATE_PLACEMENT")]
LISTING_CATEGORIES = ("notification on the listing of shares", "notification on listing of shares")
NOT_ACTION = re.compile(r"\bdebenture|\bwarrant|\bunit trust|\bcommercial paper|\bbond\b", re.I)


def kinds_for(category_norm, kind_map, text=""):
    """(kinds, how): the action kinds of a category. how = 'map' (local kind map), 'rule' (pattern on an unseen
    category), 'listing' (a plain listing notice classified by its own text) or None (not an action notice)."""
    if category_norm in kind_map:
        return set(kind_map[category_norm]), "map"
    if category_norm in LISTING_CATEGORIES:
        k = {kind for rx, kind in RULES if re.search(rx, clean(text))}
        return (k, "listing") if k else (set(), None)
    if NOT_ACTION.search(category_norm):
        return set(), None
    k = {kind for rx, kind in RULES if re.search(rx, category_norm)}
    if not k and re.search(r"\bdividend\b", category_norm):
        k = {"CASH_DIVIDEND"}
    return (k, "rule") if k else (set(), None)


FOLLOW_UP = [("postponed", r"postpon|deferr|resched|suspension of the|withdraw|cancel"),
             ("amended", r"amend|ammend|revis|addendum|change of date|correction|errata"),
             ("dates", r"\bdates?\b"),
             ("listing", r"listing of (?:the )?(?:new )?shares|notification on the listing"),
             ("subscription", r"subscription|subscribed|provisional (?:letters? of )?allotment|basis of allotment|allotment of (?:the )?(?:new |rights )?shares"),
             ("further", r"further (?:to|announcement)|in continuation|subsequent|offer document")]
DATED_REMARK = re.compile(r"(?:^|\s)dates?$|\bdates of\b|\bwith dates\b")


def follow_up_type(category_norm, remarks=""):
    """What kind of follow-up a notice is, or None for an initial (proposal / first) notice. The category is read
    first, then the remarks ('Further to the announcement dated ...', 'Level of Subscription', ...)."""
    if re.search(r"to be notified", category_norm):
        return None
    for name, rx in FOLLOW_UP:
        if re.search(rx, category_norm):
            return name
    r = clean(remarks)
    for name, rx in FOLLOW_UP:
        if name != "dates" and re.search(rx, r):
            return name
    if DATED_REMARK.search(r):         # 'Repurchase of Shares - Dates', 'Rights Issue 2026 Dates', 'Dates of Rights Issue'
        return "dates"
    return None


def prints_dates(base):
    """A general (free-form) notice that carries event dates in its structured fields acts as a dates notice."""
    return any((base or {}).get(k) for k in ("xr", "xd", "xc", "recordDate", "allotment", "tradingCommencement", "subDivisionBasedOnShareholdingsAsAt"))


def names_both_classes(*texts):
    t = clean(" ".join(x or "" for x in texts))
    return bool(re.search(r"\bvoting\b.{0,40}\bnon voting\b|\bnon voting\b.{0,40}\bvoting\b|\bboth classes\b", t))


# ---- detail identity --------------------------------------------------------------------------------------------------
def check_detail(fmt, aid, d, ticker=None, company_id=None):
    """(status, base, dtype) for a cached detail response. A wrong-system id silently returns another company's record,
    so a NEW detail must carry the same id (when present) and the expected ticker; an OLD detail the same company id.
    status: FETCHED, NO_DETAIL, COMPANY_MISMATCH."""
    if not d:
        return "NO_DETAIL", None, None
    if fmt == "NEW":
        b = d.get("reqBaseAnnouncement")
        if not b:
            return "NO_DETAIL", None, None
        sym = (b.get("symbol") or "").strip().split(".")[0]
        if b.get("id") not in (None, aid) or (ticker and sym and sym != ticker) or (ticker and not sym):
            return "COMPANY_MISMATCH", None, None
        return "FETCHED", b, b.get("dType") or "General"     # getGeneralAnnouncementById records carry no dType
    info = d.get("infoAnnouncement")
    if not info:
        return "NO_DETAIL", None, None
    if company_id is None or info.get("securityId") != company_id:
        return "COMPANY_MISMATCH", None, None
    return "FETCHED", info, None


def documents(d):
    """PDF urls named by the detail (never guessed)."""
    out = []
    for x in (d or {}).get("reqAnnouncementDocs") or []:
        if x.get("fileUrl"):
            out.append((x.get("baseUrl") or "https://cdn.cse.lk/").rstrip("/") + "/" + x["fileUrl"].lstrip("/"))
    info = (d or {}).get("infoAnnouncement") or {}
    if info.get("filePath"):
        out.append("https://cdn.cse.lk/cmt/" + info["filePath"].lstrip("/"))
    return out


# ---- values (port of load_actions.values) -------------------------------------------------------------------------
def stage(b):
    if b.get("firstAndFinal"):
        return "FIRST_AND_FINAL"
    if b.get("finalDividend") or b.get("typeFinalInt"):
        return "FINAL"
    if any(b.get(k) for k in ("typeFirstInt", "typeSecondInt", "typeThirdInt", "typeFourthInt")):
        return "INTERIM"
    if b.get("typeOther"):
        return "OTHER"
    return None


def values(kind, base=None, dtype=None, z=None, body=None):
    """Rows of values per share class printed by one notice for one kind: [{cls, level, <field>: value, ...}].
    base: structured detail (NEW) or None; dtype: its dType ('General' for general notices); z: the notice's own row
    in CSE's feed/calendar (or {}); body: old-format notice text."""
    b, z, out = base or {}, z or {}, []
    common = dict(record_date=day(b.get("recordDate")) or day(z.get("recordDate")))

    def row(cls, **kw):
        a = dict(common)
        a.update(kw)
        a["cls"] = cls
        out.append({k: v for k, v in a.items() if v is not None})

    if base and dtype != "General" and body is None:
        if kind == "CASH_DIVIDEND":
            common.update(financial_year=clean_text(b.get("financialYear")), dividend_stage=stage(b),
                          ex_date=day(b.get("xd")) or day(z.get("xd")), payment_date=day(b.get("payment")) or day(z.get("paymentDate")))
            v, n = positive(b.get("votingDivPerShare")) or positive(b.get("divPerShare")), positive(b.get("nonVotingDivPerShare"))
            if v:
                row("VOTING", per_share=v, level="STRUCTURED")
            if n:
                row("NON_VOTING", per_share=n, level="STRUCTURED")
            if not v and not n:
                row("VOTING", level="TITLE_ONLY")
        elif kind == "SCRIP_DIVIDEND":
            common.update(financial_year=clean_text(b.get("financialYear")), ex_date=day(b.get("xd")) or day(z.get("xd")),
                          payment_date=day(b.get("payment")) or day(z.get("paymentDate")))
            if b.get("votingPropotion") or b.get("votingNumSharesIssued"):
                row("VOTING", ratio_text=clean_text(b.get("votingPropotion")), shares_issued=number(b.get("votingNumSharesIssued")),
                    issue_price=number(b.get("votingConsideration")), level="STRUCTURED", _total=number(b.get("reservesToBeCapitalized")))
            if b.get("nonVotingPropotion") or b.get("nonVotingNumSharesIssued"):
                row("NON_VOTING", ratio_text=clean_text(b.get("nonVotingPropotion")), shares_issued=number(b.get("nonVotingNumSharesIssued")),
                    issue_price=number(b.get("nonVotingConsideration")), level="STRUCTURED")
            if not out:
                row("VOTING", level="TITLE_ONLY")
        elif kind in ("SUB_DIVISION", "CONSOLIDATION"):
            if b.get("votingProportion") or b.get("votingExistingNumOfShares"):
                row("VOTING", ratio_text=clean_text(b.get("votingProportion")), shares_before=number(b.get("votingExistingNumOfShares")),
                    shares_after=number(b.get("votingResultingNumOfShares")), level="STRUCTURED")
            np_ = clean_text(b.get("nonVotingProportion"))
            if (np_ and np_.upper() != "N/A") or b.get("nonVotingExistingNumOfShares"):
                row("NON_VOTING", ratio_text=np_, shares_before=number(b.get("nonVotingExistingNumOfShares")),
                    shares_after=number(b.get("nonVotingResultingNumOfShares")), level="STRUCTURED")
            if not out:
                row("VOTING", level="TITLE_ONLY")
        elif kind == "CAPITALISATION":
            common.update(ex_date=day(b.get("xc")) or day(z.get("xc")), effective_date=day(b.get("allotment")) or day(z.get("allotment")))
            if b.get("votingProportion") or b.get("votingNumSharesIssued"):
                row("VOTING", ratio_text=clean_text(b.get("votingProportion")), shares_issued=number(b.get("votingNumSharesIssued")), level="STRUCTURED")
            if b.get("nonVotingProportion") or b.get("nonVotingNumSharesIssued"):
                row("NON_VOTING", ratio_text=clean_text(b.get("nonVotingProportion")), shares_issued=number(b.get("nonVotingNumSharesIssued")), level="STRUCTURED")
            if not out:
                row("VOTING", level="TITLE_ONLY")
        elif kind == "RIGHTS_ISSUE":
            common.update(ex_date=day(b.get("xr")) or day(z.get("xr")),
                          effective_date=day(b.get("tradingCommencement")) or day(z.get("tradingCommencement")))
            if b.get("votingShrsPropToBeIssued") or b.get("numOfVotingShrsIssued"):
                row("VOTING", ratio_text=clean_text(b.get("votingShrsPropToBeIssued")), shares_issued=number(b.get("numOfVotingShrsIssued")),
                    issue_price=number(b.get("votingShareConsideration")), level="STRUCTURED")
            if b.get("nonVotingShrsPropToBeIssued") or b.get("numOfNonVotingShrsIssued"):
                row("NON_VOTING", ratio_text=clean_text(b.get("nonVotingShrsPropToBeIssued")), shares_issued=number(b.get("numOfNonVotingShrsIssued")),
                    issue_price=number(b.get("nonVotingShareConsideration")), level="STRUCTURED")
            if not out:
                row("VOTING", level="TITLE_ONLY")
        else:
            row("VOTING", level="TITLE_ONLY")
    elif base and body is None:   # general announcement: dates and free text
        ex_field = {"RIGHTS_ISSUE": "xr", "CAPITALISATION": "xc", "CASH_DIVIDEND": "xd", "SCRIP_DIVIDEND": "xd"}.get(kind)
        common.update(effective_date=day(b.get("tradingCommencement")) or day(z.get("tradingCommencement")) or day(b.get("allotment")),
                      record_date=day(b.get("subDivisionBasedOnShareholdingsAsAt")) or common.get("record_date"),
                      ex_date=(day(b.get(ex_field)) or day(z.get(ex_field))) if ex_field else None)
        vp = clean_text(b.get("votingProportion"))
        has_values = vp or common.get("ex_date") or common.get("effective_date")
        row("VOTING", ratio_text=vp, level="STRUCTURED" if has_values else "TITLE_ONLY")
        np_ = clean_text(b.get("nonVotingProportion"))
        if np_ and np_.upper() != "N/A":
            row("NON_VOTING", ratio_text=np_, level="STRUCTURED")
    elif body:   # 2012-2019 old format: labelled text
        t = body
        grab = lambda label: (re.search(label + r"\s*:?\s*-?\s*(\d{1,2}\s*[./ -]\s*[A-Za-z]{3}\s*[./ -]\s*\d{4})", t, re.I) or [None, None])[1]
        fix = lambda s: day(" ".join(re.sub(r"[./-]", " ", s).split())) if s else None
        fy = re.search(r"Financial Year\s*:?\s*-?\s*([0-9/\-]{4,9})", t, re.I)
        if kind == "CASH_DIVIDEND":
            m = re.search(r"(?:Dividend per Share|Rate of Dividend)\s*:?\s*-?\s*Rs\.?\s*([\d.,]+)", t, re.I)
            head = t[:m.end() + 120] if m else ""
            st = "FIRST_AND_FINAL" if re.search(r"first\s*(and|&)\s*final", head, re.I) else "INTERIM" if re.search(r"interim", head, re.I) \
                else "FINAL" if re.search(r"final", head, re.I) else None
            per = positive(m.group(1).rstrip(".,")) if m else None
            shared = dict(per_share=per, dividend_stage=st, financial_year=fy.group(1) if fy else None, ex_date=fix(grab("XD")),
                          payment_date=fix(grab("Payment")), level="BODY_TEXT" if per else "TITLE_ONLY")
            row("VOTING", **shared)
            if per and re.search(r"\(\s*voting\s*(and|&)\s*non", head, re.I):
                row("NON_VOTING", **shared)
        else:
            ex = fix(grab("XD")) if kind == "SCRIP_DIVIDEND" else fix(grab("XR")) if kind == "RIGHTS_ISSUE" else fix(grab("XC")) if kind == "CAPITALISATION" else None
            eff = fix(grab("Date of commencement of trading"))
            r = re.search(r"(\d+\s*(?:share\S*\s*)?(?:for\s*(?:every\s*)?|:)\s*\d+(?:\.\d+)?)", t, re.I)
            row("VOTING", ratio_text=r.group(1) if r else None, ex_date=ex, effective_date=eff,
                level="BODY_TEXT" if (r or ex or eff) else "TITLE_ONLY")
    else:
        # no detail: only the dates CSE prints on the notice's own feed/calendar row
        ex_field = {"RIGHTS_ISSUE": "xr", "CAPITALISATION": "xc", "CASH_DIVIDEND": "xd", "SCRIP_DIVIDEND": "xd"}.get(kind)
        ex = day(z.get(ex_field)) if ex_field else None
        eff = day(z.get("tradingCommencement")) if kind == "RIGHTS_ISSUE" else None
        pay = day(z.get("paymentDate")) if kind == "CASH_DIVIDEND" else None
        row("VOTING", ex_date=ex, effective_date=eff, payment_date=pay, level="STRUCTURED" if (ex or eff) else "TITLE_ONLY")
    return out


def consideration_is_total(a):
    """CSE trap: the '...ShareConsideration' field sometimes holds the TOTAL consideration, not the price per share.
    True when the printed price would make the issue worth more than Rs 1 trillion, or the price is above Rs 100,000."""
    p, n = a.get("issue_price"), a.get("shares_issued")
    return bool(p and (p > 100000 or (n and p * n > 1e12)))


def price_inconsistent(a):
    """Scrip dividends: the 'consideration' field is sometimes the price per new share (KZOO 2026: 8.70 x 4,553,230
    = the Rs 39.6 Mn capitalised) and sometimes the dividend per existing share (MFPE 2026: 0.10). When the notice also
    prints the amount capitalised and price x new shares is not within a factor of 2 of it, the price is not a price.
    (A miss of 10-20% is normal: the share count is an 'up to' figure and fractions are paid in cash - RAL 2026.)"""
    p, n, tot = a.get("issue_price"), a.get("shares_issued"), a.get("_total")
    return bool(p and n and tot and not 0.5 <= p * n / tot <= 2)


def terms_disagree(kind, a, b):
    """True when both sides print a term and the terms differ (per share amount, ratio, or price)."""
    if kind == "CASH_DIVIDEND":
        x, y = a.get("per_share"), b.get("per_share")
        return bool(x and y and abs(float(x) - float(y)) > 0.0051)
    ra, rb = ratio(a.get("ratio_text")), ratio(b.get("ratio_text"))
    if ra and rb and abs(ra[0] / ra[1] - rb[0] / rb[1]) > 1e-9:
        return True
    pa, pb = number(a.get("issue_price")), number(b.get("issue_price"))
    if kind == "RIGHTS_ISSUE" and pa and pb and abs(pa - pb) > 1e-9:
        return True
    return False


def same_terms(kind, a, b):
    """A restatement: at least two of ratio, price and number of new shares printed on both sides, and all equal.
    Only for share issues (rights, scrip, bonus); cash dividends of equal amounts recur and prove nothing."""
    if kind not in ("RIGHTS_ISSUE", "SCRIP_DIVIDEND", "CAPITALISATION"):
        return False
    n = 0
    ra, rb = ratio(a.get("ratio_text")), ratio(b.get("ratio_text"))
    if ra and rb:
        if abs(ra[0] / ra[1] - rb[0] / rb[1]) > 1e-9:
            return False
        n += 1
    for f in ("issue_price", "shares_issued"):
        x, y = number(a.get(f)), number(b.get(f))
        if x and y:
            if abs(x - y) > 1e-9:
                return False
            n += 1
    return n >= 2


def is_dated(a):
    return bool(a.get("ex_date") or a.get("effective_date") or a.get("record_date"))
