"""Fetch the CSE event-calendar sources into raw_api/events/ (cached; a cached URL+params is never refetched unless the
caller asks for a refresh).

Usage:
  python -I pipeline/fetch_events.py --daily            # incremental daily refresh (what refresh_events.py runs)
  python -I pipeline/fetch_events.py [--months-back 12] [--months-ahead 3] [--refresh-current]   # initial/backfill build

One request at a time, >= 3 s + jitter between requests, browser UA, Origin/Referer https://www.cse.lk/.
Stops (exit 2) on HTTP 429, or 403 / WAF page; a refresh writes the new response only after it was read in full, so a
stopped run leaves the previous cache file in place.

--daily (incremental) fetches only what can have changed since the last run:
  * corporateCalender for the current year (plus the previous year in January): 1-2 requests. The endpoint ignores
    `month` and returns every announcement created in `year` with its current dates, so a moved or withdrawn
    announcement shows up here. The file is replaced; the replaced version is kept under raw_api/events/history/.
  * getAgmEgmEvents for the current month and the next 3 (4 requests, replaced the same way; older months stay cached).
  * Rolling feeds that keep only the newest records - approvedAnnouncement (~53 announcements, ~4 days) and
    getFinancialAnnouncement (~80 filings, ~7 days) - are MERGED into one dated snapshot per day
    (`<endpoint>__day-YYYY-MM-DD.json`), so records survive after they roll off the feed. If today's feed does not
    overlap the newest earlier snapshot, records may have been missed: a warning is printed and written to
    raw_api/events/feed_gaps.json (see docs/EVENTS_DAILY_REFRESH.md).
  * upcomingListings (P/I/S) and ruleBooks LR: only when the cached copy is older than 7 days.
Announcement details are fetched by fetch_event_details.py, which only requests ids not already cached.
"""
import argparse, datetime as dt, json, os, pathlib, random, shutil, sys, time, urllib.error, urllib.parse, urllib.request

ROOT = pathlib.Path(os.environ.get("EVENTS_ROOT") or pathlib.Path(__file__).resolve().parent.parent / "work")  # data dir: db/, raw_api/, logs/
CACHE = ROOT / "raw_api" / "events"
HISTORY = CACHE / "history"
GAPS = CACHE / "feed_gaps.json"
BASE = "https://www.cse.lk/api/"
H = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
     "Origin": "https://www.cse.lk", "Referer": "https://www.cse.lk/", "Accept": "application/json"}
COLO = dt.timezone(dt.timedelta(hours=5, minutes=30))
# rolling feeds: endpoint -> (list field inside data, id field)
FEEDS = {"approvedAnnouncement": ("approvedAnnouncements", "announcementId"),
         "getFinancialAnnouncement": ("reqFinancialAnnouncemnets", "id")}
STATIC_MAX_AGE_DAYS = 7
_last = [0.0]
STATS = {"requests": 0}


class Stop(Exception):
    pass


def cache_path(endpoint, **params):
    key = endpoint.replace("/", "__") + "".join(f"__{k}-{v}" for k, v in sorted(params.items()))
    return CACHE / (key + ".json")


def _request(endpoint, verb, params):
    wait = 3.0 + random.uniform(0.2, 1.5) - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    if verb == "POST":
        req = urllib.request.Request(BASE + endpoint, data=urllib.parse.urlencode(params).encode(),
                                     headers={**H, "Content-Type": "application/x-www-form-urlencoded"})
    else:
        req = urllib.request.Request(BASE + endpoint + ("?" + urllib.parse.urlencode(params) if params else ""), headers=H)
    try:
        raw = urllib.request.urlopen(req, timeout=40).read()
    except urllib.error.HTTPError as e:
        _last[0] = time.time()
        if e.code in (429, 403):
            raise Stop(f"HTTP {e.code} on {endpoint} {params}")
        raise
    _last[0] = time.time()
    STATS["requests"] += 1
    txt = raw.decode("utf-8", "replace")
    if txt.lstrip().lower().startswith(("<!doctype", "<html")):
        raise Stop(f"HTML/WAF page on {endpoint} {params}")
    return json.loads(txt) if txt.strip() else None


def write_json(path, obj):
    """Atomic write (temp file + rename) so a crash never leaves a half-written cache file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj))
    os.replace(tmp, path)


def archive(path):
    """Keep the version about to be replaced under history/<its fetch date>/."""
    if not path.exists():
        return
    try:
        ts = json.loads(path.read_text()).get("_fetched_at") or ""
        day = dt.datetime.fromisoformat(ts).astimezone(COLO).date().isoformat()
    except Exception:
        day = "unknown"
    d = HISTORY / day
    d.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, d / path.name)


def call(endpoint, verb="POST", refresh=False, **params):
    """Returns (cache record {"_fetched_at", "data"}, from_cache). Cache key = endpoint + sorted params.
    refresh=True fetches even when cached and replaces the file (old version archived) only after a full response."""
    f = cache_path(endpoint, **params)
    if f.exists() and not refresh:
        return json.loads(f.read_text()), True
    data = _request(endpoint, verb, params)
    rec = {"_fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(), "data": data}
    if refresh:
        archive(f)
    write_json(f, rec)
    return rec, False


def read_cache(path):
    j = json.loads(pathlib.Path(path).read_text())
    return (j["data"], j["_fetched_at"]) if isinstance(j, dict) and "_fetched_at" in j else (j, None)


def months(back, ahead, today=None):
    t = today or dt.date.today()
    y, m = t.year, t.month - back
    while m < 1:
        y, m = y - 1, m + 12
    out = []
    for _ in range(back + ahead + 1):
        out.append((y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def detail_record(aid, expected_symbol=None, root=None):
    """Announcement detail {reqBaseAnnouncement, reqAnnouncementDocs} or None. getAnnouncementById first (cache), then
    the legacy ann_detail copy, then getGeneralAnnouncementById - which some announcements need (getAnnouncementById
    returns null for them). The general record is used only when its id equals aid and, when given, its symbol equals
    expected_symbol: a wrong-system id silently returns another company's record."""
    root = pathlib.Path(root) if root else ROOT
    cache = root / "raw_api" / "events"
    f = cache / f"getAnnouncementById__announcementId-{aid}.json"
    if f.exists():
        d = read_cache(f)[0]
        if d:
            return d
    f = root / "raw_api" / "ann_detail" / f"new_{aid}.json"
    if f.exists():
        d = json.loads(f.read_text())
        if d:
            return d
    f = cache / f"getGeneralAnnouncementById__announcementId-{aid}.json"
    if f.exists():
        d = read_cache(f)[0]
        b = (d or {}).get("reqBaseAnnouncement") or {}
        if b.get("id") == aid and (expected_symbol is None or b.get("symbol") == expected_symbol):
            return d
    return None


# ---- incremental helpers (pure; tested in test_events_incremental.py) -----------------------------------------
def calendar_years(today):
    """Years whose corporateCalender can still change: the current one, plus the previous one in January (late
    approvals of December announcements)."""
    return [today.year - 1, today.year] if today.month == 1 else [today.year]


def is_stale(path, max_age_days, now):
    """True when the cache file is missing, unreadable, or was fetched more than max_age_days before `now`."""
    try:
        ts = json.loads(pathlib.Path(path).read_text()).get("_fetched_at")
        return now - dt.datetime.fromisoformat(ts) > dt.timedelta(days=max_age_days)
    except Exception:
        return True


def feed_records(data, endpoint):
    field, _ = FEEDS[endpoint]
    return list((data or {}).get(field) or [])


def merge_records(old, new, idf):
    """Union of two record lists by id; the newer copy of a record wins. Sorted newest id first."""
    out = {r[idf]: r for r in old}
    out.update({r[idf]: r for r in new})
    return sorted(out.values(), key=lambda r: r[idf], reverse=True)


def feed_gap(prev_ids, new_ids):
    """Records may have been missed when the new feed and everything seen before do not overlap: the oldest id in
    today's feed is newer than the newest id already seen. Returns (gap: bool, newest_prev, oldest_new)."""
    if not prev_ids or not new_ids:
        return False, max(prev_ids, default=None), min(new_ids, default=None)
    hi, lo = max(prev_ids), min(new_ids)
    return lo > hi, hi, lo


def feed_snapshots(endpoint):
    """All stored copies of a rolling feed: the legacy single file plus the dated snapshots."""
    return sorted(CACHE.glob(f"{endpoint}.json")) + sorted(CACHE.glob(f"{endpoint}__day-*.json"))


def all_feed_records(endpoint):
    _, idf = FEEDS[endpoint]
    recs = []
    for f in feed_snapshots(endpoint):
        recs = merge_records(recs, feed_records(read_cache(f)[0], endpoint), idf)
    return recs


def fetch_feed(endpoint, today):
    """Fetch a rolling feed and merge it into today's snapshot. Returns (n_new_records, gap_info or None)."""
    field, idf = FEEDS[endpoint]
    seen = {r[idf] for r in all_feed_records(endpoint)}
    data = _request(endpoint, "POST", {})
    recs = feed_records(data, endpoint)
    if not recs:
        raise Stop(f"{endpoint} returned no records (shape changed or blocked?)")
    ids = {r[idf] for r in recs}
    gap, hi, lo = feed_gap(seen, ids)
    f = CACHE / f"{endpoint}__day-{today.isoformat()}.json"
    old = feed_records(read_cache(f)[0], endpoint) if f.exists() else []
    write_json(f, {"_fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(), "data": {field: merge_records(old, recs, idf)}})
    info = None
    if gap:
        info = {"endpoint": endpoint, "day": today.isoformat(), "newest_id_before": hi, "oldest_id_now": lo,
                "note": "feed did not overlap the previous snapshot; records between these ids may be missing"}
    return len(ids - seen), info


def record_gap(info):
    gaps = json.loads(GAPS.read_text()) if GAPS.exists() else []
    gaps.append(info)
    write_json(GAPS, gaps)


def daily(today=None, now=None):
    """Incremental refresh. Returns a summary dict (also printed as JSON on the last line)."""
    today = today or dt.datetime.now(COLO).date()
    now = now or dt.datetime.now(dt.timezone.utc)
    summary = {"day": today.isoformat(), "gaps": [], "new_feed_records": {}}
    for y in calendar_years(today):
        # one canonical file per year; the earlier month-keyed copies (the endpoint ignores month) are superseded
        call("corporateCalender", refresh=True, year=y, month=today.month if y == today.year else 12)
        for f in CACHE.glob(f"corporateCalender__month-*__year-{y}.json"):
            if f != cache_path("corporateCalender", year=y, month=today.month if y == today.year else 12):
                archive(f)
                f.unlink()
        print("corporateCalender", y, "refreshed", flush=True)
    for y, m in months(0, 3, today):
        call("getAgmEgmEvents", refresh=True, year=y, month=m)
        print("getAgmEgmEvents", y, m, "refreshed", flush=True)
    for ep in FEEDS:
        n_new, gap = fetch_feed(ep, today)
        summary["new_feed_records"][ep] = n_new
        print(ep, "new records:", n_new, flush=True)
        if gap:
            record_gap(gap)
            summary["gaps"].append(gap)
            print("WARNING feed gap:", json.dumps(gap), file=sys.stderr, flush=True)
    for t in "PIS":
        if is_stale(cache_path("upcomingListings", newListingType=t), STATIC_MAX_AGE_DAYS, now):
            call("upcomingListings", "GET", refresh=True, newListingType=t)
            print("upcomingListings", t, "refreshed", flush=True)
    if is_stale(cache_path("ruleBooks/web/list", ruleType="LR"), STATIC_MAX_AGE_DAYS, now):
        call("ruleBooks/web/list", "GET", refresh=True, ruleType="LR")
        print("ruleBooks LR refreshed", flush=True)
    summary["requests"] = STATS["requests"]
    return summary


def backfill(a):
    today = dt.date.today()
    ms = months(a.months_back, a.months_ahead, today)
    years = list(range(ms[0][0], today.year + 1))
    n = 0
    for y in years:
        mm = today.month if y == today.year else 12
        _, cached = call("corporateCalender", refresh=a.refresh_current and y == today.year, year=y, month=mm)
        n += not cached
        print("corporateCalender", y, "cached" if cached else "fetched", flush=True)
    for y, m in ms:
        _, cached = call("getAgmEgmEvents", refresh=a.refresh_current and (y, m) >= (today.year, today.month), year=y, month=m)
        n += not cached
        print("getAgmEgmEvents", y, m, "cached" if cached else "fetched", flush=True)
    if not a.no_static:
        for t in "PIS":
            _, c = call("upcomingListings", "GET", refresh=a.refresh_current, newListingType=t); n += not c
        for ep in FEEDS:
            fetch_feed(ep, dt.datetime.now(COLO).date()); n += 1
        _, c = call("ruleBooks/web/list", "GET", refresh=a.refresh_current, ruleType="LR"); n += not c
    print("requests made:", n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", action="store_true", help="incremental daily refresh (see module docstring)")
    ap.add_argument("--months-back", type=int, default=12)
    ap.add_argument("--months-ahead", type=int, default=3)
    ap.add_argument("--refresh-current", action="store_true", help="backfill mode: refetch current/future months")
    ap.add_argument("--no-static", action="store_true", help="backfill mode: skip listings/rule books/rolling feeds")
    a = ap.parse_args()
    CACHE.mkdir(parents=True, exist_ok=True)
    try:
        if a.daily:
            print(json.dumps(daily()))
        else:
            backfill(a)
    except Stop as e:
        print("STOP:", e, file=sys.stderr)
        sys.exit(2)


def fetch_file(url, dest):
    """Static file download with the same pacing; cached by destination path."""
    dest = pathlib.Path(dest)
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    wait = 3.0 + random.uniform(0.2, 1.5) - (time.time() - _last[0])
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(urllib.parse.quote(url, safe=":/%"), headers={**H, "Accept": "application/pdf,*/*"})
    try:
        data = urllib.request.urlopen(req, timeout=60).read()
    except urllib.error.HTTPError as e:
        _last[0] = time.time()
        if e.code in (429, 403):
            raise Stop(f"HTTP {e.code} on {url}")
        raise
    _last[0] = time.time()
    dest.write_bytes(data)
    return dest


if __name__ == "__main__":
    main()
