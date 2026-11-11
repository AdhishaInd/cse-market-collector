"""Daily archive of CSE (Colombo Stock Exchange) market data that the cse.lk API only serves for the current day.

  python collect.py collect   save today's data; skips non-trading days, waits if the market is still open
  python collect.py check     verify every recent trading day the API reports is in the archive
  python collect.py upload F  load a locally saved day file into Turso
Storage: Turso when TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are set (tables archive_day, archive_blob, trade,
day_summary), otherwise local files.

Trading days and hours are not hard-coded: the script asks the API whether the market is open and whether
anything traded today, so holidays, special sessions and changed hours are handled. Exit code 1 = needs attention.
Local output: data/YYYY/MM/YYYY-MM-DD.json.gz."""
import datetime as dt, gzip, json, os, random, sys, time, urllib.error, urllib.parse, urllib.request

BASE = "https://www.cse.lk/api/"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36",
           "Origin": "https://www.cse.lk", "Referer": "https://www.cse.lk/", "Accept": "application/json"}
TZ = dt.timezone(dt.timedelta(hours=5, minutes=30))
GAP = 2.0                 # seconds between requests (plus jitter)
MAX_WAIT_OPEN = 150 * 60  # wait at most this long for the market to close
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
_last = 0.0
calls = 0


class Fail(Exception):
    pass


def api(endpoint, params=None, method="POST", tries=4):
    global _last, calls
    params = params or {}
    url, data = BASE + endpoint, None
    if method == "GET":
        url += ("?" + urllib.parse.urlencode(params)) if params else ""
    else:
        data = urllib.parse.urlencode(params).encode()
    err = None
    for attempt in range(tries):
        wait = GAP + random.uniform(0, 0.8) - (time.time() - _last)
        if wait > 0:
            time.sleep(wait)
        h = dict(HEADERS)
        if data is not None:
            h["Content-Type"] = "application/x-www-form-urlencoded"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=h, method=method), timeout=60) as r:
                body = r.read()
            _last = time.time(); calls += 1
            return json.loads(body)
        except urllib.error.HTTPError as e:
            _last = time.time(); calls += 1; err = f"HTTP {e.code}"
            if e.code in (403, 429):  # blocked or rate limited: back off hard, then give up
                time.sleep(600 if attempt == 0 else 0)
                if attempt >= 1:
                    raise Fail(f"{endpoint}: {err} (blocked or rate limited)")
            else:
                time.sleep(10 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, ValueError, ConnectionError) as e:
            _last = time.time(); err = f"{type(e).__name__}: {e}"
            time.sleep(10 * (attempt + 1))
    raise Fail(f"{endpoint}: {err}")


def now():
    return dt.datetime.now(TZ)


# ---------- storage: Turso (libSQL over HTTP) when TURSO_DATABASE_URL is set, else local files ----------
TURSO_URL = os.environ.get("TURSO_DATABASE_URL", "").replace("libsql://", "https://").rstrip("/")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")
SCHEMA = [
    """CREATE TABLE IF NOT EXISTS archive_day (date TEXT PRIMARY KEY, fetched_at TEXT, stocks INTEGER,
       trade_rows INTEGER, short_stocks INTEGER, raw_bytes INTEGER, requests INTEGER)""",
    "CREATE TABLE IF NOT EXISTS archive_blob (date TEXT, part INTEGER, data BLOB, PRIMARY KEY (date, part))",
    """CREATE TABLE IF NOT EXISTS trade (trade_id INTEGER PRIMARY KEY, date TEXT, symbol TEXT, t INTEGER,
       price REAL, qty INTEGER)""",
    "CREATE INDEX IF NOT EXISTS trade_sym_date ON trade (symbol, date)",
    """CREATE TABLE IF NOT EXISTS day_summary (date TEXT, symbol TEXT, name TEXT, open REAL, high REAL, low REAL,
       close REAL, prev_close REAL, share_volume INTEGER, trades INTEGER, turnover REAL, market_cap REAL,
       PRIMARY KEY (date, symbol))""",
]


def _arg(v):
    import base64
    if v is None:
        return {"type": "null"}
    if isinstance(v, bool) or isinstance(v, int):
        return {"type": "integer", "value": str(int(v))}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    if isinstance(v, bytes):
        return {"type": "blob", "base64": base64.b64encode(v).decode()}
    return {"type": "text", "value": str(v)}


def sql(statements):
    """Run [(sql, args), ...] in one Turso pipeline; returns the results. Raises Fail on any error."""
    reqs = [{"type": "execute", "stmt": {"sql": s, "args": [_arg(a) for a in args]}} for s, args in statements]
    body = json.dumps({"requests": reqs + [{"type": "close"}]}).encode()
    err = None
    for attempt in range(4):
        try:
            r = urllib.request.Request(TURSO_URL + "/v2/pipeline", data=body, method="POST", headers={
                "Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"})
            with urllib.request.urlopen(r, timeout=120) as resp:
                out = json.loads(resp.read())
            bad = [x for x in out["results"] if x.get("type") == "error"]
            if bad:
                raise Fail(f"Turso: {bad[0].get('error')}")
            return [x["response"].get("result") for x in out["results"] if x.get("type") == "ok"]
        except Fail:
            raise
        except Exception as e:
            err = e; time.sleep(5 * (attempt + 1))
    raise Fail(f"Turso unreachable: {err}")


def archived(day):
    if TURSO_URL:
        res = sql([(SCHEMA[0], []), ("SELECT 1 FROM archive_day WHERE date = ?", [day.isoformat()])])
        return bool(res[1]["rows"])
    return os.path.exists(path_for(day))


def first_archived():
    if TURSO_URL:
        rows = sql([(SCHEMA[0], []), ("SELECT MIN(date) FROM archive_day", [])])[1]["rows"]
        v = rows[0][0].get("value") if rows and rows[0] else None
        return dt.date.fromisoformat(v) if v else None
    have = sorted(f[:10] for _, _, fs in os.walk(ROOT) for f in fs if f.endswith(".json.gz"))
    return dt.date.fromisoformat(have[0]) if have else None


def store(rec):
    """Save one day. Turso: raw gzip JSON in 1 MB parts + trade and day_summary rows; archive_day is written last,
    so a day counts as archived only when everything else is in."""
    day = rec["date"]
    raw = gzip.compress(json.dumps(rec, separators=(",", ":"), ensure_ascii=False).encode(), 9)
    if not TURSO_URL:
        target = path_for(dt.date.fromisoformat(day))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target + ".part", "wb") as f:
            f.write(raw)
        os.replace(target + ".part", target)
        return len(raw)
    sql([(s, []) for s in SCHEMA] + [(f"DELETE FROM {t} WHERE date = ?", [day])
                                     for t in ("archive_day", "archive_blob", "trade", "day_summary")])
    parts = [raw[i:i + 1_000_000] for i in range(0, len(raw), 1_000_000)]
    for i, p in enumerate(parts):
        sql([("INSERT INTO archive_blob (date, part, data) VALUES (?, ?, ?)", [day, i, p])])
    rows = [(r.get("id") or r.get("s"), day, sym, r["t"], r.get("p"), r.get("q"))
            for sym, v in rec["trades"].items() for r in v["rows"] if r.get("t")]
    for i in range(0, len(rows), 2000):
        chunk = rows[i:i + 2000]
        stmts = []
        for j in range(0, len(chunk), 400):
            c = chunk[j:j + 400]
            stmts.append(("INSERT OR REPLACE INTO trade (trade_id, date, symbol, t, price, qty) VALUES "
                          + ",".join(["(?,?,?,?,?,?)"] * len(c)), [a for row in c for a in row]))
        sql(stmts)
    summ = [(day, x["symbol"], x.get("name"), x.get("open"), x.get("high"), x.get("low"), x.get("price"),
             x.get("previousClose"), x.get("sharevolume"), x.get("tradevolume"), x.get("turnover"), x.get("marketCap"))
            for x in rec["tradeSummary"]]
    for j in range(0, len(summ), 200):
        c = summ[j:j + 200]
        sql([("INSERT OR REPLACE INTO day_summary VALUES " + ",".join(["(?,?,?,?,?,?,?,?,?,?,?,?)"] * len(c)),
              [a for row in c for a in row])])
    ck = rec["checks"]
    sql([("INSERT INTO archive_day VALUES (?, ?, ?, ?, ?, ?, ?)", [day, rec["fetched_at"], ck["stocks"],
          ck["trade_rows"], len(ck["short_stocks"]), len(raw), ck["requests"]])])
    return len(raw)


def day_of(ms):
    return dt.datetime.fromtimestamp(ms / 1000, TZ).date()


def path_for(day):
    return os.path.join(ROOT, f"{day:%Y}", f"{day:%m}", f"{day.isoformat()}.json.gz")


def recent_trading_days():
    """Trading days the API itself reports (today and the previous session) from dailyMarketSummery."""
    out = {}
    for block in api("dailyMarketSummery") or []:
        for r in block or []:
            if r.get("tradeDate") and (r.get("marketTrades") or r.get("tradesNo") or 0) > 0:
                out[day_of(r["tradeDate"])] = r
    return out


def collect():
    today = now().date()
    if archived(today):
        print(f"{today}: already archived, nothing to do"); return
    # 1. wait for the close (hours are not assumed)
    t0 = time.time()
    while True:
        status = (api("marketStatus") or {}).get("status", "")
        if "closed" in status.lower():
            break
        if time.time() - t0 > MAX_WAIT_OPEN:
            raise Fail(f"market still not closed after {MAX_WAIT_OPEN // 60} min (status: {status!r})")
        print(f"market status {status!r}; waiting 5 min", flush=True)
        time.sleep(300)
    # 2. did anything trade today?
    ts = (api("tradeSummary") or {}).get("reqTradeSummery") or []
    last = max((day_of(x["lastTradedTime"]) for x in ts if x.get("lastTradedTime")), default=None)
    reported = recent_trading_days()
    if last != today:
        if today in reported:
            raise Fail(f"dailyMarketSummery reports trading on {today} but tradeSummary is empty/stale "
                       f"(latest trade {last}); data may already be gone or the API changed")
        print(f"{today}: no trading today (latest trade {last}); nothing to archive"); return
    # 3. market-wide snapshots
    rec = {"date": today.isoformat(), "fetched_at": now().isoformat(), "market_status": status, "tradeSummary": ts}
    for name, ep, params, method in [
        ("detailedTrades", "detailedTrades", None, "POST"),
        ("marketSummery", "marketSummery", None, "POST"),
        ("dailyMarketSummery", "dailyMarketSummery", None, "POST"),
        ("marketIndices", "marketIndices", None, "POST"),
        ("GICSSectorSummery", "GICSSectorSummery", None, "POST"),
        ("getContributorsChart", "getContributorsChart", None, "POST"),
        ("getContributors", "getContributors", None, "POST"),
        ("mostActiveVolumes", "mostActiveVolumes", None, "POST"),
        ("list_by_market_cap", "list_by_market_cap", None, "POST"),
        ("aspi_minute", "chartData", {"symbol": "ASI", "chartId": 1, "period": 1}, "POST"),
        ("snp_minute", "chartData", {"symbol": "ASI", "chartId": 40, "period": 1}, "POST"),
        ("aspi_high_low", "sectorHighLow", {"sectorId": 1}, "POST"),
        ("snp_high_low", "sectorHighLow", {"sectorId": 40}, "POST"),
    ]:
        rec[name] = api(ep, params, method)
    # 4. every trade of every stock that traded (only served today)
    trades, short = {}, []
    for x in ts:
        rows = (api("companyChartDataByStock", {"stockId": x["id"], "period": 1}) or {}).get("chartData") or []
        if len(rows) < 0.95 * (x.get("tradevolume") or 0):   # chart can exceed the count (crossings), not fall short
            rows2 = (api("companyChartDataByStock", {"stockId": x["id"], "period": 1}) or {}).get("chartData") or []
            rows = rows2 if len(rows2) > len(rows) else rows
            if len(rows) < 0.95 * (x.get("tradevolume") or 0):
                short.append(f"{x['symbol']} {len(rows)}/{x.get('tradevolume')}")
        trades[x["symbol"]] = {"id": x["id"], "rows": rows}
    rec["trades"] = trades
    rec["checks"] = {"stocks": len(ts), "trade_rows": sum(len(v["rows"]) for v in trades.values()),
                     "short_stocks": short, "requests": calls}
    stamps = [r["t"] for v in trades.values() for r in v["rows"] if r.get("t")]
    if not stamps or day_of(max(stamps)) != today:
        raise Fail("per-stock trade lists are empty or not for today (data reset or API change)")
    # 5. store (Turso, or local files when no database is configured)
    size = store(rec)
    print(f"{today}: archived {len(ts)} stocks, {rec['checks']['trade_rows']} trades, "
          f"{size // 1024} KB compressed, {calls} requests")
    if len(short) > max(3, 0.05 * len(ts)):
        raise Fail(f"saved, but {len(short)} stocks returned fewer trades than reported: {short[:10]}")


def check():
    days = recent_trading_days()
    today = now().date()
    start = first_archived() or today   # days before the archive began are not "missing"
    days = {d: v for d, v in days.items() if d >= start and not (d == today and now().hour < 23)}
    missing = [d for d in sorted(days) if not archived(d)]   # today is only checked from 23:00
    if missing:
        raise Fail("trading days missing from the archive: " + ", ".join(map(str, missing))
                   + ". Same-day data cannot be fetched after it resets.")
    print("archive complete for checked trading days:", ", ".join(map(str, sorted(days))) or "none")



def upload(path):
    """Load a locally saved day file (data/YYYY/MM/date.json.gz) into Turso."""
    if not TURSO_URL:
        raise Fail("TURSO_DATABASE_URL is not set")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        rec = json.load(f)
    print(f"{rec['date']}: uploaded, {store(rec) // 1024} KB compressed")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "collect"
    try:
        if mode == "upload":
            upload(sys.argv[2])
        else:
            {"collect": collect, "check": check}[mode]()
    except Fail as e:
        msg = f"{mode} failed at {now():%Y-%m-%d %H:%M} Colombo: {e}"
        print(msg, file=sys.stderr)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"error={msg}\n")
        sys.exit(1)
