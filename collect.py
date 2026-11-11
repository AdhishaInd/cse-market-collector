"""Daily archive of CSE (Colombo Stock Exchange) market data that the cse.lk API only serves for the current day.

  python collect.py collect   save today's data; skips non-trading days, waits if the market is still open
  python collect.py check     verify every recent trading day the API reports is in the archive

Trading days and hours are not hard-coded: the script asks the API whether the market is open and whether
anything traded today, so holidays, special sessions and changed hours are handled. Exit code 1 = needs attention.
Output: data/YYYY/MM/YYYY-MM-DD.json.gz plus one line per day in data/index.csv."""
import csv, datetime as dt, gzip, json, os, random, sys, time, urllib.error, urllib.parse, urllib.request

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
    target = path_for(today)
    if os.path.exists(target):
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
    # 5. write atomically, then index
    os.makedirs(os.path.dirname(target), exist_ok=True)
    tmp = target + ".part"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(rec, f, separators=(",", ":"), ensure_ascii=False)
    os.replace(tmp, target)
    idx = os.path.join(ROOT, "index.csv"); new = not os.path.exists(idx)
    with open(idx, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["date", "stocks", "trade_rows", "short_stocks", "bytes", "fetched_at", "requests"])
        w.writerow([today, len(ts), rec["checks"]["trade_rows"], len(short), os.path.getsize(target), rec["fetched_at"], calls])
    print(f"{today}: archived {len(ts)} stocks, {rec['checks']['trade_rows']} trades, "
          f"{os.path.getsize(target) // 1024} KB, {calls} requests")
    if len(short) > max(3, 0.05 * len(ts)):
        raise Fail(f"saved, but {len(short)} stocks returned fewer trades than reported: {short[:10]}")


def check():
    days = recent_trading_days()
    today = now().date()
    have = sorted(f[:10] for _, _, fs in os.walk(ROOT) for f in fs if f.endswith(".json.gz"))
    start = dt.date.fromisoformat(have[0]) if have else today   # days before the archive began are not "missing"
    days = {d: v for d, v in days.items() if d >= start}
    missing = [d for d in sorted(days) if not os.path.exists(path_for(d))
               and not (d == today and now().hour < 23)]  # today's runs may still be pending
    if missing:
        raise Fail("trading days missing from the archive: " + ", ".join(map(str, missing))
                   + ". Same-day data cannot be fetched after it resets.")
    print("archive complete for reported trading days:", ", ".join(map(str, sorted(days))) or "none")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "collect"
    try:
        {"collect": collect, "check": check}[mode]()
    except Fail as e:
        msg = f"{mode} failed at {now():%Y-%m-%d %H:%M} Colombo: {e}"
        print(msg, file=sys.stderr)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"error={msg}\n")
        sys.exit(1)
