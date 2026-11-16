"""Paced, cached CSE client for the corporate-action job (same rules as the events job's fetch_events.py).

* One request at a time, >= 3 s + jitter apart, normal browser User-Agent, Origin/Referer https://www.cse.lk/.
* Every response is cached under <root>/raw_api/actions/ ({"_fetched_at", "data"}); a cached call never repeats.
* HTTP 429 / 403, an HTML (WAF) page or a non-JSON reply raises Stop: the run ends and is reported. A refused request
  is never retried.
* A per-run budget (MAX_REQUESTS) caps what one run may ask for; the rest waits for the next run.
"""
import datetime as dt, json, os, pathlib, random, time, urllib.error, urllib.parse, urllib.request

BASE = "https://www.cse.lk/api/"
H = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
     "Origin": "https://www.cse.lk", "Referer": "https://www.cse.lk/", "Accept": "application/json"}


class Stop(Exception):
    pass


class Budget(Exception):
    pass


class Client:
    def __init__(self, root, max_requests=40, min_gap=3.0):
        self.cache = pathlib.Path(root) / "raw_api" / "actions"
        self.cache.mkdir(parents=True, exist_ok=True)
        self.max, self.gap, self.requests, self._last = max_requests, min_gap, 0, 0.0

    def path(self, endpoint, **params):
        return self.cache / (endpoint.replace("/", "__") + "".join(f"__{k}-{v}" for k, v in sorted(params.items())) + ".json")

    def _request(self, endpoint, verb, params):
        if self.requests >= self.max:
            raise Budget(f"request budget {self.max} used")
        wait = self.gap + random.uniform(0.2, 1.5) - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        if verb == "POST":
            req = urllib.request.Request(BASE + endpoint, data=urllib.parse.urlencode(params).encode(),
                                         headers={**H, "Content-Type": "application/x-www-form-urlencoded"})
        else:
            req = urllib.request.Request(BASE + endpoint + ("?" + urllib.parse.urlencode(params) if params else ""), headers=H)
        self.requests += 1
        try:
            raw = urllib.request.urlopen(req, timeout=40).read()
        except urllib.error.HTTPError as e:
            self._last = time.time()
            if e.code in (429, 403):
                raise Stop(f"HTTP {e.code} on {endpoint}")
            raise Stop(f"HTTP {e.code} on {endpoint}")
        self._last = time.time()
        txt = raw.decode("utf-8", "replace")
        if txt.lstrip().lower().startswith(("<!doctype", "<html")):
            raise Stop(f"HTML/WAF page on {endpoint}")
        try:
            return json.loads(txt) if txt.strip() else None
        except ValueError:
            raise Stop(f"non-JSON reply on {endpoint}")

    def call(self, endpoint, verb="POST", refresh=False, **params):
        """(data, from_cache). refresh=True asks again and replaces the cache file after a full response."""
        f = self.path(endpoint, **params)
        if f.exists() and not refresh:
            return json.loads(f.read_text())["data"], True
        data = self._request(endpoint, verb, params)
        tmp = f.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"_fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(), "data": data}))
        os.replace(tmp, f)
        return data, False


def read_cached(path):
    """A cached response in either layout: {"_fetched_at", "data"} (job caches) or the bare response (legacy ann_detail)."""
    p = pathlib.Path(path)
    if not p.exists():
        return None, False
    j = json.loads(p.read_text())
    if isinstance(j, dict) and "_fetched_at" in j:
        return j["data"], True
    return j, True


def detail_paths(root, fmt, aid):
    """Where an announcement detail may already be cached: this job's cache, the events job's cache, legacy copies."""
    root = pathlib.Path(root)
    if fmt == "OLD":
        return [root / "raw_api/actions" / f"announcementById__id-{aid}.json", root / "raw_api/ann_detail" / f"old_{aid}.json"]
    return [root / "raw_api/actions" / f"getAnnouncementById__announcementId-{aid}.json",
            root / "raw_api/events" / f"getAnnouncementById__announcementId-{aid}.json",
            root / "raw_api/ann_detail" / f"new_{aid}.json",
            root / "raw_api/actions" / f"getGeneralAnnouncementById__announcementId-{aid}.json",
            root / "raw_api/events" / f"getGeneralAnnouncementById__announcementId-{aid}.json"]


def get_detail(root, fmt, aid, ticker, company_id, check, client=None):
    """(detail or None, status, requests made). Cached copies first; then, with a client, the endpoint of the id's
    system: OLD ids -> announcementById; NEW ids -> getAnnouncementById, and getGeneralAnnouncementById only when that
    returned nothing. `check(fmt, aid, d, ticker, company_id)` decides identity: a record of another company is refused
    and never used (status COMPANY_MISMATCH), whichever endpoint returned it."""
    seen_mismatch = False
    for p in detail_paths(root, fmt, aid):
        d, ok = read_cached(p)
        if ok and d:
            st = check(fmt, aid, d, ticker, company_id)[0]
            if st == "FETCHED":
                return d, st, 0
            seen_mismatch |= st == "COMPANY_MISMATCH"
    if client is None:
        return None, "COMPANY_MISMATCH" if seen_mismatch else "NOT_FETCHED", 0
    before = client.requests
    eps = [("announcementById", {"id": aid})] if fmt == "OLD" else [("getAnnouncementById", {"announcementId": aid}),
                                                                     ("getGeneralAnnouncementById", {"announcementId": aid})]
    status = "NO_DETAIL"
    for ep, params in eps:
        d, _ = client.call(ep, **params)
        if not d:
            continue
        st = check(fmt, aid, d, ticker, company_id)[0]
        if st == "FETCHED":
            return d, st, client.requests - before
        if st == "COMPANY_MISMATCH":
            status = "COMPANY_MISMATCH"
            break
    return None, status, client.requests - before
