"""Fetch getAnnouncementById detail for AGM/EGM announcements (dateOfAgm / dateOfEgm, time, venue).
Reads the announcement index from db/cse_financials.db READ-ONLY (or --ids-file). Cached in raw_api/events/, resumable.
Usage: python -I pipeline/fetch_event_details.py [--since 2025-10-01] [--max N] [--db PATH]
Incremental: an id already cached is never requested again; new ids come from the corporateCalender cache and every
stored snapshot of the approvedAnnouncement feed.
Order: AGM initial, EGM initial, EGM, AGM approved (newest first within each).
Stops (exit 2) on 429/403/WAF page."""
import argparse, pathlib, re, sqlite3, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import fetch_events as F

CATS = ["annual general meeting initial", "extraordinary general meeting initial",
        "extraordinary general meeting", "annual general meeting approved"]

def norm_cat(c):
    return re.sub(r"[^a-z ]", "", (c or "").lower()).strip()


def new_detail_ids(calendar_rows, feed_rows, have, known):
    """Announcement ids whose detail is still needed: every calendar announcement without a stored detail, and every
    AGM/EGM notice from the rolling announcement feed that is not in the database index. Newest first, no repeats."""
    ids = [r["announcementId"] for r in calendar_rows if r["announcementId"] not in have]
    ids += [r["announcementId"] for r in feed_rows if norm_cat(r.get("announcementCategory")) in CATS and r["announcementId"] not in known]
    return sorted(dict.fromkeys(ids), reverse=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2025-10-01")
    ap.add_argument("--max", type=int, default=10**9)
    ap.add_argument("--extra-ids", default="", help="comma list of announcementIds to fetch as well")
    ap.add_argument("--db", help="database to read the announcement index from (read only); default main")
    a = ap.parse_args()
    F.CACHE.mkdir(parents=True, exist_ok=True)
    ap_db = a.db or str(F.ROOT / "db" / "cse_financials.db")
    db = sqlite3.connect(f"file:{ap_db}?mode=ro", uri=True)
    todo = []
    for cat in CATS:
        todo += [r[0] for r in db.execute(
            "select cse_announcement_id from announcement where normalized_category=? and cse_format='NEW' "
            "and announced_on>=? order by announced_on desc", (cat, a.since))]
    todo += [int(x) for x in a.extra_ids.split(",") if x]
    # incremental additions: calendar announcements and meeting notices newer than the database snapshot.
    # call() skips every id already cached in raw_api/events/, so only new ids cost a request.
    have = {int(x.stem.split("_")[1]) for x in F.ROOT.joinpath("raw_api", "ann_detail").glob("new_*.json")}
    known = {r[0] for r in db.execute("select cse_announcement_id from announcement where cse_format='NEW'")}
    have |= known - {r[0] for r in db.execute(
        "select cse_announcement_id from announcement where cse_format='NEW' and detail_status='NOT_FETCHED'")}
    cal = []
    for f in sorted(F.CACHE.glob("corporateCalender__*.json")):
        cal += (F.read_cache(f)[0] or {}).get("approvedCoopCalAnnouncements") or []
    new_ids = new_detail_ids(cal, F.all_feed_records("approvedAnnouncement"), have, known)
    todo = list(dict.fromkeys(new_ids + todo))
    n = 0
    try:
        for i, aid in enumerate(todo):
            if n >= a.max: break
            d, cached = F.call("getAnnouncementById", announcementId=aid)
            n += not cached
            if not cached and n % 25 == 0:
                print(f"{i+1}/{len(todo)} fetched={n}", flush=True)
    except F.Stop as e:
        print("STOP:", e, file=sys.stderr); sys.exit(2)
    # getAnnouncementById returns null for some announcements (seen for new RIGHTS ISSUE (DATES) notices): try the
    # general-announcement endpoint for those (checked against the id/symbol where the record is used)
    try:
        for aid in new_ids:
            f = F.cache_path("getAnnouncementById", announcementId=aid)
            if f.exists() and F.read_cache(f)[0] is None and n < a.max:
                _, cached = F.call("getGeneralAnnouncementById", announcementId=aid)
                n += not cached
    except F.Stop as e:
        print("STOP:", e, file=sys.stderr); sys.exit(2)
    print("done; requests made:", n, "of", len(todo))

if __name__ == "__main__":
    main()
