"""Tests for the incremental event-calendar refresh: fetch_events (--daily), fetch_event_details (new ids only),
load_events (feed snapshots, new filings), event_diff and the refresh swap guard. No network: _request is replaced.
Run: python3 -I -m unittest pipeline/test_events_incremental.py  (from the project root) or python3 -I pipeline/test_events_incremental.py"""
import datetime as dt, json, os, pathlib, sqlite3, sys, tempfile, unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import fetch_events as F            # noqa: E402
import fetch_event_details as FD    # noqa: E402
import load_events as L             # noqa: E402
import event_diff as ED             # noqa: E402
import refresh_events as R          # noqa: E402
import letters as LT                # noqa: E402
import events_sync as SY            # noqa: E402

NOW = dt.datetime(2026, 11, 14, 13, 0, tzinfo=dt.timezone.utc)
TODAY = dt.date(2026, 11, 14)


def rec(ts, data):
    return {"_fetched_at": ts.isoformat() if isinstance(ts, dt.datetime) else ts, "data": data}


class TmpCache(unittest.TestCase):
    """Points fetch_events at an empty temporary cache."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        c = pathlib.Path(self.tmp.name)
        self.p = [mock.patch.object(F, "CACHE", c), mock.patch.object(F, "HISTORY", c / "history"),
                  mock.patch.object(F, "GAPS", c / "feed_gaps.json"), mock.patch.object(F.time, "sleep", lambda s: None)]
        for x in self.p:
            x.start()
        self.c = c
        self.calls = []

    def tearDown(self):
        for x in self.p:
            x.stop()
        self.tmp.cleanup()

    def put(self, name, obj):
        (self.c / name).write_text(json.dumps(obj))


class Helpers(unittest.TestCase):
    def test_calendar_years(self):
        self.assertEqual(F.calendar_years(dt.date(2026, 11, 14)), [2026])
        self.assertEqual(F.calendar_years(dt.date(2027, 1, 3)), [2026, 2027])

    def test_merge_records_union_newer_wins(self):
        old = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
        new = [{"id": 2, "v": "B"}, {"id": 3, "v": "c"}]
        self.assertEqual(F.merge_records(old, new, "id"), [{"id": 3, "v": "c"}, {"id": 2, "v": "B"}, {"id": 1, "v": "a"}])

    def test_feed_gap(self):
        self.assertEqual(F.feed_gap({10, 20}, {15, 30})[0], False)     # overlap in id range
        self.assertEqual(F.feed_gap({10, 20}, {21, 30}), (True, 20, 21))
        self.assertEqual(F.feed_gap(set(), {21, 30})[0], False)          # first run: nothing to compare

    def test_is_stale(self):
        with tempfile.TemporaryDirectory() as d:
            f = pathlib.Path(d) / "x.json"
            self.assertTrue(F.is_stale(f, 7, NOW))                       # missing
            f.write_text(json.dumps(rec(NOW - dt.timedelta(days=2), {})))
            self.assertFalse(F.is_stale(f, 7, NOW))
            f.write_text(json.dumps(rec(NOW - dt.timedelta(days=8), {})))
            self.assertTrue(F.is_stale(f, 7, NOW))
            f.write_text("not json")
            self.assertTrue(F.is_stale(f, 7, NOW))


class Fetch(TmpCache):
    def fake(self, responses):
        def _req(endpoint, verb, params):
            self.calls.append((endpoint, tuple(sorted(params.items()))))
            r = responses(endpoint, params)
            if isinstance(r, Exception):
                raise r
            return r
        return mock.patch.object(F, "_request", _req)

    def test_call_uses_cache_and_refresh_archives(self):
        self.put("corporateCalender__month-11__year-2026.json", rec("2026-11-11T20:00:00+00:00", {"v": 1}))
        with self.fake(lambda e, p: {"v": 2}):
            d, cached = F.call("corporateCalender", year=2026, month=11)
            self.assertTrue(cached)
            self.assertEqual(self.calls, [])
            d, cached = F.call("corporateCalender", refresh=True, year=2026, month=11)
        self.assertFalse(cached)
        self.assertEqual(d["data"], {"v": 2})
        self.assertEqual(json.loads((self.c / "history" / "2026-11-12" / "corporateCalender__month-11__year-2026.json").read_text())["data"], {"v": 1})

    def test_refresh_stop_keeps_old_cache(self):
        self.put("corporateCalender__month-11__year-2026.json", rec("2026-11-11T20:00:00+00:00", {"v": 1}))
        with self.fake(lambda e, p: F.Stop("HTTP 429")):
            with self.assertRaises(F.Stop):
                F.call("corporateCalender", refresh=True, year=2026, month=11)
        self.assertEqual(json.loads((self.c / "corporateCalender__month-11__year-2026.json").read_text())["data"], {"v": 1})

    def test_feed_merges_into_day_snapshot_and_detects_gap(self):
        self.put("approvedAnnouncement.json", rec("2026-11-11T20:00:00+00:00", {"approvedAnnouncements": [{"announcementId": i} for i in (5, 6, 7)]}))
        with self.fake(lambda e, p: {"approvedAnnouncements": [{"announcementId": i} for i in (7, 8)]}):
            n, gap = F.fetch_feed("approvedAnnouncement", TODAY)
        self.assertEqual((n, gap), (1, None))
        with self.fake(lambda e, p: {"approvedAnnouncements": [{"announcementId": i} for i in (9, 10)]}):
            n, gap = F.fetch_feed("approvedAnnouncement", TODAY)
        self.assertEqual(n, 2)
        self.assertEqual((gap["newest_id_before"], gap["oldest_id_now"]), (8, 9))
        snap = json.loads((self.c / "approvedAnnouncement__day-2026-11-14.json").read_text())["data"]["approvedAnnouncements"]
        self.assertEqual([r["announcementId"] for r in snap], [10, 9, 8, 7])     # same-day runs merge, nothing lost
        self.assertEqual([r["announcementId"] for r in F.all_feed_records("approvedAnnouncement")], [10, 9, 8, 7, 6, 5])

    def test_empty_feed_stops(self):
        with self.fake(lambda e, p: {"approvedAnnouncements": []}):
            with self.assertRaises(F.Stop):
                F.fetch_feed("approvedAnnouncement", TODAY)

    def test_daily_requests_only_what_can_change(self):
        fresh = rec(NOW - dt.timedelta(days=1), {"content": []})
        for t in "PIS":
            self.put(f"upcomingListings__newListingType-{t}.json", fresh)
        self.put("ruleBooks__web__list__ruleType-LR.json", rec(NOW - dt.timedelta(days=10), {}))
        self.put("corporateCalender__month-3__year-2026.json", rec("2026-03-01T00:00:00+00:00", {}))   # legacy month key
        self.put("corporateCalender__month-12__year-2025.json", rec("2025-12-01T00:00:00+00:00", {}))  # past year: kept
        self.put("getAgmEgmEvents__month-10__year-2026.json", rec("2026-11-11T00:00:00+00:00", []))   # past month: kept

        def resp(e, p):
            if e in F.FEEDS:
                return {F.FEEDS[e][0]: [{F.FEEDS[e][1]: 1}]}
            return {}
        with self.fake(resp):
            s = F.daily(TODAY, NOW)
        eps = [c[0] for c in self.calls]
        self.assertEqual(eps.count("corporateCalender"), 1)
        self.assertEqual([dict(c[1]) for c in self.calls if c[0] == "getAgmEgmEvents"],
                         [{"year": 2026, "month": 11}, {"year": 2026, "month": 12}, {"year": 2027, "month": 1}, {"year": 2027, "month": 2}])
        self.assertNotIn("upcomingListings", eps)                    # fresh: not refetched
        self.assertIn("ruleBooks/web/list", eps)                     # 10 days old: refetched
        self.assertFalse((self.c / "corporateCalender__month-3__year-2026.json").exists())
        self.assertTrue((self.c / "history" / "2026-03-01" / "corporateCalender__month-3__year-2026.json").exists())
        self.assertTrue((self.c / "corporateCalender__month-12__year-2025.json").exists())
        self.assertTrue((self.c / "getAgmEgmEvents__month-10__year-2026.json").exists())
        self.assertEqual(len(self.calls), 1 + 4 + 2 + 1)

    def test_new_detail_ids(self):
        cal = [{"announcementId": 100}, {"announcementId": 101}]
        feed = [{"announcementId": 200, "announcementCategory": "ANNUAL GENERAL MEETING (INITIAL)"},
                {"announcementId": 201, "announcementCategory": "CASH DIVIDEND"},
                {"announcementId": 202, "announcementCategory": "EXTRAORDINARY GENERAL MEETING"},
                {"announcementId": 203, "announcementCategory": "ANNUAL GENERAL MEETING (INITIAL)"}]
        self.assertEqual(FD.new_detail_ids(cal, feed, have={100}, known={203}), [202, 200, 101])


class Loader(unittest.TestCase):
    def test_feed_records_merge_snapshots(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "getFinancialAnnouncement.json").write_text(json.dumps(rec("2026-11-11T00:00:00+00:00", {"reqFinancialAnnouncemnets": [{"id": 1, "v": "old"}, {"id": 2}]})))
            (d / "getFinancialAnnouncement__day-2026-11-14.json").write_text(json.dumps(rec("2026-11-14T00:00:00+00:00", {"reqFinancialAnnouncemnets": [{"id": 1, "v": "new"}, {"id": 3}]})))
            out = L.feed_records("getFinancialAnnouncement", "reqFinancialAnnouncemnets", "id", raw=d)
        self.assertEqual([r["id"] for r, _ in out], [3, 2, 1])
        self.assertEqual(out[2][0]["v"], "new")

    def test_feed_filing_titles(self):
        self.assertEqual(L.feed_filing("Interim Financial Statements for the Quarter ended 30th September 2026", 3),
                         ("INTERIM", dt.date(2026, 9, 30)))
        self.assertEqual(L.feed_filing("Interim Financial Statements for the Period ended 31st  March 2026", 12),
                         ("INTERIM", dt.date(2026, 3, 31)))
        self.assertEqual(L.feed_filing("Annual Report 2025/26", 3), ("ANNUAL", dt.date(2026, 3, 31)))
        self.assertEqual(L.feed_filing("Annual Report 2025", 12), ("ANNUAL", dt.date(2025, 12, 31)))
        for t in ("Errata to the Interim Financial Statements for the Quarter ended 30th September 2026", "Trust Deed",
                  "Prospectus", "Interim Financial Statements for the Quarter ended 31st September 2026",
                  "Annual Report 2025/28", "Audited Financial Statements for the year ended 31st March 2026"):
            self.assertIsNone(L.feed_filing(t, 3), t)
        self.assertIsNone(L.feed_filing("Annual Report 2025/26", None))
        self.assertEqual(L.ts_to_colombo_date("11 Nov 2026 04:44:14 PM"), dt.date(2026, 11, 11))
        self.assertIsNone(L.ts_to_colombo_date("garbage"))

    def _db(self):
        db = sqlite3.connect(":memory:")
        db.executescript("""create table company(company_id, ticker, name, listing_status);
            create table report_document(document_id, company_id, category, title, period_end_date, uploaded_at);""")
        db.execute("insert into company values (1,'AAA','AAA PLC','LISTED')")
        rows = [(10, "ANNUAL", "Annual Report 2025/26", "2026-03-31", "2026-08-20T10:00:00+05:30")]
        for i, (pe, up) in enumerate([("2025-06-30", "2025-08-10"), ("2025-09-30", "2025-11-10"), ("2025-12-31", "2026-02-10"),
                                      ("2026-03-31", "2026-05-25"), ("2026-06-30", "2026-08-10")]):
            rows.append((20 + i, "INTERIM", "Interim Financial Statements", pe, up + "T10:00:00+05:30"))
        db.executemany("insert into report_document values (?,1,?,?,?,?)", rows)
        return db

    def test_feed_filing_suppresses_due_estimate(self):
        db = self._db()
        feed = [({"id": 99, "symbol": "AAA", "fileText": "Interim Financial Statements for the Quarter ended 30th September 2026",
                  "uploadedDate": "12 Nov 2026 03:00:00 PM"}, "x"),
                ({"id": 24, "symbol": "AAA", "fileText": "Interim Financial Statements for the Quarter ended 30th June 2026",
                  "uploadedDate": "01 Jan 2026 03:00:00 PM"}, "x")]          # already indexed (id 24): ignored
        Fx, fy = L.filings(db, feed)
        self.assertEqual(fy[1], 3)
        self.assertEqual(Fx[1][("Q2", dt.date(2026, 9, 30))], (dt.date(2026, 11, 12), "gfa:99"))
        self.assertEqual(Fx[1][("Q1", dt.date(2026, 6, 30))][1], 24)
        for fd, expect_due in ((None, True), (feed, False)):
            E = L.Events(TODAY, TODAY - dt.timedelta(365), __import__("collections").Counter())
            L.results_events(db, E, TODAY, TODAY - dt.timedelta(365), {1: "AAA PLC"}, fd)
            due = [r for r in E.rows.values() if r["event_type"] == "RESULTS_DUE" and r["detail"].startswith("Q2 ")]
            self.assertEqual(bool(due), expect_due)
            if not expect_due:
                filed = [r for r in E.rows.values() if r["event_type"] == "RESULTS_FILED" and r["primary"][0] == "getFinancialAnnouncement"]
                self.assertEqual([(r["event_date"], r["primary"][1]) for r in filed], [("2026-11-12", "99")])


def ev(t, d, cid=1, sym=None, kind="confirmed", amount=None, status=None, src=("corporateCalender", "1"), detail=None, ticker="AAA"):
    return dict(event_id=None, company_id=cid, company_name="AAA PLC", symbol=sym, event_type=t, event_date=d, date_kind=kind,
                amount=amount, status=status, detail=detail, source_endpoint=src[0], source_id=src[1], announced_on=None, ticker=ticker)


class Diff(unittest.TestCase):
    def kinds(self, old, new, since="2025-11-14"):
        return sorted((c["change_type"], c["row"]["event_type"], c["old_date"], c["new_date"]) for c in ED.diff(old, new, since))

    def test_unchanged(self):
        rows = [ev("CASH_DIV_XD", "2026-11-20", sym="AAA.N0000", amount=1)]
        self.assertEqual(ED.diff(rows, [dict(r) for r in rows], "2025-11-14"), [])

    def test_new_moved_removed_changed(self):
        old = [ev("CASH_DIV_XD", "2026-11-20", sym="AAA.N0000", src=("corporateCalender", "1")),
               ev("AGM", "2026-12-01", src=("getAnnouncementById", "5")),
               ev("RIGHTS_XR", "2026-12-10", sym="AAA.N0000", kind="announced_tentative"),
               ev("CASH_DIV_PAYMENT", "2026-12-05", cid=2, sym="BBB.N0000", ticker="BBB")]
        new = [ev("CASH_DIV_XD", "2026-11-23", sym="AAA.N0000", src=("corporateCalender", "2")),    # further announcement
               ev("AGM", "2027-02-15", src=("getAnnouncementById", "6")),                         # rescheduled (76 days)
               ev("RIGHTS_XR", "2026-12-10", sym="AAA.N0000", kind="confirmed"),                  # EGM held
               ev("TRADING_HALT", "2026-11-14")]
        self.assertEqual(self.kinds(old, new), [
            ("CHANGED", "RIGHTS_XR", "2026-12-10", "2026-12-10"),
            ("MOVED", "AGM", "2026-12-01", "2027-02-15"),
            ("MOVED", "CASH_DIV_XD", "2026-11-20", "2026-11-23"),
            ("NEW", "TRADING_HALT", None, "2026-11-14"),
            ("REMOVED", "CASH_DIV_PAYMENT", "2026-12-05", None)])

    def test_move_window_and_nearest_pairing(self):
        old = [ev("CASH_DIV_XD", "2026-01-10"), ev("CASH_DIV_XD", "2026-06-10")]
        new = [ev("CASH_DIV_XD", "2026-06-12"), ev("CASH_DIV_XD", "2026-10-01")]
        self.assertEqual(self.kinds(old, new), [("MOVED", "CASH_DIV_XD", "2026-06-10", "2026-06-12"),
                                                ("NEW", "CASH_DIV_XD", None, "2026-10-01"),
                                                ("REMOVED", "CASH_DIV_XD", "2026-01-10", None)])

    def test_symbol_only_change(self):
        old = [ev("RIGHTS_TRADING_START", "2025-11-26", sym="AAA.N0000")]
        new = [ev("RIGHTS_TRADING_START", "2025-11-26", sym=None)]
        c = ED.diff(old, new, "2025-11-14")
        self.assertEqual([(x["change_type"], x["what"]) for x in c], [("CHANGED", "symbol AAA.N0000 -> None")])

    def test_results_and_aging(self):
        due = lambda d, st="UPCOMING": ev("RESULTS_DUE", d, kind="estimated", status=st, src=("report_document", "1:Q2:2026-09-30"),
                                          detail="Q2 for period ending 2026-09-30")
        old = [due("2026-11-13"), ev("TRADING_HALT", "2025-11-01"),
               ev("RESULTS_DUE", "2026-11-12", cid=2, kind="estimated", src=("report_document", "2:Q2:2026-09-30"), detail="Q2 for period ending 2026-09-30", ticker="BBB"),
               ev("RESULTS_DUE", "2026-11-14", cid=3, kind="estimated", status="UPCOMING", src=("report_document", "3:Q2:2026-09-30"), detail="Q2 for period ending 2026-09-30", ticker="CCC")]
        new = [ev("RESULTS_FILED", "2026-11-12", kind="confirmed", src=("getFinancialAnnouncement", "99"), detail="Q2 period ending 2026-09-30"),
               ev("RESULTS_DUE", "2026-11-14", cid=3, kind="estimated", status="OVERDUE", src=("report_document", "3:Q2:2026-09-30"), detail="Q2 for period ending 2026-09-30", ticker="CCC"),
               ev("RESULTS_DUE", "2027-02-11", cid=3, kind="estimated", src=("report_document", "3:Q3:2026-12-31"), detail="Q3 for period ending 2026-12-31", ticker="CCC")]
        self.assertEqual(self.kinds(old, new), [
            ("AGED_OUT", "TRADING_HALT", "2025-11-01", None),
            ("CHANGED", "RESULTS_DUE", "2026-11-14", "2026-11-14"),
            ("ESTIMATE_DROPPED", "RESULTS_DUE", "2026-11-12", None),
            ("FILED", "RESULTS_DUE", "2026-11-13", None),
            ("NEW", "RESULTS_FILED", None, "2026-11-12"),
            ("NEW_ESTIMATE", "RESULTS_DUE", None, "2027-02-11")])

    def test_sanity_limits(self):
        old = [ev("CASH_DIV_XD", f"2026-12-{d:02d}", cid=d) for d in range(1, 31)]
        new = old[:26]
        ch = ED.diff(old, new, "2025-11-14")
        self.assertEqual(ED.sanity(ch, old, new, "2026-11-14"), [])           # 4 removed <= max(10, 6); 87% kept
        new = old[:15]
        ch = ED.diff(old, new, "2025-11-14")
        self.assertEqual(len(ED.sanity(ch, old, new, "2026-11-14")), 2)        # 15 removed > 10, and < 85% kept

    def test_store_and_report(self):
        db = sqlite3.connect(":memory:")
        ch = ED.diff([ev("AGM", "2026-12-01")], [ev("AGM", "2026-12-08"), ev("TRADING_HALT", "2025-10-01")], "2025-11-14")
        ED.store(db, ch, "2026-11-14T18:30:00+05:30", "2026-11-14")
        self.assertEqual(db.execute("select change_type, old_date, new_date from company_event_change order by 1").fetchall(),
                         [("MOVED", "2026-12-01", "2026-12-08"), ("NEW", None, "2025-10-01")])
        md = ED.report(ch, "2026-11-14")
        self.assertIn("## Moved dates (1)", md)
        self.assertIn("AGM AAA : 2026-12-01 -> 2026-12-08", md)


class SwapGuard(unittest.TestCase):
    def test_swap_refused_when_main_changed_and_done_otherwise(self):
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "db").mkdir()
            main, work = d / "db" / "cse_financials.db", d / "db" / "work_events.db"
            main.write_text("old"); work.write_text("new")
            with mock.patch.object(R, "MAIN", main), mock.patch.object(R, "WORK", work), mock.patch.object(R, "ROOT", d):
                fp = R.fingerprint(main)
                os.utime(main, ns=(1, 1))                       # another writer touched main
                self.assertFalse(R.swap(fp))
                self.assertEqual(main.read_text(), "old")
                fp = R.fingerprint(main)
                self.assertTrue(R.swap(fp))
                self.assertEqual(main.read_text(), "new")
                self.assertFalse(work.exists())
                backups = list((d / "db").glob("backup_events_*.db"))
                self.assertEqual([b.read_text() for b in backups], ["old"])

    def test_same_day_report_appends(self):
        with tempfile.TemporaryDirectory() as d:
            md, js = pathlib.Path(d) / "r.md", pathlib.Path(d) / "r.json"
            R.write_report(md, js, "# Event calendar changes - X\nfirst", json.dumps([{"a": 1}]))
            R.write_report(md, js, "# Event calendar changes - X\nsecond", json.dumps([{"a": 2}]))
            t = md.read_text()
            self.assertTrue(t.startswith("# Event calendar changes - X\nfirst"))
            self.assertIn("second", t)
            self.assertEqual([x["a"] for x in json.loads(js.read_text())], [1, 2])

    def test_journal_counts_as_busy(self):
        with tempfile.TemporaryDirectory() as d:
            m = pathlib.Path(d) / "x.db"
            m.write_text("x")
            self.assertEqual(R.fingerprint(m)[3], ())
            pathlib.Path(str(m) + "-journal").write_text("j")
            self.assertEqual(R.fingerprint(m)[3], ("-journal",))


# ---- rights letters ------------------------------------------------------------------------------------------
def ms(y, m, d):
    return int(dt.datetime(y, m, d, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))).timestamp() * 1000)


MFPE_TEXT = """Chart Business Systems (Private) Limited 09th January 2026 Dear Madam, MAHARAJA FOODS PLC - RIGHTS ISSUE DATES
Further to the approval of the Colombo Stock Exchange on 09th January 2026, of the captioned Rights Issue, please note
that the following are the dates for carrying on the process pertaining to this Rights Issue.
I. The date of the Extraordinary General Meeting - 30th January 2026 II. XR date - 02nd February 2026
III. Record date and Date of Provisional allotment - 03rd February 2026 IV. Date of dispatch of letters of provisional
allotment - 11th February 2026 V. Trade commencement date of Rights - 17th February 2026
VI. Last date of renunciation to CDS - 24th February 2026 VII. Last date of acceptance and payment for Rights
- 27th February 2026 Your Faithfully, Company Secretaries to Maharaja Foods PLC 09th January 2026"""
MFPE_CAL = {"xr": ms(2026, 2, 2), "recordDate": ms(2026, 2, 3), "allotment": ms(2026, 2, 11), "tradingCommencement": ms(2026, 2, 17)}


class Letters(unittest.TestCase):
    def test_clean_letter_read_and_confirmed_by_calendar(self):
        r = LT.read_text(MFPE_TEXT, MFPE_CAL)
        self.assertEqual((r["status"], r["renunciation"], r["acceptance"]), ("READ", "2026-02-24", "2026-02-27"))
        self.assertTrue(any(c.startswith("recordDate") for c in r["checks"]))

    def test_calendar_mismatch_refused(self):
        cal = dict(MFPE_CAL, recordDate=ms(2026, 2, 4))
        r = LT.read_text(MFPE_TEXT, cal)
        self.assertEqual(r["status"], "NEEDS_READING")
        self.assertIn("disagrees", r["reason"])

    def test_garbled_dates_refused(self):
        t = MFPE_TEXT.replace("24th February 2026", "23,a Aprl,2026")
        self.assertEqual(LT.read_text(t, MFPE_CAL)["status"], "NEEDS_READING")

    def test_scan_and_missing_label_refused(self):
        self.assertIn("text layer", LT.read_text("   ", MFPE_CAL)["reason"])
        t = MFPE_TEXT.replace("Last date of acceptance and payment for Rights", "Final payment")
        self.assertIn("label for acceptance", LT.read_text(t, MFPE_CAL)["reason"])

    def test_unverified_text_layer_refused(self):
        self.assertIn("unverified", LT.read_text(MFPE_TEXT, {})["reason"])

    def test_order_rules(self):
        d = dt.date
        self.assertIsNone(LT.order_problems(d(2026, 2, 24), d(2026, 2, 27), MFPE_CAL))
        self.assertIn("before renunciation", LT.order_problems(d(2026, 2, 27), d(2026, 2, 24), MFPE_CAL))
        self.assertIn("before trading start", LT.order_problems(d(2026, 2, 10), d(2026, 2, 12), MFPE_CAL))
        self.assertIn("35 days", LT.order_problems(d(2026, 3, 20), d(2026, 3, 30), MFPE_CAL))

    def test_reviewed_reading_wins(self):
        with tempfile.TemporaryDirectory() as dd:
            raw = pathlib.Path(dd)
            (raw / "letters").mkdir()
            (raw / "letters" / "5.json").write_text(json.dumps({"status": "READ", "renunciation": "2026-02-24", "acceptance": "2026-02-27"}))
            self.assertEqual(LT.letter_dates(5, raw, {})[2], "rightsLetter")
            rv = {5: {"renunciation": "2026-02-25", "acceptance": "2026-02-27"}}
            self.assertEqual(LT.letter_dates(5, raw, rv), (dt.date(2026, 2, 25), dt.date(2026, 2, 27), "rightsLetterReviewed"))
            self.assertIsNone(LT.letter_dates(6, raw, {}))


class DetailFallback(unittest.TestCase):
    def test_general_record_only_when_id_and_symbol_match(self):
        with tempfile.TemporaryDirectory() as dd:
            root = pathlib.Path(dd)
            c = root / "raw_api" / "events"
            c.mkdir(parents=True)
            (c / "getAnnouncementById__announcementId-7.json").write_text(json.dumps(rec("2026-11-14T00:00:00+00:00", None)))
            (c / "getGeneralAnnouncementById__announcementId-7.json").write_text(json.dumps(rec("2026-11-14T00:00:00+00:00",
                {"reqBaseAnnouncement": {"id": 7, "symbol": "BERU"}, "reqAnnouncementDocs": []})))
            self.assertEqual(F.detail_record(7, "BERU", root)["reqBaseAnnouncement"]["symbol"], "BERU")
            self.assertIsNone(F.detail_record(7, "LWL", root))           # another company's record: refused
            (c / "getGeneralAnnouncementById__announcementId-8.json").write_text(json.dumps(rec("x", {"reqBaseAnnouncement": {"id": 9, "symbol": "BERU"}})))
            self.assertIsNone(F.detail_record(8, "BERU", root))           # id mismatch: refused


class RefreshGuard(unittest.TestCase):
    def test_empty_data_dir_refused_before_any_request(self):
        with tempfile.TemporaryDirectory() as dd:
            d = pathlib.Path(dd)
            with mock.patch.object(R, "ROOT", d), mock.patch.object(R, "MAIN", d / "db" / "x.db"), \
                    mock.patch.object(R, "LOGDIR", d / "logs"), mock.patch.object(R, "run") as run, \
                    mock.patch.object(sys, "argv", ["refresh_events.py"]):
                with self.assertRaises(SystemExit) as cm:
                    R.main()
                self.assertEqual(cm.exception.code, 1)
                run.assert_not_called()
                self.assertIn("pull it first", json.loads((d / "logs" / "LAST_RUN.json").read_text())["error"])


class TursoTx(unittest.TestCase):
    def make(self, step_error_at=None):
        t = SY.Turso("libsql://x", "tok")
        sent = []

        def post(reqs):
            sent.append(reqs)
            if reqs[0]["type"] == "batch":
                steps = reqs[0]["batch"]["steps"]
                errs = [None] * len(steps)
                res = [{"cols": [], "rows": []}] * len(steps)
                if step_error_at is not None and step_error_at < len(steps):
                    errs[step_error_at] = {"message": "UNIQUE constraint failed"}
                    res = res[:step_error_at] + [None] * (len(steps) - step_error_at)
                return {"baton": "b1", "results": [{"type": "ok", "response": {"type": "batch", "result": {"step_results": res, "step_errors": errs}}}]}
            return {"baton": None, "results": [{"type": "ok", "response": {"type": "execute", "result": {"cols": [], "rows": []}}}]}
        t._post = post
        return t, sent

    def test_steps_are_chained_and_begin_commit_wrap(self):
        t, sent = self.make()
        tx = SY.Tx(t)
        tx.add("DELETE FROM a"); tx.add("INSERT INTO a VALUES (?)", [1]); tx.commit()
        steps = sent[0][0]["batch"]["steps"]
        self.assertEqual([s["stmt"]["sql"] for s in steps], ["BEGIN", "DELETE FROM a", "INSERT INTO a VALUES (?)", "COMMIT"])
        self.assertNotIn("condition", steps[0])
        self.assertTrue(all(s["condition"] == {"type": "ok", "step": i} for i, s in enumerate(steps[1:])))
        self.assertEqual(sent[0][-1], {"type": "close"})

    def test_error_rolls_back(self):
        t, sent = self.make(step_error_at=1)
        tx = SY.Tx(t, chunk_bytes=10)
        with self.assertRaises(SY.Fail):
            tx.add("DELETE FROM a"); tx.add("INSERT INTO a VALUES (1)"); tx.add("INSERT INTO a VALUES (2)"); tx.commit()
        self.assertEqual(sent[-1][0]["stmt"]["sql"], "ROLLBACK")

    def test_query_inside_open_transaction_refused(self):
        t, _ = self.make()
        tx = SY.Tx(t, chunk_bytes=10)
        tx.add("DELETE FROM a")                 # flushed: stream open
        with self.assertRaises(SY.Fail):
            t.query("select 1")


if __name__ == "__main__":
    unittest.main()
