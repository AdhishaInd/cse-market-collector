"""Tests for the corporate-action capture: discovery, detail identity checks, the incremental builder (attach / supersede /
postpone / both classes / queues / idempotence / id continuity), the pull into a local main database, and the public
log. No network: details and feeds are fixture files; the CSE client is replaced.
Run: python3 -I actions/pipeline/test_actions_incremental.py"""
import contextlib, datetime as dt, io, json, os, pathlib, sqlite3, subprocess, sys, tempfile, unittest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import actions_sync as S         # noqa: E402
import build_actions as B        # noqa: E402
import cse_client as C           # noqa: E402
import notices as N              # noqa: E402
import refresh_actions as R      # noqa: E402

TODAY = dt.date(2026, 11, 14)
SYMBOLS = ("AAA", "BBB", "CCC", "ALPHA", "BETA", "GAMMA")
INPUT_DDL = """
CREATE TABLE company (company_id INTEGER PRIMARY KEY, ticker TEXT NOT NULL UNIQUE, name TEXT NOT NULL UNIQUE);
CREATE TABLE security (security_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL REFERENCES company(company_id), symbol TEXT NOT NULL UNIQUE,
    share_class TEXT NOT NULL, listing_status TEXT NOT NULL, UNIQUE (company_id, security_id));
CREATE TABLE announcement (announcement_pk INTEGER PRIMARY KEY, cse_format TEXT NOT NULL, cse_announcement_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL REFERENCES company(company_id), category_raw TEXT NOT NULL, normalized_category TEXT NOT NULL, announced_on TEXT,
    detail_status TEXT NOT NULL DEFAULT 'NOT_FETCHED', cse_detail_type TEXT, remarks TEXT, file_url TEXT, UNIQUE (cse_format, cse_announcement_id));
CREATE TABLE corporate_action (action_id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL, security_id INTEGER NOT NULL, action_type TEXT NOT NULL,
    announced_on TEXT, dividend_stage TEXT, financial_year TEXT, per_share NUMERIC, ratio_text TEXT, shares_before INTEGER, shares_after INTEGER,
    shares_issued INTEGER, issue_price NUMERIC, adjustment_factor NUMERIC, record_date TEXT, ex_date TEXT, effective_date TEXT, payment_date TEXT,
    detail_level TEXT NOT NULL, FOREIGN KEY (company_id, security_id) REFERENCES security(company_id, security_id));
CREATE UNIQUE INDEX ux_action_ex_date ON corporate_action(security_id, action_type, ex_date, COALESCE(per_share, -1)) WHERE ex_date IS NOT NULL;
CREATE TABLE corporate_action_announcement (action_id INTEGER NOT NULL REFERENCES corporate_action(action_id),
    announcement_pk INTEGER NOT NULL REFERENCES announcement(announcement_pk), PRIMARY KEY (action_id, announcement_pk));
"""


def ms(y, m, d):
    return int(dt.datetime(y, m, d, tzinfo=N.COLO).timestamp() * 1000)


def input_db(path):
    db = sqlite3.connect(path)
    db.executescript(INPUT_DDL)
    db.executemany("insert into company values (?,?,?)", [(1, "AAA", "ALPHA HOLDINGS PLC"), (2, "BBB", "BETA BANK PLC"), (3, "CCC", "GAMMA PLC")])
    db.executemany("insert into security values (?,?,?,?,?)", [(10, 1, "AAA.N0000", "VOTING", "LISTED"), (20, 2, "BBB.N0000", "VOTING", "LISTED"),
                                                             (21, 2, "BBB.X0000", "NON_VOTING", "LISTED"), (30, 3, "CCC.N0000", "VOTING", "LISTED")])
    db.execute("insert into announcement values (500,'NEW',30000,3,'CASH DIVIDEND','cash dividend','2026-11-01','FETCHED','CashDividendWithDates',null,null)")
    db.execute("insert into corporate_action (action_id, company_id, security_id, action_type, announced_on, per_share, ex_date, detail_level) "
               "values (4182, 3, 30, 'CASH_DIVIDEND', '2026-11-01', 1.5, '2026-11-12', 'STRUCTURED')")
    db.execute("insert into corporate_action_announcement values (4182, 500)")
    db.commit()
    return db


class Fixture(unittest.TestCase):
    """A data directory like the one `actions_sync.py pull` builds: db/actions.db and raw_api/."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        (self.root / "db").mkdir()
        (self.root / "raw_api" / "actions").mkdir(parents=True)
        (self.root / "raw_api" / "events").mkdir(parents=True)
        db = input_db(self.root / "db" / "actions.db")
        B.ensure_schema(db)
        db.commit()
        db.close()
        self.feed, self.n = {}, 0

    def tearDown(self):
        self.tmp.cleanup()

    def notice(self, aid, cat, company, on, detail, general=False):
        """Put a notice in today's feed snapshot and its detail in the cache."""
        self.feed[aid] = {"announcementId": aid, "announcementCategory": cat, "company": company, "symbol": None,
                          "dateOfAnnouncement": on.strftime("%d %b %Y")}
        if detail is not None:
            ep = "getGeneralAnnouncementById" if general else "getAnnouncementById"
            if general:
                (self.root / "raw_api/actions" / f"getAnnouncementById__announcementId-{aid}.json").write_text(json.dumps({"_fetched_at": "x", "data": None}))
            (self.root / "raw_api/actions" / f"{ep}__announcementId-{aid}.json").write_text(json.dumps({"_fetched_at": "x", "data": detail}))

    def run_build(self, reviewed=None, decisions=None, allow_large=False):
        (self.root / "raw_api/actions" / "approvedAnnouncement__day-2026-11-14.json").write_text(
            json.dumps({"_fetched_at": "x", "data": {"approvedAnnouncements": list(self.feed.values())}}))
        self.n += 1
        run_at = f"2026-11-14T18:{30 + self.n:02d}:00+05:30"
        b, stopped = R.build(self.root, run_at, TODAY, None, allow_large, reviewed or {}, decisions or {})
        os.replace(self.root / "db" / "work_actions.db", self.root / "db" / "actions.db")
        b.db = sqlite3.connect(self.root / "db" / "actions.db")
        return b

    def q(self, sql, *args):
        db = sqlite3.connect(self.root / "db" / "actions.db")
        try:
            return db.execute(sql, args).fetchall()
        finally:
            db.close()


def rights(aid, sym, on, ratio="1 for 10", shares=1000000, price=25.0, docs=()):
    return {"reqBaseAnnouncement": {"id": aid, "dType": "RightsIssue", "symbol": sym, "dateOfAnnouncement": on, "remarks": "Rights Issue 2026",
                                    "votingShrsPropToBeIssued": ratio, "numOfVotingShrsIssued": shares, "votingShareConsideration": price},
            "reqAnnouncementDocs": [{"fileUrl": d, "contentType": "application/pdf"} for d in docs]}


def general(aid, sym, on, title, remarks="", docs=("cmt/x.pdf",), **dates):
    b = {"id": aid, "symbol": sym, "dateOfAnnouncement": on, "title": title, "remarks": remarks}
    b.update(dates)
    return {"reqBaseAnnouncement": b, "reqAnnouncementDocs": [{"fileUrl": d, "contentType": "application/pdf"} for d in docs]}


class Notices(unittest.TestCase):
    def test_kinds_and_follow_up(self):
        km = N.load_kind_map()
        self.assertEqual(N.kinds_for("rights issue dates", km), ({"RIGHTS_ISSUE"}, "map"))
        self.assertEqual(N.kinds_for("rights issue postponement", km), ({"RIGHTS_ISSUE"}, "rule"))
        self.assertEqual(N.kinds_for("notification on the listing of shares", km, "Listing of shares by way of a Scrip Dividend")[0], {"SCRIP_DIVIDEND"})
        self.assertEqual(N.kinds_for("debenture issue", km), (set(), None))
        self.assertEqual(N.follow_up_type("rights issue dates"), "dates")
        self.assertEqual(N.follow_up_type("rights issue amended dates"), "amended")
        self.assertEqual(N.follow_up_type("rights issue", "Further to the announcement dated 23rd July 2026"), "further")
        self.assertEqual(N.follow_up_type("rights issue", "Level of Subscription - Rights Issue"), "subscription")
        self.assertEqual(N.follow_up_type("repurchase of shares", "Ceylon X PLC - Repurchase of Shares - Dates"), "dates")
        self.assertIsNone(N.follow_up_type("cash dividend dates to be notified"))
        self.assertIsNone(N.follow_up_type("rights issue", "Rights Issue - 2026"))

    def test_referenced_dates(self):
        self.assertEqual(N.referenced_dates("Further to the announcement dated 23rd July 2026-Rights issue"), {"2026-07-23"})
        self.assertEqual(N.referenced_dates("XD 24 Sep 2026; payment 2 Oct 2026"), set())

    def test_check_detail_identity(self):
        d = {"reqBaseAnnouncement": {"id": 7, "symbol": "AAA"}}
        self.assertEqual(N.check_detail("NEW", 7, d, "AAA")[0], "FETCHED")
        self.assertEqual(N.check_detail("NEW", 7, d, "BBB")[0], "COMPANY_MISMATCH")
        self.assertEqual(N.check_detail("NEW", 8, d, "AAA")[0], "COMPANY_MISMATCH")
        self.assertEqual(N.check_detail("OLD", 7, {"infoAnnouncement": {"securityId": 2}}, None, 1)[0], "COMPANY_MISMATCH")
        self.assertEqual(N.check_detail("OLD", 7, {"infoAnnouncement": {"securityId": 1}}, None, 1)[0], "FETCHED")
        self.assertEqual(N.check_detail("NEW", 7, None, "AAA")[0], "NO_DETAIL")

    def test_price_traps(self):
        self.assertTrue(N.consideration_is_total({"issue_price": 2009028227, "shares_issued": 100}))
        self.assertTrue(N.price_inconsistent({"issue_price": 0.1, "shares_issued": 730468, "_total": 11687500}))
        self.assertFalse(N.price_inconsistent({"issue_price": 11.1, "shares_issued": 6117161, "_total": 79882930.5}))
        self.assertFalse(N.price_inconsistent({"issue_price": 8.7, "shares_issued": 4553230, "_total": 39613109.34}))


class DetailFetch(unittest.TestCase):
    """The wrong endpoint silently returns another company's record: always check the company."""
    class Fake:
        def __init__(self, answers):
            self.answers, self.calls, self.requests = answers, [], 0

        def call(self, ep, verb="POST", refresh=False, **params):
            self.calls.append(ep)
            self.requests += 1
            return self.answers.get(ep), False

    def test_new_fallback_only_when_id_and_symbol_match(self):
        with tempfile.TemporaryDirectory() as d:
            ok = self.Fake({"getAnnouncementById": None, "getGeneralAnnouncementById": {"reqBaseAnnouncement": {"id": 7, "symbol": "AAA"}}})
            det, st, n = C.get_detail(d, "NEW", 7, "AAA", 1, N.check_detail, ok)
            self.assertEqual((st, ok.calls), ("FETCHED", ["getAnnouncementById", "getGeneralAnnouncementById"]))
            bad = self.Fake({"getAnnouncementById": None, "getGeneralAnnouncementById": {"reqBaseAnnouncement": {"id": 7, "symbol": "ZZZ"}}})
            det, st, n = C.get_detail(d, "NEW", 7, "AAA", 1, N.check_detail, bad)
            self.assertEqual((det, st), (None, "COMPANY_MISMATCH"))
            wrong = self.Fake({"getAnnouncementById": {"reqBaseAnnouncement": {"id": 7, "symbol": "ZZZ"}}})
            det, st, n = C.get_detail(d, "NEW", 7, "AAA", 1, N.check_detail, wrong)
            self.assertEqual((det, st, wrong.calls), (None, "COMPANY_MISMATCH", ["getAnnouncementById"]))   # no second try

    def test_old_ids_use_their_own_endpoint_and_company(self):
        with tempfile.TemporaryDirectory() as d:
            f = self.Fake({"announcementById": {"infoAnnouncement": {"securityId": 99, "body": "x"}}})
            det, st, n = C.get_detail(d, "OLD", 5, None, 1, N.check_detail, f)
            self.assertEqual((det, st, f.calls), (None, "COMPANY_MISMATCH", ["announcementById"]))

    def test_cached_copy_used_without_request(self):
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "raw_api/events"
            p.mkdir(parents=True)
            (p / "getAnnouncementById__announcementId-7.json").write_text(json.dumps({"_fetched_at": "x", "data": {"reqBaseAnnouncement": {"id": 7, "symbol": "AAA"}}}))
            f = self.Fake({})
            det, st, n = C.get_detail(d, "NEW", 7, "AAA", 1, N.check_detail, f)
            self.assertEqual((st, n, f.calls), ("FETCHED", 0, []))

    def test_stop_on_waf_and_budget(self):
        with tempfile.TemporaryDirectory() as d:
            c = C.Client(d, max_requests=1, min_gap=0)
            import urllib.request
            class Resp:
                def read(self):
                    return b"<html>blocked</html>"
            orig = urllib.request.urlopen
            urllib.request.urlopen = lambda *a, **k: Resp()
            try:
                with self.assertRaises(C.Stop):
                    c.call("getAnnouncementById", announcementId=1)
                with self.assertRaises(C.Budget):
                    c.call("getAnnouncementById", announcementId=2)
            finally:
                urllib.request.urlopen = orig
            self.assertFalse(c.path("getAnnouncementById", announcementId=1).exists())    # a refused reply is not cached


class Builder(Fixture):
    def test_proposal_creates_one_action_and_dates_notice_attaches(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 10), rights(40001, "AAA", "10 Nov 2026"))
        b = self.run_build()
        acts = self.q("select action_id, security_id, ratio_text, issue_price, shares_issued, ex_date from ac_action")
        self.assertEqual(acts, [(4183, 10, "1 for 10", 25.0, 1000000, None)])            # id continues the table (max 4182)
        self.assertEqual(b.counts.get("actions_created"), 1)
        self.notice(40002, "RIGHTS ISSUE (DATES)", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 13),
                    general(40002, "AAA", "13 Nov 2026", "RIGHTS ISSUE (DATES)", xr=ms(2026, 11, 20), recordDate=ms(2026, 11, 21),
                            tradingCommencement=ms(2026, 12, 1), votingProportion="1 for 10"), general=True)
        b = self.run_build()
        self.assertEqual(self.q("select count(*) from ac_action")[0][0], 1)
        self.assertEqual(self.q("select ex_date, record_date, effective_date, ratio_text from ac_action"), [("2026-11-20", "2026-11-21", "2026-12-01", "1 for 10")])
        self.assertEqual(self.q("select evidence from ac_link where announcement_pk=502"), [("only_open",)])
        self.assertEqual(self.q("select count(*) from ac_queue"), [(0,)])

    def test_amended_dates_supersede_and_postponement(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 1), rights(40001, "AAA", "01 Nov 2026"))
        self.notice(40002, "RIGHTS ISSUE (DATES)", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 3),
                    general(40002, "AAA", "03 Nov 2026", "RIGHTS ISSUE (DATES)", xr=ms(2026, 11, 20)), general=True)
        self.notice(40003, "RIGHTS ISSUE (AMENDED DATES)", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 5),
                    general(40003, "AAA", "05 Nov 2026", "RIGHTS ISSUE (AMENDED DATES)", "Further to the announcement dated 3rd November 2026", xr=ms(2026, 11, 27)), general=True)
        self.run_build()
        self.assertEqual(self.q("select ex_date from ac_action"), [("2026-11-27",)])
        hist = self.q("select value, status, source_pk from ac_value where field='ex_date' order by value_id")
        self.assertEqual(hist, [("2026-11-20", "SUPERSEDED", 502), ("2026-11-27", "CURRENT", 503)])
        self.assertEqual(self.q("select evidence from ac_link where announcement_pk=503"), [("text",)])
        # postponement: the dates are withdrawn (kept as superseded), the action is open again
        self.notice(40004, "RIGHTS ISSUE - POSTPONEMENT", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 8),
                    general(40004, "AAA", "08 Nov 2026", "RIGHTS ISSUE - POSTPONEMENT", "Further to the announcement dated 5th November 2026"), general=True)
        b = self.run_build()
        self.assertEqual(self.q("select ex_date, status from ac_action"), [(None, "POSTPONED")])
        self.assertEqual(b.counts.get("postponed"), 1)
        self.assertEqual(self.q("select count(*) from ac_value where field='ex_date' and status='SUPERSEDED'"), [(2,)])
        self.notice(40005, "RIGHTS ISSUE (DATES)", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 12),
                    general(40005, "AAA", "12 Nov 2026", "RIGHTS ISSUE (DATES)", xr=ms(2026, 12, 10)), general=True)
        self.run_build()
        self.assertEqual(self.q("select ex_date, status from ac_action"), [("2026-12-10", "ACTIVE")])
        self.assertEqual(self.q("select count(*) from ac_action"), [(1,)])

    def test_notice_naming_both_classes_applies_to_both(self):
        d = {"reqBaseAnnouncement": {"id": 40010, "dType": "ScripDividendToBeNotified", "symbol": "BBB", "votingPropotion": "1 for 50",
                                     "votingNumSharesIssued": 1000, "nonVotingPropotion": "1 for 50", "nonVotingNumSharesIssued": 100}}
        self.notice(40010, "SCRIP DIVIDEND", "BETA BANK PLC", dt.date(2026, 11, 2), d)
        self.notice(40011, "SCRIP DIVIDEND (DATES) - VOTING AND NON VOTING", "BETA BANK PLC", dt.date(2026, 11, 6),
                    general(40011, "BBB", "06 Nov 2026", "SCRIP DIVIDEND (DATES)", "Scrip dividend dates - Voting and Non-Voting shares", xd=ms(2026, 11, 25)), general=True)
        self.run_build()
        self.assertEqual(self.q("select security_id, ex_date from ac_action order by security_id"), [(20, "2026-11-25"), (21, "2026-11-25")])
        self.assertEqual(self.q("select count(*) from ac_link where announcement_pk=502"), [(2,)])

    def test_unlinked_dates_notice_goes_to_review(self):
        self.notice(40020, "SUB-DIVISION OF SHARES (DATES)", "GAMMA PLC", dt.date(2026, 11, 6),
                    general(40020, "CCC", "06 Nov 2026", "SUB-DIVISION OF SHARES (DATES)", votingProportion="1 into 2",
                            subDivisionBasedOnShareholdingsAsAt=ms(2026, 11, 20)), general=True)
        b = self.run_build()
        self.assertEqual(self.q("select action_type from ac_action"), [("SUB_DIVISION",)])
        self.assertEqual(self.q("select queue, item_key like 'unlinked:%' from ac_queue"), [("needs-review", 1)])

    def test_rerun_is_idempotent(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 10), rights(40001, "AAA", "10 Nov 2026"))
        self.notice(40030, "CASH DIVIDEND", "GAMMA PLC", dt.date(2026, 11, 11),
                    {"reqBaseAnnouncement": {"id": 40030, "dType": "CashDividendWithDates", "symbol": "CCC", "votingDivPerShare": 2.0,
                                             "xd": "20 Nov 2026", "payment": "01 Dec 2026", "typeFirstInt": True}})
        self.run_build()
        snap = [self.q(f"select * from {t} order by 1") for t in ("ac_announcement", "ac_action", "ac_link", "ac_value", "ac_queue")]
        b = self.run_build()
        self.assertEqual([self.q(f"select * from {t} order by 1") for t in ("ac_announcement", "ac_action", "ac_link", "ac_value", "ac_queue")], snap)
        self.assertEqual(b.counts.get("actions_created", 0) + b.counts.get("notices_stored", 0), 0)
        self.assertEqual(self.q("select action_id from ac_action order by 1"), [(4183,), (4184,)])

    def test_id_floor_and_existing_ids_never_reused(self):
        db = sqlite3.connect(self.root / "db" / "actions.db")
        db.execute("insert into ac_meta values ('floor_action_id', '5000', 'x')")
        db.execute("insert into ac_meta values ('floor_announcement_pk', '60000', 'x')")
        db.commit(); db.close()
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 10), rights(40001, "AAA", "10 Nov 2026"))
        self.run_build()
        self.assertEqual(self.q("select action_id from ac_action"), [(5001,)])
        self.assertEqual(self.q("select announcement_pk from ac_announcement"), [(60001,)])

    def test_follow_up_attaches_to_existing_main_action_and_keeps_main_value(self):
        self.notice(40040, "DIVIDEND ANNOUNCEMENT (AMENDED DATES)", "GAMMA PLC", dt.date(2026, 11, 5),
                    {"reqBaseAnnouncement": {"id": 40040, "dType": "CashDividendWithDates", "symbol": "CCC", "votingDivPerShare": 1.5,
                                             "xd": "14 Nov 2026", "dateOfInitialAnnouncement": "01 Nov 2026", "remarks": "Amended dates"}})
        self.run_build()
        self.assertEqual(self.q("select count(*) from ac_action"), [(0,)])
        self.assertEqual(self.q("select action_id, evidence from ac_link"), [(4182, "structured")])
        self.assertEqual(self.q("select value, status, source_kind from ac_value where field='ex_date' order by value_id"),
                         [("2026-11-12", "SUPERSEDED", "main"), ("2026-11-14", "CURRENT", "structured")])
        self.assertEqual(self.q("select ex_date from corporate_action where action_id=4182"), [("2026-11-12",)])   # input row untouched

    def test_wrong_company_detail_refused(self):
        self.notice(40050, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 10), rights(40050, "ZZZ", "10 Nov 2026"))
        b = self.run_build()
        self.assertEqual(self.q("select detail_status from ac_announcement"), [("COMPANY_MISMATCH",)])
        self.assertEqual(self.q("select count(*) from ac_action"), [(0,)])
        self.assertEqual(b.counts.get("detail_refused_wrong_company"), 1)

    def test_scanned_letter_queued_then_reviewed_reading_applied(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 1), rights(40001, "AAA", "01 Nov 2026"))
        self.notice(40060, "RIGHTS ISSUE (DATES)", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 6),
                    general(40060, "AAA", "06 Nov 2026", "RIGHTS ISSUE (DATES)", docs=("cmt/letter.pdf",)), general=True)
        self.run_build()
        self.assertEqual(self.q("select queue, status, documents from ac_queue"), [("needs-reading", "OPEN", json.dumps(["https://cdn.cse.lk/cmt/letter.pdf"]))])
        self.assertEqual(self.q("select ex_date from ac_action"), [(None,)])
        rv = {40060: {"kind": "RIGHTS_ISSUE", "values": {"ex_date": "2026-11-24", "record_date": "2026-11-25"}, "read_from": "x page 1"}}
        b = self.run_build(reviewed=rv)
        self.assertEqual(self.q("select ex_date, record_date, detail_level from ac_action"), [("2026-11-24", "2026-11-25", "SCAN_REVIEWED")])
        self.assertEqual(self.q("select status from ac_queue"), [("RESOLVED",)])
        self.assertEqual(self.q("select distinct source_kind from ac_value where field='ex_date'"), [("reviewed",)])

    def test_possible_duplicate_and_same_as_decision(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 1), rights(40001, "AAA", "01 Nov 2026"))
        self.notice(40070, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 4), {"reqBaseAnnouncement": {"id": 40070, "dType": "RightsIssue", "symbol": "AAA"}})
        self.run_build()
        items = self.q("select item_key, action_id from ac_queue")
        self.assertEqual(len(items), 1)
        self.assertTrue(items[0][0].startswith("possible-duplicate:"))
        self.run_build(decisions={items[0][0]: {"decision": "same_as", "action_id": 4183, "note": "test"}})
        self.assertEqual(self.q("select action_id, status from ac_action order by 1"), [(4183, "ACTIVE"), (4184, "SAME_AS:4183")])
        self.assertIn((4183, 502), self.q("select action_id, announcement_pk from ac_link"))

    def test_same_ex_date_and_amount_is_the_same_event(self):
        self.notice(40080, "CASH DIVIDEND", "GAMMA PLC", dt.date(2026, 11, 3),
                    {"reqBaseAnnouncement": {"id": 40080, "dType": "CashDividendWithDates", "symbol": "CCC", "votingDivPerShare": 1.5, "xd": "12 Nov 2026"}})
        self.run_build()        # same ex-date and amount as action 4182: attached, not a second action
        self.assertEqual(self.q("select count(*) from ac_action"), [(0,)])
        self.assertEqual(self.q("select action_id, evidence from ac_link"), [(4182, "same_event")])


class PullIntoMain(Fixture):
    def make_main(self):
        p = self.root / "main.db"
        input_db(p).close()
        return p

    def rows(self):
        db = sqlite3.connect(self.root / "db" / "actions.db")
        db.row_factory = sqlite3.Row
        out = {t: [dict(r) for r in db.execute(f"select * from {t}")] for t in S.AC[:-1]}
        db.close()
        return out

    def test_pull_adds_rows_is_idempotent_and_keeps_backup(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 1), rights(40001, "AAA", "01 Nov 2026"))
        self.notice(40002, "RIGHTS ISSUE (DATES)", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 3),
                    general(40002, "AAA", "03 Nov 2026", "RIGHTS ISSUE (DATES)", xr=ms(2026, 11, 20)), general=True)
        self.notice(40040, "DIVIDEND ANNOUNCEMENT (AMENDED DATES)", "GAMMA PLC", dt.date(2026, 11, 5),
                    {"reqBaseAnnouncement": {"id": 40040, "dType": "CashDividendWithDates", "symbol": "CCC", "votingDivPerShare": 1.5,
                                             "xd": "14 Nov 2026", "dateOfInitialAnnouncement": "01 Nov 2026"}})
        self.run_build()
        main = self.make_main()
        rep, backup = S.pull_into(main, self.rows())
        self.assertEqual((rep["notices_added"], rep["actions_added"], rep["links_added"], rep["values_applied"]), (3, 1, 3, 1))
        self.assertTrue(backup.exists())
        db = sqlite3.connect(main)
        self.assertEqual(db.execute("select ex_date from corporate_action where action_id=4183").fetchone(), ("2026-11-20",))
        self.assertEqual(db.execute("select ex_date from corporate_action where action_id=4182").fetchone(), ("2026-11-14",))
        self.assertEqual(db.execute("select count(*) from action_value_history").fetchone()[0], 6)
        db.close()
        rep2, _ = S.pull_into(main, self.rows())
        self.assertEqual((rep2["notices_added"], rep2["actions_added"], rep2["links_added"], rep2["values_applied"]), (0, 0, 0, 0))

    def test_hand_reviewed_main_value_is_kept(self):
        self.notice(40040, "DIVIDEND ANNOUNCEMENT (AMENDED DATES)", "GAMMA PLC", dt.date(2026, 11, 5),
                    {"reqBaseAnnouncement": {"id": 40040, "dType": "CashDividendWithDates", "symbol": "CCC", "votingDivPerShare": 1.5,
                                             "xd": "14 Nov 2026", "dateOfInitialAnnouncement": "01 Nov 2026"}})
        self.run_build()
        main = self.make_main()
        db = sqlite3.connect(main)
        db.execute("update corporate_action set ex_date='2026-11-13' where action_id=4182")      # reviewed locally meanwhile
        db.commit(); db.close()
        rep, _ = S.pull_into(main, self.rows())
        self.assertEqual(len(rep["value_conflicts"]), 1)
        self.assertEqual(sqlite3.connect(main).execute("select ex_date from corporate_action where action_id=4182").fetchone(), ("2026-11-13",))

    def test_id_clash_refused_and_review_items_held(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 1), rights(40001, "AAA", "01 Nov 2026"))
        self.notice(40070, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 4), {"reqBaseAnnouncement": {"id": 40070, "dType": "RightsIssue", "symbol": "AAA"}})
        self.run_build()
        main = self.make_main()
        rep, _ = S.pull_into(main, self.rows(), dry_run=True)
        self.assertEqual((rep["actions_added"], rep["held_for_review"]), (1, 1))
        db = sqlite3.connect(main)
        db.execute("insert into corporate_action (action_id, company_id, security_id, action_type, detail_level) values (4183, 3, 30, 'REPURCHASE', 'TITLE_ONLY')")
        db.commit(); db.close()
        with self.assertRaises(S.Fail):
            S.pull_into(main, self.rows())


class PublicLog(Fixture):
    def test_log_lines_carry_no_symbols_or_names(self):
        self.notice(40001, "RIGHTS ISSUE", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 1), rights(40001, "AAA", "01 Nov 2026"))
        self.notice(40002, "RIGHTS ISSUE (DATES)", "ALPHA HOLDINGS PLC", dt.date(2026, 11, 3),
                    general(40002, "AAA", "03 Nov 2026", "RIGHTS ISSUE (DATES)"), general=True)
        (self.root / "raw_api/actions" / "approvedAnnouncement__day-2026-11-14.json").write_text(
            json.dumps({"_fetched_at": "x", "data": {"approvedAnnouncements": list(self.feed.values())}}))
        p = subprocess.run([sys.executable, "-I", str(HERE / "refresh_actions.py"), "--no-fetch"], capture_output=True, text=True,
                           env={**os.environ, "ACTIONS_ROOT": str(self.root)})
        self.assertEqual(p.returncode, 0, p.stderr)
        out = p.stdout + p.stderr
        self.assertIn("actions_created", out)
        for s in SYMBOLS + ("2026-11-0", "Nov 2026", "letter.pdf", "cmt/"):
            self.assertNotIn(s, out)
        status = json.loads((self.root / "logs" / "LAST_RUN.json").read_text())
        self.assertEqual(status["queues"], {"needs-reading": 1})
        self.assertIn("AAA", (self.root / "logs").joinpath(pathlib.Path(status["report"]).name).read_text())   # private report has them


class Sync(unittest.TestCase):
    def test_run_rows_only_this_run_and_no_local_meta(self):
        db = sqlite3.connect(":memory:")
        B.ensure_schema(db)
        db.execute("insert into ac_meta values ('local:inputs_corporate_action_pushed_at','x','R1')")
        db.execute("insert into ac_meta values ('floor_action_id','5','R1')")
        db.execute("insert into ac_queue values ('k','needs-review',1,2,'r','[]','OPEN','R0',null,null,'R0')")
        db.execute("insert into ac_queue values ('k2','needs-review',1,2,'r','[]','OPEN','R1',null,null,'R1')")
        out = S.run_rows(db, "R1")
        self.assertEqual([r[0] for r in out["ac_meta"][1]], ["floor_action_id"])
        self.assertEqual([r[0] for r in out["ac_queue"][1]], ["k2"])
        self.assertIn("ON CONFLICT(action_id,announcement_pk)", S.upsert_sql("ac_link", ["action_id", "announcement_pk", "evidence"]))


if __name__ == "__main__":
    unittest.main()
