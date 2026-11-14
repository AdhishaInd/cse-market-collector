# CSE event calendar

A daily job that keeps a calendar of dated company events on the Colombo Stock Exchange: dividends (XD, record and
payment dates), rights issues, scrip dividends, share splits, AGMs and EGMs, trading halts, watch-list changes,
new-listing notices, filed results and estimated results-due dates. Runs on GitHub Actions
(`.github/workflows/events.yml`). Data lives in a private Turso database (`cse-events`); this repository holds code
only, and the job's logs show counts only.

## What a run does

1. **Pull.** `events_sync.py pull` builds a working copy from Turso: the input tables (company, security, announcement,
   corporate_action, corporate_action_announcement, listing_event, report_document, plus sector and reporting_template),
   the event tables, and the cache of earlier API responses.
2. **Refresh.** `refresh_events.py` runs these steps:
   - **Fetch, incremental**, about 8 requests to `https://www.cse.lk/api/`, one at a time, 3-4 s apart, normal browser
     User-Agent, no login:
     - `corporateCalender` for this year: every announcement made this year with its current dates.
     - `getAgmEgmEvents` for the next 4 months.
     - The two rolling feeds `approvedAnnouncement` and `getFinancialAnnouncement`. They keep only a few days, so each
       day is saved.
     - Announcement details only for ids not seen before (`getAnnouncementById`; `getGeneralAnnouncementById` when that
       returns null, accepted only when the id and symbol match).
     - Rights issue letters not read yet (`letters.py`).

     A 429, 403 or non-JSON reply stops the run (exit 2).
   - **Load** into a copy of the working database (`load_events.py`). The loader refuses when the data looks wrong:
     fewer than 100 calendar announcements, more than 5% date conflicts with corporate_action, or an estimate without
     its method.
   - **Check the copy:**
     - the database integrity check passes;
     - as_of is today;
     - no other table changed;
     - not more than max(10, 20%) of upcoming confirmed events disappear;
     - the event count does not fall below 85%.
   - **Compare** old and new: NEW, MOVED, REMOVED (withdrawn or cancelled), CHANGED, FILED. The changes are appended to
     `company_event_change`.
3. **Push.** `events_sync.py push-results` replaces the event tables, appends the changes, uploads new cache files and
   stores the run (status and report) in `event_run`, all in one Turso transaction. The cache is pushed even when the
   load was refused, so the rolling-feed snapshots are never lost.

Schedule:
- 18:30 Colombo daily;
- 15:00 Colombo on the 10th-16th of Feb, May, Aug and Nov (filing weeks);
- a 22:00 retry that does nothing when today already succeeded.

A failure opens a GitHub issue (label `events-failure`); the next success closes it.

## The data (Turso `cse-events`)

| Table | Contents |
|---|---|
| `company_event` | One row per company, share class, event type and date. `date_kind` is `confirmed` (stated in an official notice), `announced_tentative` (still needs shareholder approval) or `estimated` (results-due only; `method` and `rule_deadline` say how). Also amount, detail, announced_on, and the source endpoint and id |
| `company_event_source` | Every source of each row |
| `company_event_change` | Daily changes, per run |
| `result_estimate_backtest` | Accuracy of the results-due estimate on past filings |
| `event_load_meta` | as_of, window, counts |
| `event_run` | One row per run: ok, exit code, counts, the full report |
| `event_cache` | Raw API responses and letter readings (gzip) |

Rights issues have two dates that exist only in the PDF letter: `RIGHTS_RENUNCIATION_LAST` and
`RIGHTS_ACCEPTANCE_LAST`. Their source is `rightsLetter` (read automatically) or `rightsLetterReviewed` (read from the
page image by a person or agent).

## Local commands

Credentials go in `~/.config/cse-events.env`, holding `TURSO_EVENTS_URL` and `TURSO_EVENTS_TOKEN` (the same values are
the repository secrets). Load them with `set -a; . ~/.config/cse-events.env; set +a`.

```
python3 -I events/pipeline/events_sync.py pull-events --main <path to cse_financials.db>   # events into the local main db (copy -> check -> swap)
python3 -I events/pipeline/events_sync.py push-inputs --main <path to cse_financials.db>   # after a local refresh of reports / announcements / corporate actions
python3 -I events/pipeline/events_sync.py needs-reading                                     # letters waiting for a reader
python3 -I events/pipeline/test_events_incremental.py                                       # tests, no network
gh workflow run events.yml -R AdhishaInd/cse-market-collector                               # run now
```

`push-inputs` sends only tables whose content changed. Until it runs, the job uses the inputs from the last push. New
dividends, meetings, halts and filings still arrive daily from the API; corporate actions and the report index catch
up at the next push.

## Reading flagged letters (on demand, for a person or an agent)

The automatic reader takes a letter's dates only when the text layer is clean and the letter's own record, XR or
trading dates match the calendar. Scans and garbled text are flagged, and the issue `Rights letters waiting to be read`
shows how many. To clear them:

1. `set -a; . ~/.config/cse-events.env; set +a; python3 -I events/pipeline/events_sync.py pull --root events/work`
2. `EVENTS_ROOT=events/work python3 -I events/pipeline/letters.py needs-reading` lists each letter with its document
   URL (taken from the API, never guessed) and the reason it was refused.
3. For each letter:
   - Download the PDF into a scratch folder.
   - Render each page to an image (pymupdf, about 150 dpi) and read the page image, not the text layer.
   - Record the **last date of renunciation** and the **last date of acceptance and payment** exactly as printed.
   - If the letter does not state them, record no dates and say so in `note`. The letter then leaves the queue.
4. Add the entry to `events/reviewed/letter_readings.json`:
   `"39979": {"renunciation": "2026-12-24", "acceptance": "2026-12-30", "read_from": "<url> page 1", "reader": "...", "read_on": "YYYY-MM-DD", "note": ""}`
5. `EVENTS_ROOT=events/work python3 -I events/pipeline/letters.py check-reviewed` must print OK. It checks the date
   order and the window around the calendar's trading start.
6. Commit and push. The next run, or `gh workflow run events.yml`, loads the dates.

## On failure

The run's status is in `event_run` (Turso) and in the job summary. The data in Turso changes only after every check
has passed.

| Exit | Meaning | What to do |
|---|---|---|
| 1 | A check failed (loader refusal, integrity, as_of, other tables touched, change limits) or the data directory was empty | Read the job log. For change limits, rerun locally with `--dry-run` and read the report. If the changes are real (e.g. a mass withdrawal), run once with `--allow-large-change`. |
| 2 | The CSE returned 429, 403 or a non-JSON page, or a feed came back empty | Do not rerun soon. The next scheduled run is soon enough. If it repeats, check by hand whether the endpoint moved. Endpoint names come only from the site's own JavaScript or the reference. |
| Turso error | Pull or push failed | The push is one transaction, so a failed push leaves the previous state. Rerun the workflow. |

**Feed gap** (`gaps` in the run status): two runs more than about 4 days apart, so announcements or filings between
them may be missing from the rolling feeds. Halts and meeting notices come back with the next local `push-inputs`
after an announcement-index refresh. The calendar's dividend, rights and split dates are not affected.

## Limits

- REMOVED needs a person: the CSE has no explicit "cancelled" flag.
- Results-due dates are estimates, with a median error of 2 days for interims and 3 days for annual reports.
- IPO subscription dates are still only in the offer documents.
