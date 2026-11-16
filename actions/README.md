# CSE corporate actions (daily capture)

A daily job that captures NEW Colombo Stock Exchange corporate-action notices (cash and scrip dividends, rights issues,
sub-divisions, consolidations, capitalisations, repurchases, capital reductions, private placements) and builds the
actions incrementally: existing action ids never change, a follow-up notice is attached to its action instead of
becoming a second one, and revised values keep their history. Runs on GitHub Actions (`.github/workflows/actions.yml`).
Data lives in the private Turso database `cse-events`, next to the event calendar; this repository holds code only, and
the job's public log and issues show counts only.

## What a run does

1. **Pull.** `actions_sync.py pull` builds `actions/work/db/actions.db` from Turso: the input copies of company,
   security, announcement, corporate_action and corporate_action_announcement (pushed from the local main database by
   `events/pipeline/events_sync.py push-inputs`), the job's own `ac_` tables, and the cached CSE responses: this job's
   (`ac_cache`) and, read only, the event calendar's feed snapshots, corporate calendar and announcement details.
2. **Refresh** (`refresh_actions.py`):
   - **Fetch** `approvedAnnouncement` (1 request), merged into today's snapshot.
   - **Discover** every notice in this job's and the event calendar's feed snapshots and in the corporate calendar that
     is not stored yet and was announced on or after (newest stored notice − 14 days). The company comes from the
     calendar's symbol or the exact normalised company name; an unresolved name is skipped and counted.
   - **Details**, oldest notice first, only for corporate-action notices and plain "notification on the listing of
     shares" notices (whose text says which issue they concern): cached copies first, else `getAnnouncementById`,
     then `getGeneralAnnouncementById` when that returned nothing. A record is used only when its id and symbol match
     the notice (old-format ids would use `announcementById` and must carry the same company id); another company's
     record is refused and the notice is stored as `COMPANY_MISMATCH`.
   - **Build** on a copy (`build_actions.py`), **check** the copy, **swap**. Requests go through `cse_client.py`: one at
     a time, ≥ 3 s apart, cached, at most 40 per run; HTTP 429/403, an HTML/WAF page or a non-JSON reply stops the run
     (exit 2) and nothing is retried. Notices left over when the budget runs out wait for the next run.
3. **Push.** `actions_sync.py push-results` writes this run's `ac_` rows, new cache files and the run record (`ac_run`)
   in one Turso transaction. The cache and run record are pushed even when the build was refused.

Schedule: 19:30 Colombo daily (after the event calendar at 18:30 and the evening filings), and a 23:00 retry that does
nothing when today already succeeded or the CSE refused a request today. A failure opens an issue (label `actions-failure`), closed by the next success.
The issue `Corporate actions waiting for review` (label `actions-queue`) shows the queue counts.

## How a notice becomes an action

Values are taken only as CSE prints them: the structured detail, the notice's own row in the feed or calendar (same
announcement id and symbol), labelled body text of old notices, or a reviewed reading of the letter. Nothing is
estimated; `adjustment_factor` stays NULL (the local apply computes factors). Value extraction is a port of the local
loader (`cse_financials/scripts/load_actions.py`).

A notice is **attached** to an existing action of the same security and kind when, in this order:

| Evidence | Rule |
|---|---|
| `text` | the notice names the date of a notice already linked to the action ("further to the announcement dated 23rd July 2026") |
| `structured` | CSE's detail links it (cash dividends: `dateOfInitialAnnouncement`) |
| `same_event` | same ex-date and the terms do not disagree (a restatement) |
| `only_open` | a dates / amended / postponement / listing / subscription / further notice, and exactly one open (undated) action announced in the 500 days before, with terms that do not disagree |
| `only_recent` | a listing / subscription / allotment / offer-document notice (these follow the event, so the action is usually dated) and exactly one action of that kind announced in the 180 days before |
| `same_terms` | rights, scrip or bonus: at least two of ratio, price and number of new shares printed on both and all equal, within 365 days |

Otherwise a **new action** gets the next id. A follow-up notice that links to nothing, and a new action while another
action of the same kind is open (or, except cash dividends, announced in the last 180 days), go to `needs-review`.

- **Revised values**: the new value becomes current with the notice as its source; the old one is kept as
  `SUPERSEDED` in `ac_value` (main's own value is recorded there too before it is replaced). A changed term (ratio,
  price, amount) also goes to `needs-review`.
- **Postponement**: the dates are superseded (current value NULL) and the action's status is `POSTPONED`; the next
  dates notice re-dates it (status `ACTIVE`).
- **Share classes**: a notice naming voting and non-voting shares applies to both securities.
- **CSE traps handled**: the "consideration" field holding the total consideration (price > Rs 100,000 or issue
  worth > Rs 1 trillion) or, for scrip dividends, the dividend per share (price × new shares not within a factor of 2
  of the amount capitalised): the price is not stored and the notice goes to `needs-review`.
- **Letters**: a dates / amended notice of a share issue that prints no date in any structured field has its dates
  only in the PDF. The job does not read PDFs; it queues the notice in `needs-reading`.

## The data (Turso `cse-events`, tables written by this job only)

| Table | Contents |
|---|---|
| `ac_announcement` | Captured notices, same columns as `announcement` (+ kinds, follow-up type, document urls, where found) |
| `ac_action` | New actions, same columns as `corporate_action` (+ status `ACTIVE` / `POSTPONED` / `SAME_AS:<id>`, created by notice) |
| `ac_link` | Notice → action links with their evidence (to new and to existing actions) |
| `ac_value` | Every value the job set: field, value, `CURRENT` / `SUPERSEDED`, source notice and kind (`structured`, `body`, `reviewed`, `postponement`, `main`) |
| `ac_queue` | `needs-review` and `needs-reading` items, `OPEN` / `RESOLVED` |
| `ac_meta` | Id floors set by `init` |
| `ac_run` | One row per run: ok, exit code, counts, the private report (with symbols) |
| `ac_cache` | Raw CSE responses of this job (gzip) |

The event calendar's tables (including its input copies) are only read.

## Who owns which ids

- From go-live, **the cloud job is the only writer of new `announcement_pk` and `action_id` values.** It allocates
  `max(input table, ac_ table, floor) + 1`. `init --main` sets the floor from the local main database and copies into
  the `ac_` tables any rows main has that the Turso input copy lacks (with main's ids, marked pulled), so a local refresh
  done before go-live is neither captured twice nor collided with.
- **The local machine no longer creates notice or action ids** (no `load_actions.py`, no staging inserts of new notices
  or actions). It pulls the job's rows into main (`pull-actions`, same ids). Local review results (corrections of
  values, `action_review`, factors) stay local and keep flowing up through `push-inputs`; for the job, main's value wins
  for anything pulled before the last `push-inputs` of `corporate_action`.
- `pull-actions` refuses on any clash (an id that names a different notice/action in main, or a notice main holds under
  another pk) and changes nothing.

## Local commands

Credentials: `set -a; . ~/.config/cse-events.env; set +a` (`TURSO_EVENTS_URL`, `TURSO_EVENTS_TOKEN`; the same values
are the repository secrets).

```
MAIN=/home/adhisha/stocks/cse_financials/db/cse_financials.db
python3 -I actions/pipeline/actions_sync.py init --main $MAIN          # tables, id floor, seed main's newer rows (safe to rerun)
python3 -I actions/pipeline/actions_sync.py pull-actions --main $MAIN [--dry-run]   # captured rows into main: copy -> check -> swap
python3 -I actions/pipeline/actions_sync.py needs-reading               # letters to read (prints symbols: local only)
python3 -I actions/pipeline/actions_sync.py needs-review                # items to review (local only)
python3 -I actions/pipeline/refresh_actions.py --check-reviewed         # check actions/reviewed/*.json
python3 -I actions/pipeline/test_actions_incremental.py                 # tests, no network
gh workflow run actions.yml -R AdhishaInd/cse-market-collector          # run now
# a full run on this machine:
export ACTIONS_ROOT=$PWD/actions/work
python3 -I actions/pipeline/actions_sync.py pull --root $ACTIONS_ROOT && python3 -I actions/pipeline/refresh_actions.py \
  && python3 -I actions/pipeline/actions_sync.py push-results --root $ACTIONS_ROOT
```

`pull-actions` inserts the new notices, actions and links into main, applies value changes to actions main already
has (only where main still holds the value the job replaced, or NULL; otherwise main is kept and the conflict printed),
and replaces three history tables in main: `action_value_history`, `action_capture_queue`, `action_capture_link`. New
actions with an open `needs-review` item are held back until the item is resolved (`--include-unreviewed` overrides).
Main is kept as `db/backup_actions_<time>.db` (hard link, the 7 newest kept). Then it marks the rows pulled in Turso.

### After a pull: refresh factors locally

```
cd /home/adhisha/stocks/cse_financials
python3 -I /home/adhisha/stocks/cse-market-archive/events/pipeline/events_sync.py push-inputs --main db/cse_financials.db
.venv/bin/python -I scripts/apply_actions_round3.py --asof YYYY-MM-DD \
    --prepare work/actions4/review/prepare_new_rights.py --prepare work/actions5/prepare_round6.py           # staging + checks
.venv/bin/python -I scripts/apply_actions_round3.py --asof YYYY-MM-DD \
    --prepare work/actions4/review/prepare_new_rights.py --prepare work/actions5/prepare_round6.py --swap    # swap
```

(see `cse_financials/docs/ACTIONS_ROUND3_2026-11-15.md`, round 6; prices first with `scripts/fetch_turso_prices.py`
when the as-of date moves.)

## Queues

**needs-reading** (dates only in a scanned letter):

1. `python3 -I actions/pipeline/actions_sync.py needs-reading` lists each item with its document urls (from the API).
2. Download the PDF into a scratch folder, render each page (pymupdf, ~150 dpi) and read the page image.
3. Record in `actions/reviewed/notice_readings.json` under the announcement id:
   `"39979": {"kind": "RIGHTS_ISSUE", "values": {"ex_date": "...", "record_date": "...", "effective_date": "..."}, "read_from": "<url> page 1", "reader": "...", "read_on": "YYYY-MM-DD", "note": ""}`.
   Only what the letter prints. No values + a note closes the item without changes.
4. `refresh_actions.py --check-reviewed` must print OK; commit and push. The next run applies the values
   (source `reviewed`, detail level `SCAN_REVIEWED`) and resolves the item.

**needs-review** (nothing linked the notice, a possible duplicate, revised terms, a suspect price):

1. `python3 -I actions/pipeline/actions_sync.py needs-review`.
2. Decide from the notices (and the letter if needed), then add to `actions/reviewed/review_decisions.json`:
   `"<item key>": {"decision": "keep", "note": "..."}` or `{"decision": "same_as", "action_id": N, "note": "..."}`.
   `same_as` moves the new action's links and values to action N and leaves it as `SAME_AS:N` (never pulled; its id is
   not reused). It is refused for an action already pulled into main: correct that one locally.
3. Check, commit, push as above.

## Going live (for the parent / owner)

1. Locally: `events_sync.py push-inputs --main $MAIN` (Turso's input copy was behind main on 2026-11-17), then
   `actions_sync.py init --main $MAIN` (already done once on 2026-11-17; rerunning is safe).
2. Push the branch `actions-capture` and merge it; the workflow then runs daily. First run: `gh workflow run actions.yml`.
3. After a run: `actions_sync.py pull-actions --main $MAIN` (try `--dry-run` first), then the local apply above.
4. Stop creating notice / action ids locally (see "Who owns which ids").

## On failure

| Exit | Meaning | What to do |
|---|---|---|
| 1 | A check failed (id clash, input tables changed, broken links, two actions with one ex-date and amount, more than 60 new actions / 400 notices, bad reviewed file) or the data directory was empty | The reason is in Turso `ac_run.error` (not in the public log). For an id clash: someone created ids locally - resolve before the next run. For the size limit: check the report, then run once with `--allow-large-change`. |
| 2 | The CSE refused (429/403/WAF/non-JSON) | Do not rerun soon; the 23:00 retry skips that day, the next daily run is soon enough. If it repeats, check by hand whether the endpoint moved. |
| 4 | Another run is active | Nothing. |
| Turso error | Pull or push failed | The push is one transaction; rerun the workflow. |

## Limits

- Discovery depends on the rolling feed (~4 days) and the corporate calendar (dated notices only). If both jobs stop
  for more than ~4 days, proposals announced in the gap are missed until a local announcement-list refresh.
- Start-up gap: main's notice index stops at 2 Nov 2026 and the oldest stored feed snapshot reaches back to 5 Nov, so
  notices of 3-4 Nov 2026 were captured only if they are in the corporate calendar (dated dividends, rights, scrip,
  split and repurchase notices). A proposal published on those two days would be missing.
- Two undated proposals of the same kind make `only_open` ambiguous: the notice becomes a new action in `needs-review`.
