# CSE market collector

Scheduled collector for Colombo Stock Exchange data that the cse.lk website API serves only for the current day:
every trade of every stock, volume at each price, index by minute, and the day's market, sector and foreign-flow
snapshots. Runs on GitHub Actions (`.github/workflows/archive.yml`, script `collect.py`, Python standard library)
and stores into a private Turso (libSQL) database; this repository holds code only.

Also here: the **event calendar** job (dividends, rights issues, AGMs, results dates; `events/`, workflow
`.github/workflows/events.yml`), described in [events/README.md](events/README.md).

## Storage

Set repository secrets `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`. Tables:

| Table | Contents |
|---|---|
| `archive_day` | One row per archived trading day (stocks, trade rows, size, fetch time). Written last: a day counts as archived only when everything else is stored |
| `trade` | Every trade: `trade_id`, `date`, `symbol`, `t` (ms), `price`, `qty` |
| `day_summary` | Per stock per day: open, high, low, close, previous close, share volume, trades, turnover, market cap |
| `archive_blob` | The complete raw day (gzip JSON, 1 MB parts, ordered by `part`): `tradeSummary`, `trades`, `detailedTrades`, `marketSummery`, `dailyMarketSummery`, `marketIndices`, `GICSSectorSummery`, `getContributorsChart`, `getContributors`, `mostActiveVolumes`, `list_by_market_cap`, `aspi_minute`, `snp_minute`, `aspi_high_low`, `snp_high_low`, `checks` |

Without the secrets, `collect.py` writes `data/YYYY/MM/YYYY-MM-DD.json.gz` locally instead;
`python collect.py upload <file>` loads such a file into Turso.

## Schedule and edge cases

- Runs every day at 14:50, 16:30, 19:00 and 22:00 Colombo time; the first successful run saves the day and the rest
  do nothing. Nothing about trading days or hours is hard-coded:
  - holidays and weekends: nothing traded today, so nothing is saved;
  - extended or late sessions: the run waits (up to 150 min) until the API reports the market closed;
  - special weekend sessions: picked up, because the runs are daily.
- Checks at 23:15 (today) and 08:00 (previous day) compare the archive with the trading days the API itself
  reports, which catches days when GitHub skipped or delayed every run.
- On any failure an issue labelled `collection-failure` is opened (or commented on), which emails the repo owner;
  the next run that archives a day or passes a check closes it (a run that finds nothing to do leaves it open).
  Rerun by hand from Actions -> CSE daily archive -> Run workflow.
- Completeness is judged by trade ids, which run in one market-wide sequence: a day is flagged when more than
  max(50, 1%) ids are missing inside the day's range (normal days: 3-21). Stocks whose trade list is shorter than
  the per-stock summary count only raise a warning: on some days CSE's summary counts exceed the complete feed.
  When more than max(3, 5%) stocks are short, an issue labelled `count-mismatch` is opened (never auto-closed).
- Requests are sequential, about 2 seconds apart (~300 per day).
