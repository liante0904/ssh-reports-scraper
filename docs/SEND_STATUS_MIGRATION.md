# Canonical Column Migration

> Created: 2026-06-21
> Updated: 2026-07-09 KST
> Scope: historical `report_unique_key`/`key`, `save_at`/`save_time`, and send-status migration notes across scraper, scheduler, backend API, and frontend consumers.

## Read This First

If you only remember one rule, remember this:

**Scraping/upsert code must never decide that a report is unsent.**

The scraper collects report metadata. Telegram delivery state is a side effect and must be changed only by the explicit send-status writer.

### Current Direction

Canonical columns:

- `report_unique_key` replaced legacy `key`.
- `save_at` replaced legacy `save_time`.
- `report_date` replaced legacy `reg_dt`.
- `telegram_sent` replaced legacy `main_ch_send_yn` / older `is_sent` naming.

Current production rule:

- 2026-07-09 KST production `public.tbl_sec_reports` has 35 physical columns.
- `key`, `reg_dt`, `save_time`, and `main_ch_send_yn` are already absent as physical columns.
- New operational SQL must use `report_unique_key`, `report_date`, `save_at`, and `telegram_sent`.
- Historical fallback examples below are retained as incident context, not as current DDL instructions.

### Highest Priority Work

1. Keep `insert_json_data_list()` limited to metadata upsert.
2. Keep send completion inside `mark_reports_sent()` / `daily_update_data(type='send')`.
3. Remove or quarantine any code that sets `telegram_sent=false` during scrape/upsert.
4. Canonicalize URL-based keys before insert, starting with brokers that changed domains.
5. Treat duplicate `report_unique_key` indexes as a cleanup candidate only; do not execute DDL from this doc.

### Never Do This

- Do not set `telegram_sent = EXCLUDED.telegram_sent` in metadata upsert SQL.
- Do not reset `telegram_sent=false` because title/date/firm changed or duplicated.
- Do not use raw source URLs as keys when the broker is known to change domains, protocols, or URL paths.
- Do not reference `key`, `reg_dt`, `save_time`, or `main_ch_send_yn` as current production columns.
- Do not retry a scraper job blindly after Telegram messages were already sent.

### Safe Mental Model

Think of the system as three separate lanes:

| Lane | What it can change | What it must not change |
|------|--------------------|-------------------------|
| Scrape/upsert | report title, URLs, dates, analyst, canonical key | delivery state |
| Send pipeline | `telegram_sent=true` | report identity |
| Migration/view | canonical reads, one-way historical backfill if needed | destructive resets |

## Summary

The canonical columns are:

| Canonical | Legacy | Purpose |
|-----------|--------|---------|
| `report_unique_key` | `key` | report dedupe key |
| `save_at` | `save_time` | scrape/save timestamp |
| `report_date` | `reg_dt` | report date |
| `telegram_sent` | `main_ch_send_yn` / older `is_sent` naming | Telegram main-channel send status |

The legacy physical columns above have already been removed from production. This document should no longer be used as a “drop later” checklist.

## Incident Analysis

Duplicate Telegram channel messages were not caused by the expected cross-board duplicate case where securities firms post the same report to multiple boards.

The likely failure path was send-status regression:

- `models/SecReportsManager.py` upserted existing rows with `is_sent = EXCLUDED.is_sent`.
- Scraper payloads normally do not carry `is_sent=true`, so re-scraping an already sent report could reset `is_sent` to `false`.
- `scheduler.py` GA import broadcast sent Telegram messages immediately, but did not mark imported rows as sent afterward.
- The backend startup migration used `main_ch_send_yn` as the source of truth:

```sql
UPDATE tbl_sec_reports
SET is_sent = (main_ch_send_yn = 'Y')
WHERE (is_sent IS NULL) OR (is_sent != (main_ch_send_yn = 'Y'));
```

That migration can turn a valid `is_sent=true` row back to `false` if `main_ch_send_yn` is still `N`.

## Observations On 2026-06-21

Recent 3-day production check:

| Check | Result |
|-------|--------|
| Rows in recent window | 620 |
| Missing `key` | 0 |
| Missing `report_unique_key` | 0 |
| `key` / `report_unique_key` mismatch | 0 |
| Duplicate `key` groups | 0 |
| Duplicate `report_unique_key` groups | 0 |
| Duplicate canonical key groups | 0 |
| Missing `save_at` | 177 |
| Missing `save_time` | 0 |
| `save_at` / `save_time` date mismatch | 0 |
| `is_sent=true`, `main_ch_send_yn!='Y'` | 10 |
| `is_sent=false`, `main_ch_send_yn='Y'` | 50 |

Production now has duplicate indexes on the canonical report key:

- `idx_report_unique_uid` on `report_unique_key`
- `tb_sec_reports_uid_key` on `report_unique_key`
- `idx_report_unique_key` on `report_unique_key` (non-unique)

This is a cleanup candidate, not an instruction to run DDL from this doc.

### Shinhan URL-Domain Duplication

Shinhan Securities has same-title rows split across URL variants:

- `bbs2.shinhaninvest.com`
- `bbs2.shinhansec.com`
- `http` / `https`
- `/file.do` / `/file.pdf.do`

Recent 3-day check found 335 same-title/date groups with more than one URL domain, covering 1,105 rows. These do not violate current unique indexes because the URL string itself differs.

Mitigation:

- `scrapers/shinhan_core.py` now canonicalizes Shinhan report URLs before using them as `key` / `report_unique_key`.
- Future rows with only domain/protocol/path migration differences should collapse to the same canonical key.

## Current Production Cleanup Status

`main_ch_send_yn`, `key`, `reg_dt`, and `save_time` have already been removed from the production `public.tbl_sec_reports` physical schema. Remaining references in older incident notes, tests, SQLite fixtures, or migration examples are historical/compatibility context and must not be copied into current production SQL.

## Current Safe Alternative

### Canonical Read Path

Use the physical canonical columns directly in new production queries:

- `report_unique_key`
- `report_date`
- `save_at`
- `telegram_sent`

Do not introduce aliases such as `report_key` or `notification_sent` in new read paths. They add another translation layer without fixing the underlying schema.

The read path is not a substitute for base-table uniqueness. Production currently has duplicate `report_unique_key` indexes (`idx_report_unique_uid`, `tb_sec_reports_uid_key`, and non-unique `idx_report_unique_key`), which should be reviewed separately before any DDL cleanup.

### Write Path

When a report is marked sent:

```sql
SET telegram_sent = true
```

When a report is intentionally reset for resend:

```sql
SET telegram_sent = false
```

Upsert must preserve existing sent status:

```sql
telegram_sent = existing.telegram_sent OR incoming.telegram_sent
```

For report keys:

```sql
report_unique_key = canonical_key
```

For save timestamps:

```sql
save_at = canonical timestamptz
```

For report dates:

```sql
report_date = canonical report date
```

### Read Path

Public report fetches should use the compatibility predicate:

```sql
telegram_sent = true
```

Older OR predicates that reference `main_ch_send_yn` are not valid against the current production physical schema.

### Migration Path

Historical startup/backfill migration had to be one-way only:

```sql
UPDATE tbl_sec_reports
SET is_sent = true
WHERE main_ch_send_yn = 'Y'
  AND COALESCE(is_sent, false) = false;
```

This snippet is retained as incident context. It is not valid current production SQL because `main_ch_send_yn` is no longer a physical column. Current code should write `telegram_sent` only.

### Historical Applied Changes

The following items describe the 2026-06-21 incident-era fixes and may reference fields that are no longer physical columns in production.

### Scraper Repository

- `models/db_factory.py`
  - `DB_BACKEND=postgres` now routes to the local scraper `SecReportsManager` compatibility wrapper.
  - This is critical because production was using `DB_BACKEND=postgres`, which bypassed the local override and called `ssh_library.reports.insert_json_data_list()` directly.

- `models/SecReportsManager.py`
  - Preserves existing `is_sent=true` on upsert.
  - Mirrors sent status to `main_ch_send_yn`.
  - Marks send completion with both columns.
  - Disables the historical title/date/firm duplicate reset path because it mutated send status and caused Telegram re-sends.

- `scheduler.py`
  - GA broadcast now marks rows as sent after successful Telegram send.
  - **[Added 2026-06-21]** Refactored `_broadcast_ga_reports` to update sent status in DB immediately after each message chunk (under 3,500 chars limit) is successfully sent. This prevents partial delivery failure from re-triggering duplicates for already sent reports on subsequent runs.

- `tests/test_sec_reports_manager.py`
  - Guards against reintroducing `is_sent = EXCLUDED.is_sent`.
  - Guards send completion writing both `is_sent` and `main_ch_send_yn`.
  - Guards against the duplicate reset path touching DB/send status.

- `scrapers/shinhan_core.py`
  - Canonicalizes Shinhan report URLs before assigning `key` / `report_unique_key`.
  - Normalizes `shinhaninvest.com` → `shinhansec.com`, `http` → `https`, and `/file.do` → `/file.pdf.do`.

- `scrapers/imfn_core.py`, `scrapers/ds_core.py`, `scrapers/eugene_core.py`, `scrapers/shinyoung_core.py`
  - **[Added 2026-06-21]** Added missing `key` and `report_unique_key` (and `article_url` for IM Securities) in returned report payloads. This guarantees that reports imported via GA Standalone json are not dropped/filtered out due to missing unique keys.

- `tests/test_shinhan_core.py`
  - Guards Shinhan URL canonicalization.

- `tests/test_scheduler_ga_broadcast.py`
  - **[Added 2026-06-21]** Guards `_broadcast_ga_reports` chunk-based Telegram send and DB status updates. It verifies chunk separation under 3,500-character limits, call counts, and proper database partial status marking during individual delivery success and failures.

### Backend Repository

- `app/main.py`
  - Startup migration now only backfills `is_sent=true` from legacy `main_ch_send_yn='Y'`.
  - It no longer turns `is_sent=true` back into `false`.

- `app/routers/external_api.py`
  - Public fetch predicates use `is_sent=true OR main_ch_send_yn='Y'`.

## Production Checks

Use these checks to confirm the current post-drop schema and cleanup candidates:

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'tbl_sec_reports'
ORDER BY ordinal_position;
```

```sql
SELECT
  count(*) FILTER (WHERE NULLIF(report_unique_key, '') IS NULL) AS missing_report_unique_key,
  count(*) FILTER (WHERE save_at IS NULL) AS missing_save_at,
  count(*) FILTER (WHERE report_date IS NULL) AS missing_report_date,
  count(*) FILTER (WHERE telegram_sent IS NULL) AS missing_telegram_sent
FROM tbl_sec_reports;
```

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'tbl_sec_reports'
  AND indexdef ILIKE '%report_unique_key%';
```

Expected production facts as of 2026-07-09 KST:

- Physical column count: 35
- Absent physical columns: `key`, `reg_dt`, `save_time`, `main_ch_send_yn`
- Duplicate key indexes present: unique `idx_report_unique_uid`, unique `tb_sec_reports_uid_key`, non-unique `idx_report_unique_key`

## Remaining Cleanup Candidates

Do not use this document to run DDL directly.

1. Review whether both unique `report_unique_key` indexes are still needed.
2. Review whether the non-unique `idx_report_unique_key` adds value beyond the unique indexes.
3. Remove or archive old migration/test snippets that still imply `key`, `reg_dt`, `save_time`, or `main_ch_send_yn` are production physical columns.

## Anti-Regression Rules

- Do not write `telegram_sent = EXCLUDED.telegram_sent` in metadata upsert code.
- Do not run a migration that derives `telegram_sent=false` from old send-status fields.
- Do not send Telegram from a path that skips the post-send status update.
- Do not switch readers back to old `is_sent` / `main_ch_send_yn` naming.
- Do not use a raw URL as a report key for sources with known URL migrations unless the URL is canonicalized first.
- Do not rely on the canonical view for dedupe enforcement; dedupe must be protected by base-table unique indexes.

## Operational Findings And Blockers

### 1. `DB_BACKEND=postgres` bypassed the scraper override

Production was configured with `DB_BACKEND=postgres`. Before the fix, that path returned `models.PostgreSQLManager`, which was only an alias to `ssh_library.PostgreSQLManager`. The alias was removed on 2026-07-05; use `models.SecReportsManager`/`models.db_factory.get_db()` for scraper DB access.

Impact:

- The local `models/SecReportsManager.py` compatibility fix was not used.
- Logs showed `ssh_library.reports:insert_json_data_list`.
- Existing sent rows could still be updated with `is_sent=false`.

Fix:

- Route `DB_BACKEND=postgres` to the local scraper `SecReportsManager`.
- Add a regression test for the `postgres` backend alias.

### 2. Scheduler can send before failing health checks

At 2026-06-21 10:20 KST, `scraper.py` sent Telegram messages and updated DB state, then raised a health-check `RuntimeError` because BNK returned 0 rows.

Impact:

- The job looked failed at the scheduler level even though Telegram side effects already happened.
- Retrying or restarting around this state is dangerous unless send state is protected.

Preferred follow-up:

- Separate side-effect completion from health-report failure.
- Health failures should be reported without making the whole process look retry-safe after messages have already been sent.

### 3. Title/date/firm duplicate reset is unsafe in a sender pipeline

`_reset_duplicate_send_yn()` reset existing sent rows to `is_sent=false, main_ch_send_yn='N'` when the same title/date/firm appeared with a different key.

Impact:

- Shinhan URL variants made old sent rows eligible for Telegram delivery again.
- At 2026-06-21 10:48 KST, the hot-patched container correctly used `models.SecReportsManager`, but this reset path still created a send candidate and caused another duplicate message.

Fix:

- `_reset_duplicate_send_yn()` is now a no-op.
- Dedupe must be handled through canonical keys, source URL normalization, read-side grouping, or explicit manual repair, not by mutating delivery flags.

### 4. Manual stdin scripts should not call bare `load_dotenv()`

Running `python - <<'PY'` inside the production container triggered:

```text
AssertionError in dotenv.find_dotenv()
```

Fix:

```python
load_dotenv("/app/.env")
```

Use explicit `.env` paths for operational one-off scripts.

### 5. Backend compile/test from sandbox can fail on `.pyc` writes

`py_compile` in the backend repo failed because the sandbox could not write `__pycache__`.

Use one of these instead:

```bash
PYTHONDONTWRITEBYTECODE=1 python -m py_compile ...
```

or AST parse:

```python
ast.parse(Path(path).read_text(), filename=path)
```

### 6. Docs and DB metadata differ depending on where you look

`pg_constraint` only showed the primary key, but `pg_indexes` showed unique indexes that application docs had missed. As of 2026-07-09 KST, the relevant cleanup candidate is duplicate indexing on `report_unique_key`.

Operational check should inspect both:

```sql
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'tbl_sec_reports'::regclass;

SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'tbl_sec_reports';
```

### 7. Shinhan duplicates are URL canonicalization issues, not DB uniqueness failures

The DB had no duplicate groups by the report key in the historical incident window, but Shinhan had same-title rows split by URL variants.

The prevention point is source-specific key canonicalization before insert, not a generic DB view.

## 2026-06-21 Operational Actions

- Stopped `ssh-reports-scraper-main-scraper-green` after the 10:20 duplicate send to prevent the 10:30 run.
- Hot patched the running container with:
  - `models/SecReportsManager.py`
  - `models/db_factory.py`
  - `scheduler.py`
  - `scrapers/shinhan_core.py`
- Restarted `ssh-reports-scraper-main-scraper-green`.
- Verified production container now resolves:

```text
DB_BACKEND=postgres
db_type=<class 'models.SecReportsManager.SecReportsManager'>
insert_module=/app/models/SecReportsManager.py
daily_update_module=/app/models/SecReportsManager.py
```

- Re-sent weekend Telegram messages for reports saved on or after 2026-06-20:
  - Rows: 7
  - Telegram message chunks: 1
  - Post-send status update: completed
- After the 10:48 duplicate caused by `_reset_duplicate_send_yn()`, stopped the main scraper again.
- Marked the two reset Shinhan candidates back to `is_sent=true, main_ch_send_yn='Y'`.
- Hot patched the container again with `_reset_duplicate_send_yn()` disabled.
- Committed hotfix source must be pushed to `main` so GitHub Actions builds and deploys the durable image. Container hot patches and local `docker commit` are emergency recovery only.
