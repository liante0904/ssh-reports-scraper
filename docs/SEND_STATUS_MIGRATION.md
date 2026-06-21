# Canonical Column Migration

> Created: 2026-06-21
> Scope: `report_unique_key`/`key`, `save_at`/`save_time`, and `is_sent`/`main_ch_send_yn` handling across scraper, scheduler, backend API, and frontend consumers.

## Summary

The canonical columns are:

| Canonical | Legacy | Purpose |
|-----------|--------|---------|
| `report_unique_key` | `key` | report dedupe key |
| `save_at` | `save_time` | scrape/save timestamp |
| `is_sent` | `main_ch_send_yn` | Telegram main-channel send status |

The legacy columns cannot be removed in one sweep yet.

The safe transition plan is:

1. Dual-write send status during the transition.
2. Read with `is_sent = true OR main_ch_send_yn = 'Y'`.
3. Stop any migration from turning `is_sent=true` back to `false`.
4. Verify both columns stay aligned in production.
5. Drop `main_ch_send_yn` only after all readers and migration code are no longer dependent on it.

The same pattern applies to `key` and `save_time`: canonical first, legacy fallback during reads, dual-populated writes until consumers are migrated.

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

Production has unique indexes on both legacy and canonical report keys:

- `tb_sec_reports_key_key` on `key`
- `idx_report_unique_uid` on `report_unique_key`
- `tb_sec_reports_uid_key` on `report_unique_key`

There were no recent or all-time duplicate groups by `key`, `report_unique_key`, or canonical `COALESCE(report_unique_key, key)` at the time of inspection.

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

## Why Not Remove `main_ch_send_yn` Globally Now

Global removal is risky because the column still appears in more than one role:

- Legacy API response compatibility.
- Backend startup migration / backfill code.
- Existing tests and maintenance scripts.
- Historical SQLite/PostgreSQL migration scripts.
- Documentation and contract examples.

Removing all references at once can create a worse failure mode: `is_sent` may become the only writer, while an old backend or migration still reads `main_ch_send_yn` and hides reports or resets send state.

## Current Safe Alternative

### Canonical Read View

Use `sql/canonical_sec_reports_view.sql` as the read/API/analysis transition layer:

```sql
CREATE OR REPLACE VIEW public.v_sec_reports_canonical AS
SELECT
    r.*,
    COALESCE(NULLIF(r.report_unique_key, ''), NULLIF(r.key, '')) AS report_key,
    COALESCE(r.save_at, save_time_fallback) AS scraped_at,
    (COALESCE(r.is_sent, false) OR r.main_ch_send_yn = 'Y') AS notification_sent
FROM public.tbl_sec_reports r;
```

This view makes readers consume one canonical name without forcing all writers and old scripts to change at the same time.

The view is not a substitute for base-table uniqueness. Keep unique indexes on `key` and `report_unique_key` until legacy writes are gone; then keep the unique guarantee on `report_unique_key` or a generated canonical key.

### Write Path

When a report is marked sent:

```sql
SET is_sent = true,
    main_ch_send_yn = 'Y'
```

When a report is intentionally reset for resend:

```sql
SET is_sent = false,
    main_ch_send_yn = 'N'
```

Upsert must preserve existing sent status:

```sql
is_sent = existing.is_sent OR incoming.is_sent
```

The legacy flag should mirror the combined sent state during the transition.

For report keys:

```sql
report_unique_key = canonical_key
key = canonical_key
```

During migration, `key` should mirror `report_unique_key`; after migration, only `report_unique_key` should be used by writers.

For save timestamps:

```sql
save_at = canonical timestamptz
save_time = legacy string mirror
```

During migration, `save_time` should mirror `save_at` for old scripts; after migration, readers should use `save_at` / `scraped_at`.

### Read Path

Public report fetches should use the compatibility predicate:

```sql
is_sent = true OR main_ch_send_yn = 'Y'
```

This avoids hiding rows when one column is ahead of the other during deploys, backfills, or partial migrations.

### Migration Path

Startup/backfill migration must be one-way only:

```sql
UPDATE tbl_sec_reports
SET is_sent = true
WHERE main_ch_send_yn = 'Y'
  AND COALESCE(is_sent, false) = false;
```

It must not set `is_sent=false` from `main_ch_send_yn='N'`.

## Applied Changes

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

- `tests/test_sec_reports_manager.py`
  - Guards against reintroducing `is_sent = EXCLUDED.is_sent`.
  - Guards send completion writing both `is_sent` and `main_ch_send_yn`.
  - Guards against the duplicate reset path touching DB/send status.

- `scrapers/shinhan_core.py`
  - Canonicalizes Shinhan report URLs before assigning `key` / `report_unique_key`.
  - Normalizes `shinhaninvest.com` → `shinhansec.com`, `http` → `https`, and `/file.do` → `/file.pdf.do`.

- `tests/test_shinhan_core.py`
  - Guards Shinhan URL canonicalization.

### Backend Repository

- `app/main.py`
  - Startup migration now only backfills `is_sent=true` from legacy `main_ch_send_yn='Y'`.
  - It no longer turns `is_sent=true` back into `false`.

- `app/routers/external_api.py`
  - Public fetch predicates use `is_sent=true OR main_ch_send_yn='Y'`.

## Production Checks

Run these before dropping legacy columns:

```sql
SELECT
  count(*) FILTER (WHERE is_sent IS true AND main_ch_send_yn IS DISTINCT FROM 'Y') AS is_sent_true_legacy_not_y,
  count(*) FILTER (WHERE COALESCE(is_sent, false) = false AND main_ch_send_yn = 'Y') AS legacy_y_is_sent_false
FROM tbl_sec_reports;
```

```sql
SELECT
  count(*) FILTER (WHERE NULLIF(key, '') IS NULL) AS missing_key,
  count(*) FILTER (WHERE NULLIF(report_unique_key, '') IS NULL) AS missing_report_unique_key,
  count(*) FILTER (
    WHERE NULLIF(key, '') IS NOT NULL
      AND NULLIF(report_unique_key, '') IS NOT NULL
      AND key <> report_unique_key
  ) AS key_mismatch,
  count(*) FILTER (WHERE save_at IS NULL) AS missing_save_at,
  count(*) FILTER (WHERE NULLIF(save_time, '') IS NULL) AS missing_save_time
FROM tbl_sec_reports;
```

The desired result is both counts at `0` for a full observation window.

Recommended observation window:

- At least one full trading day.
- Preferably one full weekly cycle including GA import, server fallback scraping, enrichment, Telegram send, backend restart, and API cache invalidation.

## Drop Plan

Do not drop `key`, `save_time`, or `main_ch_send_yn` until all steps are complete.

1. Keep dual-write and OR-read in production.
2. Add and use `v_sec_reports_canonical` for read/API/analysis paths.
3. Remove backend startup dependency on legacy columns.
4. Update frontend and Netlify/API tests if they consume legacy response fields.
5. Update maintenance scripts and migration scripts.
6. Verify mismatch counts stay `0`.
7. Remove legacy columns from application models and insert/update SQL.
8. Drop legacy columns in DB migrations.

## Anti-Regression Rules

- Do not write `is_sent = EXCLUDED.is_sent` in upsert code.
- Do not run a migration that derives `is_sent=false` from `main_ch_send_yn='N'`.
- Do not send Telegram from a path that skips the post-send status update.
- Do not switch readers back to `is_sent=true` only until legacy writes are removed and DB mismatch checks are clean.
- Do not use a raw URL as a report key for sources with known URL migrations unless the URL is canonicalized first.
- Do not rely on the canonical view for dedupe enforcement; dedupe must be protected by base-table unique indexes.

## Operational Findings And Blockers

### 1. `DB_BACKEND=postgres` bypassed the scraper override

Production was configured with `DB_BACKEND=postgres`. Before the fix, that path returned `models.PostgreSQLManager`, which is only an alias to `ssh_library.PostgreSQLManager`.

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

`pg_constraint` only showed the primary key, but `pg_indexes` showed unique indexes on both `key` and `report_unique_key`.

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

The DB had no duplicate groups by `key`, `report_unique_key`, or `COALESCE(report_unique_key, key)`, but Shinhan had same-title rows split by URL variants.

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
