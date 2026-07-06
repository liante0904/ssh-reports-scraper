# Debug Entrypoints

This file is the first stop for Codex or another LLM debugging this repo. Use it
to choose the smallest relevant source set before reading broad history docs.

## Fast Orientation

Current runtime shape:

- `scheduler.py`: APScheduler entrypoint in the server container. It runs the
  main scraper subprocess, imports GA JSON files from `/app/incoming/ga-scrapes`,
  broadcasts newly imported reports, and triggers FnGuide matching.
- `scraper.py`: server-side scrape/enrich/send pipeline. It imports firm modules
  from `modules/*`, runs regular server-only scrapers, full-scrape fallback, LS
  and DBfi enrichment, and Telegram send.
- `.github/workflows/scrape-*.yml`: per-firm GitHub Actions scrapers.
- `run/standalone/*.py`: per-firm GA entrypoints. Most use
  `run/standalone/_runner.py`, but LS, BNK, and KoreaInvestment have special
  paths.
- `scrapers/*_core.py`: reusable core logic for many GA/server wrappers.
- `modules/*`: server-side firm modules and legacy-compatible wrappers.
- `models/SecReportsManager.py`: PostgreSQL reads/writes and send-state updates.
- `models/ConfigManager.py`: runtime secrets/config lookup.
- `models/db_factory.py`: DB factory. Current runtime DB is PostgreSQL.

## Before Editing

Run these first unless the task is docs-only:

```bash
git status --short --branch
make test-imports
uv run pytest tests/test_standalone_runner.py tests/test_db_factory.py tests/test_config_manager.py -q
```

Avoid using `tests/test_scrapers_health.py` as a quick smoke test. It can touch
real scraper paths and take network time.

## Common Debug Paths

### GA Workflow Fails

Read in this order:

1. `.github/workflows/scrape-{firm}.yml`
2. `run/standalone/{firm}.py`
3. `run/standalone/_runner.py`
4. `scrapers/{firm}_core.py`
5. `scripts/validate_scrape_result.py`

Useful commands:

```bash
bash scripts/verify_standalones.sh
uv run pytest tests/test_standalone_runner.py tests/test_core_contract.py -q
```

Check the workflow env name before changing code. Some legacy entrypoints still
read `urls`; newer ones should prefer `{FIRM}_URLS_JSON`.

### Server Scrape Fails

Read in this order:

1. `scheduler.py`
2. `scraper.py`
3. `scraper_config.py`
4. `modules/{FirmModule}.py`
5. `models/ConfigManager.py`

For production evidence:

```bash
bash scripts/ops_tail_errors.sh --scraper --since "09:00"
bash scripts/ops_tail_errors.sh --service ssh-reports-scraper-main-scraper-green --since "09:00"
```

Do not infer health from local docs alone. Prefer current logs and current
workflow runs.

### GA Import or Telegram Broadcast Fails

Read in this order:

1. `scheduler.py`
2. `scripts/validate_scrape_result.py`
3. `models/SecReportsManager.py`
4. `utils/telegram_message_builder.py`
5. `utils/telegram_util.py`

Relevant tests:

```bash
uv run pytest tests/test_scheduler_ga_broadcast.py tests/test_telegram_send_audit.py tests/test_sec_reports_manager.py -q
```

Canonical fields are `report_unique_key`, `report_date`, `save_at`, and
`telegram_sent`. Legacy names may exist only for compatibility or migration
guards.

### DB Insert, Column, or Canonical Field Issue

Read in this order:

1. `models/SecReportsManager.py`
2. `models/db_factory.py`
3. `scripts/validate_scrape_result.py`
4. `docs/COLUMN_DEPRECATION_SCRAPER_STATUS.md`
5. `docs/COLUMN_DEPRECATION_IMPL_SPEC.md`

Relevant tests:

```bash
uv run pytest tests/test_sec_reports_manager.py tests/test_db_factory.py tests/test_report_json_store.py -q
```

Do not reintroduce SQLite runtime paths. SQLite history lives on
`archive/sqlite-legacy-20260705`.

### FnGuide Matcher Trigger Fails

Read in this order:

1. `scheduler.py` (`run_fnguide_matcher`)
2. Backend repo route/service for `/admin/fnguide/match-internal`
3. `docs/OPS_LOG_TAIL.md`

The scheduler default backend port is `8002`, not `8000`.

### LS or BNK Special Cases

Read in this order:

1. `modules/LS_0.py` or `modules/BNKfn_23.py`
2. `run/standalone/ls.py`, `run/standalone/ls_v2.py`, or
   `scripts/standalone_bnk_scraper.py`
3. `.github/workflows/scrape-ls*.yml` or `.github/workflows/scrape-bnk.yml`
4. `docs/LLM_GUIDE.md`

LS and BNK have source-IP and legacy-contract constraints. Do not force them
into the normal `_runner.py` pattern without a focused migration.

### Heungkuk PDF Fallback Fails

Read in this order:

1. `scrapers/heungkuk_core.py`
2. `modules/Heungkuk_28.py`
3. `run/standalone/heungkuk.py`
4. `tests/test_heungkuk_core.py`

Invariant: if PDF resolution fails, keep the report row, set Telegram link to
the article URL, and do not pretend the article URL is a PDF/download URL.

## What Not To Read First

- Old merge reports unless the task asks for history.
- Broad refactor roadmaps before checking the concrete failing path.
- Missing split-out LLM harness docs. The current consolidated docs are
  `docs/LLM_GUIDE.md` and `docs/LLM_HARNESS.md`.

## Minimum Handoff Format

When handing work to another LLM, include only:

```text
Problem:
Evidence:
Files to read:
Files allowed to edit:
Commands to run:
Forbidden operations:
Expected result:
```

For delegated work, prefer the JSON task files documented in `docs/LLM_GUIDE.md`.
