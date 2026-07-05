# LLM Token Refactor Handoff

Date: 2026-07-05
Repo: `ssh-reports-scraper`
Goal: reduce future LLM context cost without breaking production scraper/telegram flows.

## Current Fixed Baseline

- `main` cleanup is pushed through `2385e09`.
- SQLite runtime/tooling/compose/manager removed from main.
- Legacy SQLite code preserved on `archive/sqlite-legacy-20260705`.
- `models/PostgreSQLManager.py` alias removed; use `models.SecReportsManager` or `models.db_factory.get_db()`.
- News standalone workflow/core removed; news is owned by `naver-stock-news`.
- Telegram message chunk builder is `utils/telegram_message_builder.py`; 3500 char limit is intentional.

## Highest ROI Work

1. `scraper.py` split
   - Why: largest token sink; mixes scheduling, GA fallback, scraping, DB upsert, enrich/postprocess, and broadcast concerns.
   - Safe plan: no behavior change; extract small named helpers around run-mode selection, scrape execution, DB upsert, postprocess/enrich, broadcast trigger.
   - Review focus: preserve existing scheduler timings, GA fallback, LS/DBfi/Heungkuk behavior.

2. Company-specific postprocess isolation
   - Why: DBfi/LS/BNK/Heungkuk exceptions make common flow hard to scan.
   - Safe plan: move special rules into explicit company policy/postprocess modules after tests exist.
   - Hold: LS/BNK have compatibility contracts; do not rewrite blindly.

3. `json_util.py` / `report_json_store.py`
   - Why: names hide actual responsibilities; old local-json + telegram behavior is confusing.
   - Hold: observe current 2026-07-05 json/telegram changes for ~3 days before deprecating.
   - Safe plan later: keep `json_util.py` as thin legacy facade; real logic should live in purpose-named modules.

4. GA artifact contract cleanup
   - Files: `scripts/validate_scrape_result.py`, `scripts/standalone_bnk_scraper.py`, `scripts/standalone_ls_scraper.py`.
   - Why: still contains `reg_dt` / `save_time` / `key` compatibility.
   - Safe plan: validator should accept canonical keys first and legacy keys only as fallback; then migrate BNK/LS outputs.

5. `modules/` vs `scrapers/` split
   - Why: same firm can span server module, core scraper, standalone entrypoint, and workflow.
   - Safe plan: document and apply gradually: core logic in `scrapers/*_core.py`, server wrapper thin in `modules/*`.

6. Firm/config access cleanup
   - Why: global `FirmInfo.firm_names` style access forces runtime-state reasoning.
   - Safe plan: migrate call sites gradually to explicit helpers like `get_firm_name(firm_id)`.

## Do Not Touch Without Deeper Runtime Check

- `utils/json_util.py`
- `utils/report_json_store.py`
- `scripts/validate_scrape_result.py`
- `scripts/standalone_bnk_scraper.py`
- `scripts/standalone_ls_scraper.py`
- `modules/LS_0.py`
- `modules/BNKfn_23.py`

Reason: compatibility with Telegram resend, local-json, GA artifact validation, or LS/BNK scraper behavior.

## Suggested Next Task

Start with `scraper.py` helper extraction only.

Minimal first patch:
- add helper names that describe existing branches;
- move code blocks without changing SQL, env names, timings, return values, or exception handling;
- run existing scheduler/scraper import tests plus telegram/sec_reports tests;
- ask another LLM to review only for behavior drift.
