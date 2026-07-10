# LLM Delegation Contract

Read `docs/DEBUG_ENTRYPOINTS.md` first. This file only defines the current
DeepSeek/Gemini-AGY handoff contract. Runtime truth comes from code,
`config/firms.yaml`, tests, current workflow runs, and live evidence.

## Roles

**DeepSeek** handles bounded implementation, workflow investigation, and test
execution. **Gemini/AGY** handles documentation, inventories, and low-risk
review. **Codex** reviews diffs, reruns validation on the integrated tree, and
owns any `main` merge, push, deployment, or production write.

Every task must state allowed files, forbidden operations, exact validation,
and an acceptance result. Delegates must not merge/push `main`, deploy, restart
services, write production data, change secrets, or broaden scope unless the
task explicitly authorizes it.

## Fixed Files

Use only these gitignored files for direct dispatch:

```text
.agent_tasks/deepseek_next.json
.agent_tasks/deepseek_result.json
.agent_tasks/gemini_agy_next.json
.agent_tasks/gemini_agy_result.json
```

Do not commit `.agent_tasks/` files. `scripts/llm_dispatch.sh` consumes the JSON
paths above. The older `scripts/llm_task_queue.py` renders Markdown task files
and is not compatible with direct dispatch; do not use it until the formats are
aligned.

## Compact Task Format

```json
{
  "agt": "deepseek|gemini",
  "ver": "YYYY-MM-DD HH:MM:SS KST",
  "typ": "impl|investigate|audit|doc|review",
  "goal": "one bounded result",
  "ctx": "only evidence needed for this task",
  "src": ["read-only/file"],
  "mod": [{"f": "allowed/file", "do": "specific change"}],
  "ban": ["op:main_merge", "op:push", "op:deploy", "op:db_write"],
  "tst": ["exact validation command"],
  "br": "branch-name",
  "msg": "commit message",
  "out": ".agent_tasks/<agent>_result.json"
}
```

Keep one independently reviewable concern per task. Include the concrete error,
workflow, firm, or DB query in `ctx`; do not paste broad history. Production
evidence is read-only unless the user explicitly approves a write.

## Compact Result Format

```json
{
  "agt": "deepseek|gemini",
  "ts": "YYYY-MM-DD HH:MM:SS KST",
  "sum": "what changed or was learned",
  "br": "branch-name",
  "cm": "commit sha or empty",
  "files": ["changed/file"],
  "tst": [{"cmd": "exact command", "ok": true, "result": "count/output"}],
  "blk": [{"what": "blocker"}],
  "risk": "unverified behavior"
}
```

`ok: true` means only that the named command passed. Use the evidence tiers in
`docs/DEBUG_ENTRYPOINTS.md`; never translate local unit success into `built`,
`deployed`, or `production verified`.

## Dispatch

Dry-run and inspect the target/prompt first:

```bash
bash scripts/llm_dispatch.sh deepseek
bash scripts/llm_dispatch.sh gemini
bash scripts/llm_dispatch.sh both --parallel
```

Send only after confirming each tmux target contains the intended LLM CLI:

```bash
bash scripts/llm_dispatch.sh deepseek --send --wait
bash scripts/llm_dispatch.sh gemini --send --wait
bash scripts/llm_dispatch.sh both --send --wait --parallel
```

After delegation, Codex must inspect the commit diff, check for unrelated dirty
files, integrate onto the latest target tree, and rerun the required Tier 1
commands. A delegate's source-branch result does not verify the merged commit.

## Runtime Contracts

### Firm routing

`config/firms.yaml` is the firm manifest. `scraper_registry.py` loads and
validates active entries. Do not maintain manual firm counts or lists in docs.
For a firm failure, follow the file order in `docs/DEBUG_ENTRYPOINTS.md`.

### Report payload

`models/report_payload.py` and `scrapers/validate.py` own normalization.
`scripts/validate_scrape_result.py` enforces the manifest `empty_policy` and
firm identity. Do not introduce a firm-specific payload schema without a
contract test.

### Database fields

Live `pg_catalog`/`information_schema` is authoritative for production. The
canonical physical fields include:

| Physical field | Dropped legacy name |
|---|---|
| `report_unique_key` | `key` |
| `report_date` | `reg_dt` |
| `save_at` | `save_time` |
| `telegram_sent` | `main_ch_send_yn` |
| `pdf_url` | `download_url` |

`source_url` and `article_url` are nullable compatibility-view aliases; they do
not map to `telegram_url`. `pdf_file_url`, `scraped_at`, `market_type`, and
`firm_name` are view aliases, not physical insert fields.

### Logs and operations

Use `docs/OPS_LOG_TAIL.md` for current read-only log commands. Always include an
exact time window. Treat repeated watchdog messages as symptoms until the first
underlying service error is identified.

## Stop Conditions

Stop and return a blocker when the task requires an unlisted file, a secret is
missing, the candidate tree contains unexplained changes, deterministic tests
fail, or production mutation/restart/deploy would be required. Do not hide the
condition with broad exception handling, weaker assertions, or skipped tests.
