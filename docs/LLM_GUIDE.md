# LLM Runtime Guide

Start with [DEBUG_ENTRYPOINTS.md](DEBUG_ENTRYPOINTS.md). It selects the
smallest source set for the active runtime path.

## Delegation

The sole authority for LLM queue, dispatch, result files, and role boundaries
is `/home/ubuntu/workspace/LLM_HARNESS_STANDARD.md`. Do not duplicate its task
format or fixed-file contract here. In particular, do not commit `.agent_tasks/`
and do not perform production DB writes, restarts, deploys, pushes, or main
merges without the required approval.

## Runtime contracts

- `config/firms.yaml` is the firm manifest; `scraper_registry.py` validates it.
- `models/report_payload.py` normalizes scraper rows; `report_unique_key` is
  the idempotency key.
- The scraper writes only its core delivery fields. Enrichment is a separate
  pipeline, so an inserted row is not evidence that tags, company mapping,
  price targets, article text, or an LLM summary exist.
- `public.v_sec_reports_canonical` and `public.v_sec_reports_full` are the
  preferred read surfaces. Live `pg_catalog` is authoritative for physical
  schema; `sql/TB_SEC_REPORTS.sql` is a non-executable pointer.
- Canonical physical fields: `report_unique_key`, `report_date`, `save_at`,
  `telegram_sent`, and `pdf_url`. `article_url`/`source_url` are nullable
  compatibility aliases, not a source-of-truth URL.

For read-only production queries, follow
`/home/ubuntu/workspace/lib/ssh_library/docs/USAGE.md` and use the
`ssh_reports_hub` role. Include a bounded time window for logs and data-quality
queries.
