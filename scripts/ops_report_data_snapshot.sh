#!/usr/bin/env bash
# Read-only production snapshot for tbl_sec_reports.
# Emits one compact JSON object so an LLM can inspect the live data product
# without traversing SSH, schema, scraper, and enrichment paths separately.
set -euo pipefail

days=7
include_schema=false

usage() {
  cat <<'EOF'
Usage: bash scripts/ops_report_data_snapshot.sh [--days N] [--schema]

Read-only OCI snapshot of tbl_sec_reports. Default output is one compact JSON
object covering freshness, delivery/archive state, and investment-data coverage
for the latest N saved rows (default: 7 days). --schema appends physical columns.
EOF
}

while (($#)); do
  case "$1" in
    --days)
      [[ ${2:-} =~ ^[1-9][0-9]{0,3}$ ]] || { echo "--days must be 1..9999" >&2; exit 2; }
      days=$2
      shift 2
      ;;
    --schema)
      include_schema=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

schema_sql="'[]'::jsonb"
if [[ $include_schema == true ]]; then
  schema_sql="(
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'name', column_name, 'type', data_type, 'nullable', (is_nullable = 'YES')
    ) ORDER BY ordinal_position), '[]'::jsonb)
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'tbl_sec_reports'
  )"
fi

ssh oci 'docker exec -i main-postgres psql -X -q -U ssh_reports_hub -d ssh_reports_hub -At -v ON_ERROR_STOP=1' <<SQL
WITH scope AS (
  SELECT *
  FROM public.tbl_sec_reports
  WHERE save_at >= now() - interval '${days} days'
), latest_by_firm AS (
  SELECT firm_id, max(firm_nm) AS firm_nm, max(save_at) AS last_saved_at
  FROM public.tbl_sec_reports
  WHERE firm_id IS NOT NULL
  GROUP BY firm_id
), stale_firms AS (
  SELECT coalesce(jsonb_agg(jsonb_build_object('firm_id', firm_id, 'firm_nm', firm_nm, 'last_saved_at', last_saved_at)
    ORDER BY last_saved_at), '[]'::jsonb) AS items
  FROM latest_by_firm
  WHERE last_saved_at < now() - interval '48 hours'
)
SELECT jsonb_build_object(
  'generated_at', now(),
  'window_days', ${days},
  'table', 'public.tbl_sec_reports',
  'freshness', jsonb_build_object(
    'total_rows', (SELECT count(*) FROM public.tbl_sec_reports),
    'window_rows', (SELECT count(*) FROM scope),
    'last_saved_at', (SELECT max(save_at) FROM public.tbl_sec_reports),
    'last_report_date', (SELECT max(report_date) FROM public.tbl_sec_reports),
    'future_dated_rows', (SELECT count(*) FROM public.tbl_sec_reports WHERE report_date > current_date)
  ),
  'core_quality', jsonb_build_object(
    'missing_title', (SELECT count(*) FROM scope WHERE coalesce(nullif(btrim(article_title), ''), '') = ''),
    'missing_pdf_url', (SELECT count(*) FROM scope WHERE coalesce(nullif(btrim(pdf_url), ''), '') = ''),
    'missing_telegram_url', (SELECT count(*) FROM scope WHERE coalesce(nullif(btrim(telegram_url), ''), '') = '')
  ),
  'investment_coverage', jsonb_build_object(
    'with_tickers', (SELECT count(*) FROM scope WHERE stock_tickers <> '[]'::jsonb),
    'with_tags', (SELECT count(*) FROM scope WHERE tags <> '[]'::jsonb),
    'with_article_text', (SELECT count(*) FROM scope WHERE article_text IS NOT NULL AND btrim(article_text) <> ''),
    'with_llm_summary', (SELECT count(*) FROM scope WHERE gemini_summary IS NOT NULL AND btrim(gemini_summary) <> ''),
    'with_target_price', (SELECT count(*) FROM scope WHERE target_price IS NOT NULL),
    'with_fnguide_match', (SELECT count(*) FROM scope WHERE fnguide_summary_id IS NOT NULL)
  ),
  'pipeline_state', jsonb_build_object(
    'by_sync_and_pdf_status', (
      SELECT coalesce(jsonb_agg(jsonb_build_object('sync_status', sync_status, 'pdf_sync_status', pdf_sync_status, 'count', count)
        ORDER BY count DESC), '[]'::jsonb)
      FROM (
        SELECT sync_status, pdf_sync_status, count(*) AS count
        FROM scope GROUP BY sync_status, pdf_sync_status
      ) status_counts
    ),
    'stale_firms_over_48h', (SELECT items FROM stale_firms)
  ),
  'schema', ${schema_sql}
);
SQL
