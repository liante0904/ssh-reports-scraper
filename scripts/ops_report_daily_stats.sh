#!/usr/bin/env bash
# Read-only yearly firm/day coverage for tbl_sec_reports.
set -euo pipefail

year=$(date +%Y)
mode=summary

usage() {
  cat <<'EOF'
Usage: bash scripts/ops_report_daily_stats.sh [--year YYYY] [--summary|--daily|--gaps]

Read-only OCI export for tbl_sec_reports (Asia/Seoul dates, business days only).
  --summary  one row per firm: annual counts and report/save trailing gaps (default)
  --daily    one row per firm x business day; report_count is keyed by report_date,
             save_count by save_at, so delayed imports/backfills remain visible
  --gaps     only business-day rows with no report; use with --daily data before
             declaring a scraper outage, because a source may not publish daily
EOF
}

while (($#)); do
  case "$1" in
    --year)
      [[ ${2:-} =~ ^20[0-9]{2}$ ]] || { echo "--year must be YYYY" >&2; exit 2; }
      year=$2; shift 2 ;;
    --summary) mode=summary; shift ;;
    --daily) mode=daily; shift ;;
    --gaps) mode=gaps; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

end_date="${year}-12-31"
if [[ $year == "$(date +%Y)" ]]; then end_date=$(date +%F); fi

case "$mode" in
  summary)
    query="
WITH params AS (SELECT date '${year}-01-01' AS start_day, date '${end_date}' AS end_day),
firms AS (
  SELECT firm_id, max(firm_nm) AS firm_nm FROM public.tbl_sec_reports
  WHERE firm_id IS NOT NULL GROUP BY firm_id
), report_counts AS (
  SELECT firm_id, report_date::date AS day, count(*) AS count
  FROM public.tbl_sec_reports, params
  WHERE report_date BETWEEN params.start_day AND params.end_day
  GROUP BY firm_id, report_date::date
), save_counts AS (
  SELECT firm_id, (save_at AT TIME ZONE 'Asia/Seoul')::date AS day, count(*) AS count
  FROM public.tbl_sec_reports, params
  WHERE save_at >= params.start_day AND save_at < params.end_day + interval '1 day'
  GROUP BY firm_id, (save_at AT TIME ZONE 'Asia/Seoul')::date
), daily AS (
  SELECT f.firm_id, f.firm_nm, d::date AS day,
    coalesce(rc.count, 0) AS report_count, coalesce(sc.count, 0) AS save_count
  FROM firms f CROSS JOIN params p
  CROSS JOIN LATERAL generate_series(p.start_day, p.end_day, interval '1 day') d
  LEFT JOIN report_counts rc ON rc.firm_id = f.firm_id AND rc.day = d::date
  LEFT JOIN save_counts sc ON sc.firm_id = f.firm_id AND sc.day = d::date
  WHERE extract(isodow FROM d) BETWEEN 1 AND 5
), aggregate AS (
  SELECT firm_id, max(firm_nm) AS firm_nm, count(*) AS business_days,
    sum(report_count) AS reports, sum(save_count) AS saved_rows,
    max(day) FILTER (WHERE report_count > 0) AS last_report_day,
    max(day) FILTER (WHERE save_count > 0) AS last_save_day,
    count(*) FILTER (WHERE report_count = 0) AS no_report_days,
    count(*) FILTER (WHERE save_count = 0) AS no_save_days
  FROM daily GROUP BY firm_id
)
SELECT firm_id, firm_nm, business_days, reports, saved_rows, last_report_day, last_save_day,
  (SELECT count(*) FROM daily x WHERE x.firm_id = a.firm_id AND x.day > a.last_report_day) AS trailing_report_gap_business_days,
  (SELECT count(*) FROM daily x WHERE x.firm_id = a.firm_id AND x.day > a.last_save_day) AS trailing_save_gap_business_days,
  no_report_days, no_save_days
FROM aggregate a ORDER BY trailing_save_gap_business_days DESC NULLS LAST, firm_id;"
    ;;
  daily|gaps)
    where=""
    [[ $mode == gaps ]] && where="AND coalesce(rc.count, 0) = 0"
    query="
WITH params AS (SELECT date '${year}-01-01' AS start_day, date '${end_date}' AS end_day),
firms AS (SELECT firm_id, max(firm_nm) AS firm_nm FROM public.tbl_sec_reports WHERE firm_id IS NOT NULL GROUP BY firm_id),
report_counts AS (
  SELECT firm_id, report_date::date AS day, count(*) AS count
  FROM public.tbl_sec_reports, params WHERE report_date BETWEEN params.start_day AND params.end_day
  GROUP BY firm_id, report_date::date
), save_counts AS (
  SELECT firm_id, (save_at AT TIME ZONE 'Asia/Seoul')::date AS day, count(*) AS count,
    max(save_at AT TIME ZONE 'Asia/Seoul') AS latest_saved_at
  FROM public.tbl_sec_reports, params
  WHERE save_at >= params.start_day AND save_at < params.end_day + interval '1 day'
  GROUP BY firm_id, (save_at AT TIME ZONE 'Asia/Seoul')::date
)
SELECT f.firm_id, f.firm_nm, d::date AS business_day,
  coalesce(rc.count, 0) AS report_count, coalesce(sc.count, 0) AS save_count, sc.latest_saved_at
FROM firms f CROSS JOIN params p
CROSS JOIN LATERAL generate_series(p.start_day, p.end_day, interval '1 day') d
LEFT JOIN report_counts rc ON rc.firm_id = f.firm_id AND rc.day = d::date
LEFT JOIN save_counts sc ON sc.firm_id = f.firm_id AND sc.day = d::date
WHERE extract(isodow FROM d) BETWEEN 1 AND 5
${where}
ORDER BY f.firm_id, business_day;"
    ;;
esac

ssh oci 'docker exec -i main-postgres psql -X -q -U ssh_reports_hub -d ssh_reports_hub -v ON_ERROR_STOP=1 --csv -P pager=off' <<SQL
${query}
SQL
