#!/usr/bin/env python3
"""증권사별 마지막 레포트 일자 건강검진 — 각 증권사의 최신 데이터 확인."""
import sys, os
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv()
from models.db_factory import get_db

db = get_db()
conn = db.get_connection()
cur = conn.cursor()
cur.execute("""
    SELECT firm_id, firm_nm, COUNT(*) as total,
           MAX(report_date) as last_report_date, MAX(save_at::date) as last_save
    FROM tbl_sec_reports WHERE firm_id IS NOT NULL
    GROUP BY firm_id, firm_nm ORDER BY firm_id
""")

today = date.today()
URGENT_DAYS = 30
WARN_DAYS = 7

print(f"{'ord':>3} {'firm':<22} {'total':>7} {'last_report':>12} {'last_save':>12} {'days_ago':>9} status")
print("-" * 85)

alerts = []
for r in cur.fetchall():
    o, nm, total, report_date, save_dt = r
    report_date_str = str(report_date or "-")
    save_str = str(save_dt or "-")

    try:
        last_date = report_date if isinstance(report_date, date) else date.fromisoformat(report_date_str)
        days_ago = (today - last_date).days
    except Exception:
        days_ago = -1

    if days_ago >= URGENT_DAYS:
        status = "STALE"
        alerts.append((o, nm, days_ago, "URGENT"))
    elif days_ago >= WARN_DAYS:
        status = "WARN"
        alerts.append((o, nm, days_ago, "WARN"))
    elif days_ago < 0:
        status = "??"
    else:
        status = "OK"

    print(f"{o:>3} {str(nm or '?'):<22} {total:>7} {report_date_str:>12} {save_str:>12} {days_ago:>8}d {status}")

conn.close()

if alerts:
    print("\nALERTS:")
    for o, nm, days, level in sorted(alerts, key=lambda x: -x[2]):
        tag = "STALE" if level == "URGENT" else "WARN"
        print(f"  [{tag}] [{o}] {nm}: {days}d stale")
else:
    print("\nAll firms OK")
