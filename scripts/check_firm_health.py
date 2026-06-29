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
    SELECT sec_firm_order, firm_nm, COUNT(*) as total,
           MAX(reg_dt) as last_reg_dt, MAX(save_time::date) as last_save
    FROM tbl_sec_reports WHERE sec_firm_order IS NOT NULL
    GROUP BY sec_firm_order, firm_nm ORDER BY sec_firm_order
""")

today = date.today()
URGENT_DAYS = 30
WARN_DAYS = 7

print(f"{'ord':>3} {'firm':<22} {'total':>7} {'last_reg_dt':>12} {'last_save':>12} {'days_ago':>9} status")
print("-" * 85)

alerts = []
for r in cur.fetchall():
    o, nm, total, reg_dt, save_dt = r
    reg_str = str(reg_dt or "-")
    save_str = str(save_dt or "-")

    try:
        last_date = date(int(reg_str[:4]), int(reg_str[4:6]), int(reg_str[6:8]))
        days_ago = (today - last_date).days
    except:
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

    print(f"{o:>3} {str(nm or '?'):<22} {total:>7} {reg_str:>12} {save_str:>12} {days_ago:>8}d {status}")

conn.close()

if alerts:
    print("\nALERTS:")
    for o, nm, days, level in sorted(alerts, key=lambda x: -x[2]):
        tag = "STALE" if level == "URGENT" else "WARN"
        print(f"  [{tag}] [{o}] {nm}: {days}d stale")
else:
    print("\nAll firms OK")
