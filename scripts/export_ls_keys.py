#!/usr/bin/env python3
"""
LS증권 existing key + writer 목록을 JSON 파일로 export.
OCI cron이 주기적으로 실행 → GitHub repo에 commit/push → GA가 checkout 후 사용.

출력: data/ls_existing_keys.json
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.db_factory import get_db


def export_ls_keys(output_path: str = None) -> dict:
    """LS증권(firm_id=0)의 모든 report_unique_key + writer + article_title 조회"""
    db = get_db()

    rows = db._fetchall("""
        SELECT report_unique_key, writer, article_title
        FROM tbl_sec_reports
        WHERE firm_id = 0 AND report_unique_key IS NOT NULL AND report_unique_key != ''
        ORDER BY report_unique_key
    """)

    keys = []
    key_writer_map = {}

    for r in rows:
        k = r["report_unique_key"]
        keys.append(k)
        w = r.get("writer") or ""
        if w and k not in key_writer_map:
            key_writer_map[k] = w

    # 2. writer → emp_id 매핑 (성공한 msg URL에서 사번 추출)
    #    GA detail에서 2순위 URL 구성용 (DB 없이 CDN URL 직접 생성)
    emp_rows = db._fetchall("""
        SELECT DISTINCT writer,
               SUBSTRING(telegram_url FROM 'eum/K_\\d{8}_(.+)_\\d+\\.pdf$') AS emp_id
        FROM tbl_sec_reports
        WHERE firm_id = 0
          AND telegram_url LIKE 'https://msg.ls-sec.co.kr/eum/K_%'
          AND writer IS NOT NULL AND writer != ''
    """)
    writer_emp_map = {}
    for r in emp_rows:
        w = r["writer"]
        eid = r["emp_id"]
        if w and eid and w not in writer_emp_map:
            writer_emp_map[w] = eid

    result = {
        "exported_at": __import__("datetime").datetime.now().isoformat(),
        "count": len(keys),
        "keys": keys,
        "key_writer_map": key_writer_map,
        "writer_emp_map": writer_emp_map,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
        print(f"[export_ls_keys] {len(keys)} keys written to {output_path}")

    return result


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "data/ls_existing_keys.json"
    export_ls_keys(output)
