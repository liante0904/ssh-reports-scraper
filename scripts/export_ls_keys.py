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
    """LS증권(sec_firm_order=0)의 모든 key + writer + article_title 조회"""
    db = get_db()

    rows = db._fetchall("""
        SELECT key, writer, article_title
        FROM tbl_sec_reports
        WHERE sec_firm_order = 0 AND key IS NOT NULL AND key != ''
        ORDER BY key
    """)

    keys = []
    key_writer_map = {}

    for r in rows:
        k = r["key"]
        keys.append(k)
        w = r.get("writer") or ""
        if w and k not in key_writer_map:
            key_writer_map[k] = w

    result = {
        "exported_at": __import__("datetime").datetime.now().isoformat(),
        "count": len(keys),
        "keys": keys,
        "key_writer_map": key_writer_map,
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
