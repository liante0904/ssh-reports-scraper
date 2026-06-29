#!/usr/bin/env python3
"""RAG 임베딩 배치 — Reports Hub → Private Hub 파이프라인 (2단계).

tbl_sec_reports.article_title을 읽어 임베딩 벡터를 생성,
private-hub의 tbl_report_embeddings에 저장.

사용법:
    # 최근 3일치만 (프로토타입)
    uv run python run/rag_embed_batch.py --days 3 --dry-run

    # 전체 백필 (배치: 1000건씩)
    uv run python run/rag_embed_batch.py --batch-size 1000 --batch-count 10

    # 특정 증권사만
    uv run python run/rag_embed_batch.py --firm 4 --days 30  # KB증권 최근 30일

환경변수:
    DEEPSEEK_API_KEY  — DeepSeek API 키 (기본)
    OPENAI_API_KEY    — OpenAI API 키 (fallback)
    EMBED_MODEL       — 모델명 (기본: deepseek-chat)
    POSTGRES_HOST/PORT/DB/USER/PASSWORD — DB 접속
"""
import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
import requests


# ── 설정 ──
EMBED_MODEL = os.getenv("EMBED_MODEL", "deepseek-chat")
EMBED_API_KEY = os.getenv("DEEPSEEK_API_KEY") or os.getenv("OPENAI_API_KEY")
EMBED_API_URL = os.getenv("EMBED_API_URL", "https://api.deepseek.com/v1/embeddings")
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "20"))  # API 한번에 보낼 텍스트 수


def get_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def fetch_reports(conn, days=None, firm=None, limit=1000, offset=0):
    """임베딩 안 된 리포트만 조회."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    where = ["r.article_title IS NOT NULL", "r.article_title != ''",
             "e.report_id IS NULL"]  # 아직 임베딩 없는 것만
    params = []

    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        where.append("r.report_date >= %s")
        params.append(cutoff)
    if firm:
        where.append("r.sec_firm_order = %s")
        params.append(firm)

    query = f"""
        SELECT r.report_id, r.article_title, r.firm_nm, r.writer, r.report_date
        FROM tbl_sec_reports r
        LEFT JOIN tbl_report_embeddings e ON r.report_id = e.report_id
        WHERE {' AND '.join(where)}
        ORDER BY r.report_date DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    cur.execute(query, params)
    return cur.fetchall()


def generate_embeddings(texts: list[str]) -> list[list[float]]:
    """API로 임베딩 벡터 생성."""
    if not EMBED_API_KEY:
        # API 키 없으면 dummy hash 기반 pseudo-embedding
        print("[WARN] No API key — using hash-based pseudo embeddings", file=sys.stderr)
        return [[float(ord(c) % 100) / 100.0 for c in hashlib.md5(t.encode()).hexdigest()[:32]] for t in texts]

    headers = {
        "Authorization": f"Bearer {EMBED_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": EMBED_MODEL,
        "input": texts,
    }
    resp = requests.post(EMBED_API_URL, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return [item["embedding"] for item in data["data"]]


def save_embeddings(conn, embeddings: list[dict], dry_run=False):
    """임베딩 결과를 tbl_report_embeddings에 저장."""
    cur = conn.cursor()
    inserted = 0
    for emb in embeddings:
        if dry_run:
            print(f"  [DRY-RUN] report_id={emb['report_id']}: {emb['title'][:50]}...")
            inserted += 1
            continue
        try:
            cur.execute("""
                INSERT INTO tbl_report_embeddings (report_id, chunk_id, chunk_text, embedding)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (report_id, chunk_id) DO NOTHING
            """, (emb["report_id"], emb["chunk_id"], emb["chunk_text"],
                  json.dumps(emb["embedding"])))
            inserted += 1
        except Exception as e:
            print(f"  [ERR] report_id={emb['report_id']}: {e}", file=sys.stderr)
    if not dry_run:
        conn.commit()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="RAG Embedding Batch")
    parser.add_argument("--days", type=int, help="최근 N일만 처리")
    parser.add_argument("--firm", type=int, help="특정 sec_firm_order만")
    parser.add_argument("--batch-size", type=int, default=1000, help="DB에서 가져올 건수")
    parser.add_argument("--batch-count", type=int, default=1, help="반복 횟수")
    parser.add_argument("--dry-run", action="store_true", help="DB 저장 없이 출력만")
    args = parser.parse_args()

    conn = get_db()
    total_embedded = 0

    for batch_no in range(args.batch_count):
        offset = batch_no * args.batch_size
        reports = fetch_reports(conn, days=args.days, firm=args.firm,
                                limit=args.batch_size, offset=offset)
        if not reports:
            print(f"Batch {batch_no + 1}: No more reports to embed")
            break

        print(f"Batch {batch_no + 1}: {len(reports)} reports")

        # API 배치 단위로 임베딩 생성
        embeddings = []
        for i in range(0, len(reports), BATCH_SIZE):
            chunk = reports[i:i + BATCH_SIZE]
            texts = [r["article_title"] for r in chunk]
            try:
                vectors = generate_embeddings(texts)
                for r, vec in zip(chunk, vectors):
                    embeddings.append({
                        "report_id": r["report_id"],
                        "chunk_id": 0,
                        "chunk_text": r["article_title"],
                        "embedding": vec,
                        "title": r["article_title"],
                    })
            except Exception as e:
                print(f"  Embed API error: {e}", file=sys.stderr)
            time.sleep(0.5)  # rate limit

        n = save_embeddings(conn, embeddings, dry_run=args.dry_run)
        total_embedded += n
        print(f"  Saved: {n} embeddings (total: {total_embedded})")

    conn.close()
    print(f"\nDONE: {total_embedded} embeddings {'(DRY-RUN)' if args.dry_run else 'saved'}")


if __name__ == "__main__":
    main()
