#!/usr/bin/env python3
"""RAG 임베딩 배치 — Reports Hub → Private Hub 파이프라인 (2단계).

tbl_sec_reports.article_title을 읽어 임베딩 벡터를 생성,
private-hub의 tbl_report_embeddings에 저장.

사용법:
    # dry-run — API 호출 / DB 저장 없이 후보만 출력
    uv run python run/rag_embed_batch.py --days 3 --dry-run

    # 오늘 insert/save된 하나증권 후보만 확인 후 실행
    uv run python run/rag_embed_batch.py --firm 3 --inserted-today --dry-run
    uv run python run/rag_embed_batch.py --firm 3 --inserted-today --batch-size 20

    # live — 실제 임베딩 생성 + DB 저장
    uv run python run/rag_embed_batch.py --days 3

    # 전체 백필
    uv run python run/rag_embed_batch.py --batch-size 20

    # 특정 증권사만
    uv run python run/rag_embed_batch.py --firm 4 --days 30  # KB증권 최근 30일

환경변수:
    EMBED_PROVIDER          — openai 또는 gemini (기본: openai)
    OPENAI_API_KEY          — OpenAI API 키
    GEMINI_API_KEY          — Gemini API 키
    EMBED_API_KEY           — provider별 API 키 대신 명시적 override
    EMBED_MODEL             — provider별 모델명 override
    EMBED_API_URL           — API URL (기본: https://api.openai.com/v1/embeddings)
    GEMINI_EMBED_API_URL    — Gemini API URL prefix
    EMBED_BATCH_SIZE        — API 한 번에 보낼 텍스트 수 (기본: 20)
    POSTGRES_HOST/PORT/DB/USER/PASSWORD — DB 접속
"""
import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

import psycopg2
import psycopg2.extras
import requests


# ── 설정 ──
EMBED_PROVIDER = os.getenv("EMBED_PROVIDER", "openai").strip().lower()
_MODEL_OVERRIDE = os.getenv("EMBED_MODEL")
EMBED_MODEL = (
    os.getenv("OPENAI_EMBED_MODEL")
    or (_MODEL_OVERRIDE if EMBED_PROVIDER == "openai" and _MODEL_OVERRIDE else None)
    or "text-embedding-3-small"
)
GEMINI_EMBED_MODEL = (
    os.getenv("GEMINI_EMBED_MODEL")
    or (_MODEL_OVERRIDE if EMBED_PROVIDER == "gemini" and _MODEL_OVERRIDE else None)
    or "gemini-embedding-001"
)
OPENAI_EMBED_API_KEY = os.getenv("EMBED_API_KEY") or os.getenv("OPENAI_API_KEY", "")
GEMINI_EMBED_API_KEY = os.getenv("EMBED_API_KEY") or os.getenv("GEMINI_API_KEY", "")
EMBED_API_URL = os.getenv(
    "EMBED_API_URL", "https://api.openai.com/v1/embeddings"
)
GEMINI_EMBED_API_URL = os.getenv(
    "GEMINI_EMBED_API_URL", "https://generativelanguage.googleapis.com/v1beta"
).rstrip("/")
API_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "20"))
EMBED_DIM = int(os.getenv("EMBED_DIM", "0") or "0")  # 0 = 첫 응답에서 자동 감지


def get_embedding_api_key() -> str:
    if EMBED_PROVIDER == "gemini":
        return GEMINI_EMBED_API_KEY
    if EMBED_PROVIDER == "openai":
        return OPENAI_EMBED_API_KEY
    raise ValueError(f"Unsupported EMBED_PROVIDER: {EMBED_PROVIDER}")


def _format_missing_key_message() -> str:
    if EMBED_PROVIDER == "gemini":
        return "GEMINI_API_KEY (or EMBED_API_KEY) is required for live run."
    if EMBED_PROVIDER == "openai":
        return "OPENAI_API_KEY (or EMBED_API_KEY) is required for live run."
    return f"Unsupported EMBED_PROVIDER: {EMBED_PROVIDER}"


def _validate_embedding_response(
    data: dict, expected_count: int
) -> list[list[float]]:
    """API 응답 검증 — 개수·인덱스·구조 불일치 시 ValueError."""
    try:
        items = data["data"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"embedding response missing 'data' key: {exc}") from exc

    if not isinstance(items, list):
        raise ValueError(
            f"embedding response 'data' is not a list: {type(items).__name__}"
        )

    if len(items) != expected_count:
        raise ValueError(
            f"embedding count mismatch: expected {expected_count}, got {len(items)}"
        )

    # Validate each item and build (index, vector) pairs
    indexed: list[tuple[int, list[float]]] = []
    for idx_in_list, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(
                f"data item at {idx_in_list} is not an object: {type(item).__name__}"
            )
        vec = item.get("embedding")
        if vec is None:
            raise ValueError(f"data item at {idx_in_list} missing 'embedding' key")
        if not isinstance(vec, list):
            raise ValueError(
                f"data item at {idx_in_list}.embedding is not a list: {type(vec).__name__}"
            )
        idx = item.get("index")
        if idx is None:
            raise ValueError(f"data item at {idx_in_list} missing 'index' key")
        if not isinstance(idx, int):
            raise ValueError(f"data item at {idx_in_list}.index is not an int: {type(idx).__name__}")
        indexed.append((idx, vec))

    # Sort by index to handle out-of-order responses safely
    indexed.sort(key=lambda x: x[0])

    # Detect duplicate or missing indices
    seen_indices = set()
    for idx, _ in indexed:
        if idx in seen_indices:
            raise ValueError(f"duplicate embedding index: {idx}")
        if idx < 0 or idx >= expected_count:
            raise ValueError(
                f"embedding index {idx} out of range [0, {expected_count})"
            )
        seen_indices.add(idx)

    if len(seen_indices) != expected_count:
        missing = set(range(expected_count)) - seen_indices
        raise ValueError(f"missing embedding indices: {sorted(missing)}")

    return [vec for _, vec in indexed]


def _validate_gemini_embedding_response(data: dict, expected_count: int) -> list[list[float]]:
    try:
        embeddings = data["embeddings"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Gemini embedding response missing 'embeddings' key: {exc}") from exc

    if not isinstance(embeddings, list):
        raise ValueError(
            f"Gemini embedding response 'embeddings' is not a list: {type(embeddings).__name__}"
        )
    if len(embeddings) != expected_count:
        raise ValueError(
            f"Gemini embedding count mismatch: expected {expected_count}, got {len(embeddings)}"
        )

    vectors = []
    for index, item in enumerate(embeddings):
        if not isinstance(item, dict):
            raise ValueError(
                f"Gemini embedding item at {index} is not an object: {type(item).__name__}"
            )
        values = item.get("values")
        if not isinstance(values, list):
            raise ValueError(
                f"Gemini embedding item at {index}.values is not a list: {type(values).__name__}"
            )
        vectors.append(values)
    return vectors


def get_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def parse_inserted_date(value: str | None, inserted_today: bool) -> date | None:
    if value and inserted_today:
        raise ValueError("--inserted-date and --inserted-today cannot be used together")
    if inserted_today:
        return date.today()
    if value:
        return date.fromisoformat(value)
    return None


def append_report_filters(where: list[str], params: list, *, days=None, firm=None, inserted_date=None):
    if days:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        where.append("r.report_date >= %s")
        params.append(cutoff)
    if firm:
        where.append("r.firm_id = %s")
        params.append(firm)
    if inserted_date:
        where.append("r.save_at::date = %s")
        params.append(inserted_date)


def fetch_reports(conn, days=None, firm=None, limit=1000, inserted_date=None):
    """임베딩 안 된 리포트만 조회 (offset 없음 — live는 매번 재조회)."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    where = [
        "r.article_title IS NOT NULL",
        "r.article_title != ''",
        "e.report_id IS NULL",  # 아직 임베딩 없는 것만
    ]
    params: list = []
    append_report_filters(where, params, days=days, firm=firm, inserted_date=inserted_date)

    query = f"""
        SELECT r.report_id, r.article_title, r.firm_nm, r.writer, r.report_date
        FROM tbl_sec_reports r
        LEFT JOIN tbl_report_embeddings e ON r.report_id = e.report_id
        WHERE {' AND '.join(where)}
        ORDER BY r.report_date DESC
        LIMIT %s
    """
    params.append(limit)
    cur.execute(query, params)
    return cur.fetchall()


def _post_embedding_request(url: str, *, headers: dict, payload: dict) -> dict:
    last_exc = None
    for attempt in range(3):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            last_exc = e
            status = e.response.status_code if e.response is not None else 0
            if status == 429:
                wait = int(e.response.headers.get("Retry-After", 2 ** attempt))
                print(f"  Rate limited (429), waiting {wait}s...", file=sys.stderr)
                time.sleep(wait)
            elif status >= 500:
                time.sleep(2 ** attempt)
            else:
                raise
        except requests.RequestException as e:
            last_exc = e
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Embedding API failed after 3 attempts: {last_exc}") from last_exc


def generate_openai_embeddings(
    texts: list[str], *, model: str = EMBED_MODEL, dim: int | None = None,
) -> list[list[float]]:
    api_key = OPENAI_EMBED_API_KEY
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY (or EMBED_API_KEY) is not set. Set the environment variable and retry.")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload: dict = {"model": model, "input": texts}
    if dim:
        payload["dimensions"] = dim
    data = _post_embedding_request(EMBED_API_URL, headers=headers, payload=payload)
    return _validate_embedding_response(data, len(texts))


def _gemini_model_resource(model: str) -> str:
    return model if model.startswith("models/") else f"models/{model}"


def generate_gemini_embeddings(
    texts: list[str], *, model: str = GEMINI_EMBED_MODEL, dim: int | None = None,
) -> list[list[float]]:
    api_key = GEMINI_EMBED_API_KEY
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY (or EMBED_API_KEY) is not set. Set the environment variable and retry.")

    model_resource = _gemini_model_resource(model)
    endpoint = f"{GEMINI_EMBED_API_URL}/{model_resource}:batchEmbedContents"
    requests_payload = []
    for text in texts:
        request = {
            "model": model_resource,
            "content": {"parts": [{"text": text}]},
            "taskType": "RETRIEVAL_DOCUMENT",
        }
        if dim:
            request["outputDimensionality"] = dim
        requests_payload.append(request)

    data = _post_embedding_request(
        endpoint,
        headers={
            "x-goog-api-key": api_key,
            "Content-Type": "application/json",
        },
        payload={"requests": requests_payload},
    )
    return _validate_gemini_embedding_response(data, len(texts))


def generate_embeddings(
    texts: list[str], *, model: str | None = None, dim: int | None = None,
) -> list[list[float]]:
    """Provider별 embedding API 호출. texts는 빈 리스트 금지."""
    if not texts:
        raise ValueError("generate_embeddings called with empty texts")

    if EMBED_PROVIDER == "gemini":
        return generate_gemini_embeddings(texts, model=model or GEMINI_EMBED_MODEL, dim=dim)
    if EMBED_PROVIDER == "openai":
        return generate_openai_embeddings(texts, model=model or EMBED_MODEL, dim=dim)
    raise RuntimeError(f"Unsupported EMBED_PROVIDER: {EMBED_PROVIDER}")


_embedding_type = None


def get_embedding_type(conn) -> str:
    """tbl_report_embeddings.embedding 컬럼의 데이터 타입을 조회하여 'vector' 또는 'json' 반환."""
    global _embedding_type
    if _embedding_type is not None:
        return _embedding_type
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_name = 'tbl_report_embeddings'
                  AND column_name = 'embedding'
                """
            )
            row = cur.fetchone()
            if row:
                udt = row[0]
                if udt == "vector":
                    _embedding_type = "vector"
                elif udt in ("jsonb", "json"):
                    _embedding_type = "json"
            if not _embedding_type:
                _embedding_type = "json"
    except Exception:
        _embedding_type = "json"
    return _embedding_type


def save_embeddings(conn, embeddings: list[dict]) -> int:
    """임베딩 결과를 tbl_report_embeddings에 저장. 실패 시 rollback + raise."""
    if not embeddings:
        return 0

    emb_type = get_embedding_type(conn)
    cur = conn.cursor()
    inserted_count = 0
    try:
        for emb in embeddings:
            val = emb["embedding"]
            if emb_type == "json":
                db_val = json.dumps(val)
            else:
                db_val = str(val)

            cur.execute(
                """
                INSERT INTO tbl_report_embeddings
                    (report_id, chunk_id, chunk_text, embedding)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (report_id, chunk_id) DO NOTHING
                """,
                (
                    emb["report_id"],
                    emb["chunk_id"],
                    emb["chunk_text"],
                    db_val,
                ),
            )
            inserted_count += cur.rowcount
        conn.commit()
        return inserted_count
    except Exception:
        conn.rollback()
        raise


def _print_dry_run(reports, limit=10):
    """API 호출 / DB 저장 없이 후보만 출력."""
    print(f"[DRY-RUN] {len(reports)} unembedded report(s) found.")
    for r in reports[:limit]:
        title = (r.get("article_title") or "")[:80]
        print(
            f"  report_id={r['report_id']} "
            f"firm={r.get('firm_nm','?')} "
            f"date={r.get('report_date','?')} "
            f"title={title}"
        )
    if len(reports) > limit:
        print(f"  ... and {len(reports) - limit} more (use --batch-size to see all)")


def main():
    parser = argparse.ArgumentParser(description="RAG Embedding Batch")
    parser.add_argument("--days", type=int, help="최근 N일만 처리")
    parser.add_argument("--firm", type=int, help="특정 firm_id만")
    parser.add_argument(
        "--inserted-date",
        help="특정 insert/save 날짜만 처리 (YYYY-MM-DD, save_at 기준)",
    )
    parser.add_argument(
        "--inserted-today",
        action="store_true",
        help="오늘 insert/save된 레포트만 처리 (save_at 기준)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000,
        help="DB에서 가져올 최대 건수 (dry-run에서만 page용 offset 사용)",
    )
    parser.add_argument(
        "--batch-count", type=int, default=None,
        help="dry-run에서 조회할 최대 페이지 수 (기본: 전체)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="API 호출/DB 저장 없이 후보만 출력",
    )
    parser.add_argument(
        "--embed-dim", type=int, default=None,
        help="embedding 차원 (기본: 자동 감지)",
    )
    args = parser.parse_args()
    try:
        inserted_date = parse_inserted_date(args.inserted_date, args.inserted_today)
    except ValueError as e:
        parser.error(str(e))

    try:
        api_key = get_embedding_api_key()
    except ValueError as e:
        parser.error(str(e))

    # ── Live run guard: API key required (before DB connection) ──
    if not args.dry_run and not api_key:
        print(f"FATAL: {_format_missing_key_message()}", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        conn = get_db()

        # ── Dry-run: API 없이 후보 출력만 ──
        if args.dry_run:
            # dry-run은 offset으로 여러 페이지 조회 가능
            max_pages = args.batch_count or (1 << 30)  # None → unbounded
            total = 0
            for page in range(max_pages):
                offset = page * args.batch_size
                cur = conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                )
                where = [
                    "r.article_title IS NOT NULL",
                    "r.article_title != ''",
                    "e.report_id IS NULL",
                ]
                params: list = []
                append_report_filters(
                    where,
                    params,
                    days=args.days,
                    firm=args.firm,
                    inserted_date=inserted_date,
                )

                query = f"""
                    SELECT r.report_id, r.article_title, r.firm_nm, r.report_date
                    FROM tbl_sec_reports r
                    LEFT JOIN tbl_report_embeddings e ON r.report_id = e.report_id
                    WHERE {' AND '.join(where)}
                    ORDER BY r.report_date DESC
                    LIMIT %s OFFSET %s
                """
                params.extend([args.batch_size, offset])
                cur.execute(query, params)
                rows = cur.fetchall()
                if not rows:
                    break
                _print_dry_run(rows, limit=20)
                total += len(rows)
                if len(rows) < args.batch_size:
                    break

            print(
                f"\n[DRY-RUN] Total: {total} unembedded reports "
                f"(no API calls, no DB writes)"
            )
            # API 키 없어도 dry-run은 정상 종료
            if not api_key:
                print(f"[WARN] {_format_missing_key_message()}", file=sys.stderr)
            return

        # ── Live run: offset 없이 반복 (embedding 저장 후 재조회하면 자연 감소) ──
        embed_dim = args.embed_dim or (EMBED_DIM if EMBED_DIM > 0 else None)
        total_embedded = 0
        consecutive_empty = 0

        while True:
            reports = fetch_reports(
                conn,
                days=args.days,
                firm=args.firm,
                limit=args.batch_size,
                inserted_date=inserted_date,
            )
            if not reports:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                time.sleep(1)
                continue
            consecutive_empty = 0

            print(f"[live] {len(reports)} reports to embed")

            # API 배치 단위로 임베딩 생성
            embeddings = []
            for i in range(0, len(reports), API_BATCH_SIZE):
                chunk = reports[i : i + API_BATCH_SIZE]
                texts = [r["article_title"] for r in chunk]
                try:
                    vectors = generate_embeddings(texts, dim=embed_dim)
                except RuntimeError as e:
                    # 이전 chunk에서 성공한 embedding 저장 후 fail-fast
                    print(f"  Embed API fatal error: {e}", file=sys.stderr)
                    if embeddings:
                        try:
                            saved = save_embeddings(conn, embeddings)
                            total_embedded += saved
                            print(
                                f"  saved {saved} embeddings from earlier chunks "
                                "before exit.",
                                file=sys.stderr,
                            )
                        except Exception as db_err:
                            print(
                                f"  DB save also failed: {db_err}",
                                file=sys.stderr,
                            )
                    sys.exit(1)

                if len(chunk) != len(vectors):
                    raise ValueError(
                        f"Chunk size mismatch: expected {len(chunk)} vectors, but got {len(vectors)}"
                    )

                for r, vec in zip(chunk, vectors):
                    embeddings.append(
                        {
                            "report_id": r["report_id"],
                            "chunk_id": 0,
                            "chunk_text": r["article_title"],
                            "embedding": vec,
                        }
                    )

                # auto-detect dimension from first successful response
                if embed_dim is None and vectors:
                    embed_dim = len(vectors[0])
                    print(f"  [info] detected embedding dim={embed_dim}", file=sys.stderr)

                time.sleep(0.3)  # rate limit

            # DB 저장
            if embeddings:
                try:
                    saved = save_embeddings(conn, embeddings)
                    total_embedded += saved
                    print(f"  saved: {saved} (total: {total_embedded})")
                except Exception as e:
                    print(
                        f"  DB insert failed: {e} — aborting",
                        file=sys.stderr,
                    )
                    sys.exit(1)

            if len(reports) < args.batch_size:
                # 모든 후보 소진
                break

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    print(
        f"\nDONE: {total_embedded} embeddings saved"
    )


if __name__ == "__main__":
    main()
