import sys
from datetime import date
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

import run.rag_embed_batch as batch


class FakeCursor:
    def __init__(self, udt_name="jsonb"):
        self.udt_name = udt_name
        self.executed_queries = []
        self.rowcount = 1

    def execute(self, query, params=None):
        self.executed_queries.append((query, params))

    def fetchone(self):
        if "information_schema.columns" in "".join(self.executed_queries[-1][0]):
            return (self.udt_name,)
        return None

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeConnection:
    def __init__(self, udt_name="jsonb", raise_on_execute=False):
        self.cursor_instance = FakeCursor(udt_name)
        self.rolled_back = False
        self.committed = False
        self.raise_on_execute = raise_on_execute

    def cursor(self, *args, **kwargs):
        if self.raise_on_execute:
            class ErrorCursor(FakeCursor):
                def execute(self, query, params=None):
                    raise RuntimeError("DB Error")
            return ErrorCursor()
        return self.cursor_instance

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def test_validate_embedding_response_success():
    data = {
        "data": [
            {"embedding": [0.1, 0.2], "index": 0},
            {"embedding": [0.3, 0.4], "index": 1},
        ]
    }
    vectors = batch._validate_embedding_response(data, 2)
    assert vectors == [[0.1, 0.2], [0.3, 0.4]]


def test_validate_embedding_response_length_mismatch():
    data = {
        "data": [
            {"embedding": [0.1, 0.2], "index": 0},
        ]
    }
    with pytest.raises(ValueError, match="embedding count mismatch"):
        batch._validate_embedding_response(data, 2)


def test_validate_embedding_response_index_out_of_order_sorted():
    """Out-of-order index should be sorted, not rejected."""
    data = {
        "data": [
            {"embedding": [0.3, 0.4], "index": 1},
            {"embedding": [0.1, 0.2], "index": 0},
        ]
    }
    vectors = batch._validate_embedding_response(data, 2)
    assert vectors == [[0.1, 0.2], [0.3, 0.4]]


def test_validate_embedding_response_index_missing():
    # index 키가 아예 없는 경우
    data = {
        "data": [
            {"embedding": [0.1, 0.2]},
        ]
    }
    with pytest.raises(ValueError, match="missing 'index' key"):
        batch._validate_embedding_response(data, 1)


def test_validate_embedding_response_index_duplicate():
    # index가 중복된 경우
    data = {
        "data": [
            {"embedding": [0.1, 0.2], "index": 0},
            {"embedding": [0.3, 0.4], "index": 0},
        ]
    }
    with pytest.raises(ValueError, match="duplicate embedding index"):
        batch._validate_embedding_response(data, 2)


def test_generate_embeddings_raises_on_no_api_key(monkeypatch):
    monkeypatch.setattr(batch, "EMBED_PROVIDER", "openai")
    monkeypatch.setattr(batch, "OPENAI_EMBED_API_KEY", "")
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY.*is not set"):
        batch.generate_embeddings(["test"])


def test_validate_gemini_embedding_response_success():
    data = {
        "embeddings": [
            {"values": [0.1, 0.2]},
            {"values": [0.3, 0.4]},
        ]
    }

    vectors = batch._validate_gemini_embedding_response(data, 2)

    assert vectors == [[0.1, 0.2], [0.3, 0.4]]


def test_generate_gemini_embeddings_uses_gemini_api_key(monkeypatch):
    calls = []

    def fake_post(url, *, headers, payload):
        calls.append((url, headers, payload))
        return {"embeddings": [{"values": [0.1]}, {"values": [0.2]}]}

    monkeypatch.setattr(batch, "EMBED_PROVIDER", "gemini")
    monkeypatch.setattr(batch, "GEMINI_EMBED_API_KEY", "gemini-key")
    monkeypatch.setattr(batch, "GEMINI_EMBED_API_URL", "https://generativelanguage.googleapis.com/v1beta")
    monkeypatch.setattr(batch, "_post_embedding_request", fake_post)

    vectors = batch.generate_embeddings(["a", "b"], dim=768)

    assert vectors == [[0.1], [0.2]]
    url, headers, payload = calls[0]
    assert url.endswith("/models/gemini-embedding-001:batchEmbedContents")
    assert headers["x-goog-api-key"] == "gemini-key"
    assert payload["requests"][0]["content"]["parts"][0]["text"] == "a"
    assert payload["requests"][0]["outputDimensionality"] == 768


def test_save_embeddings_json_type(monkeypatch):
    monkeypatch.setattr(batch, "_embedding_type", None)
    conn = FakeConnection(udt_name="jsonb")
    embeddings = [
        {"report_id": 1, "chunk_id": 0, "chunk_text": "text1", "embedding": [0.1, 0.2]}
    ]
    inserted = batch.save_embeddings(conn, embeddings)
    
    assert inserted == 1
    assert conn.committed is True
    # JSON 직렬화된 데이터가 들어가 있어야 함
    last_query = conn.cursor_instance.executed_queries[-1]
    assert last_query[1][3] == "[0.1, 0.2]"


def test_save_embeddings_vector_type(monkeypatch):
    monkeypatch.setattr(batch, "_embedding_type", None)
    conn = FakeConnection(udt_name="vector")
    embeddings = [
        {"report_id": 2, "chunk_id": 0, "chunk_text": "text2", "embedding": [0.5, 0.6]}
    ]
    inserted = batch.save_embeddings(conn, embeddings)
    
    assert inserted == 1
    assert conn.committed is True
    # Vector 타입인 경우 str() 형식으로 들어가야 함
    last_query = conn.cursor_instance.executed_queries[-1]
    assert last_query[1][3] == "[0.5, 0.6]"


def test_save_embeddings_rollback_on_error():
    conn = FakeConnection(raise_on_execute=True)
    embeddings = [
        {"report_id": 3, "chunk_id": 0, "chunk_text": "text3", "embedding": [0.9]}
    ]
    with pytest.raises(RuntimeError, match="DB Error"):
        batch.save_embeddings(conn, embeddings)
    assert conn.rolled_back is True


def test_main_live_run_fail_fast_on_no_api_key(monkeypatch):
    monkeypatch.setattr(batch, "EMBED_PROVIDER", "openai")
    monkeypatch.setattr(batch, "OPENAI_EMBED_API_KEY", "")
    import sys
    exit_code = None
    def fake_exit(code):
        nonlocal exit_code
        exit_code = code
        raise SystemExit(code)

    monkeypatch.setattr(sys, "exit", fake_exit)
    monkeypatch.setattr(sys, "argv", ["rag_embed_batch.py"])

    with pytest.raises(SystemExit):
        batch.main()

    assert exit_code == 1


def test_parse_inserted_date_rejects_conflicting_options():
    with pytest.raises(ValueError, match="cannot be used together"):
        batch.parse_inserted_date("2026-07-04", True)


def test_fetch_reports_can_filter_by_inserted_date():
    conn = FakeConnection()

    batch.fetch_reports(
        conn,
        days=0,
        firm=3,
        limit=20,
        inserted_date=date(2026, 7, 4),
    )

    query, params = conn.cursor_instance.executed_queries[-1]
    assert "r.save_at::date = %s" in query
    assert "r.firm_id = %s" in query
    assert params == [3, date(2026, 7, 4), 20]
