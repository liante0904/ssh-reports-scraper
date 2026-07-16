# -*- coding:utf-8 -*-
import sys
import os
import asyncio
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

# Mock DB 매니저 클래스 정의
class MockDB:
    def __init__(self, rows):
        self.rows = rows
        self.daily_update_calls = []

    def _fetchall(self, sql, params):
        # 쿼리가 report_unique_key IN (...) 형태이므로, rows에서 매칭되는 것 필터링
        return [r for r in self.rows if r.get("report_unique_key") in params]

    async def daily_update_data(self, fetched_rows, type):
        self.daily_update_calls.append((fetched_rows, type))
        return {"status": "success"}


def test_process_ga_file_retries_empty_result_without_writing_to_db(tmp_path):
    """An SCP-created empty destination is retried instead of treated as failed."""
    from scheduler import GAImportRetryableError, _process_ga_file

    result = tmp_path / "sks_result.json"
    result.write_text("", encoding="utf-8")

    class NoWriteDB:
        def insert_json_data_list(self, _rows):
            raise AssertionError("empty result must not reach the database")

    with pytest.raises(GAImportRetryableError, match="empty"):
        _process_ga_file(result, NoWriteDB())


def test_process_ga_file_retries_recent_partial_json(tmp_path):
    """A recently modified partial SCP result is left for the next poll."""
    from scheduler import GAImportRetryableError, _process_ga_file

    result = tmp_path / "toss_result.json"
    result.write_text('[{"report_unique_key": "incomplete"}', encoding="utf-8")

    class NoWriteDB:
        def insert_json_data_list(self, _rows):
            raise AssertionError("partial result must not reach the database")

    with pytest.raises(GAImportRetryableError, match="incomplete"):
        _process_ga_file(result, NoWriteDB())


# sendMarkDownText 모킹을 위한 헬퍼 클래스
class MockTelegramSender:
    def __init__(self, fail_on_chunk_index=None):
        self.sent_messages = []
        self.fail_on_chunk_index = fail_on_chunk_index
        self.call_count = 0

    async def send_markdown(self, token, chat_id, sendMessageText):
        self.call_count += 1
        if self.fail_on_chunk_index is not None and self.call_count == self.fail_on_chunk_index:
            raise RuntimeError("Telegram API Temporary Outage (Mocked Error)")
        self.sent_messages.append(sendMessageText)


def test_broadcast_ga_reports_success(monkeypatch):
    """GA 브로드캐스트 전송이 모두 성공할 때 청크가 잘 분리되어 전송되고 각각 DB 업데이트가 즉시 호출되는지 테스트"""
    from scheduler import _broadcast_ga_reports

    # 테스트 환경 변수 설정
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "mock_token")
    monkeypatch.setenv("TELEGRAM_CHANNEL_ID_REPORT_ALARM", "mock_chat_id")

    # 3500자를 가뿐히 초과하도록 긴 리포트 생성 (한 리포트당 약 100자)
    mock_rows = []
    keys = []
    for i in range(40):
        key = f"key_{i}"
        keys.append(key)
        mock_rows.append({
            "report_id": i,
            "firm_id": 1,
            "firm_nm": "테스트증권",
            "article_title": f"매우 긴 리포트 제목 번호 {i}번 " * 5,  # 약 80자
            "report_unique_key": key,
            "telegram_url": f"https://example.test/pdf_{i}.pdf",
        })

    db = MockDB(mock_rows)
    sender = MockTelegramSender()

    # sendMarkDownText 함수를 MockTelegramSender의 메서드로 대체
    monkeypatch.setattr("utils.telegram_util.sendMarkDownText", sender.send_markdown)

    # 함수 실행
    _broadcast_ga_reports(db, keys)

    # 1. 텔레그램 메시지가 여러 개의 청크로 나뉘어 전송되었는지 확인 (최소 2개 이상)
    assert len(sender.sent_messages) >= 2
    assert sender.call_count == len(sender.sent_messages)

    # 2. 텔레그램을 보낸 청크 횟수만큼 DB daily_update_data가 호출되었는지 확인
    assert len(db.daily_update_calls) == len(sender.sent_messages)

    # 3. DB에 업데이트된 총 리포트 행 수가 전체 전송 행 수와 일치하는지 확인
    total_marked_rows = 0
    for updated_rows, update_type in db.daily_update_calls:
        assert update_type == "send"
        total_marked_rows += len(updated_rows)

    assert total_marked_rows == len(mock_rows)


def test_broadcast_ga_reports_partial_failure(monkeypatch):
    """일부 텔레그램 메시지 발송 실패 시, 성공한 청크들만 DB에 즉시 성공 마킹이 되고 에러난 청크는 마킹되지 않는지 테스트"""
    from scheduler import _broadcast_ga_reports

    # 테스트 환경 변수 설정
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "mock_token")
    monkeypatch.setenv("TELEGRAM_CHANNEL_ID_REPORT_ALARM", "mock_chat_id")

    # 3500자 초과를 위해 40개의 대량 리포트 생성
    mock_rows = []
    keys = []
    for i in range(40):
        key = f"key_{i}"
        keys.append(key)
        mock_rows.append({
            "report_id": i,
            "firm_id": 1,
            "firm_nm": "테스트증권",
            "article_title": f"매우 긴 리포트 제목 번호 {i}번 " * 5,
            "report_unique_key": key,
            "telegram_url": f"https://example.test/pdf_{i}.pdf",
        })

    db = MockDB(mock_rows)
    
    # 2번째 청크 전송 시 에러를 유발하도록 설정
    sender = MockTelegramSender(fail_on_chunk_index=2)

    monkeypatch.setattr("utils.telegram_util.sendMarkDownText", sender.send_markdown)

    # 함수 실행 (로그에 에러가 출력되겠지만 스크립트는 중단되지 않아야 함)
    _broadcast_ga_reports(db, keys)

    # 1. 텔레그램 전송 호출 횟수는 2번 이상이어야 함 (2번째에서 실패했으나 루프는 끝까지 돎)
    assert sender.call_count >= 2

    # 2. 실제로 성공하여 텔레그램이 전송된 갯수는 에러난 1건을 제외한 수
    # (예: 총 3개의 청크가 만들어졌다면, 1번 성공, 2번 실패, 3번 성공하여 총 2개 메시지가 전송됨)
    assert len(sender.sent_messages) == sender.call_count - 1

    # 3. DB 업데이트(daily_update_data)는 성공하여 텔레그램 전송 완료된 청크들에 대해서만 성공적으로 호출되어야 함
    assert len(db.daily_update_calls) == len(sender.sent_messages)

    # 4. 실패한 청크에 담긴 리포트는 daily_update_calls 목록에 포함되어 있지 않아야 함
    marked_report_ids = set()
    for updated_rows, _ in db.daily_update_calls:
        for r in updated_rows:
            marked_report_ids.add(r["report_id"])

    # 40개 중 실패한 청크에 들어 있던 리포트 ID들은 셋에 누락되어 있어야 함
    assert len(marked_report_ids) < len(mock_rows)


def test_broadcast_ga_reports_filters_unresolved_dbfi(monkeypatch):
    """DB증권은 streamdocs pdf_url 확정 전에는 GA 브로드캐스트 대상에서 제외된다."""
    from scheduler import _broadcast_ga_reports

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "mock_token")
    monkeypatch.setenv("TELEGRAM_CHANNEL_ID_REPORT_ALARM", "mock_chat_id")
    monkeypatch.setenv("DBFI_GATE_URL_PREFIX", "https://dbfi.example.test/pv/gate")
    monkeypatch.setenv("DBFI_STREAMDOCS_URL_PREFIX", "https://dbfi.example.test/streamdocs/v4/documents")

    keys = ["dbfi_unresolved", "dbfi_ready"]
    mock_rows = [
        {
            "report_id": 1,
            "firm_id": 19,
            "firm_nm": "DB증권",
            "article_title": "미확정 DBFI",
            "report_unique_key": "dbfi_unresolved",
            "telegram_url": "https://dbfi.example.test/pv/gate?q=abc",
            "pdf_file_url": "https://dbfi.example.test/pv/gate?q=abc",
        },
        {
            "report_id": 2,
            "firm_id": 19,
            "firm_nm": "DB증권",
            "article_title": "확정 DBFI",
            "report_unique_key": "dbfi_ready",
            "telegram_url": "https://dbfi.example.test/pv/gate?q=def",
            "pdf_file_url": "https://dbfi.example.test/streamdocs/v4/documents/doc-id",
        },
    ]

    class FilteringMockDB(MockDB):
        def _fetchall(self, sql, params):
            assert "pdf_url LIKE 'https://dbfi.example.test/streamdocs/v4/documents%%'" in sql
            assert "firm_nm NOT IN" not in sql
            return [
                r for r in self.rows
                if r.get("report_unique_key") in params
                and r.get("pdf_file_url", "").startswith("https://dbfi.example.test/streamdocs/v4/documents/")
            ]

    db = FilteringMockDB(mock_rows)
    sender = MockTelegramSender()
    monkeypatch.setattr("utils.telegram_util.sendMarkDownText", sender.send_markdown)

    _broadcast_ga_reports(db, keys)

    assert len(sender.sent_messages) == 1
    assert "확정 DBFI" in sender.sent_messages[0]
    assert "미확정 DBFI" not in sender.sent_messages[0]
    assert "https://dbfi.example.test/pv/gate?q=def" in sender.sent_messages[0]
    assert db.daily_update_calls[0][0][0]["report_id"] == 2
