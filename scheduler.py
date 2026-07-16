# -*- coding:utf-8 -*- 
import os
import signal
import subprocess
import sys
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from dotenv import load_dotenv

# 공통 로그 설정 적용
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils.logger_util import setup_logger
setup_logger("scheduler")

load_dotenv()

# ---------------------------------------------------------------------------
# Redis 캐시 무효화 — 새 데이터 insert 후 FastAPI에 알림

from scraper_config import invalidate_api_cache


def _scraper_process_timeout() -> int:
    """Return the hard limit for one scraper job."""
    try:
        return max(30, int(os.getenv("SCRAPER_PROCESS_TIMEOUT_SECONDS", "900")))
    except ValueError:
        return 900


def _run_scraper_process() -> subprocess.CompletedProcess:
    """Run scraper in an isolated process group and reap it on timeout."""
    process = subprocess.Popen(
        [sys.executable, "scraper.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=_scraper_process_timeout())
    except subprocess.TimeoutExpired:
        logger.error(
            f"Scraper process exceeded {_scraper_process_timeout()}s; terminating process group"
        )
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
        return subprocess.CompletedProcess(
            process.args, process.returncode or 124, stdout, stderr
        )
    return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)


def run_scraper():
    """메인 스크래퍼 실행 (scraper.py) — subprocess 중복 실행 방지 포함"""
    import fcntl
    SCRAPER_LOCK = "/tmp/ssh_reports_scraper.lock"
    _fd = None
    try:
        _fd = open(SCRAPER_LOCK, "w")
        fcntl.flock(_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _fd.write(str(os.getpid()))
        _fd.flush()
    except (BlockingIOError, PermissionError):
        logger.warning("Another scraper is already running. Skipping this job.")
        return
    except Exception:
        pass  # lock 없이 진행

    try:
        logger.info("--- [Job Start] Main Scraper (scraper.py) ---")
        try:
            result = _run_scraper_process()
            if result.returncode != 0:
                logger.error(f"Scraper process exited with error code {result.returncode}")
                if result.stderr:
                    logger.error(f"Scraper Error Output:\n{result.stderr}")
            else:
                logger.success("Scraper job completed successfully.")
                invalidate_api_cache()
        except Exception as e:
            logger.error(f"Execution Error: {e}")
        logger.info("--- [Job End] Main Scraper ---")
    finally:
        if _fd:
            try:
                fcntl.flock(_fd, fcntl.LOCK_UN)
                _fd.close()
                os.remove(SCRAPER_LOCK)
            except Exception:
                pass


def _discover_ga_files():
    """GA import 대상 디렉토리 확인 후 JSON 파일 목록과 경로들을 반환.

    Returns:
        (json_files, archive_dir, failed_dir) — 파일이 없으면 ([], None, None)
    """
    from pathlib import Path

    incoming_dir = Path("/app/incoming/ga-scrapes")
    if not incoming_dir.exists():
        return [], None, None

    archive_dir = incoming_dir / "archive"
    failed_dir = incoming_dir / "failed"
    archive_dir.mkdir(exist_ok=True)
    failed_dir.mkdir(exist_ok=True)

    json_files = sorted(incoming_dir.glob("*.json"))
    if json_files:
        logger.info(f"[GA-Import] {len(json_files)} file(s) found in {incoming_dir}")
    return json_files, archive_dir, failed_dir


class GAImportRetryableError(ValueError):
    """A GA result file is still being transferred and should be retried."""


def _ga_import_partial_file_grace_seconds() -> int:
    """Return the window in which an incomplete SCP result is retried."""
    try:
        return max(0, int(os.getenv("GA_IMPORT_PARTIAL_FILE_GRACE_SECONDS", "120")))
    except ValueError:
        return 120


def _process_ga_file(fpath, db) -> tuple[int, list[str]]:
    """GA JSON 파일 1개를 읽어 DB에 insert.

    Returns:
        (inserted_count, new_keys) — new_keys는 이번에 새로 insert된 report_unique_key 목록.
    Raises:
        ValueError: JSON 형식 오류.
        Exception: 그 외 DB/IO 오류 (호출자가 archive/failed 결정).
    """
    import json
    import time

    raw_data = fpath.read_text(encoding="utf-8")
    if not raw_data.strip():
        raise GAImportRetryableError("result file is empty; SCP transfer may still be in progress")

    try:
        data = json.loads(raw_data)
    except json.JSONDecodeError as exc:
        file_age = time.time() - fpath.stat().st_mtime
        if file_age < _ga_import_partial_file_grace_seconds():
            raise GAImportRetryableError(
                f"result file is incomplete ({exc}); will retry after transfer settles"
            ) from exc
        raise ValueError(f"Invalid JSON: {exc}") from exc
    if not isinstance(data, list):
        raise ValueError(f"Expected JSON array, got {type(data).__name__}")

    # 배치 내 중복 제거 (같은 게시판 중복 등재 방지)
    deduped = {}
    for d in data:
        k = d.get("report_unique_key")
        if k and k not in deduped:
            deduped[k] = d
    deduped_list = list(deduped.values())
    if len(deduped_list) < len(data):
        logger.info(f"[GA-Import] {fpath.name}: deduped {len(data)} → {len(deduped_list)}")

    ins, upd = db.insert_json_data_list(deduped_list)
    logger.success(f"[GA-Import] {fpath.name}: {ins} inserted, {upd} updated")

    new_keys = getattr(db, "_last_inserted_keys", []) if ins > 0 else []
    return ins, new_keys


def run_ga_import():
    """GA에서 SCP로 전송된 JSON 파일을 DB에 import → 텔레그램 발송.

    흐름: 파일 발견 → 개별 처리(read→dedup→insert) → broadcast → archive/failed.
    """
    import shutil
    from models.db_factory import get_db

    json_files, archive_dir, failed_dir = _discover_ga_files()
    if not json_files:
        return

    db = get_db()
    for fpath in json_files:
        # 💡 자가 치유 가드: 다른 프로세스가 이미 파일을 처리한 경우 스킵
        if not fpath.exists():
            logger.info(f"[GA-Import] {fpath.name} already processed by another instance. Skipping.")
            continue
        try:
            ins, new_keys = _process_ga_file(fpath, db)
            if ins > 0:
                invalidate_api_cache()
                if new_keys:
                    try:
                        _broadcast_ga_reports(db, new_keys)
                    except Exception as e:
                        logger.warning(f"[GA-Import] broadcast failed (non-fatal): {e}")
            shutil.move(str(fpath), str(archive_dir / fpath.name))
        except GAImportRetryableError as e:
            # SCP creates the destination before its transfer is complete.  Do not
            # move a transiently empty/partial file to failed; the next poll will
            # import the completed result.
            logger.warning(f"[GA-Import] {fpath.name} deferred: {e}")
        except Exception as e:
            logger.error(f"[GA-Import] {fpath.name} failed: {e}")
            shutil.move(str(fpath), str(failed_dir / fpath.name))


def _broadcast_ga_reports(db, keys: list[str]) -> None:
    """GA import된 신규 리포트를 텔레그램 채널에 발송.

    2026-06-21 fix: 개별 텔레그램 메시지 청크 발송이 성공할 때마다 해당 청크 내 리포트들만
    우선적으로 DB 상태(telegram_sent=true)를 마킹하여, 전체 전송 중 일부
    실패 시의 중복 재발송 문제를 차단합니다.

    2026-07-06 fix: httpx.ConnectError 등 일시적 네트워크 오류에 대해 chunk별 최대 3회
    재시도 (exponential backoff: 1s → 2s → 4s). 영구적 오류(HTTP 4xx 등)는 재시도 없이 즉시 실패.
    """
    import asyncio
    token = os.getenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "")
    chat_id = os.getenv("TELEGRAM_CHANNEL_ID_REPORT_ALARM", "")
    if not token or not chat_id or not keys:
        return

    try:
        from utils.telegram_util import sendMarkDownText
        from models.SecReportsManager import SecReportsManager

        # report_unique_key로 DB에서 실제 row 조회 (미발송 건만)
        placeholders = ",".join(["%s"] * len(keys))
        dbfi_ready = f"AND {SecReportsManager.dbfi_ready_condition()}"
        rows = db._fetchall(
            f"""
            SELECT *
            FROM tbl_sec_reports
            WHERE report_unique_key IN ({placeholders})
              AND (telegram_sent IS NOT true)
              AND (
                firm_id = 11
                OR COALESCE(telegram_url, '') <> ''
                OR COALESCE(pdf_url, '') <> ''
              )
              {dbfi_ready}
            """,
            keys,
        )
        if not rows:
            return

        from utils.telegram_message_builder import build_telegram_message_chunks

        chunks = build_telegram_message_chunks(rows)
        for chunk in chunks:
            chunk_rows = chunk["rows"]
            _send_chunk_with_retry(token, chat_id, chunk, db, chunk_rows)

    except Exception as e:
        logger.warning(f"[GA-Broadcast] error: {e}")


def _send_chunk_with_retry(token: str, chat_id: str, chunk: dict, db, chunk_rows: list) -> None:
    """단일 청크 전송. 일시적 네트워크 오류 시 최대 3회 재시도."""
    import asyncio
    from utils.telegram_util import sendMarkDownText

    max_retries = 3
    for attempt in range(max_retries):
        try:
            asyncio.run(sendMarkDownText(
                token=token, chat_id=chat_id, sendMessageText=chunk["message"]
            ))
            asyncio.run(db.daily_update_data(fetched_rows=chunk_rows, type="send"))
            logger.info(f"[GA-Broadcast] Chunk sent and marked: {len(chunk_rows)} reports")
            return  # 성공
        except Exception as tx_err:
            is_transient = _is_transient_error(tx_err)
            if is_transient and attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
                logger.warning(
                    f"[GA-Broadcast] Transient error (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {wait}s: {tx_err}"
                )
                import time
                time.sleep(wait)
            else:
                logger.error(f"[GA-Broadcast] Chunk send failed: {tx_err}")
                return  # 영구적 오류 또는 재시도 소진


def _is_transient_error(err: Exception) -> bool:
    """일시적 네트워크 오류인지 판별. httpx.ConnectError, TimeoutException 등은 True."""
    err_type = type(err).__name__
    # httpx 네트워크 계층 오류
    if err_type in ("ConnectError", "ConnectTimeout", "ReadTimeout",
                    "WriteTimeout", "PoolTimeout", "RemoteProtocolError"):
        return True
    # 표준 라이브러리 네트워크 오류
    if err_type in ("TimeoutError", "ConnectionError", "ConnectionResetError",
                    "ConnectionRefusedError", "BrokenPipeError"):
        return True
    # httpx.HTTPStatusError 등 HTTP 응답 오류 (4xx, 5xx) → 재시도 안 함
    return False


def run_fnguide_matcher():
    """FnGuide 요약 리포트 유사도 매칭 배치 자동 실행"""
    logger.info("--- [Job Start] FnGuide Report Matcher ---")
    try:
        import requests
        
        # 1) 환경 변수에서 BACKEND_API_URL 및 JWT_SECRET_KEY 추출
        # 💡 [주의 - 포트 번호 8002 매핑 가이드]
        # - FastAPI 백엔드 서비스의 도커 외부 바인딩 포트는 8000이 아닌 8002번 포트입니다.
        # - 이에 따라 로컬 호스트 및 호스트 네트워크 연동 시 폴백 기본 포트는 'http://localhost:8002'로 고정됩니다.
        # - 타 LLM은 이를 임의로 8000으로 교체하여 Connection Refused 장애를 일으키지 않도록 각별히 유의하십시오.
        backend_api_url = os.getenv("BACKEND_API_URL", "http://localhost:8002").rstrip("/")
        jwt_secret_key = os.getenv("JWT_SECRET_KEY")
        
        if not jwt_secret_key:
            logger.warning("FnGuide Matcher skipped: JWT_SECRET_KEY environment variable is not set.")
            logger.info("--- [Job End] FnGuide Report Matcher ---")
            return
            
        url = f"{backend_api_url}/admin/fnguide/match-internal?limit=300"
        headers = {
            "X-Internal-Token": jwt_secret_key,
            "Accept": "application/json"
        }
        
        logger.info(f"Triggering matcher API: {url}")
        response = requests.post(url, headers=headers, timeout=120)  # 매칭 연산 대기를 위한 충분한 타임아웃
        
        if response.status_code != 200:
            logger.error(f"FnGuide Matcher API exited with status code {response.status_code}")
            logger.error(f"Response: {response.text}")
        else:
            result = response.json()
            if result.get("status") == "success" or "matched_count" in result:
                logger.success(
                    f"FnGuide Matcher job completed successfully. "
                    f"Matched {result.get('matched_count', 0)}/{result.get('total_processed', 0)} reports."
                )
            else:
                logger.error(f"FnGuide Matcher API logic error: {result.get('message')}")
    except Exception as e:
        logger.error(f"FnGuide Matcher Execution Error: {e}")
    logger.info("--- [Job End] FnGuide Report Matcher ---")


scheduler = BlockingScheduler()

# [스케줄 1a] 메인 스크래퍼 (기본 시간대: 0시, 5~19시) — 매 30분 (정각 + 30분)
scheduler.add_job(
    run_scraper,
    CronTrigger(minute='*/30', hour='0,5-19', jitter=60),
    id="main_scraper_job",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=600,
)

# [스케줄 1b] 메인 스크래퍼 (저녁 시간대: 20~23시) — 각 시 30분만 (정각 호출 제외)
scheduler.add_job(
    run_scraper,
    CronTrigger(minute='30', hour='20-23', jitter=60),
    id="main_scraper_evening_job",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=600,
)

# [스케줄 2] GA import 폴링: 5분마다 incoming 디렉토리 확인
scheduler.add_job(
    run_ga_import,
    CronTrigger(minute='*/5'),
    id="ga_import_job"
)

# [스케줄 5] FnGuide 매칭 배치: 30분마다 가동 (메인 스크래퍼 구동 10분 뒤)
scheduler.add_job(
    run_fnguide_matcher,
    CronTrigger(minute='10,40', hour='0,5-23', jitter=120),
    id="fnguide_matcher_job"
)

if __name__ == "__main__":
    import signal
    import fcntl

    # ── Lock file: 중복 실행 방지 ──
    # /tmp/를 1순위로 사용. Docker에서 /app/이 root 소유인 경우에도
    # /tmp/는 world-writable(1777)이므로 PermissionError 없이 lock 획득 가능.
    # 만약 /tmp/마저 쓸 수 없는 극단적 상황이면 /dev/shm/으로 fallback.
    LOCK_FILE = None
    _lock_fd = None
    for _candidate in ("/tmp/ssh_reports_scheduler.lock",
                       "/dev/shm/ssh_reports_scheduler.lock"):
        try:
            _lock_fd = open(_candidate, "w")
            LOCK_FILE = _candidate
            break
        except PermissionError:
            logger.warning(f"Cannot write lock file to {_candidate}, trying next candidate...")
        except Exception:
            continue

    if _lock_fd is None:
        # 최후의 수단: lock 없이 진행 (중복 실행 위험보다 crash가 더 나쁨)
        logger.error("Cannot create lock file in any candidate path. Running WITHOUT lock.")
    else:
        try:
            fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _lock_fd.write(str(os.getpid()))
            _lock_fd.flush()
        except BlockingIOError:
            # 이미 다른 scheduler가 실행 중 → 즉시 종료
            try:
                with open(LOCK_FILE) as lf:
                    existing_pid = lf.read().strip()
                logger.warning(f"Another scheduler is already running (PID={existing_pid}). Exiting.")
            except Exception:
                logger.warning("Another scheduler is already running. Exiting.")
            sys.exit(0)

    def _cleanup_lock():
        if _lock_fd is None:
            return
        try:
            fcntl.flock(_lock_fd, fcntl.LOCK_UN)
            _lock_fd.close()
            if LOCK_FILE:
                os.remove(LOCK_FILE)
        except Exception:
            pass

    def handle_sigterm(signum, frame):
        logger.warning("Received SIGTERM. Shutting down scheduler gracefully...")
        _cleanup_lock()
        if scheduler.running:
            scheduler.shutdown(wait=False)
        sys.exit(0)

    # Docker stop이 송출하는 SIGTERM 신호 핸들러 등록
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    logger.info("🚀 Master Scheduler starting up...")
    logger.info("Registered Jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"- {job.id}: {job.trigger}")

    # 시작 시 즉시 한 번 실행
    import threading
    threading.Thread(target=run_scraper, daemon=True).start()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.warning("Scheduler stopped.")
    finally:
        _cleanup_lock()
