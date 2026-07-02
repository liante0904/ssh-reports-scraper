# -*- coding:utf-8 -*- 
import os
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
# ---------------------------------------------------------------------------

CACHE_INVALIDATE_URL = os.getenv("CACHE_INVALIDATE_URL", "http://localhost:8002/external/api/internal/cache/invalidate")
INTERNAL_CACHE_TOKEN = os.getenv("INTERNAL_CACHE_TOKEN", "")


def invalidate_api_cache() -> bool:
    """FastAPI의 Redis 캐시를 무효화한다. 실패 시 False 반환 (non-fatal)."""
    if not INTERNAL_CACHE_TOKEN:
        logger.debug("Cache invalidation skipped: INTERNAL_CACHE_TOKEN not set")
        return False
    try:
        import requests
        resp = requests.post(
            CACHE_INVALIDATE_URL,
            headers={"X-Internal-Token": INTERNAL_CACHE_TOKEN},
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            logger.info(f"Cache invalidated: {data.get('deleted_keys', 0)} keys deleted")
            return True
        else:
            logger.warning(f"Cache invalidation returned {resp.status_code}: {resp.text}")
            return False
    except Exception as e:
        logger.warning(f"Cache invalidation failed (non-fatal): {e}")
        return False


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
            result = subprocess.run(
                [sys.executable, "scraper.py"],
                capture_output=True,
                text=True,
                check=False
            )
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


def run_ga_import():
    """GA에서 SCP로 전송된 JSON 파일을 DB에 import.

    대상 디렉토리: /app/incoming/ga-scrapes/
    처리 완료된 파일은 archive/ 로 이동, 실패 파일은 failed/ 로 이동.
    """
    import json
    import shutil
    from pathlib import Path
    from models.db_factory import get_db

    incoming_dir = Path("/app/incoming/ga-scrapes")
    if not incoming_dir.exists():
        return  # 디렉토리 없으면 스킵 (아직 GA 연동 안 됐을 수 있음)

    archive_dir = incoming_dir / "archive"
    failed_dir = incoming_dir / "failed"
    archive_dir.mkdir(exist_ok=True)
    failed_dir.mkdir(exist_ok=True)

    json_files = sorted(incoming_dir.glob("*.json"))
    if not json_files:
        return

    logger.info(f"[GA-Import] {len(json_files)} file(s) found in {incoming_dir}")
    db = get_db()

    for fpath in json_files:
        # 💡 자가 치유 가드: 다른 프로세스가 이미 파일을 처리하여 가로챈 경우, 예외 없이 스킵합니다.
        if not fpath.exists():
            logger.info(f"[GA-Import] {fpath.name} already processed by another instance. Skipping.")
            continue
        try:
            data = json.loads(fpath.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                raise ValueError(f"Expected JSON array, got {type(data).__name__}")
            # ── 뉴스/미디어는 PostgreSQL에 insert하지 않고 완전히 제외 ──
            EXCLUDED_FIRMS = {"네이버", "조선비즈"}
            filtered = [d for d in data if d.get("firm_nm") not in EXCLUDED_FIRMS]
            if len(filtered) < len(data):
                logger.info(f"[GA-Import] {fpath.name}: stripped {len(data) - len(filtered)} news rows (네이버/조선비즈)")
                data = filtered
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
            if ins > 0:
                invalidate_api_cache()
                # 신규 insert건만 텔레그램 채널 발송 (db._last_inserted_keys 사용)
                try:
                    new_keys = getattr(db, "_last_inserted_keys", [])
                    if new_keys:
                        _broadcast_ga_reports(db, new_keys)
                except Exception as e:
                    logger.warning(f"[GA-Import] broadcast failed (non-fatal): {e}")
            shutil.move(str(fpath), str(archive_dir / fpath.name))
        except Exception as e:
            logger.error(f"[GA-Import] {fpath.name} failed: {e}")
            shutil.move(str(fpath), str(failed_dir / fpath.name))


def _broadcast_ga_reports(db, keys: list[str]) -> None:
    """GA import된 신규 리포트를 텔레그램 채널에 발송.
    
    2026-06-21 fix: 개별 텔레그램 메시지 청크 발송이 성공할 때마다 해당 청크 내 리포트들만 
    우선적으로 DB 상태(telegram_sent=true)를 마킹하여, 전체 전송 중 일부 
    실패 시의 중복 재발송 문제를 차단합니다.
    """
    import asyncio
    token = os.getenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "")
    chat_id = os.getenv("TELEGRAM_CHANNEL_ID_REPORT_ALARM", "")
    if not token or not chat_id or not keys:
        return

    try:
        from utils.telegram_util import sendMarkDownText

        # report_unique_key로 DB에서 실제 row 조회 (미발송 건만, 뉴스 제외)
        placeholders = ",".join(["%s"] * len(keys))
        dbfi_ready = (
            "AND (firm_id != 19 OR ("
            "firm_id = 19 "
            "AND telegram_url LIKE 'https://whub.dbsec.co.kr/pv/gate%%' "
            "AND pdf_url LIKE 'https://whub.dbsec.co.kr/streamdocs/v4/documents/%%'"
            "))"
        )
        rows = db._fetchall(
            f"""
            SELECT *
            FROM tbl_sec_reports
            WHERE report_unique_key IN ({placeholders})
              AND (telegram_sent IS NOT true)
              AND firm_nm NOT IN ('네이버', '조선비즈')
              {dbfi_ready}
            """,
            keys,
        )
        if not rows:
            return

        EMOJI_PICK = u'\U0001F449'
        EXCLUDED_FIRMS = {"네이버", "조선비즈"}
        message_limit = 3500

        # 청크 단위 발송을 위한 상태 저장 변수들
        message_chunk = ""
        current_chunk_rows = []
        last_firm_nm = None

        for row in rows:
            firm_nm = row.get("firm_nm")

            # 뉴스/미디어는 레포트 채널에서 제외 (헤더뿐 아니라 row 전체 스킵)
            if firm_nm in EXCLUDED_FIRMS:
                continue

            send_message_text = ""

            # 증권사명 변경 여부에 따른 헤더(구분선) 빌드
            firm_header = ""
            if firm_nm and firm_nm != last_firm_nm:
                firm_header = f"\n\n●{firm_nm}\n"

            # 제목 포맷팅
            title = row.get("article_title", "").replace("_", " ").replace("*", "")
            send_message_text += f"*{title}*\n"

            # 링크 선정 (DS증권 예외 및 일반 대체 링크 우선순위 반영)
            if row.get("firm_id") == 19:
                link_url = row.get("pdf_url") or ""
            elif row.get("firm_id") == 11:
                link_url = row.get("telegram_url") if row.get("telegram_url") else "링크없음"
            else:
                link_url = row.get("telegram_url") or row.get("download_url") or row.get("article_url") or ""

            if link_url == "링크없음":
                send_message_text += "링크없음\n"
            else:
                # URL 내 괄호 문자 때문에 마크다운 링크가 깨지는 것 방지
                _safe_url = link_url.replace("(", "%28").replace(")", "%29")
                send_message_text += f"{EMOJI_PICK}[링크]({_safe_url})\n"

            total_addition = firm_header + send_message_text

            # 3500자 임계치를 초과할 시, 현재 쌓인 청크를 즉시 전송하고 DB에 기록
            if len(message_chunk) + len(total_addition) > message_limit:
                if message_chunk.strip() and current_chunk_rows:
                    try:
                        asyncio.run(sendMarkDownText(token=token, chat_id=chat_id, sendMessageText=message_chunk.strip()))
                        asyncio.run(db.daily_update_data(fetched_rows=current_chunk_rows, type="send"))
                        logger.info(f"[GA-Broadcast] Chunk sent and marked: {len(current_chunk_rows)} reports")
                    except Exception as tx_err:
                        logger.error(f"[GA-Broadcast] Chunk send failed: {tx_err}")
                
                # 다음 청크 버퍼 세팅
                message_chunk = f"\n\n●{firm_nm}\n{send_message_text}"
                current_chunk_rows = [row]
                last_firm_nm = firm_nm
            else:
                if firm_header:
                    message_chunk += firm_header
                    last_firm_nm = firm_nm
                message_chunk += send_message_text
                current_chunk_rows.append(row)

        # 잔여 루프 버퍼 최종 전송 및 마킹
        if message_chunk.strip() and current_chunk_rows:
            try:
                asyncio.run(sendMarkDownText(token=token, chat_id=chat_id, sendMessageText=message_chunk.strip()))
                asyncio.run(db.daily_update_data(fetched_rows=current_chunk_rows, type="send"))
                logger.info(f"[GA-Broadcast] Final chunk sent and marked: {len(current_chunk_rows)} reports")
            except Exception as tx_err:
                logger.error(f"[GA-Broadcast] Final chunk send failed: {tx_err}")

    except Exception as e:
        logger.warning(f"[GA-Broadcast] error: {e}")


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
