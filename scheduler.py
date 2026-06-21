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
    """메인 스크래퍼 실행 (scraper.py)"""
    logger.info("--- [Job Start] Main Scraper (scraper.py) ---")
    try:
        # uv run scraper.py 실행 (출력 캡처)
        # 권한 문제 예방 및 오버헤드 방지를 위해 현재 실행 중인 가상환경 파이썬 인터프리터를 직접 호출합니다.
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
            # 배치 내 중복 제거 (같은 게시판 중복 등재 방지)
            deduped = {}
            for d in data:
                k = d.get("report_unique_key") or d.get("key")
                if k and k not in deduped:
                    deduped[k] = d
            deduped_list = list(deduped.values())
            if len(deduped_list) < len(data):
                logger.info(f"[GA-Import] {fpath.name}: deduped {len(data)} → {len(deduped_list)}")
            ins, upd = db.insert_json_data_list(deduped_list)
            logger.success(f"[GA-Import] {fpath.name}: {ins} inserted, {upd} updated")
            if ins > 0:
                invalidate_api_cache()
                # 신규 insert건 텔레그램 채널 발송
                try:
                    new_keys = [d.get("report_unique_key") or d.get("key") for d in deduped_list if d.get("report_unique_key") or d.get("key")]
                    if new_keys:
                        _broadcast_ga_reports(db, new_keys)
                except Exception as e:
                    logger.warning(f"[GA-Import] broadcast failed (non-fatal): {e}")
            shutil.move(str(fpath), str(archive_dir / fpath.name))
        except Exception as e:
            logger.error(f"[GA-Import] {fpath.name} failed: {e}")
            shutil.move(str(fpath), str(failed_dir / fpath.name))


def _broadcast_ga_reports(db, keys: list[str]) -> None:
    """GA import된 신규 리포트를 텔레그램 채널에 발송"""
    import asyncio
    token = os.getenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET", "")
    chat_id = os.getenv("TELEGRAM_CHANNEL_ID_REPORT_ALARM", "")
    if not token or not chat_id or not keys:
        return

    try:
        from utils.telegram_util import sendMarkDownText
        from utils.sqlite_util import convert_sql_to_telegram_messages

        # report_unique_key로 DB에서 실제 row 조회
        placeholders = ",".join(["%s"] * len(keys))
        rows = db._fetchall(
            f"SELECT * FROM tbl_sec_reports WHERE report_unique_key IN ({placeholders})",
            keys,
        )
        if not rows:
            return

        msgs = convert_sql_to_telegram_messages(rows)
        success = True
        for msg in msgs:
            try:
                asyncio.run(sendMarkDownText(token=token, chat_id=chat_id, sendMessageText=msg))
            except Exception as e:
                logger.warning(f"[GA-Broadcast] TG send error: {e}")
                success = False
        if success:
            asyncio.run(db.daily_update_data(fetched_rows=rows, type="send"))
            logger.info(f"[GA-Broadcast] {len(rows)} reports sent to channel and marked sent")
        else:
            logger.warning("[GA-Broadcast] skipped sent-status update because at least one send failed")
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

# [스케줄 1] 메인 스크래퍼: */30 0,5-12,14-23 * * * (기존 crontab 복제)
scheduler.add_job(
    run_scraper,
    CronTrigger(minute='*/30', hour='0,5-23', jitter=300), # 300초(5분) 랜덤 지터 추가
    id="main_scraper_job"
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
    
    def handle_sigterm(signum, frame):
        logger.warning("Received SIGTERM. Shutting down scheduler gracefully...")
        if scheduler.running:
            # wait=False로 설정하여 즉시 스케줄러를 정지시키고 리소스를 정리합니다.
            scheduler.shutdown(wait=False)
        sys.exit(0)

    # Docker stop이 송출하는 SIGTERM 신호 핸들러 등록
    signal.signal(signal.SIGTERM, handle_sigterm)

    logger.info("🚀 Master Scheduler starting up...")
    logger.info("Registered Jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"- {job.id}: {job.trigger}")
    
    # 시작 시 즉시 한 번 실행하려면 아래 주석 해제
    # run_scraper()
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.warning("Scheduler stopped.")
