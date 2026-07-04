import os
import subprocess
import sys
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

# 공통 로그 설정 적용
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils.logger_util import setup_logger
setup_logger("keyword_alert")

KEYWORD_ALERT_SCRIPT = "run/keyword_alert.py"
KEYWORD_ALERT_CRON_MINUTE = "*/5"


def run_job():
    logger.info("--- Keyword Alert Job Start ---")
    try:
        # RUN_ONCE=true로 설정하여 1회 실행 후 종료되게 함
        env = os.environ.copy()
        env["RUN_ONCE"] = "true"

        result = subprocess.run(
            [sys.executable, KEYWORD_ALERT_SCRIPT],
            env=env,
            check=False
        )
        if result.returncode != 0:
            logger.error(f"Job failed with return code {result.returncode}")
        else:
            logger.info("--- Keyword Alert Job Finished ---")
    except Exception as e:
        logger.error(f"Error during job execution: {e}")


scheduler = BlockingScheduler()

# 5분마다 실행 (*/5 * * * *)
scheduler.add_job(
    run_job,
    CronTrigger(minute=KEYWORD_ALERT_CRON_MINUTE),
    id="keyword_alert_job"
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

    logger.info("🚀 Keyword Alert Scheduler starting up...")
    logger.info(f"Registered Jobs: {scheduler.get_jobs()}")

    # 시작할 때 한 번 즉시 실행하고 싶으면 아래 주석을 해제하세요.
    # run_job()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
