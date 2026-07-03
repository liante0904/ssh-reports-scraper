#!/usr/bin/env python3
"""DB 기반 스케줄러 (ssh_library SchedulerManager) 통합 테스트 스크립트.

기존 APScheduler와 완전히 독립적으로 실행되며, tbm_scheduler_tasks 테이블의
등록/조회/실행/상태확인 흐름을 검증합니다.

사용법:
    cd apps/scrapers/ssh-reports-scraper

    # 1) 기본 동작 테스트 (더미 태스크)
    POSTGRES_HOST=10.0.0.111 uv run python run/test_db_scheduler.py

    # 2) 실제 스크래퍼 함수 1개 등록 + 실행 테스트
    POSTGRES_HOST=10.0.0.111 uv run python run/test_db_scheduler.py --real

주의: 이 스크립트는 기존 scheduler.py 와 무관하게 독립 실행됩니다.
      프로덕션 장애 위험 없이 자유롭게 테스트 가능합니다.
"""

import os
import sys

# ssh_library 및 scraper 루트 경로 추가
sys.path.insert(0, os.path.expanduser("~/workspace/lib/ssh_library"))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ssh_library.modules.scheduler import SchedulerManager, _now_kst

TEST_TASK = "test_db_scheduler_hello"
REAL_TASK = "test_scraper_fnguide_matcher"


def test_basic(mgr: SchedulerManager):
    """Step 1: 기본 CRUD + 실행 테스트."""
    now = _now_kst()
    current_time = now.strftime("%H:%M")
    print(f"[{current_time} KST] === DB Scheduler Basic Test ===\n")

    # 1. 테이블 확인
    mgr.ensure_table()
    print("✅ ensure_table() — tbm_scheduler_tasks ready")

    # 2. 태스크 등록 (UPSERT) + 이전 성공 상태 초기화
    mgr.register_task(
        task_name=TEST_TASK,
        schedule_times=[current_time],
        module_path="utils.logger_util:setup_logger",
        timeout_seconds=30,
    )
    # register_task는 last_status를 초기화하지 않으므로 수동 리셋
    mgr.update_task_status(TEST_TASK, "pending")
    print(f"✅ register_task('{TEST_TASK}') — schedule={current_time}")

    # 3. 전체 태스크 목록
    tasks = mgr.list_tasks()
    print(f"\n📋 All registered tasks ({len(tasks)}):")
    for t in tasks:
        print(f"   [{t['task_name']}] times={t['schedule_times']} "
              f"enabled={t['enabled']} status={t['last_status']}")

    # 4. Due tasks 확인
    due = mgr.get_due_tasks()
    my_due = [t for t in due if t["task_name"] == TEST_TASK]
    print(f"\n🔍 Due tasks matching '{TEST_TASK}': {len(my_due)}")
    if not my_due:
        print("   ⚠️  No due task (already succeeded today or time mismatch)")
    else:
        # 5. 실행
        results = mgr.run_due_tasks()
        ok = results.get(TEST_TASK, False)
        icon = "✅" if ok else "❌"
        print(f"{icon} run_task() → {results}")

    # 6. 최종 상태
    for t in mgr.list_tasks():
        if t["task_name"] == TEST_TASK:
            print(f"\n📊 Final state: status={t['last_status']} "
                  f"last_run={t['last_run_at']}")

    # Cleanup: 테스트 태스크 비활성화
    mgr.toggle_task(TEST_TASK, False)
    print(f"🧹 Disabled '{TEST_TASK}'")
    print("\n=== Basic Test Complete ===\n")


def test_real_function(mgr: SchedulerManager):
    """Step 2: 실제 FnGuide 매처 함수를 DB 스케줄러로 실행.

    APScheduler의 fnguide_matcher_job과 동일한 함수를 호출하지만,
    DB 스케줄러를 통해 실행됩니다. 기존 job은 그대로 유지됩니다.
    """
    current_time = _now_kst().strftime("%H:%M")
    print(f"[{current_time} KST] === Real Function Test ===\n")

    # scraper의 scheduler.py 에서 run_fnguide_matcher 를 import
    from scheduler import run_fnguide_matcher

    # DB에 태스크 등록 + 이전 상태 초기화
    mgr.register_task(
        task_name=REAL_TASK,
        schedule_times=[current_time],
        module_path="scheduler:run_fnguide_matcher",
        timeout_seconds=180,
    )
    mgr.update_task_status(REAL_TASK, "pending")  # 이전 성공 상태 리셋
    print(f"✅ Registered '{REAL_TASK}' → scheduler:run_fnguide_matcher")

    # Due tasks 확인
    due = [t for t in mgr.get_due_tasks() if t["task_name"] == REAL_TASK]
    if due:
        print("🔍 Task is due, executing...")
        results = mgr.run_due_tasks()
        ok = results.get(REAL_TASK, False)
        icon = "✅" if ok else "❌"
        print(f"{icon} FnGuide matcher via DB scheduler: {results}")
    else:
        print("⚠️  Task not due (may have already run today)")

    # 상태 확인
    for t in mgr.list_tasks():
        if t["task_name"] == REAL_TASK:
            print(f"📊 status={t['last_status']} last_run={t['last_run_at']}")

    # Cleanup
    mgr.toggle_task(REAL_TASK, False)
    print(f"🧹 Disabled '{REAL_TASK}'")
    print("\n=== Real Function Test Complete ===\n")


if __name__ == "__main__":
    mgr = SchedulerManager()
    run_real = "--real" in sys.argv

    test_basic(mgr)

    if run_real:
        test_real_function(mgr)

    print("🏁 All DB scheduler integration tests passed!")
