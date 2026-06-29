#!/usr/bin/env python3
"""
전체 증권사 아티팩트 Import 스크립트
=====================================
GitHub Actions에서 생성된 all-scraped-data artifact를 다운로드하여 DB에 import.

1. gh CLI로 최신 artifact 다운로드
2. JSON 파싱
3. 증권사별 DB insert (ON CONFLICT key → 자동 dedup)
4. DBfi gate URL 복구 후처리
5. unresolved URL 건들 enrich_data() 후처리

사용법:
  python3 scripts/import_all_artifact.py [--repo liante0904/ssh-reports-scraper] [--dry-run]

환경변수:
  GITHUB_REPOSITORY: artifact를 가져올 리포지토리
  ALL_ARTIFACT_NAME: artifact 이름 (기본: all-scraped-data)
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger_util import setup_logger
setup_logger("import_all")

from models.db_factory import get_db
from models.FirmInfo import FirmInfo


def download_artifact(repo: str, artifact_name: str = "all-scraped-data") -> dict | None:
    """gh CLI로 최신 workflow run의 artifact 다운로드 → JSON 반환"""
    tmpdir = tempfile.mkdtemp(prefix="all_artifact_")

    try:
        result = subprocess.run(
            [
                "gh", "run", "list",
                "--repo", repo,
                "--workflow", "scrape-all.yml",
                "--status", "success",
                "--limit", "1",
                "--json", "databaseId",
            ],
            capture_output=True, text=True, check=True,
        )
        runs = json.loads(result.stdout)
        if not runs:
            logger.warning("No successful all-firms scraper runs found")
            return None

        run_id = runs[0]["databaseId"]
        logger.info(f"Found run ID: {run_id}")

        subprocess.run(
            [
                "gh", "run", "download",
                "--repo", repo,
                str(run_id),
                "--name", artifact_name,
                "--dir", tmpdir,
            ],
            capture_output=True, text=True, check=True,
        )

        json_files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
        if not json_files:
            logger.error("No JSON file in artifact")
            return None

        json_path = os.path.join(tmpdir, json_files[0])
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(
            f"Artifact loaded: {data['total_articles']} articles from "
            f"{data['success_firms']}/{data['total_firms']} firms"
        )
        return data

    except subprocess.CalledProcessError as e:
        logger.error(f"gh CLI error: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return None


def import_all_firms(data: dict, dry_run: bool = False) -> dict:
    """전체 증권사 아티팩트 데이터를 DB에 import"""
    firms = data.get("firms", [])
    if not firms:
        logger.info("No firm data to import")
        return {"inserted": 0, "updated": 0, "errors": 0}

    db = get_db()
    total_ins = 0
    total_upd = 0
    total_err = 0

    for firm in firms:
        name = firm["name"]
        articles = firm.get("articles", [])

        if not articles:
            continue

        if dry_run:
            logger.info(f"[{name}] DRY-RUN: would insert/update {len(articles)} articles")
            continue

        try:
            ins, upd = db.insert_json_data_list(articles)
            total_ins += ins
            total_upd += upd
            status = "NEW" if ins > 0 else "UPD" if upd > 0 else "SKIP"
            logger.info(f"[{name}] {status}: {ins} inserted, {upd} updated")
        except Exception as e:
            total_err += 1
            logger.error(f"[{name}] DB insert failed: {e}")

    logger.success(
        f"Import complete: {total_ins} inserted, {total_upd} updated, "
        f"{total_err} errors"
    )
    return {"inserted": total_ins, "updated": total_upd, "errors": total_err}


async def post_process_all():
    """DBfi gate URL 복구 + LS unresolved URL 복구 등 후처리"""
    db = get_db()

    # 1. DBfi: gate URL 복구 (m.db-sec.co.kr → whub.dbsec.co.kr/pv/gate)
    from modules.DBfi_19 import DBfi_enrich_and_persist_details
    firm_info_19 = FirmInfo(sec_firm_order=19, article_board_order=0)

    records_19 = await db.fetch_all_empty_telegram_url_articles(
        firm_info=firm_info_19, days_limit=3
    )
    if records_19:
        logger.info(f"[DBfi] {len(records_19)} unresolved → gate URL 복구 시도...")
        try:
            updated = await DBfi_enrich_and_persist_details(articles=records_19, firm_info=firm_info_19, db=db)
            success = sum(1 for r in updated if r.get("telegram_url", "").startswith("https://whub.dbsec.co.kr/pv/gate"))
            logger.success(f"[DBfi] {success}/{len(updated)} gate URL 복구 완료")
        except Exception as e:
            logger.error(f"[DBfi] post-process failed: {e}")

    # 2. LS: unresolved URL 복구 (LS는 import_ls_artifact.py에서 별도 처리되지만,
    #    혹시 몰라 fallback)
    #    → 생략 (import_ls_artifact.py가 처리)

    logger.info("Post-processing complete")


async def main():
    parser = argparse.ArgumentParser(description="All-Firms Artifact Import")
    parser.add_argument(
        "--repo",
        default=os.getenv("GITHUB_REPOSITORY", "liante0904/ssh-reports-scraper"),
        help="GitHub repository",
    )
    parser.add_argument(
        "--artifact-name",
        default=os.getenv("ALL_ARTIFACT_NAME", "all-scraped-data"),
        help="Artifact name",
    )
    parser.add_argument(
        "--json-file",
        default=None,
        help="Skip download, use local JSON file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="DB insert 건너뛰기",
    )
    parser.add_argument(
        "--skip-post-process",
        action="store_true",
        help="후처리 건너뛰기",
    )
    args = parser.parse_args()

    # 아티팩트 로드
    if args.json_file:
        logger.info(f"Loading from local file: {args.json_file}")
        with open(args.json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            logger.error("Empty JSON file")
            return
    else:
        data = download_artifact(args.repo, args.artifact_name)
        if not data:
            logger.error("Failed to download artifact")
            return

    # DB import
    result = import_all_firms(data, dry_run=args.dry_run)

    # 후처리
    if not args.skip_post_process and result["inserted"] > 0:
        await post_process_all()

    logger.info("=== All-Firms Import Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
