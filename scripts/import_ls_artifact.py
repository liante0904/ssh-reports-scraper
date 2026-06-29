#!/usr/bin/env python3
"""
LS 아티팩트 Import 스크립트
===========================
GitHub Actions에서 생성된 LS scrape artifact를 다운로드하여 DB에 import.

1. gh CLI로 최신 ls-scraped-data artifact 다운로드
2. JSON 파싱
3. DB key 중복 확인 후 insert
4. unresolved URL 건들에 대해 reconstruct_msg_url_from_db 후처리

사용법:
  python3 scripts/import_ls_artifact.py [--repo liante0904/ssh-reports-scraper]
  
환경변수:
  GITHUB_REPOSITORY: artifact를 가져올 리포지토리 (기본: liante0904/ssh-reports-scraper)
  LS_ARTIFACT_NAME: artifact 이름 (기본: ls-scraped-data)
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from loguru import logger

# 프로젝트 루트 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger_util import setup_logger
setup_logger("import_ls")

from models.db_factory import get_db
from models.FirmInfo import FirmInfo


def download_artifact(repo: str, artifact_name: str = "ls-scraped-data") -> dict | None:
    """gh CLI로 최신 workflow run의 artifact 다운로드 → JSON 반환"""
    tmpdir = tempfile.mkdtemp(prefix="ls_artifact_")

    try:
        # 1. 최신 successful workflow run 찾기
        logger.info(f"Searching latest successful run for: {repo}")
        result = subprocess.run(
            [
                "gh", "run", "list",
                "--repo", repo,
                "--workflow", "scrape-ls.yml",
                "--status", "success",
                "--limit", "1",
                "--json", "databaseId",
            ],
            capture_output=True, text=True, check=True,
        )
        runs = json.loads(result.stdout)
        if not runs:
            logger.error("No successful LS scraper runs found")
            return None

        run_id = runs[0]["databaseId"]
        logger.info(f"Found run ID: {run_id}")

        # 2. artifact 다운로드
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

        # 3. JSON 파일 찾기
        json_files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
        if not json_files:
            logger.error(f"No JSON file found in artifact")
            return None

        json_path = os.path.join(tmpdir, json_files[0])
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(
            f"Artifact loaded: {data['count']} articles "
            f"(resolved={data.get('resolved_urls', 0)}, "
            f"cdn={data.get('cdn_resolved', 0)})"
        )
        return data

    except subprocess.CalledProcessError as e:
        logger.error(f"gh CLI error: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"Download failed: {e}")
        return None


def import_to_db(data: dict) -> tuple[int, int]:
    """아티팩트 데이터를 DB에 import (key 중복 제외)"""
    articles = data.get("articles", [])
    if not articles:
        logger.info("No articles to import")
        return 0, 0

    db = get_db()

    # LS증권 기존 key 조회 (최근 30일)
    existing_keys = db.fetch_existing_keys(sec_firm_order=0, days_limit=30)

    # 신규 기사만 필터
    new_articles = [a for a in articles if a.get("key") and a["key"] not in existing_keys]
    skipped = len(articles) - len(new_articles)
    if skipped:
        logger.info(f"Skipped {skipped} already-existing articles")

    if not new_articles:
        logger.info("All articles already in DB")
        return 0, skipped

    # DB insert
    ins, upd = db.insert_json_data_list(new_articles)
    logger.success(f"DB Import: {ins} inserted, {upd} updated, {skipped} skipped")
    return ins, upd


async def post_process_unresolved():
    """telegram_url이 없는 LS 레포트들에 대해 msg URL 복구 시도"""
    from modules.LS_0 import LS_detail

    db = get_db()

    # unresolved LS articles (telegram_url 없는 것)
    records = await db.fetch_all_empty_telegram_url_articles(
        firm_info=FirmInfo(sec_firm_order=0, article_board_order=0),
        days_limit=3,
    )

    if not records:
        logger.info("No unresolved LS articles to post-process")
        return

    logger.info(f"Post-processing {len(records)} unresolved LS articles...")
    try:
        enriched = await LS_detail(articles=records, db=db)
        resolved = sum(1 for a in enriched if a.get("telegram_url", "").startswith("https://msg.ls-sec.co.kr/"))
        logger.success(f"Post-process: {resolved}/{len(records)} msg URLs reconstructed")

        # update DB
        tasks = [
            db.update_telegram_url(
                r["report_id"],
                r["telegram_url"],
                r.get("article_title"),
                pdf_url=r.get("pdf_url") or r["telegram_url"],
            )
            for r in enriched
            if r.get("telegram_url")
        ]
        if tasks:
            await asyncio.gather(*tasks)

    except Exception as e:
        logger.error(f"Post-process failed: {e}")


async def main():
    parser = argparse.ArgumentParser(description="LS Artifact Import")
    parser.add_argument(
        "--repo",
        default=os.getenv("GITHUB_REPOSITORY", "liante0904/ssh-reports-scraper"),
        help="GitHub repository (owner/name)",
    )
    parser.add_argument(
        "--artifact-name",
        default=os.getenv("LS_ARTIFACT_NAME", "ls-scraped-data"),
        help="Artifact name to download",
    )
    parser.add_argument(
        "--json-file",
        default=None,
        help="Skip download, use local JSON file instead",
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
    ins, skipped = import_to_db(data)

    # unresolved 건 후처리 (DB 의존 URL 복구)
    if ins > 0:
        await post_process_unresolved()
    else:
        logger.info("No new inserts, skipping post-process")

    logger.info("=== LS Import Complete ===")


if __name__ == "__main__":
    asyncio.run(main())
