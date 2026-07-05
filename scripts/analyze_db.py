# -*- coding: utf-8 -*-
"""
Database Table Analysis Script
이 스크립트는 PostgreSQL 데이터베이스의 전체 테이블 목록과 스키마 정보, 데이터 통계를 분석하기 위해 작성되었습니다.
운영 DB에 영향을 주지 않고 안전하게 조회(Read) 작업만 수행합니다.
"""

import sys
import os
import json
from datetime import datetime

# 프로젝트 루트 경로를 Python Path에 추가하여 모델 모듈을 임포트할 수 있도록 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from models.SecReportsManager import SecReportsManager
from loguru import logger

def analyze_database():
    logger.info("데이터베이스 분석을 시작합니다.")
    
    # DB 백엔드 설정 확인
    db_backend = os.getenv("DB_BACKEND", "postgres")
    logger.info(f"현재 설정된 DB_BACKEND: {db_backend}")
    
    if db_backend.lower() != "postgres":
        logger.warning("DB_BACKEND가 postgres가 아닙니다. 하지만 운영 DB 분석을 위해 PostgreSQL 연결을 시도합니다.")

    # 환경 변수 기반 운영 DB 연결. 접속 정보는 코드에 두지 않는다.
    try:
        db_manager = SecReportsManager()
        logger.info("DB 연결 정보는 환경 변수/secret 설정을 사용합니다.")
        conn = db_manager.get_connection()
    except Exception as e:
        logger.error(f"PostgreSQL 연결 실패: {e}")
        return

    analysis_results = {
        "analyzed_at": datetime.now().isoformat(),
        "tables": {}
    }

    try:
        with conn:
            with conn.cursor() as cur:
                # 1. public 스키마의 모든 테이블 목록 조회
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """)
                tables = [row[0] for row in cur.fetchall()]
                logger.info(f"조회된 테이블 목록: {tables}")

                for table in tables:
                    table_info = {}
                    
                    # 2. 각 테이블의 총 Row 수 조회
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {table};")
                        row_count = cur.fetchone()[0]
                        table_info["row_count"] = row_count
                    except Exception as err:
                        logger.error(f"{table} 테이블 Row 수 조회 실패: {err}")
                        table_info["row_count"] = "Error"
                        conn.rollback()
                    
                    # 3. 테이블 컬럼 목록 및 데이터 타입 조회
                    try:
                        cur.execute("""
                            SELECT column_name, data_type, is_nullable
                            FROM information_schema.columns 
                            WHERE table_name = %s
                            ORDER BY ordinal_position;
                        """, (table,))
                        columns = []
                        for col in cur.fetchall():
                            columns.append({
                                "column_name": col[0],
                                "data_type": col[1],
                                "is_nullable": col[2]
                            })
                        table_info["columns"] = columns
                    except Exception as err:
                        logger.error(f"{table} 테이블 스키마 조회 실패: {err}")
                        table_info["columns"] = []
                        conn.rollback()

                    # 4. 데이터 샘플 및 최근 업데이트 시간 조회 (테이블별 성격에 따른 분기)
                    try:
                        if table == "tbl_sec_reports":
                            # 최신 리포트 정보 3개 및 최종 수집 시간
                            cur.execute("""
                                SELECT firm_id, firm_nm, article_title, writer, report_date, save_at
                                FROM tbl_sec_reports
                                ORDER BY save_at DESC NULLS LAST
                                LIMIT 3;
                            """)
                            latest_rows = cur.fetchall()
                            table_info["latest_samples"] = [
                                {
                                    "firm_id": r[0],
                                    "firm_nm": r[1],
                                    "article_title": r[2],
                                    "writer": r[3],
                                    "report_date": str(r[4]) if r[4] else None,
                                    "save_at": str(r[5]) if r[5] else None
                                } for r in latest_rows
                            ]
                        elif table == "tbm_sec_firm_info":
                            # 등록된 증권사 정보 요약
                            cur.execute("""
                                SELECT firm_id, firm_nm, telegram_update_yn
                                FROM tbm_sec_firm_info
                                ORDER BY firm_id;
                            """)
                            firms = cur.fetchall()
                            table_info["firms_summary"] = [
                                {
                                    "firm_id": f[0],
                                    "firm_nm": f[1],
                                    "telegram_update_yn": f[2]
                                } for f in firms
                            ]
                        elif table == "tbm_sec_firm_board_info":
                            # 게시판 정보 개수 및 샘플 5개
                            cur.execute("""
                                SELECT firm_id, board_id, board_nm, board_cd
                                FROM tbm_sec_firm_board_info
                                ORDER BY firm_id, board_id
                                LIMIT 5;
                            """)
                            boards = cur.fetchall()
                            table_info["boards_sample"] = [
                                {
                                    "firm_id": b[0],
                                    "board_id": b[1],
                                    "board_nm": b[2],
                                    "board_cd": b[3]
                                } for b in boards
                            ]
                        elif table == "tbl_sec_reports_alert_keywords":
                            # 키워드 알림 정보 요약 및 샘플
                            cur.execute("""
                                SELECT user_id, keyword, is_active, created_at
                                FROM tbl_sec_reports_alert_keywords
                                ORDER BY created_at DESC NULLS LAST
                                LIMIT 5;
                            """)
                            keywords = cur.fetchall()
                            table_info["keywords_sample"] = [
                                {
                                    "user_id": k[0],
                                    "keyword": k[1],
                                    "is_active": k[2],
                                    "created_at": str(k[3]) if k[3] else None
                                } for k in keywords
                            ]
                    except Exception as err:
                        logger.error(f"{table} 테이블 상세 수집 실패: {err}")
                        conn.rollback()

                    analysis_results["tables"][table] = table_info

    except Exception as e:
        logger.error(f"데이터베이스 조회 도중 오류 발생: {e}")
    finally:
        conn.close()
        logger.info("데이터베이스 연결을 닫았습니다.")

    # 결과를 JSON 파일로 임시 저장 (Artifact 화하기 위함)
    output_path = os.path.join(os.path.dirname(__file__), "db_analysis_result.json")
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2)
        logger.info(f"분석 결과가 {output_path}에 저장되었습니다.")
    except Exception as e:
        logger.error(f"결과 파일 저장 실패: {e}")

if __name__ == "__main__":
    analyze_database()
