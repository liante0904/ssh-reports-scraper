# -*- coding: utf-8 -*-
"""
tbl_sec_reports Deep Analysis Script
이 스크립트는 tbl_sec_reports 테이블 내부의 실제 데이터를 다차원적으로 쿼리하여
데이터 분포, 트렌드, 가공 현황 등 구체적인 수치와 속성을 정밀 추출하기 위해 작성되었습니다.
"""

import sys
import os
import json
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from models.SecReportsManager import SecReportsManager
from loguru import logger

def analyze_reports_details():
    logger.info("tbl_sec_reports 데이터 심층 분석 쿼리를 시작합니다.")
    
    db_manager = SecReportsManager()

    try:
        conn = db_manager.get_connection()
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        return

    results = {
        "analyzed_at": datetime.now().isoformat(),
        "total_count": 0,
        "firms_distribution": [],
        "market_distribution": [],
        "analyst_distribution": [],
        "yearly_trend": [],
        "gemini_summary_stats": {},
        "sync_status_stats": [],
        "pdf_sync_status_stats": [],
        "fields_fill_rate": {}
    }

    try:
        with conn:
            with conn.cursor() as cur:
                # 0. 전체 개수 조회
                cur.execute("SELECT COUNT(*) FROM tbl_sec_reports;")
                results["total_count"] = cur.fetchone()[0]
                logger.info(f"Total reports: {results['total_count']}")

                # 1. 증권사별 데이터 점유율 및 상위 15개사
                logger.info("1. 증권사별 분포 분석 중...")
                cur.execute("""
                    SELECT firm_nm, COUNT(*) as cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ratio
                    FROM tbl_sec_reports
                    GROUP BY firm_nm
                    ORDER BY cnt DESC
                    LIMIT 20;
                """)
                results["firms_distribution"] = [
                    {"firm_nm": r[0], "count": r[1], "ratio": float(r[2])} for r in cur.fetchall()
                ]

                # 2. 시장 구분 분포
                logger.info("2. 시장 분포 분석 중...")
                cur.execute("""
                    SELECT mkt_tp, COUNT(*) as cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ratio
                    FROM tbl_sec_reports
                    GROUP BY mkt_tp
                    ORDER BY cnt DESC;
                """)
                results["market_distribution"] = [
                    {"mkt_tp": r[0] if r[0] else "NULL/Unknown", "count": r[1], "ratio": float(r[2])} for r in cur.fetchall()
                ]

                # 3. 작성자(애널리스트) 분포 상위 20명
                logger.info("3. 작성자(애널리스트) 분포 분석 중...")
                cur.execute("""
                    SELECT writer, COUNT(*) as cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ratio
                    FROM tbl_sec_reports
                    WHERE writer IS NOT NULL AND writer != '' AND writer != '미상'
                    GROUP BY writer
                    ORDER BY cnt DESC
                    LIMIT 20;
                """)
                results["analyst_distribution"] = [
                    {"analyst": r[0], "count": r[1], "ratio": float(r[2])} for r in cur.fetchall()
                ]

                # 4. 연도별 수집 추이
                logger.info("4. 연도별 수집 추이 분석 중...")
                cur.execute("""
                    SELECT 
                        CASE 
                            WHEN report_date IS NOT NULL THEN EXTRACT(YEAR FROM report_date)::text
                            ELSE 'Unknown'
                        END as yr, 
                        COUNT(*) as cnt,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ratio
                    FROM tbl_sec_reports
                    GROUP BY yr
                    ORDER BY yr DESC;
                """)
                results["yearly_trend"] = [
                    {"year": r[0], "count": r[1], "ratio": float(r[2])} for r in cur.fetchall()
                ]

                # 5. Gemini 요약 가공 통계 (모델 분포 및 총 요약 개수)
                logger.info("5. Gemini 요약 모델 분포 분석 중...")
                cur.execute("""
                    SELECT summary_model, COUNT(*) as cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ratio
                    FROM tbl_sec_reports
                    GROUP BY summary_model
                    ORDER BY cnt DESC;
                """)
                model_dist = [
                    {"model": r[0] if r[0] else "Not Summarized", "count": r[1], "ratio": float(r[2])} for r in cur.fetchall()
                ]
                
                cur.execute("SELECT COUNT(*) FROM tbl_sec_reports WHERE gemini_summary IS NOT NULL AND gemini_summary != '';")
                summarized_cnt = cur.fetchone()[0]
                results["gemini_summary_stats"] = {
                    "total_summarized": summarized_cnt,
                    "summarized_ratio": round(summarized_cnt * 100.0 / results["total_count"], 2) if results["total_count"] > 0 else 0.0,
                    "model_distribution": model_dist
                }

                # 6. sync_status 및 pdf_sync_status 분포
                logger.info("6. 동기화 상태 분석 중...")
                cur.execute("""
                    SELECT sync_status, COUNT(*) as cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ratio
                    FROM tbl_sec_reports
                    GROUP BY sync_status
                    ORDER BY sync_status;
                """)
                results["sync_status_stats"] = [
                    {"status": r[0], "count": r[1], "ratio": float(r[2])} for r in cur.fetchall()
                ]

                cur.execute("""
                    SELECT pdf_sync_status, COUNT(*) as cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as ratio
                    FROM tbl_sec_reports
                    GROUP BY pdf_sync_status
                    ORDER BY pdf_sync_status;
                """)
                results["pdf_sync_status_stats"] = [
                    {"status": r[0], "count": r[1], "ratio": float(r[2])} for r in cur.fetchall()
                ]

                # 7. 주요 가공 필드(tags, stock_names, sector) 채워진 비율(적재율) 분석
                logger.info("7. 주요 가공 필드 채워진 비율 분석 중...")
                cur.execute("""
                    SELECT 
                        COUNT(*),
                        COUNT(CASE WHEN tags IS NOT NULL AND tags::text != '' AND tags::text != '[]' AND tags::text != 'null' THEN 1 END),
                        COUNT(CASE WHEN stock_names IS NOT NULL AND stock_names::text != '' AND stock_names::text != '[]' AND stock_names::text != 'null' THEN 1 END),
                        COUNT(CASE WHEN sector IS NOT NULL AND sector != '' THEN 1 END),
                        COUNT(CASE WHEN writer IS NOT NULL AND writer != '' THEN 1 END),
                        COUNT(CASE WHEN download_url IS NOT NULL AND download_url != '' THEN 1 END)
                    FROM tbl_sec_reports;
                """)
                row = cur.fetchone()
                tot = row[0]
                results["fields_fill_rate"] = {
                    "tags": {"count": row[1], "fill_rate": round(row[1] * 100.0 / tot, 2)},
                    "stock_names": {"count": row[2], "fill_rate": round(row[2] * 100.0 / tot, 2)},
                    "sector": {"count": row[3], "fill_rate": round(row[3] * 100.0 / tot, 2)},
                    "writer": {"count": row[4], "fill_rate": round(row[4] * 100.0 / tot, 2)},
                    "download_url": {"count": row[5], "fill_rate": round(row[5] * 100.0 / tot, 2)}
                }

    except Exception as e:
        logger.error(f"데이터 정밀 쿼리 실패: {e}")
    finally:
        conn.close()
        logger.info("DB 세션을 안전하게 종료하였습니다.")

    # 파일 출력
    out_path = os.path.join(os.path.dirname(__file__), "reports_deep_analysis.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"심층 분석 완료. {out_path} 에 저장되었습니다.")
    except Exception as e:
        logger.error(f"결과 저장 실패: {e}")

if __name__ == "__main__":
    analyze_reports_details()
