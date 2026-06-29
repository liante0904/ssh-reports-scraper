"""
LS 증권 telegram_url 실태 진단 스크립트

목적:
  - tbl_sec_reports에서 LS 증권(sec_firm_order=0) 레코드 중
    telegram_url이 https://msg.ls-sec.co.kr/ 로 시작하지 않는
    레코드들의 현황을 분석하고 재처리 가능성을 진단한다.

실행:
  uv run python tests/diagnose_ls_urls.py
  uv run python tests/diagnose_ls_urls.py --sample 3   (3건만 상세 진단)
  uv run python tests/diagnose_ls_urls.py --wider-range 30  (날짜 범위 확대 테스트)
"""
import asyncio
import os
import sys
import re
from datetime import datetime, timedelta
from collections import Counter

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.db_factory import get_db
from loguru import logger
import requests
import urllib.parse


# ── 설정 ──────────────────────────────────────────────────────────────
MSG_PREFIX = "https://msg.ls-sec.co.kr/"
UPLOAD_PREFIX = "https://www.ls-sec.co.kr/upload/"
WARP_PROXY = os.getenv("SOCKS_PROXY_URL", "socks5h://localhost:9091")


# ── 헬퍼 ──────────────────────────────────────────────────────────────

def classify_url(url: str) -> str:
    """telegram_url 상태 분류"""
    if not url:
        return "empty (빈 문자열)"
    if url.startswith(MSG_PREFIX):
        return "msg.ls-sec.co.kr (정상)"
    if url.startswith(UPLOAD_PREFIX):
        return "upload/ fallback"
    if url.endswith(".pdf"):
        return f"기타 PDF: {url[:80]}"
    return f"기타: {url[:80]}"


def count_all(db) -> dict:
    """LS 증권 전체 레코드 통계"""
    total = db._fetchall(
        "SELECT COUNT(*) AS cnt FROM tbl_sec_reports WHERE sec_firm_order = 0"
    )
    by_url = db._fetchall("""
        SELECT
            CASE
                WHEN telegram_url IS NULL OR telegram_url = '' THEN 'empty'
                WHEN telegram_url LIKE 'https://msg.ls-sec.co.kr/%%' THEN 'msg'
                WHEN telegram_url LIKE 'https://www.ls-sec.co.kr/upload/%%' THEN 'upload_fallback'
                ELSE 'other'
            END AS url_type,
            COUNT(*) AS cnt
        FROM tbl_sec_reports
        WHERE sec_firm_order = 0
        GROUP BY url_type
        ORDER BY cnt DESC
    """)
    return {"total": total[0]["cnt"], "by_url": by_url}


def get_failed_records(db, limit: int = None) -> list:
    """telegram_url이 비정상인 LS 레코드 조회"""
    query = """
        SELECT report_id, sec_firm_order, article_board_order, firm_nm,
               reg_dt, article_title, article_url, telegram_url,
               download_url, key, save_time, writer
        FROM tbl_sec_reports
        WHERE sec_firm_order = 0
          AND (telegram_url IS NULL
               OR telegram_url = ''
               OR telegram_url NOT LIKE 'https://msg.ls-sec.co.kr/%%')
        ORDER BY save_time DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    return db._fetchall(query)


async def diagnose_sample(db, records: list):
    """샘플 레코드들에 대해 상세 진단"""
    from modules.LS_0 import get_valid_url, create_fallback_url

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.ls-sec.co.kr/",
    }

    print(f"\n{'='*70}")
    print(f"  샘플 상세 진단 (총 {len(records)}건)")
    print(f"{'='*70}")

    for i, r in enumerate(records):
        print(f"\n--- [{i+1}/{len(records)}] report_id={r['report_id']} ---")
        print(f"  제목   : {r.get('article_title', '')[:60]}")
        print(f"  reg_dt : {r.get('reg_dt', 'N/A')}")
        print(f"  key    : {r.get('key', 'N/A')[:80]}")
        print(f"  현재   : {classify_url(r.get('telegram_url', ''))}")

        key = r.get("key", "")
        reg_dt = r.get("reg_dt", "")

        if not key:
            print("  → key 없음, 진단 불가")
            continue

        if ".pdf" in key:
            print("  → 이미 PDF direct 링크")
            continue

        # 1. 상세 페이지 접속 테스트
        print(f"\n  [진단1] 상세 페이지 접속...")
        try:
            resp = requests.get(key, headers=HEADERS, verify=False, timeout=15)
            print(f"    HTTP {resp.status_code} ({len(resp.content)} bytes)")
            if resp.status_code != 200:
                print(f"    → WARP 시도...")
                resp2 = requests.get(key, headers=HEADERS,
                                     proxies={"http": WARP_PROXY, "https": WARP_PROXY},
                                     verify=False, timeout=20)
                print(f"    WARP HTTP {resp2.status_code} ({len(resp2.content)} bytes)")
            else:
                # 이미지 찾기 시도
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.content, "html.parser")
                img = (soup.select_one("#contents > div.tbViewCon > div > html > body > p > img")
                       or soup.select_one("#contents > div.tbViewCon > div > p > img"))
                if img:
                    img_filename = img.get("alt") or img.get("src")
                    print(f"    이미지 발견: {img_filename}")
                    if img_filename:
                        name, ext = os.path.splitext(img_filename)
                        match = re.search(r"_(\d{8})$", name)
                        if match:
                            date_part = match.group(1)
                            new_name = re.sub(r"_(\d{8})$", "", name)
                            new_filename = f"{date_part}_{new_name}.pdf"
                            print(f"    생성된 파일명: {new_filename}")

                            # ±30일 범위로 테스트
                            print(f"    [진단2] msg.ls-sec.co.kr 탐색 (±30일)...")
                            found_any = False
                            try:
                                date_obj = datetime.strptime(date_part, "%Y%m%d")
                            except ValueError:
                                print(f"    날짜 파싱 실패: {date_part}")
                                continue

                            for offset in range(-30, 31):
                                test_date = date_obj + timedelta(days=offset)
                                test_date_str = test_date.strftime("%Y%m%d")
                                test_filename = new_filename.replace(date_part, test_date_str)
                                test_url = f"https://msg.ls-sec.co.kr/eum/K_{test_filename}"
                                try:
                                    r2 = requests.get(test_url, headers=HEADERS,
                                                      verify=False, timeout=10,
                                                      proxies={"http": WARP_PROXY, "https": WARP_PROXY})
                                    if r2.status_code == 200:
                                        print(f"    ✅ 발견! offset={offset:+d}d → {test_url}")
                                        found_any = True
                                        break
                                except Exception:
                                    pass

                            if not found_any:
                                print(f"    ❌ ±30일 내에서도 발견 안 됨")
                        else:
                            print(f"    파일명에서 날짜 추출 실패: {name}")
                else:
                    # 첨부파일 확인
                    attach_tag = soup.select_one("td.attach a")
                    if attach_tag:
                        attach_name = attach_tag.get_text(strip=True)
                        print(f"    첨부파일 발견 (대체): {attach_name}")
                    else:
                        print(f"    이미지/첨부파일 모두 없음")
        except Exception as e:
            print(f"    접속 실패: {e}")

        # 마지막 레코드 이후에는 sleep
        await asyncio.sleep(0.5)


# ── 메인 ──────────────────────────────────────────────────────────────

async def main():
    # 간단한 인자 파싱
    import argparse
    parser = argparse.ArgumentParser(description="LS URL 실태 진단")
    parser.add_argument("--sample", type=int, default=0,
                        help="상세 진단할 샘플 건수 (0=진단 안 함)")
    parser.add_argument("--limit", type=int, default=0,
                        help="조회할 실패 레코드 상한 (0=전체)")
    args = parser.parse_args()

    db = get_db()

    print(f"{'='*70}")
    print(f"  LS 증권 telegram_url 실태 진단")
    print(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")

    # 1. 전체 통계
    stats = count_all(db)
    print(f"\n[통계] LS 증권 전체 레코드: {stats['total']}건")
    print(f"{'─'*50}")
    print(f"  {'상태':<25} {'건수':>8} {'비율':>8}")
    print(f"{'─'*50}")
    for row in stats["by_url"]:
        pct = row["cnt"] / stats["total"] * 100 if stats["total"] > 0 else 0
        label = {
            "msg": "정상 (msg.ls-sec.co.kr)",
            "upload_fallback": "Fallback (upload/)",
            "empty": "빈 문자열",
            "other": "기타",
        }.get(row["url_type"], row["url_type"])
        print(f"  {label:<25} {row['cnt']:>8} {pct:>7.1f}%")

    # 2. 실패 레코드 목록
    limit = args.limit if args.limit > 0 else None
    failed = get_failed_records(db, limit=limit)

    if not failed:
        print(f"\n✅ 실패 레코드 없음! 모든 LS 레코드가 정상 msg.ls-sec.co.kr URL을 가짐")
        return

    print(f"\n[실패 레코드 목록] 총 {len(failed)}건")
    print(f"{'─'*120}")
    print(f"  {'ID':>8} {'reg_dt':<10} {'상태':<25} {'제목':<50}")
    print(f"{'─'*120}")
    for r in failed:
        status = classify_url(r.get("telegram_url", ""))
        title = (r.get("article_title") or "")[:48]
        print(f"  {r['report_id']:>8} {str(r.get('reg_dt',''))[:10]:<10} {status:<25} {title:<50}")

    # 3. 샘플 상세 진단
    if args.sample > 0 and failed:
        sample = failed[:args.sample]
        await diagnose_sample(db, sample)

    # 4. 요약 및 제안
    print(f"\n{'='*70}")
    print(f"  진단 요약")
    print(f"{'='*70}")
    empty_count = sum(1 for r in failed if not r.get("telegram_url"))
    upload_count = sum(1 for r in failed if r.get("telegram_url", "").startswith(UPLOAD_PREFIX))
    other_count = len(failed) - empty_count - upload_count

    print(f"  - 빈 문자열:        {empty_count}건  → 상세 페이지 접속 실패로 추정")
    print(f"  - upload/ fallback: {upload_count}건  → msg.ls-sec.co.kr 에서 PDF 미발견")
    print(f"  - 기타:             {other_count}건")

    if upload_count > 0:
        print(f"\n  💡 제안: upload/ fallback의 경우 get_valid_url()의 날짜 탐색 범위를")
        print(f"     ±10일 → ±30일로 확대하면 일부 복구 가능할 수 있습니다.")
    if empty_count > 0:
        print(f"\n  💡 제안: 빈 문자열의 경우 네트워크 문제(WARP 차단 등)일 가능성이 높습니다.")
        print(f"     재시도 횟수 증가 또는 상세 페이지 구조 변경 확인이 필요합니다.")

    # 5. 기존 fix 스크립트 안내
    print(f"\n  🔧 기존 도구: uv run python run/fix_ls_db.py")
    print(f"     (upload/ fallback만 재처리)")
    print(f"\n  🔧 진단 후 재처리: --sample N 인자로 더 많은 샘플 확인 후 판단")


if __name__ == "__main__":
    asyncio.run(main())
