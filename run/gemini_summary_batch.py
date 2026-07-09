import asyncio
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# 프로젝트 루트 경로 추가 (run 폴더의 상위 경로)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.db_factory import get_db
from models.GeminiManager import GeminiManager
from utils.file_util import download_file_wget

async def run_batch_summary(batch_limit=10):
    print(f"🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] AI 요약 배치 작업을 시작합니다...")
    
    db_manager = get_db()

    # 요약이 없는 최신 레포트 목록 조회
    pending_reports = await db_manager.fetch_pending_summary_reports(limit=batch_limit)
    
    if not pending_reports:
        print("✅ 요약할 대상 레포트가 없습니다. 작업을 종료합니다.")
        return

    print(f"📋 총 {len(pending_reports)}개의 레포트 요약을 시도합니다.")
    gemini = GeminiManager() # 모델: models/gemini-2.5-flash-lite
    
    success_count = 0
    fail_count = 0

    for report in pending_reports:
        # print(f"\n[작업 시작] {report['article_title']} ({report['firm_nm']})")
        
        # 유효한 PDF URL 확인
        download_url = report.get('telegram_url') or report.get('pdf_file_url')
        
        file_name = f"temp_batch_{report['report_id']}.pdf"
        
        try:
            # 1. PDF 다운로드
            success = await download_file_wget(report, URL=download_url, FILE_NAME=file_name)
            
            if not success or not os.path.exists(file_name) or os.path.getsize(file_name) < 1024:
                print(f"❌ 다운로드 실패 또는 파일 손상. (Size: {os.path.getsize(file_name) if os.path.exists(file_name) else 0} bytes)")
                fail_count += 1
                continue

            # 2. 제미나이 요약 (재시도 로직 포함)
            max_retries = 2
            summary_result = None
            
            for attempt in range(max_retries + 1):
                if attempt > 0:
                    print(f"🔄 재시도 중... ({attempt}/{max_retries})")
                
                print(f"🤖 제미나이 분석 중...")
                result = await gemini.summarize_pdf(file_name)
                
                if result['status'] == 'success':
                    summary_result = result
                    break
                elif "429" in str(result.get('error')):
                    print(f"🛑 쿼터 초과(429). 50초 대기 후 다시 시도합니다...")
                    await asyncio.sleep(50)
                    continue
                else:
                    print(f"❌ 요약 실패: {result.get('error')}")
                    break
            
            if summary_result:
                # 3. PostgreSQL 업데이트 (telegram_url 기준, 발송 완료된 최신 레코드만)
                target_url = report.get('telegram_url') or report.get('pdf_file_url')
                
                await db_manager.update_report_summary_by_telegram_url(
                    telegram_url=target_url,
                    summary=summary_result['summary'],
                    model_name=summary_result['model']
                )
                
                print(f"✨ 요약 완료 및 PostgreSQL 저장 성공! (URL 기준)")
                success_count += 1
            else:
                fail_count += 1
                # 쿼터 에러로 재시도 실패 시 이번 배치 중단
                if attempt >= max_retries:
                    print("🛑 재시도 횟수 초과 또는 심각한 쿼터 에러. 배치를 중단합니다.")
                    break

            # 4. API 부하 방지를 위한 대기 (무료 티어 안정성 확보를 위해 20초 권장)
            await asyncio.sleep(20)

        except Exception as e:
            print(f"🔥 예기치 못한 에러 발생: {e}")
            fail_count += 1
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)

    print(f"\n📊 작업 종료: 성공 {success_count}, 실패 {fail_count}")

if __name__ == "__main__":
    load_dotenv()
    
    # 실행 인자로 batch_limit을 받을 수 있도록 수정 (기본값 10)
    import sys
    limit = 10
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            limit = 10
            
    # 배치 실행
    asyncio.run(run_batch_summary(batch_limit=limit))
