import asyncio
import os
import sys
from dotenv import load_dotenv

# 프로젝트 루트 경로 추가 (tests 폴더의 상위 경로)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.db_factory import get_db
from models.GeminiManager import GeminiManager
from utils.file_util import download_file_wget
from tests.db_test_utils import postgres_available

if not postgres_available():
    import pytest
    pytest.skip("PostgreSQL에 연결할 수 없어 Gemini 테스트를 건너뜁니다.", allow_module_level=True)

async def test_summary():
    print("🚀 제미나이 PDF 요약 테스트를 시작합니다...")
    if not os.getenv("GEMINI_API_KEY"):
        import pytest
        pytest.skip("GEMINI_API_KEY가 없어서 요약 테스트를 건너뜁니다.")
    
    # 1. DB 매니저 초기화 및 대상 조회 (여유 있게 10개 조회)
    db_manager = get_db()
    pending_reports = await db_manager.fetch_pending_summary_reports(limit=10)
    
    if not pending_reports:
        print("❌ 요약할 대상 레포트가 DB에 없습니다.")
        return

    gemini = GeminiManager()
    success_found = False

    for report in pending_reports:
        print(f"\n📋 대상 레포트 시도: {report['article_title']} ({report['firm_nm']})")
        
        # 2. PDF 다운로드 대상 URL 선정
        # telegram_url, download_url 중 유효한 것 찾기
        download_url = report.get('telegram_url') or report.get('download_url') or report.get('pdf_file_url')
        
        # URL이 .pdf로 끝나지 않으면 건너뜀 (디렉토리 링크 방지)
        if not download_url or not ('.pdf' in download_url.lower() or '.PDF' in download_url.lower()):
            print(f"⚠️ 유효한 PDF URL이 아닙니다: {download_url}")
            continue

        print(f"📥 PDF 다운로드 중... ({download_url})")
        file_name = f"temp_test_{report['report_id']}.pdf"
        
        try:
            success = await download_file_wget(report, URL=download_url, FILE_NAME=file_name)
            
            if not success or not os.path.exists(file_name) or os.path.getsize(file_name) < 1024:
                print(f"❌ PDF 다운로드 실패 또는 파일이 너무 작습니다. (Size: {os.path.getsize(file_name) if os.path.exists(file_name) else 0} bytes)")
                if os.path.exists(file_name): os.remove(file_name)
                continue

            # 3. 제미나이 요약 실행
            print(f"🤖 제미나이 요약 요청 중 ({gemini.model_name})...")
            result = await gemini.summarize_pdf(file_name)
            
            if result['status'] == 'success':
                summary = result['summary']
                print("\n✨ 요약 결과:")
                print("-" * 50)
                print(summary)
                print("-" * 50)
                
                # 4. DB 업데이트
                print("💾 DB에 요약 내용 저장 중...")
                await db_manager.update_report_summary(
                    record_id=report['report_id'],
                    summary=summary,
                    model_name=result['model']
                )
                print("✅ 성공적으로 저장되었습니다!")
                success_found = True
                break # 1개 성공하면 종료
            else:
                print(f"❌ 요약 실패: {result.get('error')}")
        
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"🗑️ 임시 파일 삭제 완료: {file_name}")

    if not success_found:
        print("\n❌ 모든 시도가 실패했거나 유효한 PDF 레포트를 찾지 못했습니다.")

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(test_summary())
