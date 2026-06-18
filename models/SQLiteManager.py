import asyncio
import aiosqlite
import sqlite3
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv
from loguru import logger

# 현재 스크립트의 상위 디렉터리를 모듈 경로에 추가(package 폴더에 있으므로)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.FirmInfo import FirmInfo  # 이미 정의된 FirmInfo 클래스

# 환경 변수 로드
load_dotenv()

# 데이터베이스 파일 경로 (환경 변수 SQLITE_DB_PATH를 최우선으로 사용)
_default_db_path = os.getenv('SQLITE_DB_PATH', os.path.expanduser('~/sqlite3/telegram.db'))

class SQLiteManager:
    def __init__(self, db_path=None):
        self.db_path = db_path or _default_db_path
        self.connection = None
        self.cursor = None
        self.main_table_name = os.getenv("MAIN_TABLE_NAME", "data_main_daily_send")

    def open_connection(self):
        """데이터베이스 연결 설정"""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()

        # PRAGMA journal_mode=WAL 설정 적용
        self.cursor.execute("PRAGMA journal_mode=WAL;")
        self.connection.commit()  # 변경사항 반영
        
    def close_connection(self):
        """데이터베이스 연결 종료"""
        if self.cursor:
            try:
                self.cursor.close()
            except sqlite3.ProgrammingError:
                logger.debug("Cursor is already closed.")
        if self.connection:
            try:
                self.connection.close()
            except sqlite3.ProgrammingError:
                logger.debug("Connection is already closed.")

    def create_table(self, table_name, columns):
        """테이블 생성"""
        columns_str = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
        self.cursor.execute(query)
        self.connection.commit()
        return {"status": "success", "query": query}

    def insert_data(self, table_name, data):
        """데이터 삽입"""
        placeholders = ', '.join('?' for _ in data)
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        self.cursor.execute(query, data)
        self.connection.commit()
        return {"status": "success", "query": query, "data": data}

    def fetch_all(self, table_name):
        """모든 데이터 조회"""
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return [dict(row) for row in rows]

    def fetch_existing_keys(self, sec_firm_order: int, days_limit: int = 7) -> set:
        """특정 증권사의 key 목록을 조회하여 반환 (중복 방지용)"""
        self.open_connection()
        try:
            sql = f"SELECT key FROM {self.main_table_name} WHERE sec_firm_order = ?"
            params = [sec_firm_order]
            
            if days_limit is not None:
                cutoff = (datetime.now() - timedelta(days=days_limit)).strftime("%Y-%m-%d %H:%M:%S")
                sql += " AND save_time >= ?"
                params.append(cutoff)
                
            self.cursor.execute(sql, tuple(params))
            rows = self.cursor.fetchall()
            return {r["key"] for r in rows if r["key"]}
        finally:
            self.close_connection()

    def insert_json_data_list(self, json_data_list, table_name=None):
        """JSON 형태의 리스트 데이터를 데이터베이스 테이블에 삽입하며, 삽입 성공 및 업데이트된 건수를 출력합니다."""
        if table_name is None:
            table_name = self.main_table_name

        self.open_connection()  # 데이터베이스 연결 열기

        # 삽입 및 업데이트 건수 초기화
        inserted_count = 0
        updated_count = 0

        try:
            # 데이터 삽입 및 업데이트 시도
            for entry in json_data_list:
                self.cursor.execute(f'''
                    INSERT INTO {table_name} (
                        sec_firm_order, article_board_order, firm_nm, reg_dt,
                        article_title, article_url, is_sent,
                        download_url, telegram_url, pdf_url, writer, mkt_tp, key, save_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        reg_dt = excluded.reg_dt,  -- 항상 갱신
                        writer = excluded.writer,  -- 항상 갱신
                        mkt_tp = excluded.mkt_tp,  -- 항상 갱신
                        download_url = CASE
                            WHEN excluded.download_url IS NOT NULL AND excluded.download_url != ''
                            THEN excluded.download_url
                            ELSE download_url -- 기존 값을 유지
                        END,
                        telegram_url = CASE
                            WHEN excluded.telegram_url IS NOT NULL AND excluded.telegram_url != ''
                            THEN excluded.telegram_url
                            ELSE telegram_url -- 기존 값을 유지
                        END,
                        pdf_url = CASE
                            WHEN excluded.pdf_url IS NOT NULL AND excluded.pdf_url != ''
                            THEN excluded.pdf_url
                            ELSE pdf_url -- 기존 값을 유지
                        END
                ''', (
                    entry["sec_firm_order"],
                    entry["article_board_order"],
                    entry["firm_nm"],
                    entry.get("reg_dt", ''),
                    entry["article_title"],
                    entry.get("article_url", None),  # ARTICLE_URL이 없으면 NULL을 넣음
                    1 if entry.get("is_sent", False) else 0,  # is_sent 매핑
                    entry.get("download_url", None),  # DOWNLOAD_URL이 없으면 NULL을 넣음
                    entry.get("telegram_url", None),  # TELEGRAM_URL이 없으면 NULL을 넣음
                    entry.get("pdf_url") or entry.get("download_url") or entry.get("telegram_url", None),  # PDF_URL이 없으면 대체 URL을 넣음
                    entry.get("writer", ''),
                    entry.get("mkt_tp", "KR"),  # MKT_TP가 빈값이면 KR을 넣음
                    entry.get("key") or entry.get("pdf_url") or entry.get("download_url") or entry.get("telegram_url", ''),  # KEY가 없거나 빈 값일 때 대체 URL을 사용
                    entry["save_time"]
                ))

                # 삽입 또는 업데이트 확인
                if self.cursor.rowcount == 1:
                    inserted_count += 1  # 새로 삽입된 경우
                else:
                    updated_count += 1  # 업데이트된 경우

            # 커밋하고 결과 출력
            self.connection.commit()
            logger.info(f"SQLite Data inserted: {inserted_count} rows, updated: {updated_count} rows.")
        finally:
            self.close_connection()  # 예외 발생 여부와 무관하게 연결 종료

        return inserted_count, updated_count

    async def fetch_daily_articles_by_date(self, firm_info: FirmInfo, date_str=None):
        """
        telegram_url 갱신이 필요한 레코드를 조회합니다.
        
        Args:
            firm_info (FirmInfo): sec_firm_order와 article_board_order 속성을 포함한 FirmInfo 인스턴스.
            date_str (str, optional): 조회할 날짜 (형식: 'YYYYMMDD'). 지정하지 않으면 오늘 날짜로 설정됩니다.
        
        Returns:
            list[dict]: 조회된 기사 목록
        """
        self.open_connection()
        query_date = date_str if date_str else datetime.now().strftime('%Y%m%d')
        firmInfo = firm_info.get_state()
        logger.debug(f"Fetching daily articles for firm order: {firmInfo['sec_firm_order']}")
        query = f"""
        SELECT 
            report_id, sec_firm_order, article_board_order, firm_nm, reg_dt,
            article_title, article_url, is_sent, 
            download_url, writer, save_time, telegram_url, key, pdf_url
        FROM 
            {self.main_table_name}
        WHERE 
            reg_dt BETWEEN strftime('%Y%m%d', date(substr('{query_date}', 1, 4) || '-' || substr('{query_date}', 5, 2) || '-' || substr('{query_date}', 7, 2), '-3 days'))
                    AND strftime('%Y%m%d', date(substr('{query_date}', 1, 4) || '-' || substr('{query_date}', 5, 2) || '-' || substr('{query_date}', 7, 2), '+2 days'))
            AND sec_firm_order = '{firmInfo["sec_firm_order"]}'
            AND key IS NOT NULL
            AND telegram_url  = ''
        ORDER BY sec_firm_order, article_board_order, save_time
        """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.close_connection()
        
        return [dict(row) for row in rows]

    async def fetch_all_empty_telegram_url_articles(self, firm_info: FirmInfo, days_limit: int = None):
        """
        telegram_url 갱신이 필요한 전체 레코드를 조회합니다.
        
        Args:
            firm_info (FirmInfo): sec_firm_order와 article_board_order 속성을 포함한 FirmInfo 인스턴스.
            days_limit (int, optional): 최근 며칠 이내의 데이터를 조회할지 여부.
        
        Returns:
            list[dict]: 조회된 기사 목록
        """
        self.open_connection()
        firmInfo = firm_info.get_state()
        logger.debug(f"Fetching articles for firm order: {firmInfo['sec_firm_order']}")
        
        query = f"""
        SELECT 
            report_id, sec_firm_order, article_board_order, firm_nm, reg_dt,
            article_title, article_url, is_sent, 
            download_url, writer, save_time, telegram_url, key, pdf_url
        FROM 
            {self.main_table_name}
        WHERE 
            sec_firm_order = '{firmInfo["sec_firm_order"]}'
            AND key IS NOT NULL
            AND (telegram_url IS NULL OR telegram_url = '')
        """
        
        if days_limit:
            query += f" AND save_time >= datetime('now', '-{days_limit} days', 'localtime')"

        query += " ORDER BY reg_dt DESC, save_time DESC"

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        self.close_connection()
        
        return [dict(row) for row in rows]

    async def fetch_ls_detail_targets(self):
        """
        LS증권(sec_firm_order=0) 레포트 중 TELEGRAM_URL이 .pdf로 끝나지 않는 대상을 조회합니다.
        """
        query = f"""
        SELECT 
            report_id, sec_firm_order, article_board_order, firm_nm, reg_dt,
            pdf_url, article_title, article_url, is_sent, 
            download_url, writer, save_time, telegram_url, key
        FROM 
            {self.main_table_name}
        WHERE 
            sec_firm_order = 0
            AND (telegram_url NOT LIKE '%.pdf' OR telegram_url IS NULL OR telegram_url = '')
        """
        return await self.execute_query(query)

    async def update_telegram_url(self, record_id, telegram_url, article_title=None, pdf_url=None):
        """report_id를 기준으로 telegram_url 및 (옵션) article_title 컬럼을 비동기로 업데이트합니다."""
        async with aiosqlite.connect(self.db_path) as db:
            # pdf_url이 없으면 telegram_url을 기본값으로 사용
            if pdf_url is None:
                pdf_url = telegram_url

            # 기본 쿼리 구성
            query = f"""
            UPDATE {self.main_table_name}
            SET telegram_url = ?, pdf_url = ?
            WHERE report_id = ?
            """
            params = [telegram_url, pdf_url, record_id]  # 기본 매개변수

            # article_title이 주어진 경우 쿼리에 추가
            if article_title is not None:
                query = f"""
                UPDATE {self.main_table_name}
                SET telegram_url = ?, pdf_url = ?, article_title = ?
                WHERE report_id = ?
                """
                params = [telegram_url, pdf_url, article_title, record_id]

            # 쿼리 실행 및 커밋
            await db.execute(query, params)
            await db.commit()

        return {
            "status": "success",
            "query": query,
            "record_id": record_id,
            "telegram_url": telegram_url,
            "pdf_url": pdf_url,
            "article_title": article_title
        }
    
    async def execute_query(self, query, params=None, close=False):
        """주어진 쿼리를 실행하고 결과를 반환합니다. 필요 시 커넥션을 종료합니다."""
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.cursor() as cursor:
                try:
                    if params:
                        await cursor.execute(query, params)
                    else:
                        await cursor.execute(query)
                    
                    # SELECT 쿼리인 경우 fetch 결과 반환
                    if query.strip().lower().startswith("select"):
                        rows = await cursor.fetchall()
                        result = [dict(row) for row in rows]
                    else:
                        # INSERT, UPDATE, DELETE 쿼리인 경우 commit 후 영향받은 행 반환
                        await conn.commit()
                        result = {"status": "success", "affected_rows": cursor.rowcount}
                except Exception as e:
                    logger.error(f"SQLite Query Error: {e}")
                    result = {"status": "error", "error": str(e)}
                finally:
                    if close:  # close가 True일 경우 커넥션을 종료
                        await conn.close()
        return result
    
    async def daily_select_data(self, date_str=None, type=None):
        """{self.main_table_name} 테이블에서 지정된 날짜 또는 당일 데이터를 조회합니다."""
        logger.debug(f"daily_select_data called with date_str: {date_str}, type: {type}")
        
        # 'type' 파라미터가 필수임을 확인
        if type not in ['send', 'download']:
            raise ValueError("Invalid type. Must be 'send' or 'download'.")

        if date_str is None:
            # date_str가 없으면 현재 날짜 사용
            query_date = datetime.now().strftime('%Y-%m-%d')
            query_reg_dt = (datetime.now() + timedelta(days=2)).strftime('%Y%m%d')  # 2일 추가
        else:
            # yyyymmdd 형식의 날짜를 yyyy-mm-dd로 변환
            query_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            query_reg_dt = (datetime.strptime(date_str, '%Y%m%d') + timedelta(days=2)).strftime('%Y%m%d')  # 2일 추가

        # 쿼리 타입에 따라 조건을 다르게 설정
        if type == 'send':
            query_condition = "(is_sent = false OR is_sent IS NULL)"
            query_condition += "AND (sec_firm_order != 19 OR (sec_firm_order = 19 AND telegram_url <> ''))"
        elif type == 'download':
            query_condition = "is_sent = true AND pdf_sync_status != 2"

        # 3일 이내 조건 추가
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')

        query = f"""
        SELECT 
            report_id, sec_firm_order, article_board_order, firm_nm, reg_dt,
            pdf_url, article_title, article_url, is_sent, 
            download_url, writer, save_time, telegram_url
        FROM 
            {self.main_table_name} 
        WHERE 
            DATE(save_time) = '{query_date}'
            AND reg_dt >= '{three_days_ago}'
            AND reg_dt <= '{query_reg_dt}'
            AND {query_condition}
        GROUP BY (CASE WHEN telegram_url IS NULL OR telegram_url = '' THEN report_id ELSE telegram_url END)
        ORDER BY sec_firm_order, article_board_order, save_time
        """
        
        return await self.execute_query(query)

    async def daily_update_data(self, date_str=None, fetched_rows=None, type=None):
        """데이터를 업데이트합니다. type에 따라 업데이트 쿼리가 달라집니다."""
        
        if date_str is None:
            query_date = datetime.now().strftime('%Y-%m-%d')
        else:
            query_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

        if type not in ['send', 'download']:
            raise ValueError("Invalid type. Must be 'send' or 'download'.")

        if type == 'send':
            for row in fetched_rows:
                telegram_url = row.get('telegram_url')
                if telegram_url:
                    update_query = f"UPDATE {self.main_table_name} SET is_sent = true WHERE telegram_url = ?"
                    param = (telegram_url,)
                else:
                    update_query = f"UPDATE {self.main_table_name} SET is_sent = true WHERE report_id = ?"
                    param = (row['report_id'],)
                
                await self.execute_query(update_query, param)

        elif type == 'download':
            update_query = f"UPDATE {self.main_table_name} SET pdf_sync_status = 2 WHERE report_id = ?"
            for row in fetched_rows:
                await self.execute_query(update_query, (row['report_id'],))
        
        return {"status": "success"}

    async def update_report_summary_by_telegram_url(self, telegram_url, summary, model_name):
        """TELEGRAM_URL이 일치하고 발송완료(main_ch_send_yn='Y')된 레코드 중 report_id가 가장 큰 최신 레코드에 요약 정보를 업데이트합니다."""
        query = f"""
        UPDATE {self.main_table_name}
        SET gemini_summary = ?, 
            summary_time = ?, 
            summary_model = ?
        WHERE telegram_url = ?
          AND is_sent = true
          AND report_id = (
              SELECT MAX(report_id) 
              FROM {self.main_table_name} 
              WHERE telegram_url = ? 
                AND is_sent = true
          )
        """
        now = datetime.now().isoformat()
        params = (summary, now, model_name, telegram_url, telegram_url)
        return await self.execute_query(query, params)

    async def update_report_summary(self, record_id, summary, model_name):
        """{self.main_table_name} 테이블의 특정 report_id 레코드에 제미나이 요약 내용을 업데이트합니다."""
        query = f"""
        UPDATE {self.main_table_name}
        SET gemini_summary = ?, 
            summary_time = ?, 
            summary_model = ?
        WHERE report_id = ?
        """
        now = datetime.now().isoformat()
        params = (summary, now, model_name, record_id)
        
        return await self.execute_query(query, params)

    async def fetch_pending_summary_reports(self, limit=10):
        """요약이 아직 생성되지 않은 최근 레포트 목록을 조회합니다. (보안 PDF 증권사 등은 제외)"""
        # 제외 대상: 19(DB증권)
        exclude_firms = (19,) 
        
        query = f"""
        SELECT *
        FROM {self.main_table_name}
        WHERE (gemini_summary "IS NULL" OR gemini_summary = '')
        AND (telegram_url IS NOT NULL AND telegram_url != '')
        AND sec_firm_order NOT IN ({", ".join(map(str, exclude_firms))})
        ORDER BY save_time DESC
        LIMIT ?
        """
        
        return await self.execute_query(query, (limit,))

if __name__ == "__main__":
    async def main():
        db = SQLiteManager()
        rows = await db.daily_select_data(type='send')
        logger.info(f"Fetched {len(rows)} rows for sending.")
        if rows:
            r = await db.daily_update_data(fetched_rows=rows, type='send')
            logger.info(f"Update result: {r}")

    asyncio.run(main())
