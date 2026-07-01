import pymysql
from urllib.parse import urlparse

class MariaDB:
    def __init__(self, db_url):
        self.db_url = urlparse(db_url)
        self.conn = None
        self.cursor = None

    def open_connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.db_url.hostname,
                user=self.db_url.username,
                password=self.db_url.password,
                charset='utf8',
                db=self.db_url.path.replace('/', ''),
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            print("MySQL 데이터베이스 연결 실패:", e)
            self.conn = None
            self.cursor = None

    def close_connect(self):
        if self.conn:
            self.conn.close()

    def SelNxtKey(self, firm_id, board_id):
        query = """
            SELECT firm_nm, BOARD_NM, firm_id, board_id, BOARD_URL, 
                   NXT_KEY, NXT_KEY_BF, NXT_KEY_ARTICLE_TITLE, SEND_YN, CHANGE_DATE_TIME, 
                   TODAY_SEND_YN, TIMESTAMPDIFF(second, CHANGE_DATE_TIME, CURRENT_TIMESTAMP) as SEND_TIME_TERM 
            FROM NXT_KEY
            WHERE firm_id = %s AND board_id = %s
        """
        self.cursor.execute(query, (firm_id, board_id))
        return self.cursor.fetchone()

    def InsNxtKey(self, firm_id, board_id, FIRST_NXT_KEY):
        query = """
            INSERT INTO NXT_KEY (firm_id, board_id, NXT_KEY, CHANGE_DATE_TIME)
            VALUES (%s, %s, %s, DEFAULT)
        """
        self.cursor.execute(query, (firm_id, board_id, FIRST_NXT_KEY))
        self.conn.commit()

    def UpdNxtKey(self, firm_id, board_id, FIRST_NXT_KEY, NXT_KEY_ARTICLE_TITLE):
        query = """
            UPDATE NXT_KEY SET NXT_KEY = %s, NXT_KEY_ARTICLE_TITLE = %s 
            WHERE firm_id = %s AND board_id = %s
        """
        self.cursor.execute(query, (FIRST_NXT_KEY, NXT_KEY_ARTICLE_TITLE, firm_id, board_id))
        self.conn.commit()

    def UpdTodaySendKey(self, firm_id, board_id, TODAY_SEND_YN):
        query = """
            UPDATE NXT_KEY SET TODAY_SEND_YN = %s 
            WHERE firm_id = %s AND board_id = %s
        """
        self.cursor.execute(query, (TODAY_SEND_YN, firm_id, board_id))
        self.conn.commit()
