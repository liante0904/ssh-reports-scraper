import os
from loguru import logger
from models.ConfigManager import config

class MetaFirmInfo(type):
    """FirmInfo 클래스 레벨의 프로퍼티(firm_names 등)를 지원하기 위한 메타클래스"""
    @property
    def firm_names(cls):
        if not cls._is_loaded:
            cls.load_data_from_db()
        # 키(firm_id) 순으로 정렬된 firm_nm 리스트 반환
        max_order = max(cls._firm_data.keys()) if cls._firm_data else -1
        names = []
        for i in range(max_order + 1):
            names.append(cls._firm_data.get(i, {}).get("name", f"Unknown({i})"))
        return names

class FirmInfo(metaclass=MetaFirmInfo):
    """
    증권사 및 게시판 정보를 관리하는 클래스.
    데이터(DB 정보)는 클래스 수준에서 한 번만 로드하여 모든 인스턴스가 공유합니다. (데이터 싱글톤)
    """
    _firm_data = {}
    _board_data = {}
    _is_loaded = False
    _metadata_source = None  # "postgres" | "sqlite" | "static" — signals DB reliability

    @classmethod
    def load_data_from_db(cls):
        if cls._is_loaded:
            return

        backend = os.getenv("DB_BACKEND", "sqlite").lower()
        if backend == "postgres":
            cls._load_from_postgres()
        else:
            cls._load_from_sqlite()

    @classmethod
    def _load_from_postgres(cls):
        try:
            import psycopg2
            import psycopg2.extras
            from dotenv import load_dotenv
            load_dotenv(override=True)
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                dbname=os.getenv("POSTGRES_DB", "ssh_reports_hub"),
                user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
                password=os.getenv("POSTGRES_PASSWORD", ""),
            )
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute('SELECT firm_id, firm_nm, telegram_update_yn, ga_enabled_yn FROM v_sec_firm_info ORDER BY firm_id')
                for row in cur.fetchall():
                    cls._firm_data[row['firm_id']] = {
                        "name": row['firm_nm'],
                        "update_required": row['telegram_update_yn'] == 'Y',
                        "ga_enabled": row.get('ga_enabled_yn', 'N') == 'Y',
                    }
                cur.execute('SELECT firm_id, board_id, board_nm, board_cd, label_nm FROM v_sec_firm_board_info')
                for row in cur.fetchall():
                    cls._board_data[(row['firm_id'], row['board_id'])] = {
                        "name": row['board_nm'],
                        "code": row['board_cd'] or "",
                        "label": row['label_nm'] or ""
                    }
            conn.close()
            cls._is_loaded = True
            cls._metadata_source = "postgres"
            logger.debug("FirmInfo: Data successfully loaded from PostgreSQL.")
        except Exception as e:
            logger.error(f"FirmInfo Error: Failed to load data from PostgreSQL: {e}")

    @classmethod
    def _load_from_sqlite(cls):
        import sqlite3
        db_path = config.DB_PATH
        try:
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("SELECT firm_id, firm_nm, telegram_update_yn FROM v_sec_firm_info ORDER BY firm_id")
            rows = cursor.fetchall()
            # ga_enabled_yn은 SQLite 스키마에 없을 수 있으므로 graceful fallback
            has_ga_col = False
            try:
                cursor.execute("SELECT ga_enabled_yn FROM v_sec_firm_info LIMIT 0")
                has_ga_col = True
            except sqlite3.OperationalError:
                pass

            if has_ga_col:
                cursor.execute("SELECT firm_id, firm_nm, telegram_update_yn, ga_enabled_yn FROM v_sec_firm_info ORDER BY firm_id")
                for row in cursor.fetchall():
                    cls._firm_data[row['firm_id']] = {
                        "name": row['firm_nm'],
                        "update_required": row['telegram_update_yn'] == 'Y',
                        "ga_enabled": row['ga_enabled_yn'] == 'Y',
                    }
            else:
                for row in rows:
                    cls._firm_data[row['firm_id']] = {
                        "name": row['firm_nm'],
                        "update_required": row['telegram_update_yn'] == 'Y',
                        "ga_enabled": False,
                    }

            cursor.execute("SELECT firm_id, board_id, board_nm, board_cd, label_nm FROM v_sec_firm_board_info")
            for row in cursor.fetchall():
                cls._board_data[(row['firm_id'], row['board_id'])] = {
                    "name": row['board_nm'],
                    "code": row['board_cd'] or "",
                    "label": row['label_nm'] or ""
                }
            cls._is_loaded = True
            cls._metadata_source = "sqlite"
            logger.debug(f"FirmInfo: Data successfully loaded from SQLite ({db_path}).")
            conn.close()
        except Exception as e:
            logger.warning(f"FirmInfo: DB unavailable ({e}), using static fallback")
            cls._load_static_fallback()

    @classmethod
    def _load_static_fallback(cls):
        """DB 접근 불가능한 환경(GitHub Actions 등)을 위한 정적 fallback."""
        FALLBACK_FIRMS = {
            0: "LS증권", 1: "신한증권", 2: "NH투자증권", 3: "하나증권",
            4: "KB증권", 5: "삼성증권", 6: "상상인증권", 7: "신영증권",
            8: "미래에셋증권", 9: "현대차증권", 10: "키움증권", 11: "DS투자증권",
            12: "유진투자증권", 13: "한국투자증권", 14: "다올투자증권",
            15: "토스증권", 16: "리딩투자증권", 17: "대신증권", 18: "IM증권",
            19: "DB증권", 20: "메리츠증권", 21: "한화투자증권", 22: "한양증권",
            23: "BNK투자증권", 24: "교보증권", 25: "IBK투자증권", 26: "SK증권",
            27: "유안타증권", 28: "흥국증권",
        }
        for o, name in FALLBACK_FIRMS.items():
            cls._firm_data[o] = {"name": name, "update_required": False, "ga_enabled": False}
        cls._is_loaded = True
        cls._metadata_source = "static"

    def __init__(self, firm_id=0, board_id=0, firm_info=None):
        if not self._is_loaded:
            self.load_data_from_db()

        if firm_info:
            self.firm_id = firm_info.firm_id
            self.board_id = firm_info.board_id
        else:
            self.firm_id = firm_id
            self.board_id = board_id

        firm_info_cached = self._firm_data.get(self.firm_id, {})
        self.telegram_update_required = firm_info_cached.get("update_required", False)
        self.ga_enabled = firm_info_cached.get("ga_enabled", False)

    def get_firm_name(self):
        return self._firm_data.get(self.firm_id, {}).get("name", f"Unknown({self.firm_id})")

    def get_board_name(self):
        key = (self.firm_id, self.board_id)
        return self._board_data.get(key, {}).get("name", "")

    def get_board_code(self):
        key = (self.firm_id, self.board_id)
        return self._board_data.get(key, {}).get("code", "")

    def get_label_name(self):
        key = (self.firm_id, self.board_id)
        return self._board_data.get(key, {}).get("label", "")

    def set_firm_id(self, firm_id):
        self.firm_id = firm_id
        firm_data = self._firm_data.get(self.firm_id, {})
        self.telegram_update_required = firm_data.get("update_required", False)
        self.ga_enabled = firm_data.get("ga_enabled", False)

    def set_board_id(self, board_id):
        self.board_id = board_id

    def get_state(self):
        return {
            "firm_id": self.firm_id,
            "board_id": self.board_id,
            "FIRM_NAME": self.get_firm_name(),
            "BOARD_NAME": self.get_board_name(),
            "LABEL_NAME": self.get_label_name(),
            "TELEGRAM_UPDATE_REQUIRED": self.telegram_update_required,
            "GA_ENABLED": self.ga_enabled,
        }

if __name__ == "__main__":
    print(f"Total firm names: {len(FirmInfo.firm_names)}")
    test_info = FirmInfo(firm_id=27, board_id=0)
    print(f"Instance State: {test_info.get_state()}")
