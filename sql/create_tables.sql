-- SQLite 데이터베이스 파일을 생성합니다.
-- sudo apt-get install sqlite3
-- mkdir ~/sqlite3;
-- sqlite3 ~/sqlite3/telegram.db < create_tables.sql
CREATE TABLE IF NOT EXISTS hankyungconsen (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sec_firm_order INTEGER,
    article_board_order INTEGER,
    firm_nm TEXT,
    article_title TEXT,
    telegram_sent BOOLEAN DEFAULT false,
    save_time TEXT
);

CREATE TABLE IF NOT EXISTS data_main_daily_send (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sec_firm_order INTEGER,
    article_board_order INTEGER,
    firm_nm TEXT,
    article_title TEXT,
    telegram_sent BOOLEAN DEFAULT false,
    save_time TEXT
);

CREATE TABLE IF NOT EXISTS naver_research (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sec_firm_order INTEGER,
    article_board_order INTEGER,
    firm_nm TEXT,
    article_title TEXT,
    telegram_sent BOOLEAN DEFAULT false,
    save_time TEXT
);
