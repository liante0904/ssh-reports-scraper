import sqlite3

from sqlite_pg_cutover.schema import inspect_tables, normalize_identifier, render_schema, sqlite_type_to_postgres


def test_normalize_identifier():
    assert normalize_identifier("Report ID") == "report_id"
    assert normalize_identifier("SEC_FIRM_ORDER") == "sec_firm_order"
    assert normalize_identifier("123abc") == "_123abc"


def test_type_mapping():
    assert sqlite_type_to_postgres("INTEGER") == "bigint"
    assert sqlite_type_to_postgres("TEXT") == "text"
    assert sqlite_type_to_postgres("REAL") == "double precision"
    assert sqlite_type_to_postgres("BLOB") == "bytea"


def test_render_schema(tmp_path):
    db = tmp_path / "sample.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE Reports (ID INTEGER PRIMARY KEY, Title TEXT NOT NULL, price REAL)")
    conn.execute("INSERT INTO Reports (Title, price) VALUES ('hello', 1.5)")
    conn.commit()
    conn.row_factory = sqlite3.Row

    tables = inspect_tables(conn)
    ddl = render_schema(tables)

    assert 'CREATE TABLE IF NOT EXISTS "reports"' in ddl
    assert '"id" bigint NOT NULL' in ddl
    assert '"title" text NOT NULL' in ddl
    assert 'PRIMARY KEY ("id")' in ddl
