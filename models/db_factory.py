import os


def get_db():
    """Return a DB manager based on DB_BACKEND.

    DB_BACKEND=ssh_library  — SecReportsManager (production default)
    DB_BACKEND=sqlite       — SQLiteManager (legacy/test only)
    DB_BACKEND=postgres     — legacy PostgreSQLManager wrapper (deprecated)
    """
    backend = os.getenv("DB_BACKEND", "ssh_library").lower()
    if backend == "sqlite":
        from models.SQLiteManager import SQLiteManager
        return SQLiteManager()
    if backend == "postgres":
        from models.PostgreSQLManager import PostgreSQLManager
        return PostgreSQLManager()

    # default: ssh_library
    from models.SecReportsManager import SecReportsManager
    return SecReportsManager(
        db_name=os.getenv("POSTGRES_REPORT_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        keyword_db_name=os.getenv("POSTGRES_KEYWORD_DB"),
    )
