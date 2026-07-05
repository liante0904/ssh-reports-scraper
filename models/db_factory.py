import os


def get_db():
    """Return a DB manager based on DB_BACKEND.

    DB_BACKEND=ssh_library  — SecReportsManager (production default)
    DB_BACKEND=postgres     — legacy alias for the scraper SecReportsManager
    """
    # default/postgres: scraper compatibility wrapper around ssh_library
    from models.SecReportsManager import SecReportsManager
    return SecReportsManager(
        db_name=os.getenv("POSTGRES_REPORT_DB") or os.getenv("POSTGRES_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        keyword_db_name=os.getenv("POSTGRES_KEYWORD_DB"),
    )
