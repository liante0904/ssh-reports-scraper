"""Scraper DB access — single entry point for PostgreSQL connections.

Returns a ``SecReportsManager`` instance configured from environment variables.
All scraper code should use this instead of constructing managers directly.
"""

import os


def get_db():
    """Return a configured SecReportsManager for scraper DB operations.

    Reads POSTGRES_REPORT_DB (or POSTGRES_DB) and POSTGRES_USER from env.
    """
    from models.SecReportsManager import SecReportsManager

    return SecReportsManager(
        db_name=os.getenv("POSTGRES_REPORT_DB") or os.getenv("POSTGRES_DB", "ssh_reports_hub"),
        user=os.getenv("POSTGRES_USER", "ssh_reports_hub"),
        keyword_db_name=os.getenv("POSTGRES_KEYWORD_DB"),
    )
