"""Typed boundary between scraper output and the reports database row."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Mapping


class ReportPayloadError(ValueError):
    """Raised when scraper output cannot satisfy the shared report contract."""


@dataclass(frozen=True)
class ReportPayload:
    firm_id: int | None
    board_id: int | None
    firm_nm: str
    report_date: str | None
    article_title: str | None
    source_url: str
    telegram_url: str
    pdf_file_url: str
    writer: str
    mkt_tp: str
    report_unique_key: str
    save_at: datetime

    @classmethod
    def from_scraper(
        cls,
        item: Mapping[str, Any],
        *,
        require_schema: bool = False,
        require_firm_name: bool = False,
    ) -> "ReportPayload":
        if not isinstance(item, Mapping):
            raise ReportPayloadError("report item must be a mapping")

        source_url = str(item.get("source_url") or "").strip()
        unique_key = str(item.get("report_unique_key") or source_url).strip()
        report_date_value = item.get("report_date")
        report_date = str(report_date_value).strip() if report_date_value is not None else None
        firm_nm = str(item.get("firm_nm") or "").strip()

        if not unique_key:
            raise ReportPayloadError("missing report_unique_key")
        if require_schema:
            if not report_date or not re.fullmatch(r"\d{8}", report_date):
                raise ReportPayloadError(f"invalid report_date='{report_date or ''}'")
        if require_firm_name and not firm_nm:
            raise ReportPayloadError("missing firm_nm")

        save_at = _parse_save_at(item)
        telegram_url = str(item.get("telegram_url") or source_url).strip()
        pdf_file_url = str(
            item.get("pdf_file_url") or item.get("pdf_url") or telegram_url
        ).strip()

        return cls(
            firm_id=item.get("firm_id"),
            board_id=item.get("board_id"),
            firm_nm=firm_nm,
            report_date=report_date,
            article_title=item.get("article_title"),
            source_url=source_url,
            telegram_url=telegram_url,
            pdf_file_url=pdf_file_url,
            writer=str(item.get("writer") or ""),
            mkt_tp=str(item.get("mkt_tp") or "KR"),
            report_unique_key=unique_key,
            save_at=save_at,
        )

    def to_scraper_dict(self, original: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(original)
        result.update(
            report_unique_key=self.report_unique_key,
            report_date=self.report_date,
            source_url=self.source_url,
            telegram_url=self.telegram_url,
            pdf_file_url=self.pdf_file_url,
        )
        return result

    def to_db_record(self) -> tuple[Any, ...]:
        """Return values in SecReportsManager INSERT column order."""
        return (
            self.firm_id,
            self.board_id,
            self.firm_nm,
            self.report_date or None,
            self.article_title,
            self.telegram_url,
            self.pdf_file_url,
            self.writer,
            self.mkt_tp,
            self.report_unique_key,
            False,
            self.save_at,
        )


def _parse_save_at(item: Mapping[str, Any]) -> datetime:
    value = item.get("save_at")
    if isinstance(value, datetime):
        return value
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            pass

    # Explicit adapter for artifacts written before save_at became canonical.
    legacy_value = item.get("save_time")
    if legacy_value:
        try:
            return datetime.fromisoformat(str(legacy_value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            pass
    return datetime.now(timezone.utc)
