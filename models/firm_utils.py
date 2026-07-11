"""FirmInfo 함수형 wrapper — LLM/신규개발자용 단순 인터페이스.

기존 FirmInfo 메타클래스는 그대로 두고, 외부에서 호출하기 쉬운
일반 함수만 제공. 신규 모듈/GA standalone에서 FirmInfo 대신 이것만 import하면 됨.

사용법:
    from models.firm_utils import firm_name, board_name

    name = firm_name(4)          # → "KB증권"
    board = board_name(4, 0)     # → "기업분석"
"""
from models.FirmInfo import FirmInfo


def firm_name(firm_id: int) -> str:
    """증권사명 반환. 예: firm_name(4) → 'KB증권'"""
    return FirmInfo(firm_id, 0).get_firm_name()


def board_name(firm_id: int, board_id: int = 0) -> str:
    """게시판명 반환. 예: board_name(4, 7) → 'Global Insights'"""
    return FirmInfo(firm_id, board_id).get_board_name()


def all_firm_names() -> list[str]:
    """전체 증권사명 리스트 (firm_id 순)."""
    return FirmInfo.firm_names


def iter_active_firm_ids() -> list[int]:
    """Return active firm IDs from loaded metadata, preserving gaps."""
    return FirmInfo.iter_active_firm_ids()


def telegram_update_required(firm_id: int) -> bool:
    """텔레그램 발송 필요 여부."""
    return FirmInfo(firm_id, 0).telegram_update_required


def ga_enabled(firm_id: int) -> bool:
    """GA(GitHub Actions) 이관 여부. PostgreSQL v_sec_firm_info.ga_enabled_yn 기준.
    Static fallback 시 False 반환."""
    return FirmInfo(firm_id, 0).ga_enabled


def ga_enabled_orders() -> set[int] | None:
    """PostgreSQL에서 ga_enabled_yn='Y'인 모든 firm_id set 반환.
    PostgreSQL 메타데이터가 로드되지 않았거나 static fallback이면 None 반환.
    None은 "알 수 없음"을 의미 — 호출자는 전체 후보를 fallback으로 사용해야 한다."""
    fi = FirmInfo.__new__(FirmInfo)  # 인스턴스 생성 없이 클래스 속성 접근
    if not FirmInfo._is_loaded:
        FirmInfo.load_data_from_db()
    if FirmInfo._metadata_source != "postgres":
        return None
    return {
        order for order, data in FirmInfo._firm_data.items()
        if data.get("ga_enabled", False)
    }
