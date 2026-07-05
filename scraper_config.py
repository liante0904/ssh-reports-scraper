# -*- coding:utf-8 -*-
"""Scraper configuration constants.

Extracted from scraper.py to reduce module-level noise.
All values are immutable after import — safe to share across modules.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Timeouts (seconds) ──
SCRAPER_STALE_DAYS = int(os.getenv("SCRAPER_STALE_DAYS", "5"))
SCRAPER_SYNC_TIMEOUT_SECONDS = int(os.getenv("SCRAPER_SYNC_TIMEOUT_SECONDS", "180"))
SCRAPER_ASYNC_TIMEOUT_SECONDS = int(os.getenv("SCRAPER_ASYNC_TIMEOUT_SECONDS", "300"))
LS_LIST_TIMEOUT_SECONDS = int(os.getenv("LS_LIST_TIMEOUT_SECONDS", "900"))
LS_DETAIL_TIMEOUT_SECONDS = int(os.getenv("LS_DETAIL_TIMEOUT_SECONDS", "900"))

# ── Telegram ──
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_REPORT_ALARM_SECRET")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHANNEL_ID_REPORT_ALARM")

# ── Blocked by source IP (frozenset) ──
# BNKfn_23: GA & server 모두 source IP 차단. Parser rewrite로 해결 불가.
# LS_0: local server IP 차단. GA WARP 우회 가능하나 로컬 fallback에선 skip.
BLOCKED_BY_SOURCE_IP = frozenset({"BNKfn_23", "LS_0"})

# ── Known external error patterns (watchdog alert noise filter) ──
KNOWN_EXTERNAL_ERRORS = [
    "LS 직접 접속 실패",
    "ConnectTimeoutError",
    "WARP",
    "BNK.*0건",
    "blocked by source",
    "Max retries exceeded",
]

# ── Firm ID constants ──
FIRM_ID_LS = 0
FIRM_ID_DS = 11
FIRM_ID_DBFI = 19

# ── Full-scrape hours (KST) ──
FULL_SCRAPE_HOURS = frozenset({1, 7, 13, 21})

# ── Per-module stale threshold overrides ──
# Format: "FuncName=days,OtherFunc=days"
STALE_OVERRIDES: dict[str, int] = {}
_raw = os.getenv("SCRAPER_STALE_OVERRIDES", "")
if _raw:
    for pair in _raw.split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            try:
                STALE_OVERRIDES[k.strip()] = int(v.strip())
            except ValueError:
                pass
