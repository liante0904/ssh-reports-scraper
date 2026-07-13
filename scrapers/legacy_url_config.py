"""Normalize legacy URL-list secrets into the current core-parser contract.

The production secret store intentionally keeps public source URLs only.  The
parser-specific selectors and request payloads belong in versioned code, not
in an opaque secret.  A full config dict may still override these defaults for
an emergency source change.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from scrapers.config_guard import ScraperConfigError, normalize_cfg


_BROWSER_HEADERS = {"User-Agent": "Mozilla/5.0"}

FIRM_DEFAULTS: dict[str, dict[str, Any]] = {
    "Samsung": {
        "headers": _BROWSER_HEADERS,
        "item_sel": "#content > section.bbsLstWrap > ul > li",
        "title_sel": "dt > strong",
        "author_sel": "dd > span",
        "author_idx": 2,
        "url_tpl": (
            "https://www.samsungpop.com/common.do?cmd=down&saveKey=research.pdf"
            "&fileName={path}&contentType=application/pdf&inlineYn=Y"
        ),
        "firm_id": 5,
        "firm_nm": "삼성증권",
    },
    "Miraeasset": {
        "row_sel": "tbody tr",
        "skip_rows": 2,
        "cell_report_date": 1,
        "cell_title": 2,
        "cell_writer": 4,
        "attach_sel": ".bbsList_layer_icon a",
        "attach_pattern": r"javascript:downConfirm\('(.*?)'",
        "firm_id": 8,
        "firm_nm": "미래에셋증권",
    },
    "Hmsec": {
        "headers": _BROWSER_HEADERS,
        "list_key": "data_list",
        "paging_key": "paging",
        "item_keys": {
            "file": "UPLOAD_FILE1",
            "report_date": "REG_DATE",
            "title": "SUBJECT",
            "writer": "NAME",
        },
        "url_tpl": "https://www.hmsec.com/documents/research/{file}",
        "viewer_tpl": (
            "https://docs.hmsec.com/SynapDocViewServer/job?fid={url}"
            "&sync=true&fileType=URL&filePath={url}"
        ),
    },
    "Kiwoom": {
        "headers": _BROWSER_HEADERS,
        "payload": {
            "pageNo": 1,
            "pageSize": 100,
            "stdate": "{year}0101",
            "eddate": "{today}",
            "f_keyField": "",
            "f_key": "",
            "_reqAgent": "ajax",
            "dummyVal": 0,
        },
        "list_key": "researchList",
        "item_keys": {
            "menu_gb": "rMenuGb",
            "atta_file": "attaFile",
            "report_date": "makeDt",
            "title": "titl",
            "writer": "workId",
        },
        "url_tpl": (
            "https://bbn.kiwoom.com/research/SPdfFileView?rMenuGb={menu_gb}"
            "&attaFile={atta_file}&makeDt={report_date}"
        ),
    },
    "Toss": {
        "headers": _BROWSER_HEADERS,
        "item_keys": {
            "title": "title",
            "report_date": "createdAt",
            "writer": "writer",
            "files": "files",
            "contents": "contents",
            "category": "category",
        },
        "global_keyword": "global",
    },
}


def normalize_legacy_url_config(cfg: Any, *, firm_key: str) -> dict[str, Any]:
    """Return a full parser config while preserving explicit config overrides."""
    if firm_key not in FIRM_DEFAULTS:
        raise ScraperConfigError(f"unknown legacy URL-list firm: {firm_key}")

    supplied = normalize_cfg(cfg, firm_key=firm_key)
    if "url" in supplied and "urls" not in supplied:
        supplied = {**supplied, "urls": [supplied["url"]]}

    merged = deepcopy(FIRM_DEFAULTS[firm_key])
    for key, value in supplied.items():
        if key in {"headers", "item_keys", "payload"} and isinstance(value, dict):
            merged[key] = {**merged.get(key, {}), **value}
        else:
            merged[key] = value

    if not merged.get("urls"):
        raise ScraperConfigError(f"{firm_key}: no source URLs configured")
    return merged
