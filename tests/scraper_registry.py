SCRAPER_IMPORTS = [
    ("LS증권", "modules.LS_0", "LS_checkNewArticle", False, True),
    ("신한투자", "modules.ShinHanInvest_1", "ShinHanInvest_checkNewArticle", True, True),
    ("NH투자", "modules.NHQV_2", "NHQV_checkNewArticle", True, False),       # GA standalone
    ("하나증권", "modules.HANA_3", "HANA_checkNewArticle", True, True),
    ("KB증권", "modules.KBsec_4", "KB_checkNewArticle", True, False),         # GA standalone
    ("삼성증권", "modules.Samsung_5", "Samsung_checkNewArticle", False, False), # GA standalone
    ("상상인", "modules.Sangsanginib_6", "Sangsanginib_checkNewArticle", True, False), # GA standalone
    ("신영증권", "modules.Shinyoung_7", "Shinyoung_checkNewArticle", False, True),
    ("미래에셋", "modules.Miraeasset_8", "Miraeasset_checkNewArticle", False, True),
    ("현대차증권", "modules.Hmsec_9", "Hmsec_checkNewArticle", False, True),
    ("키움증권", "modules.Kiwoom_10", "Kiwoom_checkNewArticle", True, False),   # GA standalone
    ("DS투자증권", "modules.DS_11", "DS_checkNewArticle", False, True),
    ("유진투자증권", "modules.eugenefn_12", "eugene_checkNewArticle", False, False),
    ("한국투자증권", "modules.Koreainvestment_13", "Koreainvestment_playwright_checkNewArticle", False, False),
    ("다올투자증권", "modules.DAOL_14", "DAOL_checkNewArticle", True, True),
    ("토스증권", "modules.TOSSinvest_15", "TOSSinvest_checkNewArticle", False, False), # GA standalone
    ("리딩투자증권", "modules.Leading_16", "Leading_checkNewArticle", True, False),       # GA standalone
    ("대신증권", "modules.Daeshin_17", "Daeshin_checkNewArticle", True, True),
    ("iM증권", "modules.iMfnsec_18", "iMfnsec_checkNewArticle", True, False),
    ("DB증권", "modules.DBfi_19", "DBfi_checkNewArticle", True, True),
    ("메리츠증권", "modules.MERITZ_20", "MERITZ_checkNewArticle", True, True),
    ("한화투자증권", "modules.Hanwhawm_21", "Hanwha_checkNewArticle", True, False),   # GA standalone
    ("한양증권", "modules.Hygood_22", "Hanyang_checkNewArticle", True, False),          # GA standalone
    ("BNK투자증권", "modules.BNKfn_23", "BNK_checkNewArticle", False, True),
    ("교보증권", "modules.Kyobo_24", "Kyobo_checkNewArticle", True, True),
    ("IBK투자증권", "modules.IBKs_25", "IBK_checkNewArticle", True, True),
    ("SK증권", "modules.SKS_26", "Sks_checkNewArticle", False, True),
    ("유안타증권", "modules.Yuanta_27", "Yuanta_checkNewArticle", True, True),
    ("흥국증권", "modules.Heungkuk_28", "Heungkuk_checkNewArticle", False, False),   # GA standalone
]


def import_checks():
    return [(module_path, func_name) for _, module_path, func_name, _, _ in SCRAPER_IMPORTS]


def active_health_checks():
    return [
        (name, module_path, func_name, is_async)
        for name, module_path, func_name, is_async, health_enabled in SCRAPER_IMPORTS
        if health_enabled
    ]
