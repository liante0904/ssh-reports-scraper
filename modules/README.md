# modules/ — Module Index

이 디렉토리의 29개 파일은 두 종류로 나뉜다:

- **wrapper** (18개): `scrapers/*_core.py` 로 delegate 하는 thin wrapper. LLM이 읽을 필요 없음.
- **standalone** (8개): core 패턴 미이관, 독자 구현. LLM이 직접 읽어야 함.
- **LS (1개)**: 특수 경로 (DB+WARP+전역상태). DO NOT TOUCH.
- **init** (1개): 패키지 init.

## Wrapper (읽을 필요 없음 — scrapers/*_core.py 보면 됨)

| 파일 | 라인 | 증권사 | delegate |
|------|------|--------|----------|
| `KBsec_4.py` | 67 | KB증권 | `scrapers.kb_core` |
| `NHQV_2.py` | 55 | NH투자증권 | `scrapers.nhqv_core` |
| `Sangsanginib_6.py` | 39 | 상상인증권 | `scrapers.sangsangin_core` |
| `Leading_16.py` | 39 | 리딩투자증권 | `scrapers.leading_core` |
| `IBKs_25.py` | 28 | IBK투자증권 | `scrapers.ibk_core` |
| `DAOL_14.py` | 24 | 다올투자증권 | `scrapers.daol_core` |
| `ShinHanInvest_1.py` | 22 | 신한투자증권 | `scrapers.shinhan_core` |
| `Kiwoom_10.py` | 21 | 키움증권 | `scrapers.kiwoom_core` |
| `Hygood_22.py` | 21 | 한양증권 | `scrapers.hanyang_core` |
| `Hanwhawm_21.py` | 21 | 한화투자증권 | `scrapers.hanwha_core` |
| `Kyobo_24.py` | 19 | 교보증권 | `scrapers.kyobo_core` |
| `Yuanta_27.py` | 18 | 유안타증권 | `scrapers.yuanta_core` |
| `iMfnsec_18.py` | 13 | iM증권 | `scrapers.imfn_core` |
| `TOSSinvest_15.py` | 13 | 토스증권 | `scrapers.toss_core` |
| `Samsung_5.py` | 13 | 삼성증권 | `scrapers.samsung_core` |
| `MERITZ_20.py` | 13 | 메리츠증권 | `scrapers.meritz_core` |
| `Heungkuk_28.py` | 13 | 흥국증권 | `scrapers.heungkuk_core` |
| `HANA_3.py` | 13 | 하나증권 | `scrapers.hana_core` |
| `SKS_26.py` | 9 | SK증권 | `scrapers.sks_core` |
| `Miraeasset_8.py` | 9 | 미래에셋증권 | `scrapers.miraeasset_core` |
| `Hmsec_9.py` | 9 | 현대차증권 | `scrapers.hmsec_core` |

## Standalone (직접 읽어야 함 — core 미이관)

| 파일 | 라인 | 증권사 | 특징 |
|------|------|--------|------|
| `LS_0.py` | 783 | LS증권 | **DO NOT TOUCH**. DB+FirmInfo+WARP+전역상태. 특수 경로. |
| `DBfi_19.py` | 597 | DB금융투자 | PDF URL 2벌 로직 + enricher 보유 |
| `Koreainvestment_13.py` | 229 | 한국투자증권 | Selenium 의존. core 미이관. |
| `Shinyoung_7.py` | 192 | 신영증권 | 독자 구현. core 있으나 모듈이 직접 구현. |
| `Daeshin_17.py` | 153 | 대신증권 | core 미이관. 독자 구현. |
| `eugenefn_12.py` | 149 | 유진투자증권 | 세션만료로 비활성. 수동진단 전용. |
| `DS_11.py` | 146 | DS투자증권 | core 있으나 모듈이 직접 구현. `urls` env 사용. |
| `BNKfn_23.py` | 145 | BNK투자증권 | **DO NOT TOUCH**. IP차단. 수동진단 전용. |

## LLM 작업 가이드

- wrapper 모듈만 수정해야 한다면 → 실제 로직은 `scrapers/{firm}_core.py`에 있으니 그 파일을 수정
- standalone 모듈을 수정해야 한다면 → 이 파일을 직접 수정 (core 없음)
- LS/BNK → 호환 계약 보존. 수정 금지.
