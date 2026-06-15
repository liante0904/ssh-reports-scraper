# GA Standalone 전환 현황

> 갱신: 2026-06-15 | 25/29개사 GA 정상, 2개 보류, 1개 IP차단, 1개 인증만료

---

## 정상 (25개사)

### GA Workflow + Secret 작동 확인
| # | 증권사 | order | GA 건수 | 방식 | 상태 |
|:---:|------|:---:|:---:|------|:---:|
| 2 | NH투자증권 | 2 | 13 | JSON POST | ✅ 6/15 fix (_jp numeric) |
| 3 | 하나증권 | 3 | 510 | HTML 파싱 | ✅ 6/15 secret fix |
| 4 | KB증권 | 4 | 500 | JSON POST | ✅ |
| 5 | 삼성증권 | 5 | 200 | HTML 파싱 | ✅ 6/15 secret fix |
| 6 | 상상인증권 | 6 | 30 | form POST + 쿠키 | ✅ 6/15 fix (NT_NO int) |
| 7 | 신영증권 | 7 | - | 서버 모듈 | ✅ 서버 전용 |
| 8 | 미래에셋증권 | 8 | 10 | HTML 파싱 | ✅ 6/15 secret fix |
| 9 | 현대차증권 | 9 | 120 | JSON GET | ✅ 6/15 fix (viewer_tpl) |
| 10 | 키움증권 | 10 | 344 | JSON POST | ✅ 6/15 secret fix |
| 14 | 다올투자증권 | 14 | 88 | form POST | ✅ |
| 15 | 토스증권 | 15 | 194 | JSON GET | ✅ 6/15 secret fix |
| 16 | 리딩투자증권 | 16 | 100 | HTML 파싱 | ✅ 6/15 secret fix |
| 19 | DB증권 | 19 | 150 | JSON POST | ✅ |
| 20 | 메리츠증권 | 20 | 2,400 | HTML 파싱 + detail | ✅ 6/15 (local IP 차단, GA 정상) |
| 21 | 한화투자증권 | 21 | 5,000 | XML GET | ✅ 6/15 secret fix |
| 22 | 한양증권 | 22 | 30 | HTML 파싱 | ✅ 6/15 secret fix |
| 24 | 교보증권 | 24 | 70 | HTML 파싱 | ✅ 6/15 secret fix |
| 25 | IBK투자증권 | 25 | 1,426 | 보드별 POST | ✅ 6/15 fix (보드 URL) |
| 26 | SK증권 | 26 | 2,000 | JSON POST | ✅ 6/15 fix (PDF date fallback) |
| 27 | 유안타증권 | 27 | 493 | form POST | ✅ |
| 28 | 흥국증권 | 28 | 45 | HTML 파싱 (EUC-KR) | ✅ 6/15 fix (regex escape) |

### 서버 전용 (GA 미해당)
| # | 증권사 | order | 상태 |
|:---:|------|:---:|------|
| 0 | LS증권 | 0 | ✅ 서버 |
| 1 | 신한투자증권 | 1 | ✅ 6/15 fix (core delegate) |
| 11 | DS투자증권 | 11 | ✅ 서버 |
| 12 | 유진투자증권 | 12 | ✅ 서버 |
| 13 | 한국투자증권 | 13 | ✅ 서버 (Selenium) |
| 17 | 대신증권 | 17 | ✅ 서버 |

---

## 장애 (3개사)

| # | 증권사 | order | 상태 | 사유 |
|:---:|------|:---:|------|------|
| 18 | IM증권 | 18 | ❌ 인증만료 | secure key 갱신 필요 |
| 23 | BNK투자증권 | 23 | ❌ BLOCKED_BY_SOURCE_IP | 서버·GA 모두 IP 차단 |
| 28 | 상상인증권 | 6 | ⚠️ 쿠키 의존 | JSESSIONID 하드코딩 (만료 시 재발급 필요) |

---

## 6/15 수정 내역

| core | 증상 | 원인 | 수정 |
|------|------|------|------|
| `nhqv_core` | `list indices must be integers` | `_jp()`가 `.0.` numeric segment를 dict key로 처리 | `int(k) if k.isdigit()` |
| `hmsec_core` | 0건 (정상 API) | `cfg["viewer_tpl"]` KeyError → silent skip | `cfg.get("viewer_tpl", dl)` |
| `heungkuk_core` | 0건 (정상 API) | JSON config의 `\\\\` escape → regex mismatch | `.replace("\\\\","\\")` + fallback |
| `ibk_core` | 0건 (API 404) | 통합 URL 대신 보드별 POST URL 필요 | config urls → board-specific endpoint |
| `sangsangin_core` | 0건 (정상 API) | `NT_NO` int → `replace()` TypeError. URL/헤더 불일치 | `str()` + 원본 헤더/URL |
| `sks_core` | reg_dt="" 2000건 | API 응답 날짜 키 불일치 | `_extract_reg_dt()` 다단계 fallback |
| `shinhan` wrapper | 6/11 이후 0건 | API 파라미터 변경 (lastPageFlag, tran) | core delegate |
| 22개 core | `'list' object has no attribute 'get'` | standalone이 URL list 전달, core는 dict 기대 | `isinstance(cfg, list/str)` compat |

---

## 서버 부하

```
전체 29개사 중:
  GA 처리: 21개사 (GitHub Actions runner)
  서버 직접: 8개사 (LS·신한·DS·유진·한국·대신·신영·BNK)
  GA + 서버 듀얼모드: KST 1·7·13·21시 (장애 대비)
```

---

## 보안 인프라

- Secret: `*_URLS_JSON` GitHub Secrets (selector/payload/headers 포함 full config)
- SCP 전 validator: `scripts/validate_scrape_result.py` (reg_dt, unique_key 검증)
- ON CONFLICT: key + report_unique_key 이중 UNIQUE
- Backfill: `management-hub` → `POST /backfill/run` → core 직접 호출 → SCP
