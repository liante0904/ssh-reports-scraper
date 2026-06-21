# GA Standalone 전환 현황

> 갱신: 2026-06-22 | 24/29개사 GA 정상, 1개 GA 제외(IP차단), 2개 보류, 2개 장애

---

## 정상 (24개사)

### GA Workflow + Secret 작동 확인
| # | 증권사 | order | 방식 | 상태 |
|:---:|------|:---:|------|:---:|
| 2 | NH투자증권 | 2 | JSON POST | ✅ |
| 4 | KB증권 | 4 | JSON POST | ✅ |
| 5 | 삼성증권 | 5 | HTML 파싱 | ✅ |
| 6 | 상상인증권 | 6 | form POST + 쿠키 | ✅ |
| 7 | 신영증권 | 7 | 서버 모듈 | ✅ 서버 전용 |
| 8 | 미래에셋증권 | 8 | HTML 파싱 | ✅ |
| 9 | 현대차증권 | 9 | JSON GET | ✅ |
| 10 | 키움증권 | 10 | JSON POST | ✅ |
| 14 | 다올투자증권 | 14 | form POST | ✅ |
| 15 | 토스증권 | 15 | JSON GET | ✅ |
| 16 | 리딩투자증권 | 16 | HTML 파싱 | ✅ |
| 19 | DB증권 | 19 | JSON POST | ✅ |
| 20 | 메리츠증권 | 20 | HTML 파싱 + detail | ✅ |
| 21 | 한화투자증권 | 21 | XML GET | ✅ |
| 22 | 한양증권 | 22 | HTML 파싱 | ✅ |
| 24 | 교보증권 | 24 | HTML 파싱 | ✅ |
| 25 | IBK투자증권 | 25 | 보드별 POST | ✅ |
| 26 | SK증권 | 26 | JSON POST | ✅ |
| 27 | 유안타증권 | 27 | form POST | ✅ |
| 28 | 흥국증권 | 28 | HTML 파싱 (EUC-KR) | ✅ |

### 서버 전용 (GA 미해당)
| # | 증권사 | order | 상태 |
|:---:|------|:---:|------|
| 0 | LS증권 | 0 | ✅ 서버 |
| 1 | 신한투자증권 | 1 | ✅ 서버 |
| 11 | DS투자증권 | 11 | ✅ 서버 |
| 12 | 유진투자증권 | 12 | ✅ 서버 |
| 13 | 한국투자증권 | 13 | ✅ 서버 (Selenium) |
| 17 | 대신증권 | 17 | ✅ 서버 |

---

## GA 제외 (2026-06-22)

| # | 증권사 | order | 상태 | 사유 |
|:---:|------|:---:|------|------|
| 3 | 하나증권 | 3 | ❌ GA 제외 → 서버 전용 | GA 러너 IP(미국/유럽)가 www.hanaw.com에서 차단. 17개 URL 전부 timeout → 510초 소모 후 0건. 서버 scheduler.py에서 직접 스크래핑 (modules/HANA_3.py → scrapers/hana_core.py) |

---

## 장애 (2개사)

| # | 증권사 | order | 상태 | 사유 |
|:---:|------|:---:|------|------|
| 18 | IM증권 | 18 | ❌ 인증만료 | secure key 갱신 필요 |
| 23 | BNK투자증권 | 23 | ❌ BLOCKED_BY_SOURCE_IP | 서버·GA 모두 IP 차단 |

---

## 6/22 수정 내역

| 항목 | 증상 | 원인 | 수정 |
|------|------|------|------|
| **하나증권 GA 제외** | GA workflow 9분 소모 후 0건 | GA 러너(미국/유럽) IP → hanaw.com 접근 불가, 17 URL × 30s timeout | GA cron 제거, 서버 scheduler가 직접 스크래핑 |
| **하나증권 timeout 축소** | 8.5분 불필요 대기 | 30s timeout × 17 URL | 15s timeout + 연속 2회 실패 시 early abort (~30s) |
| **하나증권 article_text** | "더보기" 요약 미수집 | `li.mb7.contn` 데이터 추출 로직 없음 | 인덱스 병렬 매칭으로 `article_text` 필드 추가 (170/170건) |
| **GA cron 월요일 10분 간격** | 월요일 아침 수집 공백 | 08:50 ~ 09:00 사이 갭 | `*/10 22 * * 0` + `0 23 * * 0` (KST 07:00~08:00) 모든 GA workflow에 추가 |
| **key → report_unique_key** | deprecated key 컬럼 의존 | key/report_unique_key 이중 관리 | validate.py fallback 제거, RETURNING report_unique_key 전환 |

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
