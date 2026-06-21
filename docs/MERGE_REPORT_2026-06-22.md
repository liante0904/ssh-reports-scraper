# Branch Merge Report — `feat/eugene-tools` → `main`

> 작성일: 2026-06-22 | 대상 브랜치: `origin/feat/eugene-tools` | 미머지 커밋: 47개

---

## 🔴 Critical — 즉시 머지 필요 (프로덕션 버그 수정)

| # | 커밋 | 파일 | 내용 | 리스크 |
|---|------|------|------|:---:|
| 1 | `c191c11` | `models/WebScraper.py` | `import asyncio` 누락 → WS 재시도 시 NameError 발생 | 낮음 |
| 2 | `4b19833` | `modules/Daeshin_17.py` | 대신증권 article URL join 버그 — prod에서 URL 깨짐 | 낮음 |
| 3 | `a1a6d3e` | `modules/Daeshin_17.py` | `BASE_URL` 하드코딩 제거 → ConfigManager에서 동적 추출 | 낮음 |
| 4 | `9704192` | `.github/workflows/deploy.yml` | 배포 불완전 시 실패 감지 (early-exit 방지) | 낮음 |
| 5 | `1e769c5` | `.github/workflows/deploy.yml` | SSH env 기본값 처리 (optional 값 누락 방지) | 낮음 |
| 6 | `e47e3a9` | `.github/workflows/deploy.yml` | shell errexit 의존 → ssh 스크립트 오류 전파 | 낮음 |
| 7 | `519657d` | `modules/LS_0.py` | LS증권 WARP 최종 실패만 알람 — 중간 재시도 로그 노이즈 제거 | 낮음 |

## 🟡 Important — 머지 권장 (신뢰성/품질)

| # | 커밋 | 파일 | 내용 | 리스크 |
|---|------|------|------|:---:|
| 8 | `07c1993` | migration SQL | `MAIN_CH_SEND_YN`, `DOWNLOAD_STATUS_YN` ON CONFLICT 누락 수정 | 중간 |
| 9 | `ff9cf40` | migration SQL | `TB_SEC_REPORTS` 테이블명 따옴표 전체 수정 | 중간 |
| 10 | `e029180` | migration SQL | sequence 이름 및 테이블명 따옴표 수정 | 중간 |
| 11 | `40f0715` | `scheduler.py` | 스케줄러에 jitter 추가 → human-like 실행 패턴 (부하 분산) | 낮음 |
| 12 | `2c9487d` | `scheduler.py`, `.github/workflows/` | `TELEGRAM_ADMIN_ID_DEV` 통일 + daily health check workflow | 낮음 |
| 13 | `a81bab2` | `tests/` | DB sync 검증 테스트 (SQLite ↔ PostgreSQL 정합성) | 낮음 |

## 🟢 Nice to Have — 선택적 머지

| # | 커밋 | 파일 | 내용 | 리스크 |
|---|------|------|------|:---:|
| 14 | `e6b7531` | `models/db_factory.py` | 스크래퍼 백엔드 PostgreSQL 전환 (완전 이관) | 중간 |
| 15 | `f00d968` | `utils/telegram_util.py` | ADR-007: 텔레그램 알람 traceback 연동 강화 | 낮음 |
| 16 | `480bce4` | `utils/telegram_util.py` | ConfigManager 기반 admin ID 스마트 조회 | 낮음 |
| 17 | `e340739` | `tests/`, `.github/workflows/` | ADR-006: pytest + CI/CD 자동화 테스트 | 중간 |
| 18 | `782454d` | `tests/` | Eugene 툴링 → 테스트 헬퍼 분리 | 낮음 |
| 19 | `f1baefa` + `4ddf8fe` | `docs/` | 아키텍처 문서화 + DB audit 도구 | 낮음 |

---

## 머지 전략 제안

### Phase 1: Critical Fixes (바로 cherry-pick)
```bash
git checkout main
git cherry-pick c191c11 4b19833 a1a6d3e 9704192 1e769c5 e47e3a9 519657d
```
- 변경량: ~10개 파일, 100줄 내외
- 충돌 가능성: 낮음 (각각 독립적 수정)

### Phase 2: Quality Improvements (검토 후 merge)
```bash
git cherry-pick 07c1993 ff9cf40 e029180 40f0715 2c9487d a81bab2
```
- 변경량: ~20개 파일
- 주의: jitter 커밋(`40f0715`)은 main의 최신 scheduler와 충돌 가능성 있음 (최근 cd39b85에서 startup runner 변경)

### Phase 3: Optional
나머지는 선택적. PostgreSQL 전환(`e6b7531`)은 DB 마이그레이션 안정화 확인 후 적용 권장.

---

## 충돌 위험 분석

| main 최신 변경 | eugene-tools 변경 | 충돌 가능성 |
|---------------|-------------------|:---:|
| `cd39b85` scheduler startup runner | `40f0715` scheduler jitter | ⚠️ 중간 |
| `8f6582d` key fallback 제거 | `07c1993` migration ON CONFLICT | ⚠️ 중간 (같은 SQL 영역) |
| `hana_core.py` article_text 추가 | 해당 없음 | ✅ 없음 |

---

## 현재 main 미반영 사항 (금일 작업분)

오늘 main에 적용된 변경사항 (커밋 전):
- `scrapers/hana_core.py`: article_text 필드 추가, timeout 15초, 연속 실패 abort
- `.github/workflows/scrape-*.yml` (22개): 월요일 07~08시 KST 10분 간격 cron 추가
- `.github/workflows/scrape-hana.yml`: GA schedule 제거 (IP 차단으로 서버 전용 전환)
