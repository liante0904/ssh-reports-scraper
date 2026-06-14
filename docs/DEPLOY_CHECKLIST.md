# 배포 체크리스트 & 장애 기록

> 갱신: 2026-06-14

---

## 배포 전 체크리스트

```
[ ] CI 통과 확인 (deploy workflow green)
[ ] pre-push 훅 통과 (Dockerfile COPY + standalone 문법)
[ ] DB constraint 확인 (key + report_unique_key UNIQUE 양쪽 활성)
[ ] GA workflow 정상 동작 확인 (최소 3개)
```

## 배포 후 확인

```
[ ] 서버 컨테이너 재시작 확인 (docker ps)
[ ] scrapers/ 디렉토리 존재 확인 (docker exec ls /app/scrapers/)
[ ] ModuleNotFoundError 로그 없음 확인
[ ] GA import 정상 동작 (5분 내 incoming/ga-scrapes/ 파일 처리)
[ ] Telegram 발송 정상 확인 (Sending 메시지 로그)
```

---

## 장애 기록

### #1: Dockerfile COPY scrapers/ 누락 (2026-06-12)

| 항목 | 내용 |
|------|------|
| 원인 | `scrapers/` 디렉토리 신설 후 Dockerfile 미갱신 |
| 증상 | scraper.py ModuleNotFoundError → 모든 수집/발송 중단 |
| 영향 | 30시간 서비스 중단 |
| 재발방지 | `scripts/verify_dockerfile.sh` pre-push 훅 등록 |

### #2: standalone 문법 오류 (2026-06-12)

| 항목 | 내용 |
|------|------|
| 원인 | sed로 standalone 수정 시 괄호/따옴표 파편 발생 |
| 증상 | GA workflow 전체 SyntaxError → GA 수집 중단 |
| 영향 | 12시간 GA 수집 중단 |
| 재발방지 | `scripts/verify_standalones.sh` pre-push 훅 등록 |

### #3: UNIQUE constraint 전환기 중복 insert (2026-06-14)

| 항목 | 내용 |
|------|------|
| 원인 | DB constraint를 key→report_unique_key 전환했으나 서버 코드 배포 전 |
| 증상 | UNIQUE constraint 없이 591건 중복 insert → Telegram 중복 발송 |
| 영향 | 22:00~22:20 KST 중복 발송 |
| 재발방지 | DB 변경 전 서버 코드 배포 완료 확인 필수. key+report_unique_key 이중 UNIQUE 유지 |

---

## 현재 방어 장치

| 단계 | 장치 | 파일 |
|------|------|------|
| pre-push | Dockerfile COPY 누락 감지 | `scripts/verify_dockerfile.sh` |
| pre-push | standalone 22개 Python 문법 검증 | `scripts/verify_standalones.sh` |
| post-deploy | 컨테이너/디렉토리/로그 검증 | `scripts/smoke_test.sh` |
| DB | key + report_unique_key 이중 UNIQUE | - |

---

## #4: config 기반 core ↔ server wrapper 불일치 (2026-06-15)

| 항목 | 내용 |
|------|------|
| 원인 | core 함수 signature를 config dict로 변경했으나 server wrapper는 URL 리스트 전달 |
| 증상 | `invalid input syntax for type integer: ""` — 빈 integer 필드가 INSERT 시도됨 |
| 영향 | 07:22 KST full-scrape 시간대 DB 에러 (데이터 누락 가능성) |
| 재발방지 | 1) core 함수가 list/str도 받도록 backward compat 추가 |
|           | 2) wrapper 변경 시 core signature 일치 확인 |

## 재발방지 요약

| # | 방지책 | 적용 위치 |
|---|--------|----------|
| 1 | Dockerfile COPY 누락 → verify_dockerfile.sh | pre-push hook |
| 2 | standalone 문법 오류 → verify_standalones.sh | pre-push hook |
| 3 | core/wrapper 불일치 → backward compat + deploy smoke test | core 코드 + post-deploy |
| 4 | UNIQUE constraint 전환 → key+report_unique_key 이중 유지 (과도기) | DB |
| 5 | DB migration 전 코드 배포 확인 → deploy 완료 후 DB 변경 | 운영 절차 |
