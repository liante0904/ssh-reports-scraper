# OCI 운영 로그 조회 헬퍼

`scripts/ops_tail_errors.sh`는 로컬 개발 repo에서 OCI 운영 로그를 읽기 전용으로 조회하는 표준 진단 명령이다.

## 기본 명령

```bash
bash scripts/ops_tail_errors.sh
```

기본값:

- 대상 서버: `ssh oci`
- 대상 날짜: 오늘 KST
- 시작 시각: `06:00`
- 파일 로그: `/home/ubuntu/logs/YYYYMMDD/*.log`
- Docker 로그: 실행 중인 컨테이너
- 필터: `ERROR|FATAL|Traceback|CRITICAL|Exception|WARNING|WARN`

## 자주 쓰는 명령

```bash
bash scripts/ops_tail_errors.sh --since "09:00"
bash scripts/ops_tail_errors.sh --logs-only --since "06:00"
bash scripts/ops_tail_errors.sh --docker-only --watchdog
bash scripts/ops_tail_errors.sh --docker-only --scraper
bash scripts/ops_tail_errors.sh --service ssh-reports-scraper-main-scraper-green
bash scripts/ops_tail_errors.sh --date 20260625 --logs-only
bash scripts/ops_tail_errors.sh --firm-order 3 --firm-name 'HANA|하나|hana' --date-from 20260626 --date-to 20260629 --logs-only
```

## 회사별 누락 조사

특정 증권사가 운영에서 호출되지 않았는지 확인할 때는 `--firm-order`와 `--firm-name`을 같이 쓴다.

예시: 하나증권 주말 누락 조사

```bash
bash scripts/ops_tail_errors.sh --firm-order 3 --firm-name 'HANA|하나|hana' --date-from 20260626 --date-to 20260629 --logs-only
```

출력에서 먼저 본다:

- `Firm Metadata`: `telegram_update_yn`, `ga_enabled_yn`
- `Latest 10 Rows`: `tbl_sec_reports` 최신 저장 시각
- `Log Scan`: 일자별 `FULL-SCRAPE`, `REGULAR`, firm keyword hit 수

`firm hits=0`이고 최신 row가 오래됐다면 스케줄러/GA policy/regular path에서 제외됐을 가능성이 높다.

## LLM 사용 규칙

LLM은 운영 로그 확인 요청을 받으면, 사용자가 로그를 복붙하기 전에 이 스크립트를 먼저 사용한다.

권장 흐름:

1. `bash scripts/ops_tail_errors.sh --help`
2. `bash scripts/ops_tail_errors.sh --since "06:00"`
3. 필요한 경우 `--scraper`, `--watchdog`, `--service`로 범위를 좁힌다.
4. 특정 회사 누락 조사라면 `--firm-order`, `--firm-name`, `--date-from`, `--date-to`를 먼저 사용한다.
5. 결과에서 시간, 서비스명, 에러 패턴, 영향 범위를 요약한다.

## 안전 경계

이 스크립트는 읽기 전용이다.

금지:

- DB write
- 파일 삭제
- 로그 truncate
- 서비스 재시작
- `docker compose up/down`
- crontab 수정
- git write
- `sudo`

허용:

- `ssh oci`
- `find`
- `awk`
- `grep`
- `tail`
- `docker ps`
- `docker logs`
- `docker inspect`
- `date`

`ssh oci`가 `Operation not permitted`로 실패하면 운영 정상/비정상을 판단하지 말고, SSH/Tailscale 연결 문제로 별도 기록한다.

## 주의

이 도구는 로그 조회 도구이지 복구 도구가 아니다. 장애 수정, 컨테이너 재시작, DB 수정은 별도 명령과 사용자 승인 절차를 따른다.

## 로그 DB 저장 정책

전체 운영 로그를 PostgreSQL에 저장하는 방식은 기본값으로 쓰지 않는다.

비효율인 이유:

- 원본 로그는 이미 Docker log와 `/home/ubuntu/logs/YYYYMMDD/*.log`에 있다.
- 전체 로그는 양이 많고 중복이 많아 DB 비용과 노이즈가 커진다.
- 장애 대응에 필요한 것은 원문 전체가 아니라 "무슨 장애가 언제 생겼고 해결됐는지"다.

권장:

- 원문 로그: 파일/Docker/Dozzle/Loki 같은 로그 시스템에 보관
- DB: 장애 이벤트 요약과 해결 상태만 저장

저장 후보 스키마:

```text
incident_id
detected_at
service_name
severity
fingerprint
first_seen_at
last_seen_at
count
status: open | mitigated | resolved | ignored
owner
summary
resolution_note
source_log_ref
```

즉, 로그 전문 DB가 아니라 incident ledger를 만든다.
