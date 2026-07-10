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

`firm hits=0`가 여러 server-only firm에서 동시에 발생하거나 로그에 아래 메시지가 반복되면,
스케줄러가 살아 있어도 registry가 비어 있는 상태다.

```text
config/firms.yaml not found — registry will be empty
```

이 경우 active scraper 컨테이너에 manifest가 들어갔는지 먼저 확인한다.

```bash
ssh oci 'CT=$(docker ps --format "{{.Names}}" | grep "ssh-reports-scraper-main-scraper" | head -1); docker exec "$CT" sh -lc "ls -la /app/config /app/config/firms.yaml"'
```

`/app/config/firms.yaml`이 없으면 `scraper_registry.py`가 빈 registry를 반환하고,
`scraper.py`의 regular/GA fallback 대상 목록도 비게 된다. 하나증권처럼 GA가 꺼진
server-only firm은 이 상태에서 누락된다. 재발 방지는 `Dockerfile`의 `config/`
COPY와 `scripts/verify_dockerfile.sh` 검증이다.

## 운영 scraper exec

운영 호스트에서 실행 중인 blue/green scraper 컨테이너를 자동 선택할 때는 `ops_scraper_exec.sh`를 쓴다.

```bash
bash scripts/ops_scraper_exec.sh list
bash scripts/ops_scraper_exec.sh name
bash scripts/ops_scraper_exec.sh sh '.venv/bin/python --version'
bash scripts/ops_scraper_exec.sh py <<'PY'
print("hello from active scraper")
PY
```

규칙:

- `ssh-reports-scraper-main-scraper-*` 실행 컨테이너를 자동 선택한다.
- 강제 지정이 필요하면 `SCRAPER_CONTAINER=ssh-reports-scraper-main-scraper-green`을 붙인다.
- `py`는 `/app`에서 `.venv/bin/python -`로 stdin 스크립트를 실행한다.
- 운영 DB write나 텔레그램 실발송은 사용자 승인이 있는 경우에만 수행한다.

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

## PostgreSQL connection exhaustion

다음 로그가 watchdog에서 반복되면 scraper 오류로 단정하지 않는다.

```text
FATAL: sorry, too many clients already
```

2026-07-10 확인된 장애에서는 1분마다 시작된 `pg_dump`/`COPY` 세션이 이전 실행과
겹치면서 PostgreSQL `max_connections`를 소진했다. watchdog의 반복 `FATAL`은 원인이
아니라 신규 진단 연결까지 거부된 결과였다.

### 읽기 전용 확인 순서

```bash
# 1. 같은 시간대의 watchdog 증상과 최초 발생 시각 확인
bash scripts/ops_tail_errors.sh --docker-only --watchdog --since "06:00"

# 2. 운영 호스트에서 중첩된 dump/COPY 프로세스와 실행 시간을 확인
ssh oci 'ps -eo pid,ppid,lstart,etime,args | grep -E "[p]g_dump|[p]sql.*COPY"'

# 3. 새 DB 연결이 가능해진 뒤 연결 소유자와 상태를 확인
ssh oci 'docker exec main-postgres sh -lc '\''psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -c "select application_name, usename, state, count(*) from pg_stat_activity group by 1,2,3 order by 4 desc;"'\'''

# 4. 연결 상한과 현재 사용량 확인
ssh oci 'docker exec main-postgres sh -lc '\''psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -c "show max_connections" -c "select count(*) as current_connections from pg_stat_activity;"'\'''
```

컨테이너 이름은 고정값을 추측하지 말고 필요하면 먼저 확인한다.

```bash
ssh oci 'docker ps --format "{{.Names}}" | grep -E "postgres|watchdog"'
```

### 복구 판단

- 먼저 backup timer/cron에서 새 실행이 겹치지 않도록 잠금 또는 실행 주기를 수정한다.
- 이미 실행 중인 dump를 종료할지는 데이터 백업 소유자와 확인 후 결정한다.
- scraper/backend 재시작은 연결을 더 만들 수 있으므로 최초 대응으로 사용하지 않는다.
- watchdog은 같은 fingerprint를 집계하고 cooldown 동안 재알림하지 않아야 한다.
- 복구 완료 조건은 신규 DB 연결 성공, `pg_dump`/`COPY` 중첩 없음, 연결 수 baseline
  복귀, watchdog 반복 알림 중단이다.

운영 변경 뒤에는 같은 조회 명령의 before/after 결과를 incident 기록에 남긴다.

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
