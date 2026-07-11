# Integration Canary

The pull-request gate intentionally excludes live PostgreSQL, external API, and
network scraper tests. `.github/workflows/integration-canary.yml` runs those
checks manually or on a weekday schedule against the `integration-canary`
environment.

Configure only non-production `CANARY_POSTGRES_*` secrets. The import test is a
write operation and is skipped by default; enabling it requires a manual run,
`allow_db_import=true`, and the exact confirmation `CANARY_NON_PROD_IMPORT`.
Production-like target names are rejected before any test starts.

---

## Post-Deployment 24-72h Observation Checklist

For any production deployment (such as `main 2716191`), monitor the system status continuously using the read-only operational tools defined in `docs/OPS_LOG_TAIL.md`.

### 1. Verification Schedule
*   **24h Check**: Confirm container lifecycle stability.
    *   Command: `ssh oci 'docker ps --format "{{.Names}} {{.Status}}" | grep -E "scraper|watchdog"'`
    *   Expectation: Container Status indicates uptime > 24 hours, and restart count = 0 (no restarts since deployment).
*   **48h Check**: Check for resource leaks and memory issues (OOM).
    *   Discovery Command: `ssh oci 'docker ps -q --filter "name=scraper"'` (to discover the active scraper container ID dynamically)
    *   Verification Command: `ssh oci 'docker inspect --format "{{.State.OOMKilled}}" <CONTAINER_ID_OR_NAME>'` (check dynamically discovered active scraper/watchdog containers)
    *   Expectation: Returns `false`. Check that system RAM usage remains stable.
*   **72h Check**: Log scan for recurring errors, timeouts, or PostgreSQL connection warnings.
    *   Command: `bash scripts/ops_tail_errors.sh --since "06:00" --docker-only --scraper` (run daily check to scan scraper logs after morning routine start)
    *   Alternative for past dates: `bash scripts/ops_tail_errors.sh --date YYYYMMDD --logs-only`
    *   Expectation: Zero occurrences of `ERROR`, `FATAL`, `Traceback`, or connection exhaustion alerts.

### 2. Success & Escalation Criteria
*   **Success Criteria**:
    *   Active scraper and watchdog containers uptime exceeds 72 hours without restarts or OOM events.
    *   Zero scraper timeouts or unexpected validation filters (`firm hits` remains consistent).
*   **Escalation Criteria**:
    *   If active container restarts (`restart > 0`) or OOM occurs: Immediately alert operators to inspect logs and memory baselines.
    *   If connection exhaustion (`sorry, too many clients already`) is observed: Verify nested `pg_dump`/`COPY` processes as detailed in `docs/OPS_LOG_TAIL.md`.
    *   *Note*: Legacy zombie processes from external containers (such as `ssh-private-hub-fastapi-green`) are owned elsewhere and do not warrant scraper escalation.
