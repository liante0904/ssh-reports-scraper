# Integration Canary

The pull-request gate intentionally excludes live PostgreSQL, external API, and
network scraper tests. `.github/workflows/integration-canary.yml` runs those
checks manually or on a weekday schedule against the `integration-canary`
environment.

Configure only non-production `CANARY_POSTGRES_*` secrets. The import test is a
write operation and is skipped by default; enabling it requires a manual run,
`allow_db_import=true`, and the exact confirmation `CANARY_NON_PROD_IMPORT`.
Production-like target names are rejected before any test starts.
