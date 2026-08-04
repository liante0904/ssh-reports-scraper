# Scraper Working Guide

This repository owns broker collection, GA artifacts, server-side scheduler
execution, enrichment, and report-row writes.

## Read order

1. `docs/DEBUG_ENTRYPOINTS.md`
2. the per-firm workflow or scheduler route
3. `run/standalone/`, `modules/`, and `scrapers/*_core.py` for that firm

## Verification

Start with the focused tests named by `docs/DEBUG_ENTRYPOINTS.md`; do not use
network-heavy broad health tests as a first smoke test. Record whether evidence
is unit, CI/build, or live production evidence.

For live incidents, use the workspace operation wrappers first. The local
`scripts/ops_scraper_exec.sh` is not an OCI SSH wrapper. PDF URL candidates
must be tied to the source detail page; do not treat a 200 response or filename
as document identity proof.
