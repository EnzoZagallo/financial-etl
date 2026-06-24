# Pipeline Review Findings — 2026-06-20

Findings from a full read-through of the codebase prior to starting Phase 3 work.
Items already covered by `PRODUCTION_READINESS_PLAN.md` are **not** repeated here — this document lists what the plan did *not* already capture.

Severity scale: **blocker** (must fix before merge) · **minor** (worth fixing, not blocking) · **cosmetic** (nice-to-have polish).

---

## Phase 3 amendments

These add to the Phase 3 scope as currently written. They live on the same branch (`fix/dag-failure-propagation-and-bugs`) and the same v1.0.1 release.

### F1 — `tests/test_extract.py` will break after Phase 3.7 (blocker for the same commit)
**File:** `tests/test_extract.py:64-70` and `:116-123`

Both retry tests use a bare `Exception("API error")` / `Exception("timeout")` as the mocked failure. Phase 3.7 narrows `extract.py`'s retry loop to *network* errors only — `requests.exceptions.RequestException`, `ConnectionError`, `TimeoutError`. After the narrowing, a generic `Exception` will fall straight through without retrying, so the assertion `assert mock_dl.call_count == 3` will fail (`== 1` after the change).

**Action:** in the same commit as Phase 3.7, update both tests to raise a network exception (e.g. `requests.exceptions.ConnectionError`) so they still validate the retry path. Add one *additional* test in the same file asserting that a non-network exception (e.g. `KeyError`) does **not** retry (`call_count == 1`) — that's the actual behavioural guarantee being added.

### F2 — DAG smoke-test PYTHONPATH gotcha (documentation)
**File:** verification step in Phase 3.8

`python -c "import dags.financial_etl_dag"` will fail with `ModuleNotFoundError: No module named 'config'` if the project root is not on `sys.path`. From Git Bash on Windows:

```bash
PYTHONPATH=. python -c "import dags.financial_etl_dag"
```

This catches *parse errors* and *import-time errors* (bad imports, syntax errors, DAG-construction errors). It does **not** execute any task — `AirflowException`-raising logic is not validated. Real functional validation happens in Codespaces with `docker compose up`. The PR description should be explicit about this scope limit.

---

## Schema and database

### F3 — Foreign keys have no `ON DELETE` behaviour (minor)
**File:** `sql/schema.sql:21, 35`

`daily_prices.asset_id` and `technical_indicators.asset_id` reference `assets(asset_id)` without `ON DELETE` clauses. If an asset row is deleted, child rows would silently orphan (PostgreSQL default is `NO ACTION`, which would actually block the delete — but only at commit time, surprising behaviour).

**Recommendation:** add `ON DELETE CASCADE` to both — if you remove an asset, its history goes too. This is the cleaner default for an ETL warehouse. Address in a follow-up minor version, not Phase 3.

### F4 — Redundant indexes (cosmetic)
**File:** `sql/schema.sql:61-65`

`UNIQUE(asset_id, date)` on `daily_prices` and `technical_indicators` already creates a btree index on those columns. The named indexes `idx_prices_asset_date` and `idx_indicators_asset_date` are duplicates — harmless but consume disk and slow writes marginally. Could be dropped, but a public schema change isn't worth it for a portfolio repo. Note and move on.

---

## Docker / orchestration

### F5 — Schedule docstring is wrong half the year (minor)
**File:** `dags/financial_etl_dag.py:198`

The DAG schedule is `"0 5 * * 1-5"` (05:00 UTC). The docstring at the top of the file and the comment on line 198 say "07:00 CET". This is true in **summer** (CEST = UTC+2) but in **winter** the run is 06:00 CET (CET = UTC+1). The actual chosen run-time slot is ambiguous.

**Recommendation:** in the DAG comment, replace "07:00 CET" with "06:00 CET (winter) / 07:00 CEST (summer)" — or pick a different UTC offset if a fixed local time matters. Two-line fix. Fold into Phase 3.2 commit (which already touches the DAG date logic).

### F6 — Dead `extra_hosts` config (minor)
**File:** `docker-compose.yaml:29-31`

`extra_hosts: ["host.docker.internal:host-gateway"]` was added so containers could reach a *host-machine* PostgreSQL. The current setup runs PostgreSQL in a Docker container (`financial-etl-postgres`) — the host-gateway escape hatch is unused. Removing it is safe and cleaner.

**Recommendation:** delete in a Phase 4 `chore:` commit, not Phase 3.

### F7 — Unpinned dependency versions (minor, addressed in Phase 4)
**Files:** `requirements.txt`, `requirements-airflow.txt`

All deps use `>=` — `yfinance>=0.2.0`, `pandas>=2.0.0`, etc. A reproducible deployment needs strict pins. Phase 4 (CI + tooling) is the natural moment to either pin with `==` (after running `pip freeze` against a known-good environment) or move to `pip-tools` / `uv` and add a `requirements.lock` file. Worth deciding on approach in Phase 4 planning.

### F8 — `AIRFLOW__WEBSERVER__SECRET_KEY` and `FERNET_KEY` placeholders (deferred to Phase 6)
**File:** `docker-compose.yaml:9, 12`

Both are placeholders fine for local dev — `change_me_in_production` and `''`. They MUST be real secrets in the Azure deployment. Don't change them now; capture in the Phase 6 plan as a deployment checklist item.

---

## Stale documentation

### F9 — `CLAUDE.md` (project root) is partially stale (minor)
**File:** `CLAUDE.md`

- References `PROJECT_PLAN.md` which does not exist in the repo.
- States branch `master` and `feat/airflow-dag` — repo is now on `main` with that branch merged and deleted.
- "Known Bugs Fixed" section is still useful historical context.

**Recommendation:** rewrite under Phase 5 (docs) — or do a small `docs:` commit during Phase 4. Keep the bugs-fixed log; drop the stale branch/file references; add a pointer to `PRODUCTION_READINESS_PLAN.md` as the live planning doc.

### F10 — `AIRFLOW_SETUP.md` and `Instructions to Enzo - Airflow & Docker.md` (deletion candidates)
Both predate the merged Airflow integration. `AIRFLOW_SETUP.md` was the planning brief; `Instructions to Enzo - …` is Enzo's own learning notes on Docker/Airflow concepts. Decisions are pending — see "Deletion candidates" discussion separately.

### F11 — `github-workflow.skill` ZIP sitting in project root (cleanup)
**File:** `github-workflow.skill`

This is a Claude Code Skill archive (ZIP) — it belongs in `~/.claude/skills/`, not in the project repo. Until installed it isn't recognised as a skill by the harness.

**Recommendation:** install it (extract to `~/.claude/skills/github-workflow/`), then `git rm` from the repo. Don't commit skill archives into project repos — they're a per-user tool, not project artefacts.

---

## Test coverage gaps (informational)

Phase 3.6 fills the biggest one (`_nan_to_none` in `load.py`). Remaining gaps, in rough order of value:

- No integration test for `src/pipeline.py` orchestrator — would require a test DB. Skip for portfolio; not realistic without a fixture.
- No test for the upsert idempotency claim in `load.py` — could be added with `psycopg2` against a throwaway DB. Defer to post-Phase-3.
- No test that confirms `_rsi` returns exactly 100 / 0 on monotonic series after the Phase 3.5 fix — must be added **as part of** the 3.5 commit, not as a follow-up.

---

## Summary of what gets folded back into Phase 3

| Finding | Where it lands |
|---|---|
| F1 — fix `test_extract.py` retry tests | Same commit as 3.7 |
| F2 — document PYTHONPATH for smoke test | PR description |
| F5 — fix schedule docstring | Same commit as 3.2 |

Everything else (F3, F4, F6, F7, F8, F9, F10, F11) is deferred. F6/F7/F9 belong in Phase 4; F8 belongs in Phase 6; F3/F4 are nice-to-have minor versions; F10/F11 await deletion decisions.
