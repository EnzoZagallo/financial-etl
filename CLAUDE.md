# Claude Code — Session Context

## Project
Financial Markets ETL Pipeline. Full plan is in `PROJECT_PLAN.md`.
Airflow integration plan is in `AIRFLOW_SETUP.md`.

## Development Environment
- **Primary development now happens on GitHub Codespaces** (VS Code in browser)
- Local Windows machine had Docker/WSL issues — may be revisited later
- PostgreSQL database (`financial_etl`) runs on local Windows machine
- Codespaces cannot reach the local DB — Docker/Airflow testing requires either:
  - Fixing WSL/Docker Desktop locally, OR
  - Setting up a PostgreSQL instance inside Codespaces

## Base Pipeline — ALL STEPS COMPLETE

| Step | Task | Status |
|---|---|---|
| 1 | `sql/schema.sql` — 4 tables + indexes | Done |
| 2 | `.env`, `.gitignore`, `config/settings.py` | Done |
| 3 | `src/extract.py` — yfinance + FRED extraction | Done |
| 4 | `src/transform.py` — cleaning + indicators | Done |
| 5 | `src/load.py` — PostgreSQL upserts | Done |
| 6 | `src/pipeline.py` — orchestrator + CLI | Done |
| 7 | `tests/test_extract.py`, `tests/test_transform.py` | Done — 29/29 passing |
| 8 | `README.md` | Done |

## Airflow Integration — IN PROGRESS

| Step | Task | Status |
|---|---|---|
| 1 | Feature branch `feat/airflow-dag` | Done |
| 2 | `Dockerfile` + `requirements-airflow.txt` | Done |
| 3 | `docker-compose.yaml` (LocalExecutor, host DB via host.docker.internal) | Done |
| 4 | `dags/financial_etl_dag.py` (5 tasks, XCom, weekday schedule) | Done |
| 5 | `.gitignore` updated for `logs/`, `plugins/` | Done |
| 6 | PostgreSQL `pg_hba.conf` + `postgresql.conf` updated for Docker access | Done (local) |
| 7 | Test: `docker compose up`, verify DAG in UI, trigger manual run | **Not started** |
| 8 | Update README with Airflow instructions | Not started |
| 9 | Commit and push all Airflow files | Not started |

## Known Bugs Fixed
- `numpy.int64` → psycopg2 crash: fixed in `load.py:_nan_to_none()` by calling `.item()` on numpy scalars.

## Git State
- Branch `master`: base pipeline + screenshots (pushed to GitHub)
- Branch `feat/airflow-dag`: all Airflow files created locally, NOT yet committed or pushed

## Security
- `.env` is gitignored and blocked from Claude via PreToolUse hook in `.claude/settings.json`
