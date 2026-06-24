# Claude Code — Session Context

## Project
Financial Markets ETL Pipeline.
Live planning doc: `PRODUCTION_READINESS_PLAN.md`. Pre-Phase-3 review: `REVIEW_FINDINGS.md`.

## Development Environment
- **Primary development happens on GitHub Codespaces** (VS Code in browser)
- Local Windows machine had Docker/WSL issues — may be revisited later
- PostgreSQL database for CLI mode runs on local Windows machine
- Airflow mode runs everything inside Docker (including PostgreSQL)

## Base Pipeline — COMPLETE

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

## Airflow Integration — COMPLETE

| Step | Task | Status |
|---|---|---|
| 1 | Feature branch `feat/airflow-dag` | Done |
| 2 | `Dockerfile` + `requirements-airflow.txt` | Done |
| 3 | `docker-compose.yaml` (LocalExecutor, self-contained PostgreSQL) | Done |
| 4 | `dags/financial_etl_dag.py` — 5 tasks, XCom, Params, error handling | Done |
| 5 | `.gitignore` updated for `logs/`, `plugins/` | Done |
| 6 | Tested in GitHub Codespaces — DAG runs successfully | Done |
| 7 | README updated with Airflow instructions + screenshots | Done |
| 8 | All Airflow files committed and pushed | Done |

## Known Bugs Fixed
- `numpy.int64` → psycopg2 crash: fixed in `load.py:_nan_to_none()` by calling `.item()` on numpy scalars.
- Airflow 2.10.5 logging handler crash: downgraded to 2.9.3.
- Docker logs/ permission error: fixed with `chmod -R 777 logs/`.

## Git State
- Branch `master`: base pipeline + pgAdmin screenshots
- Branch `feat/airflow-dag`: Airflow DAG, Docker Compose, updated README + DAG screenshots

## Security
- `.env` is gitignored and blocked from Claude via PreToolUse hook in `.claude/settings.json`
- Fernet key is empty for local dev — comment in docker-compose.yaml explains how to generate for production
