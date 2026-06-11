# Claude Code — Airflow Integration Session Context

## Project
Financial Markets ETL Pipeline — Apache Airflow Orchestration.
Full project plan: `PROJECT_PLAN.md`. Upgrade roadmap: `financial_etl_next_steps.pdf`.

## Objective
Convert the existing `src/pipeline.py` orchestrator into an Apache Airflow DAG. The pipeline currently runs as a CLI script (`python -m src.pipeline --mode backfill`). The goal is to make it a scheduled, monitored, production-style orchestration using Airflow with Docker Compose — without breaking the existing pipeline logic.

## Why This Matters
Airflow appears in ~50% of Frankfurt Data Engineer job postings (based on 194 postings analysis, April 2026). This is the single highest-impact technical addition to the project. Without it, the pipeline is a script. With it, it becomes a production system.

---

## Current Project State (Do Not Modify These)

| Component | Status | Notes |
|---|---|---|
| `sql/schema.sql` — 4 tables + indexes | ✅ Done | Correct schema, NUMERIC types, indexed |
| `.env`, `.gitignore`, `config/settings.py` | ✅ Done | Env vars loaded, ticker universe defined |
| `src/extract.py` — yfinance + FRED | ✅ Done | API integration working |
| `src/transform.py` — cleaning + indicators | ✅ Done | RSI, SMA, EMA, volatility |
| `src/load.py` — PostgreSQL upserts | ✅ Done | Idempotent upserts, numpy.int64 bug fixed |
| `src/pipeline.py` — orchestrator + CLI | ✅ Done | Backfill + daily modes, argparse CLI |
| `tests/` — 29 tests (mocked) | ✅ Done | All passing |
| `README.md` | ✅ Done | |

### Existing File Structure
```
financial-etl/
├── README.md
├── PROJECT_PLAN.md
├── requirements.txt
├── .env                    # DB creds — gitignored, blocked from Claude
├── .gitignore
├── config/
│   └── settings.py         # TICKER_UNIVERSE, FRED_SERIES, LOOKBACK_DAYS, DB config
├── src/
│   ├── __init__.py
│   ├── extract.py          # extract_prices(), extract_macro()
│   ├── transform.py        # clean_prices(), calculate_indicators()
│   ├── load.py             # upsert_asset(), upsert_prices(), upsert_indicators(), upsert_macro()
│   └── pipeline.py         # Orchestrator — ties E → T → L with logging + CLI
├── sql/
│   └── schema.sql
├── tests/
│   ├── test_extract.py
│   └── test_transform.py
└── notebooks/              # (planned, not yet created)
```

### How the Pipeline Currently Works
```bash
# Full backfill (all tickers, 2 years)
python -m src.pipeline --mode backfill

# Single ticker
python -m src.pipeline --mode backfill --tickers AAPL

# Daily mode (last 5 days)
python -m src.pipeline --mode daily
```

The orchestrator in `pipeline.py` calls extract → transform → load sequentially for each ticker, then loads macro data from FRED. It uses argparse for CLI arguments and Python logging throughout.

---

## What Must Be Built

### 1. Docker Compose Setup for Airflow
- Use the **official Apache Airflow Docker Compose** file as the starting point
- Airflow version: use the latest stable image (`apache/airflow:2.x`)
- Services needed: `webserver`, `scheduler`, `postgres` (Airflow metadata DB — separate from your financial_etl DB), `init` (for first-time setup)
- The pipeline's PostgreSQL database (financial_etl) is on the host machine (localhost), NOT inside Docker. The DAG must connect to it from within the container. This requires proper networking: use `host.docker.internal` (Windows/Mac) or `network_mode: host` (Linux) or an `extra_hosts` mapping
- Mount the project source code into the container so the DAG can import `src.extract`, `src.transform`, `src.load`
- Mount `.env` into the container or pass DB credentials via Airflow Variables/Connections

**Files to create:**
- `docker-compose.yaml` — Airflow services definition
- `.env` additions or `docker.env` — any Airflow-specific env vars (AIRFLOW__CORE__EXECUTOR, AIRFLOW__CORE__FERNET_KEY, etc.)

**Critical constraint:** The existing `.env` contains DB credentials and is gitignored. Do not hardcode credentials anywhere. Use Airflow Connections or environment variables passed through Docker Compose.

### 2. Airflow DAG (`dags/financial_etl_dag.py`)
Convert `pipeline.py` logic into a DAG with **separate PythonOperator tasks**:

```
Task graph (target):

    extract_prices  ──►  transform_prices  ──►  load_prices
         │                                          │
         └──────────────────────────────────────────┤
                                                    ▼
    extract_macro  ────────────────────────►  load_macro
```

**Task breakdown:**
- `extract_prices` — calls `extract.extract_prices()` for each ticker in TICKER_UNIVERSE
- `transform_prices` — calls `transform.clean_prices()` + `transform.calculate_indicators()` on extracted data
- `load_prices` — calls `load.upsert_asset()`, `load.upsert_prices()`, `load.upsert_indicators()`
- `extract_macro` — calls `extract.extract_macro()` for each FRED series
- `load_macro` — calls `load.upsert_macro()`

**Data passing between tasks:** Use XCom to pass DataFrames between extract → transform → load. Be aware that XCom serialises data — for large DataFrames, consider writing to intermediate files (e.g., `/tmp/` or a shared volume) and passing the file path via XCom instead. Evaluate which approach is cleaner given the data volumes (~500 rows per ticker for backfill).

**DAG configuration:**
- `dag_id`: `financial_etl_daily`
- `schedule_interval`: `'0 7 * * 1-5'` — daily at 07:00 CET, weekdays only (markets are closed on weekends)
- `start_date`: a fixed past date (e.g., `datetime(2025, 1, 1)`)
- `catchup`: `False` — do not backfill missed runs
- `max_active_runs`: `1`
- `default_args`:
  - `owner`: `'airflow'`
  - `retries`: `2`
  - `retry_delay`: `timedelta(minutes=5)`
  - `email_on_failure`: `True`
  - `email`: configured for alerting (can use a placeholder initially)

### 3. Email Alerting on Failure
- Configure SMTP in `docker-compose.yaml` via Airflow env vars or `airflow.cfg`
- At minimum, set `email_on_failure: True` in `default_args`
- If SMTP setup is complex, document the configuration steps in a comment block and use a placeholder. The alerting config should be clearly visible and easy to activate

### 4. Requirements & Dependencies
- Add `apache-airflow` to `requirements.txt` (or create a separate `requirements-airflow.txt` for Docker)
- Ensure the Airflow container has access to: `yfinance`, `fredapi`, `pandas`, `psycopg2-binary`, `python-dotenv`
- Consider a custom Dockerfile extending the Airflow image to install project-specific Python dependencies

### 5. Documentation Updates
- Update `README.md` with:
  - Airflow setup instructions (Docker Compose commands)
  - Screenshot placeholder for the Airflow UI showing the DAG
  - How to trigger a manual run from the Airflow UI
- Add the following commands to the README:
  ```bash
  # Start Airflow
  docker-compose up airflow-init && docker-compose up -d

  # Access Airflow UI
  # http://localhost:8080 (default: airflow/airflow)

  # Trigger a manual DAG run
  # Via UI or: docker-compose exec airflow-webserver airflow dags trigger financial_etl_daily
  ```

---

## File Structure After Completion

```
financial-etl/
├── ...                         # (all existing files unchanged)
├── docker-compose.yaml         # Airflow services
├── Dockerfile                  # (if custom image needed for dependencies)
├── dags/
│   └── financial_etl_dag.py    # The DAG definition
├── logs/                       # Airflow logs (gitignored)
├── plugins/                    # Airflow plugins dir (can be empty)
└── .gitignore                  # Updated: add logs/, plugins/ if needed
```

---

## Git Workflow (Mandatory)

**The project currently has only 2 commits. This must change starting now.**

Before any code changes:
```bash
git checkout -b feat/airflow-dag
```

Commit after every meaningful change, not just when "done". Examples:
- `feat: add docker-compose.yaml for Airflow setup`
- `feat: add financial_etl DAG with extract/transform/load tasks`
- `feat: configure DAG scheduling for weekday 07:00 CET`
- `feat: add email alerting on DAG failure`
- `docs: update README with Airflow setup instructions`
- `chore: update .gitignore for Airflow logs and plugins`

Target: multiple meaningful, atomic commits on this feature branch. Merge to `main` via PR (or direct merge if working solo) when complete.

---

## Constraints & Gotchas

1. **Do not refactor `src/` modules.** The extract/transform/load functions work. The DAG should call them — not rewrite them.
2. **PostgreSQL networking.** The financial_etl database runs on the host, not in Docker. The Airflow containers need to reach `localhost:5432` on the host. This is the most common setup issue. Test the DB connection from inside the container early.
3. **XCom size limits.** Airflow's default XCom backend (database) has practical limits for large data. For ~500 rows this is fine, but if TICKER_UNIVERSE grows significantly, switch to file-based passing.
4. **`.env` security.** The `.env` file is gitignored and blocked from Claude via a PreToolUse hook. Do not attempt to read or print its contents. Pass credentials to Docker via env_file or Airflow Connections.
5. **Airflow executor.** Use `LocalExecutor` (not CeleryExecutor) — this is a single-machine setup. LocalExecutor uses the Airflow metadata Postgres for task scheduling without needing Redis/RabbitMQ.
6. **Python path inside container.** Ensure the project's `src/` and `config/` are importable from within the DAG. This may require mounting the project root and adding it to `PYTHONPATH` in the Docker Compose config.

---

## Verification Checklist

After implementation, verify each of these:

- [ ] `docker-compose up` starts Airflow webserver, scheduler, and metadata DB without errors
- [ ] Airflow UI accessible at `http://localhost:8080`
- [ ] DAG `financial_etl_daily` appears in the UI and is parseable (no import errors)
- [ ] Manual trigger of the DAG completes successfully — data appears in PostgreSQL
- [ ] Task dependencies are correct: transform runs after extract, load runs after transform
- [ ] DAG schedule is set to `0 7 * * 1-5` (07:00 CET, weekdays)
- [ ] `email_on_failure` is configured in default_args (even if SMTP is placeholder)
- [ ] Existing `pytest` suite still passes (`python -m pytest tests/ -v`)
- [ ] All new files are committed with meaningful messages on a feature branch
- [ ] README updated with Airflow setup instructions

---

## How to Use This Document
Paste the relevant section or the entire document as context at the start of a Claude Code session. Work through the sub-tasks in order: Docker Compose → DAG → alerting → docs → git. Commit after each sub-task.

## References
- Official Airflow Docker Compose: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- Marc Lamberti YouTube (best practical Airflow tutorials): https://youtube.com/@marclamberti
- Airflow PythonOperator docs: https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html
