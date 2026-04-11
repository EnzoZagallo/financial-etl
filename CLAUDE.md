# Claude Code — Session Context

## Project
Financial Markets ETL Pipeline. Full plan is in `PROJECT_PLAN.md`.

## Status: ALL STEPS COMPLETE

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

## Known Bugs Fixed
- `numpy.int64` → psycopg2 crash: fixed in `load.py:_nan_to_none()` by calling `.item()` on numpy scalars to convert to native Python types.

## Pipeline Verified Working
- Ran `python -m src.pipeline --mode backfill --tickers AAPL` successfully
- 501 rows loaded into `daily_prices` and `technical_indicators`
- All 5 FRED macro series loaded into `macro_indicators`

## How to Run
```bash
# Full backfill
python -m src.pipeline --mode backfill

# Single ticker test
python -m src.pipeline --mode backfill --tickers AAPL

# Tests
python -m pytest tests/ -v
```

## Possible Next Steps
- Schedule daily runs with Windows Task Scheduler
- Add `notebooks/exploration.ipynb` for EDA and charts
- Add more tickers to `TICKER_UNIVERSE` in `config/settings.py`
- Build a Streamlit dashboard to visualize the data

## Security
- `.env` is gitignored and blocked from Claude via PreToolUse hook in `.claude/settings.json`
- Hook fires on every session — no restart needed after the first one
