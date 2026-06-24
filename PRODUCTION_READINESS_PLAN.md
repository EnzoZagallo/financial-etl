# Production Readiness Plan — Financial ETL Pipeline

**Context document for Claude Code sessions.**
Reference this file at the start of any session working on the items below.

---

## Project state at start of this work

The repository was just consolidated. As of now:

- Default branch is `main` (renamed from `master`).
- `feat/airflow-dag` has been merged into `main` via PR #1 and deleted.
- The codebase contains both the base ETL pipeline (yfinance + FRED + PostgreSQL) **and** the Apache Airflow orchestration with Docker Compose.
- Tag `v1.0.0` has been created and a GitHub Release published from it.
- 29 existing pytest tests pass.
- The pipeline has been verified to run end-to-end in both CLI mode (`python -m src.pipeline`) and Airflow mode (Docker Compose in GitHub Codespaces).

**All previous work documented in `CLAUDE.md`, `PROJECT_PLAN.md`, and `AIRFLOW_SETUP.md` remains valid as historical context.** Do not rewrite or delete those files unless explicitly told to.

The remaining work is grouped into three phases (3, 4, 5). Phase 6 (Azure deployment) is intentionally deferred to a later session.

> **Cloud target updated 2026-06-20:** the original plan referenced AWS. The decision is now Azure — Enzo has a student account with broader service availability. AWS references below have been replaced with Azure equivalents at the planning level; the concrete service choices (App Service vs Container Apps vs AKS, PostgreSQL Flexible Server tier, Bicep vs Terraform) are to be decided in the dedicated Phase 6 session.

---

## Workflow rules for all phases below

These are non-negotiable. They apply to every phase, every commit.

1. **One feature branch per phase.** Branch names are specified at the start of each phase. Never commit directly to `main`.
2. **Atomic, conventional commits.** Use the Conventional Commits format (`feat:`, `fix:`, `docs:`, `chore:`, `test:`, `refactor:`). One coherent change per commit — small and often, not one giant blob at the end.
3. **Existing tests must pass.** Before opening a PR, run `python -m pytest tests/ -v` and confirm 29/29 still pass. Any new tests added in a phase increase this count and the new count becomes the baseline.
4. **Do not refactor working code.** The ETL functions in `src/extract.py`, `src/transform.py`, `src/load.py` work correctly. Touch only what each task explicitly requires.
5. **Verify before destructive actions.** Run `git status` and `git log --oneline` before any push, force-push, or branch operation.
6. **PR per phase.** Each phase closes with a PR from its feature branch into `main`. Include a description that states what changed, why, and how it was tested. Merge with a merge commit (not squash) to preserve the atomic commit history.
7. **Tag patches and features.** After each merged PR, tag the result:
   - Phase 3 (bug fixes only) → `v1.0.1`
   - Phase 4 (CI/tooling additions) → `v1.1.0`
   - Phase 5 (documentation rewrite) → no new tag unless substantial
8. **Never commit secrets.** `.env` is gitignored and blocked. Do not attempt to read it.

---

## Phase 3 — Code correctness and observability fixes

**Branch:** `fix/dag-failure-propagation-and-bugs`
**Goal:** Fix three correctness bugs and one observability bug in the existing code. After this phase, the Airflow DAG will actually signal failures to the scheduler (currently it silently swallows them), and two minor finance-correctness issues are addressed.

### Background — why these matter

Reviewers reading the DAG code today will see error handling that *looks* defensive but actually defeats Airflow's failure detection. The DAG currently catches all per-item exceptions and only logs them, then returns normally. As a result, even if every ticker fails to extract, the Airflow task shows green, `retries: 2` never engages, and `email_on_failure` never fires. This is the single most important code bug in the project.

### 3.1 — DAG: raise on aggregate failures

**File:** `dags/financial_etl_dag.py`

In each of the five task functions, the per-item try/except logs failures and appends to a `failed` list. At the end of each function, if `failed` is non-empty (or if the success count is zero), the function must raise an exception so Airflow registers the task as failed.

Use `airflow.exceptions.AirflowException` (preferred over a bare `Exception` — it signals intent and is the standard in production DAGs).

Apply this pattern to:
- `task_extract_prices` — raise if `failed` is non-empty OR if `results` is empty.
- `task_transform_prices` — raise if `clean_data` is empty (all transforms failed).
- `task_load_prices` — track failed loads in a list (currently lost in the inner except); raise at the end if non-empty.
- `task_extract_macro` — same pattern as prices.
- `task_load_macro` — same pattern as prices.

The acceptable threshold is: **task fails if ANY item failed**, not just if all of them did. A finance ETL where 5 of 6 tickers loaded silently is a worse outcome than a loud failure that triggers retry and alerting.

### 3.2 — DAG: use Airflow's logical date instead of `date.today()`

**File:** `dags/financial_etl_dag.py`

Replace the `_get_date_range(mode)` function so it derives `end` from the task context's `data_interval_end` (or `logical_date` as fallback) rather than `date.today()`. This makes the DAG idempotent in the Airflow sense — re-running yesterday's failed run pulls *yesterday's* window, not today's, and `airflow dags backfill` becomes meaningful.

Signature change: the function should take the context (or specifically the relevant datetime) as an argument, not rely on a wall-clock call.

For the `daily` mode, keep the 5-day rolling window relative to that logical date. Consider whether to widen it to ~10 days as a data-gap safety net — your upserts are idempotent so the cost is zero. Document the decision in a code comment.

### 3.3 — DAG: fix `pd.read_json` deprecation

**File:** `dags/financial_etl_dag.py`

`pd.read_json(raw_json, orient="split")` with a bare string raises `FutureWarning` in pandas ≥ 2.1 and will break in a future version. Wrap with `io.StringIO`:

```python
import io
df = pd.read_json(io.StringIO(raw_json), orient="split")
```

Apply to every `pd.read_json` call in the DAG.

### 3.4 — Transform: fix the volatility comment

**File:** `src/transform.py`

The comment above `volatility_30` says "annualised std dev of daily returns" but the code computes a plain rolling standard deviation. The schema confirms it is non-annualised. Either:

(a) Fix the comment to match the code: `"30-day rolling std dev of daily returns (non-annualised)"`, OR
(b) Annualise the code by multiplying by `sqrt(252)` and keep the comment.

**Recommendation:** Option (a). The schema is already published and downstream queries may depend on the current semantics. Add a follow-up note in `PROJECT_PLAN.md` that annualised volatility may be added as a separate column in a future minor version.

### 3.5 — Transform: RSI edge case

**File:** `src/transform.py`

In `_rsi()`, when `avg_loss` is zero (strictly monotonic uptrend within the window), the function returns `NaN` due to `avg_loss.replace(0, np.nan)`. The financial convention is that RSI = 100 in this case (and RSI = 0 when `avg_gain` is zero).

Fix the function to return 100 when `avg_loss == 0` and `avg_gain > 0`, and 0 when `avg_gain == 0` and `avg_loss > 0`. Keep NaN only for genuinely undefined periods (start of series, before enough data accumulates).

### 3.6 — Load: regression test for `_nan_to_none`

**File:** `tests/test_load.py` (new file)

The `_nan_to_none()` helper in `src/load.py` was the site of the one known production bug (`numpy.int64` → psycopg2 crash). It has no test. Add a test file `tests/test_load.py` covering:

- `_nan_to_none(None)` → `None`
- `_nan_to_none(np.nan)` → `None`
- `_nan_to_none(pd.NaT)` → `None`
- `_nan_to_none(np.int64(42))` → `42` (native Python int, not numpy)
- `_nan_to_none(np.float64(3.14))` → `3.14` (native Python float)
- `_nan_to_none("string")` → `"string"`
- `_nan_to_none(42)` → `42`

This is a pure-function test, no database needed. Use `pytest` and the `import numpy as np` / `import pandas as pd` imports already used elsewhere.

### 3.7 — Extract: tighten retry logic

**File:** `src/extract.py`

The current retry loop catches `Exception` broadly and retries 3 times on any error, including non-transient ones (invalid ticker symbol, malformed API response). Narrow the caught exception types to network-related errors only:

- `requests.exceptions.RequestException` and its subclasses
- `ConnectionError`, `TimeoutError`
- Specific yfinance/fredapi exceptions if they exist

For non-transient errors (ValueError, KeyError on parsing), fail immediately without retry. Log the distinction clearly.

### 3.8 — Verification before opening PR

```bash
python -m pytest tests/ -v        # should show 36+ tests (29 existing + 7+ new)
ruff check .                       # only if Phase 4 has already merged
docker compose up --build         # in a separate session, confirm DAG still parses without errors
```

Open PR titled: `fix: improve DAG failure propagation, idempotency, and RSI edge cases`. In the PR description, list each of the seven sub-changes above with a one-line explanation.

After merge:

```bash
git tag -a v1.0.1 -m "Fix DAG failure propagation, idempotency, RSI edge cases, and add load.py tests"
git push origin v1.0.1
```

Create a GitHub Release from `v1.0.1` with the patch notes.

---

## Phase 4 — CI, tooling, and repository signals

**Branch:** `chore/ci-tooling-and-license`
**Goal:** Add the missing professional-repo signals that hiring screeners check first: a CI badge proving tests run on every push, a linter enforcing code quality, an open-source license, and a CHANGELOG.

### 4.1 — GitHub Actions CI

**File:** `.github/workflows/ci.yml` (new file)

Create a workflow that runs on every push and every pull request to `main`. It should:

1. Run on `ubuntu-latest` with Python 3.11 (matches the Dockerfile base).
2. Install dependencies from `requirements.txt`.
3. Run `ruff check .`
4. Run `python -m pytest tests/ -v`
5. Fail the workflow on any non-zero exit.

Use the current recommended action versions. Verify with a web search the latest stable `actions/checkout` and `actions/setup-python` versions before pinning them — these change.

### 4.2 — Ruff configuration

**File:** `pyproject.toml` (new file)

Add a `[tool.ruff]` section with sensible defaults for a Python data project:

- `line-length = 100`
- `target-version = "py311"`
- Enabled lint rules: `E` (pycodestyle errors), `F` (pyflakes), `I` (isort), `W` (warnings), `B` (bugbear), `UP` (pyupgrade).
- Ignore rules that conflict with the codebase's existing style (e.g. `E501` if any lines exceed 100, fix them rather than ignore — but check first).

Add `ruff` to `requirements.txt` under a clearly-labelled development dependency comment, OR create a separate `requirements-dev.txt`.

After adding the config, run `ruff check . --fix` once and commit the auto-fixes as a single `chore: apply ruff auto-fixes` commit, separate from the config commit.

### 4.3 — LICENSE

**File:** `LICENSE` (new file)

Add an MIT License. This is the standard for portfolio projects and the most permissive for someone reviewing your code. Use the standard MIT text — substitute the copyright year (2026) and copyright holder name (Enzo Zagallo).

### 4.4 — CHANGELOG

**File:** `CHANGELOG.md` (new file)

Follow the Keep a Changelog format. Backfill entries for `v1.0.0` and `v1.0.1` from git history. Structure:

```markdown
# Changelog

All notable changes to this project are documented in this file.
The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [1.1.0] - 2026-MM-DD
### Added
- GitHub Actions CI workflow running ruff and pytest on every push and PR
- Ruff linter with configuration in pyproject.toml
- MIT License
- CHANGELOG.md following Keep a Changelog format

## [1.0.1] - 2026-MM-DD
### Fixed
- DAG tasks now raise on aggregate failures, enabling Airflow retries and email alerts
- DAG uses Airflow logical date instead of date.today() for idempotency
- pd.read_json deprecation fixed by wrapping with io.StringIO
- RSI now returns 100 (not NaN) on monotonic uptrends, 0 on monotonic downtrends
- Extract retry logic narrowed to network errors only (no longer retries on parse errors)
- Volatility column comment corrected to reflect non-annualised semantics

### Added
- tests/test_load.py with regression test for _nan_to_none (the numpy.int64 bug site)

## [1.0.0] - 2026-MM-DD
### Added
- Initial release
- ETL pipeline: yfinance + FRED extraction, technical indicators, PostgreSQL load
- Apache Airflow DAG with Docker Compose orchestration
- 29 pytest tests covering extraction and transformation
- pgAdmin and Airflow UI screenshots in README
```

Fill in the actual release dates from `git log` after each tag was pushed.

### 4.5 — README badges

**File:** `README.md`

Add three badges immediately under the title (before the architecture diagram):

- CI status badge from the GitHub Actions workflow
- License badge (MIT)
- Python version badge (3.11+)

Use shields.io URLs. Verify each badge actually renders before committing.

### 4.6 — Repository GitHub settings (manual, not code)

These are not code changes — flag them in the PR description as follow-ups for the user to do in the GitHub UI:

- Add a repository **About description**: one sentence, e.g. "Production-style ETL pipeline for financial market data — Python, PostgreSQL, Apache Airflow, Docker."
- Add **topics**: `python`, `etl`, `airflow`, `postgresql`, `docker`, `data-engineering`, `finance`.
- Add a **website link** in About if a deployed version exists (later, after Phase 6).
- Pin the repository on the user's GitHub profile.

### 4.7 — Verification before opening PR

```bash
python -m pytest tests/ -v        # all tests still pass
ruff check .                       # zero violations
cat LICENSE                        # MIT text present
cat CHANGELOG.md                   # at least v1.0.0, v1.0.1, v1.1.0 entries
```

Open PR titled: `chore: add CI, ruff linting, MIT license, and CHANGELOG`.

After merge:

```bash
git tag -a v1.1.0 -m "Add CI, ruff linting, MIT license, and CHANGELOG"
git push origin v1.1.0
```

Create the GitHub Release.

---

## Phase 5 — README rewrite

**Branch:** `docs/readme-rewrite`
**Goal:** Transform the README from technical documentation into a portfolio piece. The current README is competent but reads as generated reference material. A recruiter spends ~30 seconds on a repo page before deciding whether to read more. The README's first 5 lines, first screenshot, and stated limitations are doing 80% of the work.

### 5.1 — Constraints

- **The user (Enzo) writes the prose.** Claude Code's role is to produce a structural skeleton with placeholder sections marked `[WRITE: ...]`, NOT to write the marketing copy itself. Voice authenticity matters more than polish here.
- Keep all the technically correct material (architecture diagram, schema table, indicator formulas, useful SQL queries) — move them, don't delete them.
- Keep both setup paths (CLI and Airflow). Don't hide the simpler path.

### 5.2 — Target structure

The README should follow this order (top to bottom):

1. **Title + badges + one-sentence tagline** — what this is, in one line.
2. **`[WRITE: motivation]` — 3–4 sentences in Enzo's voice.** Why he built this — Finance background at Goethe, interest in data engineering, what specifically about financial market data is interesting. Personal but not informal.
3. **A screenshot or GIF above the fold.** The single most compelling visual goes here — probably the Airflow DAG overview or the indicators query result. Whichever shows the work running.
4. **Architecture diagram** (already exists, good as is).
5. **Tech stack table** (already exists).
6. **Quick start** — the shortest possible path to "I ran this and it worked." Three commands max for the Docker path.
7. **Detailed setup** — current "Setup" section, both paths.
8. **What it does** — current "Indicators Computed", "Data Sources", "Database Schema" sections.
9. **Running the pipeline** — current "Running with Airflow" section.
10. **Sample output** — current screenshots section.
11. **`[WRITE: limitations]` — honest section.** What this project does NOT do, on purpose. Example bullets to expand: "No real-time data — daily granularity only.", "No backtesting framework — this loads data, it doesn't trade.", "Single-machine deployment — no horizontal scaling.", "FRED API rate limits not handled aggressively." Stating limitations signals engineering maturity more than feature lists do.
12. **Roadmap** — current "Future Extensions" content, restructured into a checklist with status:
    - [x] Apache Airflow orchestration
    - [ ] Azure deployment (Container Apps or App Service + PostgreSQL Flexible Server + Blob Storage archive)
    - [ ] Infrastructure-as-code (Bicep or Terraform — decided in Phase 6)
    - [ ] Streamlit dashboard for visualisation
    - [ ] dbt for transformation layer
    - [ ] Additional data sources (earnings, news sentiment)
13. **Design decisions** (already exists, good as is — it's a strong section).
14. **License**

### 5.3 — Specific edits

- Replace the current first sentence (`"A portfolio project demonstrating data engineering skills..."`) with the placeholder `[WRITE: motivation]` so Enzo replaces it with his own opening. The current sentence is generic.
- Move "Sample Output" so the most impressive image appears in the top-of-readme placeholder slot in section 3. The query-result screenshots can stay in their current section.
- The "Useful Queries" SQL section is excellent and should stay — but consider whether to collapse it into a `<details>` block to reduce scroll length.
- Add a "Citing this project" section ONLY if the user wants one — most portfolio projects don't need it.

### 5.4 — Voice guidance for the `[WRITE: ...]` sections (for Enzo, not Claude Code)

When you replace the placeholders, aim for these qualities:

- **First person, present tense.** "I built this because..." not "This project was built to..."
- **Specific, not generic.** "Studying finance at Goethe, I wanted to see what professional-quality financial data infrastructure actually looks like" beats "A passion for data engineering led me to..."
- **Concrete over abstract.** "Pulls 6 tickers and 5 FRED macro series daily" beats "Comprehensive market data coverage."
- **One real opinion.** Pick one design decision you'd defend in an interview and put it in the motivation paragraph. Example: "I chose Airflow over cron because production-style scheduling — retries, alerting, backfills — is a skill I want to demonstrate, not because cron couldn't do this."

### 5.5 — Verification before opening PR

- The README renders correctly on GitHub (preview before committing).
- All image links resolve (no broken `assets/...` paths).
- All command examples are runnable as written.
- All placeholder `[WRITE: ...]` markers are clearly visible — these are deliberately left for Enzo.

Open PR titled: `docs: restructure README for portfolio presentation`.

No new tag for this phase unless the changes are substantial.

---

## What is NOT in this document (Phase 6 onwards)

The following are intentionally deferred to later sessions. Do not start them as part of Phases 3–5 above.

- **Azure deployment** — host the Airflow stack on Azure (Container Apps, App Service for Containers, or AKS — to be decided), with Azure Database for PostgreSQL Flexible Server as the warehouse, Blob Storage for a raw-data archive, deployed from GitHub Actions, provisioned via Bicep or Terraform. This is the single largest remaining task and deserves a dedicated planning session. The student-account credits make several architectures viable that the AWS Free Tier would have ruled out; the Phase 6 plan will pick one based on cost, ops complexity, and what best showcases data-engineering skills.
- **dbt transformation layer** — would replace `src/transform.py` with SQL-based transformations and a proper data model (staging → marts). Strong portfolio signal but adds complexity.
- **Streamlit dashboard** — visualisation layer on top of the warehouse.
- **Data quality assertions** — Great Expectations or dbt tests. Worth doing once dbt lands.
- **Slack/Discord webhook alerts** — replacement for the placeholder SMTP email setup. Cheap to add but better grouped with the AWS deployment work.

---

## Reference state at end of these phases

After Phases 3, 4, and 5 are complete, the repository should look like this:

- `main` branch with linear history of conventional commits.
- Tags: `v1.0.0`, `v1.0.1`, `v1.1.0` — each with a GitHub Release.
- Closed PRs visible in the PR history: #1 (Airflow merge — already done), #2 (Phase 3), #3 (Phase 4), #4 (Phase 5).
- Green CI badge on the README.
- 36+ passing tests.
- MIT license.
- CHANGELOG with three release entries.
- README structured for portfolio presentation with personal voice placeholders filled in.

This is what a hiring reviewer evaluates as "production-quality solo portfolio work" — not because any single piece is hard, but because the combination signals discipline.
