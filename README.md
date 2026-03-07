# iran_war_tracker

Public Streamlit dashboard + daily updater for tracking war-related metrics for Iran conflict coverage (starting late February 2026).

## What this project tracks

The updater writes one daily record to SQLite with these cumulative metrics:

- `iranian_civilians_deaths`
- `us_soldiers_deaths`
- `us_allied_soldiers_deaths`
- `iranian_soldiers_deaths`
- `usa_spending_usd`
- `schools_hospitals_destroyed`
- `countries_involved`

For each metric, the updater uses OpenAI Responses API with the built-in web search tool and provides the target date in the prompt. It also stores source metadata and reputation signals.

## Security model

- Streamlit app is read-only (no write endpoints or update actions).
- Only `updater.py` writes to the database.
- API key is read from `OPENAI_API_KEY` environment variable.
- No secrets are stored in source control.

## Daily run guard (max once/day)

`updater.py` enforces one successful run per day by checking `updater_runs`. If a success already exists for a date, it skips.

- Normal run: `python updater.py`
- Force re-run for same day: `python updater.py --force`
- Backfill a specific date: `python updater.py --date 2026-03-03`

## Source reputation system

Table: `metric_sources`

- Tracks `source_url`, `source_title`, `use_count`, `trust_score`, first/last seen dates.
- Repeated usage of a source for a metric increases frequency and updates blended trust score.

## Monotonic safeguards

All tracked values are treated as cumulative and are forced not to decrease versus prior day.

- If model returns lower value than previous day, previous day value is kept.
- If model returns `null`, previous value is carried forward.

## Files

- `iran_war_tracker.py` — Streamlit dashboard (latest KPIs, trends, source table, run history)
- `updater.py` — scheduled updater, OpenAI calls, SQLite writes, reputation tracking
- `.github/workflows/daily_update.yml` — runs updater once/day and can commit DB updates
- `requirements.txt` — Python dependencies

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Set environment variables:

```bash
export OPENAI_API_KEY="your_key_here"
export IRAN_WAR_DB_PATH="data/iran_war_tracker.db"
```

Windows PowerShell:

```powershell
$env:OPENAI_API_KEY="your_key_here"
$env:IRAN_WAR_DB_PATH="data/iran_war_tracker.db"
```

## Run locally

1) Run updater once to generate data:

```bash
python updater.py
```

If you want dashboard data immediately without calling the API, seed demo data:

```bash
python seed_data.py
```

2) Start dashboard:

```bash
streamlit run iran_war_tracker.py
```

## GitHub Actions schedule

Workflow file already included at `.github/workflows/daily_update.yml`.

Required repo secret:

- `OPENAI_API_KEY`

The workflow runs daily and can commit `data/iran_war_tracker.db` changes back to `main`.

## Debug logging

- Local updater runs write logs to `logs/` by default.
- Override log directory with `IRAN_WAR_LOG_DIR`.
- GitHub Actions uploads `logs/` as an artifact named `updater-logs-<run_id>` on every run (success or failure).

## Notes

- Model-derived numbers can be uncertain; review source citations regularly.
- For production hardening, consider moving from SQLite to managed Postgres and storing immutable source snapshots.
