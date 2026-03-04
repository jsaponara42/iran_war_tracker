import os
import sqlite3
from datetime import date

import pandas as pd
import streamlit as st


DB_PATH = os.getenv("IRAN_WAR_DB_PATH", "data/iran_war_tracker.db")

METRIC_COLUMNS = {
	"iranian_civilians_deaths": "Iranian civilians deaths",
	"us_soldiers_deaths": "US soldiers deaths",
	"us_allied_soldiers_deaths": "US allied soldiers deaths",
	"iranian_soldiers_deaths": "Iranian soldiers deaths",
	"usa_spending_usd": "USA spending (USD)",
}


def get_readonly_connection() -> sqlite3.Connection:
	absolute_path = os.path.abspath(DB_PATH)
	directory = os.path.dirname(absolute_path)
	if directory:
		os.makedirs(directory, exist_ok=True)
	uri = f"file:{absolute_path}?mode=ro"
	return sqlite3.connect(uri, uri=True)


def get_fallback_connection() -> sqlite3.Connection:
	absolute_path = os.path.abspath(DB_PATH)
	directory = os.path.dirname(absolute_path)
	if directory:
		os.makedirs(directory, exist_ok=True)
	return sqlite3.connect(absolute_path)


def read_daily_metrics() -> pd.DataFrame:
	query = """
		SELECT
			date,
			iranian_civilians_deaths,
			us_soldiers_deaths,
			us_allied_soldiers_deaths,
			iranian_soldiers_deaths,
			usa_spending_usd,
			created_at,
			updated_at
		FROM daily_metrics
		ORDER BY date ASC
	"""
	try:
		with get_readonly_connection() as conn:
			return pd.read_sql_query(query, conn)
	except sqlite3.OperationalError:
		with get_fallback_connection() as conn:
			return pd.read_sql_query(query, conn)


def read_source_reputation() -> pd.DataFrame:
	query = """
		SELECT
			metric_name,
			source_title,
			source_url,
			trust_score,
			use_count,
			first_seen_date,
			last_seen_date
		FROM metric_sources
		ORDER BY trust_score DESC, use_count DESC, metric_name ASC
	"""
	try:
		with get_readonly_connection() as conn:
			return pd.read_sql_query(query, conn)
	except sqlite3.OperationalError:
		with get_fallback_connection() as conn:
			return pd.read_sql_query(query, conn)


def read_last_run() -> pd.DataFrame:
	query = """
		SELECT run_date, started_at, completed_at, status, message
		FROM updater_runs
		ORDER BY started_at DESC
		LIMIT 10
	"""
	try:
		with get_readonly_connection() as conn:
			return pd.read_sql_query(query, conn)
	except sqlite3.OperationalError:
		with get_fallback_connection() as conn:
			return pd.read_sql_query(query, conn)


def format_number(value: float | int | None) -> str:
	if value is None:
		return "N/A"
	try:
		numeric = float(value)
	except (TypeError, ValueError):
		return "N/A"
	if numeric >= 1_000_000_000:
		return f"${numeric:,.0f}" if numeric.is_integer() else f"${numeric:,.2f}"
	return f"{numeric:,.0f}" if numeric.is_integer() else f"{numeric:,.2f}"


def render_latest_metrics(metrics_df: pd.DataFrame) -> None:
	latest = metrics_df.iloc[-1]
	st.subheader("Latest daily estimates")
	cols = st.columns(5)

	cols[0].metric(
		"Iranian civilians",
		format_number(latest["iranian_civilians_deaths"]),
	)
	cols[1].metric(
		"US soldiers",
		format_number(latest["us_soldiers_deaths"]),
	)
	cols[2].metric(
		"US allied soldiers",
		format_number(latest["us_allied_soldiers_deaths"]),
	)
	cols[3].metric(
		"Iranian soldiers",
		format_number(latest["iranian_soldiers_deaths"]),
	)
	cols[4].metric(
		"USA spending (USD)",
		format_number(latest["usa_spending_usd"]),
	)

	st.caption(f"As of {latest['date']}")


def render_trend_charts(metrics_df: pd.DataFrame) -> None:
	st.subheader("Trend over time")
	chart_df = metrics_df.copy()
	chart_df["date"] = pd.to_datetime(chart_df["date"])
	chart_df = chart_df.set_index("date")
	chart_df = chart_df[list(METRIC_COLUMNS.keys())].rename(columns=METRIC_COLUMNS)
	st.line_chart(chart_df)


def main() -> None:
	st.set_page_config(page_title="Iran War Tracker", layout="wide")
	st.title("Iran War Tracker")
	st.caption(
		"Public read-only dashboard. Data updates are performed by scheduled jobs, not by website users."
	)

	st.info(
		"Data is model-estimated from web sources and may be incomplete. Always validate against official reporting."
	)

	try:
		metrics_df = read_daily_metrics()
	except Exception as exc:
		st.error(f"Unable to read daily metrics: {exc}")
		st.stop()

	if metrics_df.empty:
		st.warning("No data available yet. Run the daily updater first.")
		st.code("python updater.py", language="bash")
		st.stop()

	render_latest_metrics(metrics_df)
	render_trend_charts(metrics_df)

	st.subheader("Daily metrics table")
	st.dataframe(metrics_df, use_container_width=True)

	st.subheader("Source reputation")
	try:
		sources_df = read_source_reputation()
		if sources_df.empty:
			st.caption("No source history captured yet.")
		else:
			st.dataframe(sources_df, use_container_width=True)
	except Exception as exc:
		st.warning(f"Could not load source reputation table: {exc}")

	st.subheader("Recent updater runs")
	try:
		runs_df = read_last_run()
		if runs_df.empty:
			st.caption("No updater run logs yet.")
		else:
			st.dataframe(runs_df, use_container_width=True)
	except Exception as exc:
		st.warning(f"Could not load updater run logs: {exc}")

	st.divider()
	st.write("Today:", date.today().isoformat())
	st.caption("No write actions are exposed in this app.")


if __name__ == "__main__":
	main()
