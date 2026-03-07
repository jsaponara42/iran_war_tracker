import os
import sqlite3
from datetime import date

import pandas as pd
import streamlit as st


def resolve_db_path() -> str:
	raw = os.getenv("IRAN_WAR_DB_PATH", "data/iran_war_tracker.db")
	cleaned = raw.strip().strip("\"").strip("'")
	return cleaned or "data/iran_war_tracker.db"


DB_PATH = resolve_db_path()
WAR_START_DATE = date(2026, 2, 28)

METRIC_COLUMNS = {
	"iranian_civilians_deaths": "Iranian civilians deaths",
	"us_soldiers_deaths": "US soldiers deaths",
	"us_allied_soldiers_deaths": "US allied soldiers deaths",
	"iranian_soldiers_deaths": "Iranian soldiers deaths",
	"usa_spending_usd": "USA spending (USD)",
	"schools_hospitals_destroyed": "Schools & hospitals destroyed",
	"countries_involved": "Countries involved",
}


def get_readonly_connection() -> sqlite3.Connection:
	absolute_path = os.path.abspath(DB_PATH)
	directory = os.path.dirname(absolute_path)
	if directory:
		os.makedirs(directory, exist_ok=True)
	uri = f"file:{absolute_path}?mode=ro"
	return sqlite3.connect(uri, uri=True)


def read_daily_metrics() -> pd.DataFrame:
	query = """
		SELECT
			date,
			iranian_civilians_deaths,
			us_soldiers_deaths,
			us_allied_soldiers_deaths,
			iranian_soldiers_deaths,
			usa_spending_usd,
			schools_hospitals_destroyed,
			countries_involved,
			created_at,
			updated_at
		FROM daily_metrics
		ORDER BY date ASC
	"""
	legacy_query = """
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
	with get_readonly_connection() as conn:
		try:
			df = pd.read_sql_query(query, conn)
		except sqlite3.OperationalError:
			df = pd.read_sql_query(legacy_query, conn)

	for missing_column in ["schools_hospitals_destroyed", "countries_involved"]:
		if missing_column not in df.columns:
			df[missing_column] = None

	return df


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
	with get_readonly_connection() as conn:
		return pd.read_sql_query(query, conn)


def read_last_run() -> pd.DataFrame:
	query = """
		SELECT run_date, started_at, completed_at, status, message
		FROM updater_runs
		ORDER BY started_at DESC
		LIMIT 10
	"""
	with get_readonly_connection() as conn:
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


def get_days_at_war(today: date) -> int:
	if today < WAR_START_DATE:
		return 0
	return (today - WAR_START_DATE).days + 1


def get_freshness_label(days: float | int | None) -> str:
	if days is None or pd.isna(days):
		return "unknown"
	if days <= 1:
		return "fresh"
	if days <= 3:
		return "recent"
	if days <= 7:
		return "aging"
	return "stale"


def add_source_freshness_columns(sources_df: pd.DataFrame, today: date) -> pd.DataFrame:
	if sources_df.empty:
		return sources_df

	formatted_df = sources_df.copy()
	last_seen_ts = pd.to_datetime(formatted_df["last_seen_date"], errors="coerce")
	today_ts = pd.Timestamp(today)
	formatted_df["source_freshness_days"] = (today_ts - last_seen_ts).dt.days
	formatted_df["source_freshness"] = formatted_df["source_freshness_days"].apply(get_freshness_label)
	formatted_df["last_seen_date"] = last_seen_ts.dt.strftime("%Y-%m-%d").fillna("unknown")
	return formatted_df


def render_header(today: date) -> None:
	days_at_war = get_days_at_war(today)
	top_cols = st.columns(2)
	top_cols[0].metric("Days at War", f"{days_at_war}")
	top_cols[1].caption(f"War start date: {WAR_START_DATE.isoformat()}")

	with st.container(border=True):
		st.subheader("Number of Hearts and Minds Won")
		st.markdown("## 0")


def render_latest_metrics(metrics_df: pd.DataFrame) -> None:
	latest = metrics_df.iloc[-1]
	st.subheader("📊 Latest daily estimates")
	cols = st.columns(4)

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

	st.metric(
		"💵 USA spending (USD)",
		format_number(latest["usa_spending_usd"]),
	)

	extra_cols = st.columns(2)
	extra_cols[0].metric(
		"🏫🏥 Schools & hospitals destroyed",
		format_number(latest["schools_hospitals_destroyed"]),
	)
	extra_cols[1].metric(
		"🌍 Countries involved",
		format_number(latest["countries_involved"]),
	)

	st.caption(f"As of {latest['date']}")


def render_trend_charts(metrics_df: pd.DataFrame) -> None:
	st.subheader("📈 Trend over time")
	chart_df = metrics_df.copy()
	chart_df["date"] = pd.to_datetime(chart_df["date"])
	chart_df = chart_df.set_index("date")
	chart_columns = [
		"iranian_civilians_deaths",
		"us_soldiers_deaths",
		"us_allied_soldiers_deaths",
		"iranian_soldiers_deaths",
	]
	chart_df = chart_df[chart_columns].rename(columns=METRIC_COLUMNS)
	st.line_chart(chart_df)


def main() -> None:
	st.set_page_config(page_title="Iran War Tracker", layout="wide")
	st.markdown(
		"""
		<style>
		div[data-testid="stMetric"] {
			border: 1px solid color-mix(in srgb, var(--text-color) 15%, transparent);
			border-radius: 10px;
			padding: 0.6rem;
			background: color-mix(in srgb, var(--background-color) 90%, var(--primary-color) 10%);
		}
		</style>
		""",
		unsafe_allow_html=True,
	)
	st.title("🇮🇷 Iran War Tracker")

	today = date.today()
	render_header(today)

	st.caption(
		"Public read-only dashboard. Data updates are performed by scheduled jobs, not by website users."
	)

	st.info(
		"Data is model-estimated from web sources and may be incomplete. And mostly meant to troll Danny."
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

	st.subheader("🧾 Daily metrics table")
	st.dataframe(metrics_df, width="stretch")

	st.subheader("🕵️ Source reputation")
	try:
		sources_df = read_source_reputation()
		if sources_df.empty:
			st.caption("No source history captured yet.")
		else:
			sources_df = add_source_freshness_columns(sources_df, today)
			st.dataframe(sources_df, width="stretch")
	except Exception as exc:
		st.warning(f"Could not load source reputation table: {exc}")

	st.subheader("🛠️ Recent updater runs")
	try:
		runs_df = read_last_run()
		if runs_df.empty:
			st.caption("No updater run logs yet.")
		else:
			st.dataframe(runs_df, width="stretch")
	except Exception as exc:
		st.warning(f"Could not load updater run logs: {exc}")

	st.divider()
	st.write("Today:", today.isoformat())
	st.caption("No write actions are exposed in this app.")


if __name__ == "__main__":
	main()
