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
MEAL_COST_USD = 3.5
CLASSROOM_COST_USD = 250000
TRAUMA_CENTER_COST_USD = 15000000
SCHOLARSHIP_COST_USD = 40000
NURSE_SALARY_USD = 85000

METRIC_COLUMNS = {
	"iranian_civilians_deaths": "Iranian civilians deaths",
	"us_soldiers_deaths": "US soldiers deaths",
	"us_allied_soldiers_deaths": "US allied soldiers deaths",
	"iranian_soldiers_deaths": "Iranian soldiers deaths",
	"usa_spending_usd": "USA spending (USD)",
	"schools_hospitals_destroyed": "Schools & hospitals destroyed",
	"countries_involved": "Countries involved",
	"civilian_displacement_total": "Civilian displacement",
	"journalist_casualties": "Journalist casualties",
	"children_out_of_school": "Children out of school",
	"ceasefire_attempts": "Ceasefire attempts",
	"escalation_events": "Escalation events",
	"humanitarian_access_incidents": "Humanitarian access incidents",
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
			civilian_displacement_total,
			journalist_casualties,
			children_out_of_school,
			ceasefire_attempts,
			escalation_events,
			humanitarian_access_incidents,
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

	for missing_column in [
		"schools_hospitals_destroyed",
		"countries_involved",
		"civilian_displacement_total",
		"journalist_casualties",
		"children_out_of_school",
		"ceasefire_attempts",
		"escalation_events",
		"humanitarian_access_incidents",
	]:
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


def to_int_or_zero(value: float | int | None) -> int:
	if value is None or pd.isna(value):
		return 0
	try:
		return int(float(value))
	except (TypeError, ValueError):
		return 0


def render_opportunity_cost(latest: pd.Series) -> None:
	spending = float(latest.get("usa_spending_usd") or 0)
	meals = int(spending / MEAL_COST_USD)
	classrooms = int(spending / CLASSROOM_COST_USD)
	trauma_centers = int(spending / TRAUMA_CENTER_COST_USD)
	scholarships = int(spending / SCHOLARSHIP_COST_USD)
	nurse_years = int(spending / NURSE_SALARY_USD)

	st.subheader("💸 Opportunity Cost of the Day")
	st.caption("Satirical framing, real arithmetic.")

	cols = st.columns(5)
	cols[0].metric("🍽️ School meals", f"{meals:,}")
	cols[1].metric("🏫 Classrooms funded", f"{classrooms:,}")
	cols[2].metric("🏥 Trauma centers", f"{trauma_centers:,}")
	cols[3].metric("🎓 Scholarships", f"{scholarships:,}")
	cols[4].metric("🩺 Nurse salary-years", f"{nurse_years:,}")

	st.info(
		f"Counterfactual ticker: ${spending:,.0f} in conflict spending equals roughly "
		f"{meals:,} school meals or {scholarships:,} scholarships."
	)


def render_diplomacy_scoreboard(latest: pd.Series) -> None:
	ceasefires = to_int_or_zero(latest.get("ceasefire_attempts"))
	escalations = to_int_or_zero(latest.get("escalation_events"))
	access_incidents = to_int_or_zero(latest.get("humanitarian_access_incidents"))

	st.subheader("🕊️ Diplomacy Scoreboard")
	cols = st.columns(3)
	cols[0].metric("Ceasefire attempts", f"{ceasefires:,}")
	cols[1].metric("Escalation events", f"{escalations:,}")
	cols[2].metric("Access denials/incidents", f"{access_incidents:,}")

	if escalations > ceasefires:
		st.warning("Spin detector: escalation outpaces diplomacy. Strategic messaging may exceed strategic results.")
	else:
		st.success("Spin detector: diplomacy signals are keeping pace with escalation signals.")


def render_humanitarian_impact(latest: pd.Series) -> None:
	st.subheader("🧯 Humanitarian Impact")
	cols = st.columns(4)
	cols[0].metric("🧳 Displaced civilians", format_number(latest.get("civilian_displacement_total")))
	cols[1].metric("🧒 Children out of school", format_number(latest.get("children_out_of_school")))
	cols[2].metric("📰 Journalist casualties", format_number(latest.get("journalist_casualties")))
	cols[3].metric("🏫🏥 Schools & hospitals destroyed", format_number(latest.get("schools_hospitals_destroyed")))


def render_methodology() -> None:
	with st.expander("🔎 Methodology and reliability notes"):
		st.markdown(
			"""
- Data is model-assisted and source-linked; treat as estimates, not official counts.
- Cumulative metrics are monotonic and never decrease once recorded.
- Updater prioritizes recency and verification, then selects highest verifiable cumulative values.
- Opportunity-cost cards are arithmetic transformations of USA spending with fixed assumptions.
"""
		)


def render_header(today: date) -> None:
	days_at_war = get_days_at_war(today)
	top_cols = st.columns(2)
	top_cols[0].metric("Days at War", f"{days_at_war}")
	top_cols[1].caption(f"War start date: {WAR_START_DATE.isoformat()}")

	with st.container(border=True):
		st.subheader("Number of Hearts and Minds Won")
		st.markdown("## 0")
		st.caption("War ROI dashboard: still waiting for positive externalities.")


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
		.stApp {
			background: radial-gradient(circle at top right, rgba(255, 210, 120, 0.08), transparent 35%),
			            radial-gradient(circle at top left, rgba(180, 220, 255, 0.08), transparent 40%);
		}
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
		"Data is model-estimated from web sources and may be incomplete. Mostly just to troll Danny."
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
	render_humanitarian_impact(metrics_df.iloc[-1])
	render_diplomacy_scoreboard(metrics_df.iloc[-1])
	render_opportunity_cost(metrics_df.iloc[-1])

	st.subheader("🧾 Daily metrics table")
	st.dataframe(metrics_df, width="stretch")

	st.subheader("🕵️ Source reputation")
	try:
		sources_df = read_source_reputation()
		if sources_df.empty:
			st.caption("No source history captured yet.")
		else:
			sources_df = add_source_freshness_columns(sources_df, today)
			staleness_count = (sources_df["source_freshness"].isin(["aging", "stale"])) .sum()
			if staleness_count > 0:
				st.warning(f"Staleness alarm: {int(staleness_count)} tracked source records are aging or stale.")
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
	render_methodology()
	st.write("Today:", today.isoformat())
	st.caption("No write actions are exposed in this app.")


if __name__ == "__main__":
	main()
