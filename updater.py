import argparse
import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from urllib.parse import urlparse
from typing import Any

from openai import OpenAI


def resolve_db_path() -> str:
    raw = os.getenv("IRAN_WAR_DB_PATH", "data/iran_war_tracker.db")
    cleaned = raw.strip().strip('"').strip("'")
    return cleaned or "data/iran_war_tracker.db"


DB_PATH = resolve_db_path()
MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1")
MAX_REQUESTS = int(os.getenv("UPDATE_MAX_REQUESTS", "20"))
LOG_DIR = os.getenv("IRAN_WAR_LOG_DIR", "logs")

METRIC_SEARCH_HINTS = {
    "iranian_civilians_deaths": (
        "Focus on same-day casualty reports for Iranian civilians from reputable international wire services,"
        " official humanitarian updates, and major outlets with timestamped reporting."
    ),
    "us_soldiers_deaths": (
        "Focus on same-day reporting from U.S. Department of Defense announcements, major wire services,"
        " and other official military/public briefings."
    ),
    "us_allied_soldiers_deaths": (
        "Focus on same-day reporting from allied defense ministries, NATO communications, and major wire services."
    ),
    "iranian_soldiers_deaths": (
        "Focus on same-day military casualty reporting from reputable news wires and official statements."
    ),
    "usa_spending_usd": (
        "Focus on same-day reported cumulative U.S. spending estimates from official U.S. sources and trusted finance/government reporting."
    ),
    "schools_hospitals_destroyed": (
        "Focus on cumulative counts of schools and hospitals destroyed, damaged beyond use, or rendered non-operational."
        " Prefer reputable humanitarian, UN, health, and education reporting plus major wire services."
    ),
    "countries_involved": (
        "Focus on total number of countries directly involved (military, active combat, or direct operational support)."
        " Prefer clear lists from reputable reporting and official statements."
    ),
    "civilian_displacement_total": (
        "Focus on total people displaced by the conflict to date."
        " Prioritize UN/OCHA/UNHCR and major wire reporting with clear cumulative estimates."
    ),
    "journalist_casualties": (
        "Focus on total journalists killed or severely injured in relation to the conflict."
        " Prioritize press freedom organizations and major verified reporting."
    ),
    "children_out_of_school": (
        "Focus on cumulative count of children whose schooling is disrupted or out of school due to conflict impacts."
        " Prioritize UNICEF/UNESCO/education cluster and major verified reporting."
    ),
    "ceasefire_attempts": (
        "Focus on total count of documented ceasefire proposals, talks, or formal attempts to date."
    ),
    "escalation_events": (
        "Focus on cumulative count of major escalation events (major strikes, new offensives, notable expansion in military scope)."
    ),
    "humanitarian_access_incidents": (
        "Focus on cumulative count of reported humanitarian access denials/incidents."
        " Prioritize OCHA/aid organizations and major verified reporting."
    ),
}


@dataclass
class MetricResult:
    metric_name: str
    value: float | None
    confidence: str
    rationale: str
    source_title: str | None
    source_url: str | None
    source_date: str | None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def configure_logging(target_date: str) -> tuple[logging.Logger, str]:
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_date = target_date.replace(":", "-")
    log_path = os.path.join(LOG_DIR, f"updater_{safe_date}_{timestamp}.log")

    logger = logging.getLogger("iran_war_updater")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.info("Logging initialized. log_path=%s", log_path)
    return logger, log_path


def ensure_db_dir() -> None:
    absolute_path = os.path.abspath(DB_PATH)
    directory = os.path.dirname(absolute_path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS daily_metrics (
            date TEXT PRIMARY KEY,
            iranian_civilians_deaths REAL,
            us_soldiers_deaths REAL,
            us_allied_soldiers_deaths REAL,
            iranian_soldiers_deaths REAL,
            usa_spending_usd REAL,
            schools_hospitals_destroyed REAL,
            countries_involved REAL,
            civilian_displacement_total REAL,
            journalist_casualties REAL,
            children_out_of_school REAL,
            ceasefire_attempts REAL,
            escalation_events REAL,
            humanitarian_access_incidents REAL,
            details_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metric_sources (
            metric_name TEXT NOT NULL,
            source_url TEXT NOT NULL,
            source_title TEXT,
            trust_score REAL NOT NULL DEFAULT 0,
            use_count INTEGER NOT NULL DEFAULT 0,
            first_seen_date TEXT NOT NULL,
            last_seen_date TEXT NOT NULL,
            PRIMARY KEY(metric_name, source_url)
        );

        CREATE TABLE IF NOT EXISTS updater_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL,
            message TEXT
        );
        """
    )

    existing_columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(daily_metrics)").fetchall()
    }
    required_columns: dict[str, str] = {
        "schools_hospitals_destroyed": "REAL",
        "countries_involved": "REAL",
        "civilian_displacement_total": "REAL",
        "journalist_casualties": "REAL",
        "children_out_of_school": "REAL",
        "ceasefire_attempts": "REAL",
        "escalation_events": "REAL",
        "humanitarian_access_incidents": "REAL",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            conn.execute(f"ALTER TABLE daily_metrics ADD COLUMN {column_name} {column_type}")

    conn.commit()


def get_previous_metrics(
    conn: sqlite3.Connection,
    target_date: str,
) -> dict[str, float | None]:
    row = conn.execute(
        """
        SELECT
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
            humanitarian_access_incidents
        FROM daily_metrics
        WHERE date < ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (target_date,),
    ).fetchone()

    if not row:
        return {
            "iranian_civilians_deaths": None,
            "us_soldiers_deaths": None,
            "us_allied_soldiers_deaths": None,
            "iranian_soldiers_deaths": None,
            "usa_spending_usd": None,
            "schools_hospitals_destroyed": None,
            "countries_involved": None,
            "civilian_displacement_total": None,
            "journalist_casualties": None,
            "children_out_of_school": None,
            "ceasefire_attempts": None,
            "escalation_events": None,
            "humanitarian_access_incidents": None,
        }

    return dict(row)


def already_ran_today(conn: sqlite3.Connection, target_date: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM updater_runs
        WHERE run_date = ? AND status = 'success'
        LIMIT 1
        """,
        (target_date,),
    ).fetchone()
    return row is not None


def start_run(conn: sqlite3.Connection, target_date: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO updater_runs (run_date, started_at, status, message)
        VALUES (?, ?, 'running', ?)
        """,
        (target_date, utc_now_iso(), "started"),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str, message: str) -> None:
    conn.execute(
        """
        UPDATE updater_runs
        SET completed_at = ?, status = ?, message = ?
        WHERE id = ?
        """,
        (utc_now_iso(), status, message, run_id),
    )
    conn.commit()


def _extract_citations(response: Any) -> list[dict[str, str]]:
    citations: list[dict[str, str]] = []
    try:
        output = getattr(response, "output", None) or []
        for item in output:
            content = getattr(item, "content", None) or []
            for part in content:
                annotations = getattr(part, "annotations", None) or []
                for ann in annotations:
                    ann_type = getattr(ann, "type", None)
                    if ann_type == "url_citation":
                        url = getattr(ann, "url", None)
                        title = getattr(ann, "title", None)
                        if url:
                            citations.append(
                                {
                                    "url": str(url),
                                    "title": str(title) if title else "",
                                }
                            )
    except Exception:
        return []
    return citations


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_preferred_domains(
    conn: sqlite3.Connection,
    metric_name: str,
    limit: int = 5,
) -> list[str]:
    rows = conn.execute(
        """
        SELECT source_url
        FROM metric_sources
        WHERE metric_name = ?
        ORDER BY trust_score DESC, use_count DESC
        LIMIT ?
        """,
        (metric_name, limit),
    ).fetchall()

    domains: list[str] = []
    for row in rows:
        parsed = urlparse(row["source_url"])
        domain = parsed.netloc.strip().lower()
        if domain and domain not in domains:
            domains.append(domain)
    return domains


def call_openai_for_metric(
    conn: sqlite3.Connection,
    client: OpenAI,
    metric_name: str,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    metric_hint = METRIC_SEARCH_HINTS.get(metric_name, "Use reputable same-day sources.")

    def fetch_payload(attempt: int) -> tuple[dict[str, Any], list[dict[str, str]], str]:
        attempt_note = (
            "First attempt." if attempt == 1 else "Retry attempt because prior result lacked sufficient evidence."
        )

        prompt = f"""
You are collecting a single metric for a public war-tracking dataset.
Metric name: {metric_name}
Target date: {target_date}
Metric guidance: {metric_hint}
Attempt context: {attempt_note}

Requirements:
1) Use web search and consider sources published on or before {target_date}.
2) Prioritize recency and verification; do not over-prioritize historical trust ranking if newer verified data exists.
3) Identify multiple plausible cumulative values and select the highest verifiable estimate.
4) Cross-check at least 2 sources when available.
5) Return only a JSON object with keys:
   - value_number (number or null)
   - confidence (low|medium|high)
   - rationale (string)
   - source_title (string or null)
   - source_url (string or null)
   - source_date (YYYY-MM-DD or null)
6) For cumulative metrics, values should not decrease over time.
   Previous stored value: {previous_value}
7) No markdown.
""".strip()

        response = client.responses.create(
            model=MODEL,
            input=prompt,
            tools=[{"type": "web_search_preview"}],
            temperature=0,
            max_output_tokens=500,
        )

        text = response.output_text
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = {
                "value_number": None,
                "confidence": "low",
                "rationale": f"Model did not return valid JSON. Raw output: {text[:500]}",
                "source_title": None,
                "source_url": None,
                "source_date": None,
            }

        citations = _extract_citations(response)
        return payload, citations, text

    payload, citations, _ = fetch_payload(attempt=1)
    weak_evidence = (not payload.get("source_url")) and not citations
    if weak_evidence:
        payload, citations, _ = fetch_payload(attempt=2)

    source_url = payload.get("source_url")
    source_title = payload.get("source_title")

    if (not source_url or not source_title) and citations:
        source_url = source_url or citations[0].get("url")
        source_title = source_title or citations[0].get("title")

    result = MetricResult(
        metric_name=metric_name,
        value=_safe_float(payload.get("value_number")),
        confidence=str(payload.get("confidence") or "low"),
        rationale=str(payload.get("rationale") or "No rationale provided."),
        source_title=str(source_title) if source_title else None,
        source_url=str(source_url) if source_url else None,
        source_date=str(payload.get("source_date")) if payload.get("source_date") else None,
    )

    if result.source_date and result.source_date > target_date:
        result.value = None
        result.rationale = (
            f"Discarded future-dated source ({result.source_date}). "
            f"Target date is {target_date}. "
            f"Original rationale: {result.rationale}"
        )

    return result


def fetch_iranian_civilians_deaths(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="iranian_civilians_deaths",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_us_soldiers_deaths(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="us_soldiers_deaths",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_us_allied_soldiers_deaths(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="us_allied_soldiers_deaths",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_iranian_soldiers_deaths(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="iranian_soldiers_deaths",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_usa_spending_usd(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="usa_spending_usd",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_schools_hospitals_destroyed(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="schools_hospitals_destroyed",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_countries_involved(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="countries_involved",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_civilian_displacement_total(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="civilian_displacement_total",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_journalist_casualties(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="journalist_casualties",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_children_out_of_school(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="children_out_of_school",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_ceasefire_attempts(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="ceasefire_attempts",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_escalation_events(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="escalation_events",
        target_date=target_date,
        previous_value=previous_value,
    )


def fetch_humanitarian_access_incidents(
    conn: sqlite3.Connection,
    client: OpenAI,
    target_date: str,
    previous_value: float | None,
) -> MetricResult:
    return call_openai_for_metric(
        conn=conn,
        client=client,
        metric_name="humanitarian_access_incidents",
        target_date=target_date,
        previous_value=previous_value,
    )


def apply_monotonic_rule(new_value: float | None, previous_value: float | None) -> float | None:
    if new_value is None:
        return previous_value
    if previous_value is None:
        return new_value
    return max(new_value, previous_value)


def upsert_source_reputation(
    conn: sqlite3.Connection,
    target_date: str,
    result: MetricResult,
) -> None:
    if not result.source_url:
        return

    confidence_score = {
        "low": 0.4,
        "medium": 0.7,
        "high": 1.0,
    }.get(result.confidence.lower(), 0.4)

    existing = conn.execute(
        """
        SELECT trust_score, use_count
        FROM metric_sources
        WHERE metric_name = ? AND source_url = ?
        """,
        (result.metric_name, result.source_url),
    ).fetchone()

    if existing:
        new_use_count = int(existing["use_count"]) + 1
        blended_score = round((float(existing["trust_score"]) * 0.8) + (confidence_score * 0.2), 4)
        conn.execute(
            """
            UPDATE metric_sources
            SET source_title = ?,
                trust_score = ?,
                use_count = ?,
                last_seen_date = ?
            WHERE metric_name = ? AND source_url = ?
            """,
            (
                result.source_title,
                blended_score,
                new_use_count,
                target_date,
                result.metric_name,
                result.source_url,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO metric_sources (
                metric_name,
                source_url,
                source_title,
                trust_score,
                use_count,
                first_seen_date,
                last_seen_date
            ) VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (
                result.metric_name,
                result.source_url,
                result.source_title,
                confidence_score,
                target_date,
                target_date,
            ),
        )


def persist_daily_metrics(
    conn: sqlite3.Connection,
    target_date: str,
    values: dict[str, float | None],
    details: dict[str, dict[str, Any]],
) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO daily_metrics (
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
            details_json,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            iranian_civilians_deaths = excluded.iranian_civilians_deaths,
            us_soldiers_deaths = excluded.us_soldiers_deaths,
            us_allied_soldiers_deaths = excluded.us_allied_soldiers_deaths,
            iranian_soldiers_deaths = excluded.iranian_soldiers_deaths,
            usa_spending_usd = excluded.usa_spending_usd,
            schools_hospitals_destroyed = excluded.schools_hospitals_destroyed,
            countries_involved = excluded.countries_involved,
            civilian_displacement_total = excluded.civilian_displacement_total,
            journalist_casualties = excluded.journalist_casualties,
            children_out_of_school = excluded.children_out_of_school,
            ceasefire_attempts = excluded.ceasefire_attempts,
            escalation_events = excluded.escalation_events,
            humanitarian_access_incidents = excluded.humanitarian_access_incidents,
            details_json = excluded.details_json,
            updated_at = excluded.updated_at
        """,
        (
            target_date,
            values["iranian_civilians_deaths"],
            values["us_soldiers_deaths"],
            values["us_allied_soldiers_deaths"],
            values["iranian_soldiers_deaths"],
            values["usa_spending_usd"],
            values["schools_hospitals_destroyed"],
            values["countries_involved"],
            values["civilian_displacement_total"],
            values["journalist_casualties"],
            values["children_out_of_school"],
            values["ceasefire_attempts"],
            values["escalation_events"],
            values["humanitarian_access_incidents"],
            json.dumps(details),
            now,
            now,
        ),
    )
    conn.commit()


def run_update(target_date: str, force: bool = False) -> None:
    logger, log_path = configure_logging(target_date)
    logger.info(
        "Updater start target_date=%s force=%s model=%s db_path=%s",
        target_date,
        force,
        MODEL,
        DB_PATH,
    )

    if not os.getenv("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY is not set.")
        raise RuntimeError("OPENAI_API_KEY is not set.")

    with get_conn() as conn:
        initialize_schema(conn)
        logger.info("Database schema ensured.")

        if not force and already_ran_today(conn, target_date):
            logger.info("Already ran successfully for %s. Skipping.", target_date)
            print(f"Already ran successfully for {target_date}. Skipping.")
            return

        run_id = start_run(conn, target_date)
        logger.info("Updater run record created run_id=%s", run_id)
        try:
            client = OpenAI()
            previous = get_previous_metrics(conn, target_date)
            logger.info("Loaded previous metrics snapshot: %s", previous)

            fetchers = [
                fetch_iranian_civilians_deaths,
                fetch_us_soldiers_deaths,
                fetch_us_allied_soldiers_deaths,
                fetch_iranian_soldiers_deaths,
                fetch_usa_spending_usd,
                fetch_schools_hospitals_destroyed,
                fetch_countries_involved,
                fetch_civilian_displacement_total,
                fetch_journalist_casualties,
                fetch_children_out_of_school,
                fetch_ceasefire_attempts,
                fetch_escalation_events,
                fetch_humanitarian_access_incidents,
            ]

            if len(fetchers) > MAX_REQUESTS:
                raise RuntimeError(
                    f"Configured MAX_REQUESTS={MAX_REQUESTS}, but {len(fetchers)} metric calls are required."
                )

            results: list[MetricResult] = []
            for fetch_fn in fetchers:
                metric_name = fetch_fn.__name__.replace("fetch_", "")
                logger.info(
                    "Fetching metric=%s previous_value=%s",
                    metric_name,
                    previous.get(metric_name),
                )
                result = fetch_fn(conn, client, target_date, previous.get(metric_name))
                results.append(result)
                logger.info(
                    "Fetched metric=%s raw_value=%s confidence=%s source_date=%s source_url=%s",
                    result.metric_name,
                    result.value,
                    result.confidence,
                    result.source_date,
                    result.source_url,
                )

            values: dict[str, float | None] = {}
            details: dict[str, dict[str, Any]] = {}

            for result in results:
                previous_value = previous.get(result.metric_name)
                monotonic_value = apply_monotonic_rule(result.value, previous_value)
                values[result.metric_name] = monotonic_value
                logger.info(
                    "Finalized metric=%s final_value=%s previous_value=%s",
                    result.metric_name,
                    monotonic_value,
                    previous_value,
                )
                details[result.metric_name] = {
                    "raw_model_value": result.value,
                    "final_value": monotonic_value,
                    "previous_value": previous_value,
                    "confidence": result.confidence,
                    "rationale": result.rationale,
                    "source_title": result.source_title,
                    "source_url": result.source_url,
                    "source_date": result.source_date,
                }
                upsert_source_reputation(conn, target_date, result)

            persist_daily_metrics(conn, target_date, values, details)
            finish_run(conn, run_id, "success", "Update completed.")
            logger.info("Persisted daily metrics and marked run successful.")
            print(f"Update completed successfully for {target_date}.")

        except Exception as exc:
            finish_run(conn, run_id, "failed", str(exc))
            logger.exception("Updater failed: %s", exc)
            raise
        finally:
            logger.info("Updater finished. log_path=%s", log_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily Iran war tracker updater")
    parser.add_argument(
        "--date",
        dest="target_date",
        default=date.today().isoformat(),
        help="Target date in YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force update even if a successful run already exists for the date.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_update(target_date=args.target_date, force=args.force)