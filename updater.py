import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from urllib.parse import urlparse
from typing import Any

from openai import OpenAI


DB_PATH = os.getenv("IRAN_WAR_DB_PATH", "data/iran_war_tracker.db")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
MAX_REQUESTS = int(os.getenv("UPDATE_MAX_REQUESTS", "10"))


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
            usa_spending_usd
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
    preferred_domains = get_preferred_domains(conn, metric_name)

    prompt = f"""
You are collecting a single metric for a public war-tracking dataset.
Metric name: {metric_name}
Target date: {target_date}

Requirements:
1) Use web search and only accept sources published on {target_date}.
2) If exact same-day data is unavailable, return value_number as null.
3) Return only JSON object with keys:
   - value_number (number or null)
   - confidence (low|medium|high)
   - rationale (string)
   - source_title (string or null)
   - source_url (string or null)
   - source_date (YYYY-MM-DD or null)
4) For cumulative metrics, values should not decrease over time.
   Previous stored value: {previous_value}
5) No markdown.
""".strip()

    tools: list[dict[str, Any]] = [{"type": "web_search_preview"}]
    if preferred_domains:
        tools = [
            {
                "type": "web_search_preview",
                "domains": preferred_domains,
            }
        ]

    response = client.responses.create(
        model=MODEL,
        input=prompt,
        tools=tools,
        temperature=0,
    )

    text = response.output_text
    payload: dict[str, Any]
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

    if result.source_date != target_date:
        result.value = None
        result.rationale = (
            f"Discarded non-same-day source ({result.source_date}). "
            f"Required source date is {target_date}. "
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
            details_json,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date) DO UPDATE SET
            iranian_civilians_deaths = excluded.iranian_civilians_deaths,
            us_soldiers_deaths = excluded.us_soldiers_deaths,
            us_allied_soldiers_deaths = excluded.us_allied_soldiers_deaths,
            iranian_soldiers_deaths = excluded.iranian_soldiers_deaths,
            usa_spending_usd = excluded.usa_spending_usd,
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
            json.dumps(details),
            now,
            now,
        ),
    )
    conn.commit()


def run_update(target_date: str, force: bool = False) -> None:
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is not set.")

    with get_conn() as conn:
        initialize_schema(conn)

        if not force and already_ran_today(conn, target_date):
            print(f"Already ran successfully for {target_date}. Skipping.")
            return

        run_id = start_run(conn, target_date)
        try:
            client = OpenAI()
            previous = get_previous_metrics(conn, target_date)

            fetchers = [
                fetch_iranian_civilians_deaths,
                fetch_us_soldiers_deaths,
                fetch_us_allied_soldiers_deaths,
                fetch_iranian_soldiers_deaths,
                fetch_usa_spending_usd,
            ]

            if len(fetchers) > MAX_REQUESTS:
                raise RuntimeError(
                    f"Configured MAX_REQUESTS={MAX_REQUESTS}, but {len(fetchers)} metric calls are required."
                )

            results: list[MetricResult] = []
            for fetch_fn in fetchers:
                metric_name = fetch_fn.__name__.replace("fetch_", "")
                result = fetch_fn(conn, client, target_date, previous.get(metric_name))
                results.append(result)

            values: dict[str, float | None] = {}
            details: dict[str, dict[str, Any]] = {}

            for result in results:
                previous_value = previous.get(result.metric_name)
                monotonic_value = apply_monotonic_rule(result.value, previous_value)
                values[result.metric_name] = monotonic_value
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
            print(f"Update completed successfully for {target_date}.")

        except Exception as exc:
            finish_run(conn, run_id, "failed", str(exc))
            raise


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