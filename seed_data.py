import os
import sqlite3
from datetime import date, datetime, timezone


DB_PATH = os.getenv("IRAN_WAR_DB_PATH", "data/iran_war_tracker.db")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_db_dir() -> None:
    absolute_path = os.path.abspath(DB_PATH)
    directory = os.path.dirname(absolute_path)
    if directory:
        os.makedirs(directory, exist_ok=True)


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


def seed() -> None:
    ensure_db_dir()
    today = date.today().isoformat()
    now = utc_now_iso()

    conn = sqlite3.connect(DB_PATH)
    try:
        initialize_schema(conn)

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
            ON CONFLICT(date) DO NOTHING
            """,
            (
                today,
                1250,
                12,
                34,
                980,
                2500000000,
                '{"note":"Seed demo row for dashboard preview."}',
                now,
                now,
            ),
        )

        source_rows = [
            ("iranian_civilians_deaths", "https://www.reuters.com", "Reuters", 0.9),
            ("us_soldiers_deaths", "https://www.defense.gov", "U.S. Department of Defense", 0.95),
            ("us_allied_soldiers_deaths", "https://www.nato.int", "NATO", 0.9),
            ("iranian_soldiers_deaths", "https://www.aljazeera.com", "Al Jazeera", 0.8),
            ("usa_spending_usd", "https://www.cbo.gov", "Congressional Budget Office", 0.95),
        ]

        for metric_name, source_url, source_title, trust_score in source_rows:
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
                ON CONFLICT(metric_name, source_url) DO UPDATE SET
                    source_title = excluded.source_title,
                    trust_score = excluded.trust_score,
                    use_count = metric_sources.use_count + 1,
                    last_seen_date = excluded.last_seen_date
                """,
                (metric_name, source_url, source_title, trust_score, today, today),
            )

        conn.execute(
            """
            INSERT INTO updater_runs (run_date, started_at, completed_at, status, message)
            VALUES (?, ?, ?, 'success', ?)
            """,
            (today, now, now, "Seeded demo data."),
        )

        conn.commit()
        print(f"Seeded sample data for {today} into {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    seed()