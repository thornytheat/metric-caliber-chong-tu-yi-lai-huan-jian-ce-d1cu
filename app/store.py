"""SQLite 访问层：指标注册表、体检记录、影响面记录、事件流水。

命名卷 metricring-data 挂到 /app/data，重建数据只需删卷。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from . import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS metrics (
    name        TEXT    NOT NULL,
    version     INTEGER NOT NULL,
    expression  TEXT    NOT NULL,
    unit        TEXT    NOT NULL DEFAULT '',
    aggregation TEXT    NOT NULL DEFAULT 'sum',
    dimensions  TEXT    NOT NULL DEFAULT '[]',
    owner       TEXT    NOT NULL DEFAULT '',
    published   INTEGER NOT NULL DEFAULT 0,
    description TEXT    NOT NULL DEFAULT '',
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL,
    PRIMARY KEY (name, version)
);
CREATE TABLE IF NOT EXISTS checks (
    check_id   TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    payload    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS impacts (
    impact_id  TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    name       TEXT NOT NULL,
    version    INTEGER NOT NULL,
    payload    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    at      TEXT NOT NULL,
    kind    TEXT NOT NULL,
    summary TEXT NOT NULL,
    detail  TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS release_batches (
    batch_id            TEXT PRIMARY KEY,
    idempotency_key     TEXT NOT NULL UNIQUE,
    request_fingerprint TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL,
    candidates          TEXT NOT NULL,
    rehearsal           TEXT NOT NULL DEFAULT '{}',
    result              TEXT NOT NULL DEFAULT '{}',
    fingerprint         TEXT NOT NULL DEFAULT '',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    applied_at          TEXT
);
CREATE TABLE IF NOT EXISTS release_batch_items (
    batch_id  TEXT NOT NULL,
    key       TEXT NOT NULL,
    ordinal   INTEGER NOT NULL,
    status    TEXT NOT NULL DEFAULT 'candidate',
    evidence  TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (batch_id, key),
    FOREIGN KEY (batch_id) REFERENCES release_batches(batch_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_release_batch_items_order
    ON release_batch_items (batch_id, ordinal);
"""


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = Path(db_path or settings.DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def session(db_path: Path | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def _row_to_metric(row: sqlite3.Row) -> dict:
    return {
        "name": row["name"],
        "version": int(row["version"]),
        "expression": row["expression"],
        "unit": row["unit"],
        "aggregation": row["aggregation"],
        "dimensions": json.loads(row["dimensions"] or "[]"),
        "owner": row["owner"],
        "published": bool(row["published"]),
        "description": row["description"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def count_metrics(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS c FROM metrics").fetchone()
    return int(row["c"])


def all_metrics(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM metrics ORDER BY name ASC, version ASC"
    ).fetchall()
    return [_row_to_metric(row) for row in rows]


def get_metric(conn: sqlite3.Connection, name: str, version: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM metrics WHERE name = ? AND version = ?", (name, int(version))
    ).fetchone()
    return _row_to_metric(row) if row else None


def upsert_metric(conn: sqlite3.Connection, metric: dict, now: str) -> None:
    conn.execute(
        """
        INSERT INTO metrics (name, version, expression, unit, aggregation, dimensions,
                             owner, published, description, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name, version) DO UPDATE SET
            expression = excluded.expression,
            unit = excluded.unit,
            aggregation = excluded.aggregation,
            dimensions = excluded.dimensions,
            owner = excluded.owner,
            published = excluded.published,
            description = excluded.description,
            updated_at = excluded.updated_at
        """,
        (
            metric["name"],
            int(metric["version"]),
            metric["expression"],
            metric.get("unit", ""),
            metric.get("aggregation", "sum"),
            json.dumps(metric.get("dimensions", []), ensure_ascii=False),
            metric.get("owner", ""),
            1 if metric.get("published") else 0,
            metric.get("description", ""),
            now,
            now,
        ),
    )


def set_published(conn: sqlite3.Connection, name: str, version: int, published: bool, now: str) -> None:
    conn.execute(
        "UPDATE metrics SET published = ?, updated_at = ? WHERE name = ? AND version = ?",
        (1 if published else 0, now, name, int(version)),
    )


def insert_metric(conn: sqlite3.Connection, metric: dict, now: str) -> None:
    conn.execute(
        """
        INSERT INTO metrics (name, version, expression, unit, aggregation, dimensions,
                             owner, published, description, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            metric["name"],
            int(metric["version"]),
            metric["expression"],
            metric.get("unit", ""),
            metric.get("aggregation", "sum"),
            json.dumps(metric.get("dimensions", []), ensure_ascii=False),
            metric.get("owner", ""),
            1 if metric.get("published") else 0,
            metric.get("description", ""),
            now,
            now,
        ),
    )


def metric_exists(conn: sqlite3.Connection, name: str, version: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM metrics WHERE name = ? AND version = ?", (name, int(version))
    ).fetchone()
    return row is not None


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def save_check(conn: sqlite3.Connection, payload: dict) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO checks (check_id, created_at, payload) VALUES (?, ?, ?)",
        (payload["check_id"], payload["created_at"], json.dumps(payload, ensure_ascii=False)),
    )


def load_check(conn: sqlite3.Connection, check_id: str) -> dict | None:
    row = conn.execute("SELECT payload FROM checks WHERE check_id = ?", (check_id,)).fetchone()
    return json.loads(row["payload"]) if row else None


def latest_check(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT payload FROM checks ORDER BY created_at DESC, rowid DESC LIMIT 1"
    ).fetchone()
    return json.loads(row["payload"]) if row else None


def list_checks(conn: sqlite3.Connection, limit: int = 10) -> list[dict]:
    rows = conn.execute(
        "SELECT check_id, created_at, payload FROM checks ORDER BY created_at DESC, rowid DESC LIMIT ?",
        (limit,),
    ).fetchall()
    items = []
    for row in rows:
        payload = json.loads(row["payload"])
        items.append(
            {
                "check_id": row["check_id"],
                "created_at": row["created_at"],
                "verdict": payload.get("verdict"),
                "stats": payload.get("stats", {}),
            }
        )
    return items


def save_impact(conn: sqlite3.Connection, payload: dict) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO impacts (impact_id, created_at, name, version, payload) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            payload["impact_id"],
            payload["created_at"],
            payload["target"]["name"],
            payload["target"]["version"],
            json.dumps(payload, ensure_ascii=False),
        ),
    )


def load_impact(conn: sqlite3.Connection, impact_id: str) -> dict | None:
    row = conn.execute("SELECT payload FROM impacts WHERE impact_id = ?", (impact_id,)).fetchone()
    return json.loads(row["payload"]) if row else None


def latest_impact(conn: sqlite3.Connection, name: str | None = None) -> dict | None:
    if name:
        row = conn.execute(
            "SELECT payload FROM impacts WHERE name = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (name,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT payload FROM impacts ORDER BY created_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
    return json.loads(row["payload"]) if row else None


def add_event(conn: sqlite3.Connection, kind: str, summary: str, detail: dict, now: str) -> None:
    conn.execute(
        "INSERT INTO events (at, kind, summary, detail) VALUES (?, ?, ?, ?)",
        (now, kind, summary, json.dumps(detail, ensure_ascii=False)),
    )


def list_events(conn: sqlite3.Connection, limit: int = 40) -> list[dict]:
    rows = conn.execute(
        "SELECT at, kind, summary, detail FROM events ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [
        {
            "at": row["at"],
            "kind": row["kind"],
            "summary": row["summary"],
            "detail": json.loads(row["detail"] or "{}"),
        }
        for row in rows
    ]


# --------------------------------------------------------------------------- 发布批次


def _row_to_batch(row: sqlite3.Row) -> dict:
    return {
        "batch_id": row["batch_id"],
        "idempotency_key": row["idempotency_key"],
        "request_fingerprint": row["request_fingerprint"],
        "status": row["status"],
        "candidates": json.loads(row["candidates"] or "[]"),
        "rehearsal": json.loads(row["rehearsal"] or "{}"),
        "result": json.loads(row["result"] or "{}"),
        "fingerprint": row["fingerprint"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "applied_at": row["applied_at"],
    }


def get_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM release_batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    return _row_to_batch(row) if row else None


def get_batch_by_idempotency(
    conn: sqlite3.Connection, idempotency_key: str
) -> dict | None:
    row = conn.execute(
        "SELECT * FROM release_batches WHERE idempotency_key = ?", (idempotency_key,)
    ).fetchone()
    return _row_to_batch(row) if row else None


def latest_batch(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT * FROM release_batches ORDER BY created_at DESC, rowid DESC LIMIT 1"
    ).fetchone()
    return _row_to_batch(row) if row else None


def list_batches(conn: sqlite3.Connection, limit: int = 10) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM release_batches ORDER BY created_at DESC, rowid DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [_row_to_batch(row) for row in rows]


def insert_batch(conn: sqlite3.Connection, batch: dict, now: str) -> None:
    conn.execute(
        """
        INSERT INTO release_batches (batch_id, idempotency_key, request_fingerprint,
                                     status, candidates, rehearsal, result, fingerprint,
                                     created_at, updated_at, applied_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            batch["batch_id"],
            batch["idempotency_key"],
            batch.get("request_fingerprint", ""),
            batch["status"],
            json.dumps(batch.get("candidates", []), ensure_ascii=False),
            json.dumps(batch.get("rehearsal", {}), ensure_ascii=False),
            json.dumps(batch.get("result", {}), ensure_ascii=False),
            batch.get("fingerprint", ""),
            now,
            now,
        ),
    )


def update_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    *,
    status: str | None = None,
    rehearsal: dict | None = None,
    result: dict | None = None,
    applied_at: str | None = None,
    now: str,
) -> None:
    parts = ["updated_at = ?"]
    values: list = [now]
    if status is not None:
        parts.append("status = ?")
        values.append(status)
    if rehearsal is not None:
        parts.append("rehearsal = ?")
        values.append(json.dumps(rehearsal, ensure_ascii=False))
    if result is not None:
        parts.append("result = ?")
        values.append(json.dumps(result, ensure_ascii=False))
    if applied_at is not None:
        parts.append("applied_at = ?")
        values.append(applied_at)
    values.append(batch_id)
    conn.execute(
        f"UPDATE release_batches SET {', '.join(parts)} WHERE batch_id = ?", values
    )


def replace_batch_items(conn: sqlite3.Connection, batch_id: str, items: list[dict]) -> None:
    conn.execute("DELETE FROM release_batch_items WHERE batch_id = ?", (batch_id,))
    for ordinal, item in enumerate(items):
        conn.execute(
            "INSERT INTO release_batch_items (batch_id, key, ordinal, status, evidence) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                batch_id,
                item["key"],
                ordinal,
                item.get("status", "candidate"),
                json.dumps(item.get("evidence", {}), ensure_ascii=False),
            ),
        )


def count_batch_items(conn: sqlite3.Connection, batch_id: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM release_batch_items WHERE batch_id = ?", (batch_id,)
    ).fetchone()
    return int(row["c"])


def batch_items(conn: sqlite3.Connection, batch_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT key, ordinal, status, evidence FROM release_batch_items "
        "WHERE batch_id = ? ORDER BY ordinal ASC",
        (batch_id,),
    ).fetchall()
    return [
        {
            "key": row["key"],
            "ordinal": int(row["ordinal"]),
            "status": row["status"],
            "evidence": json.loads(row["evidence"] or "{}"),
        }
        for row in rows
    ]


def list_metrics_for_keys(
    conn: sqlite3.Connection, keys: list[str]
) -> list[dict]:
    """按批次候选键读取实际落库的指标版本（用于切换后核对）。"""
    found: list[dict] = []
    for key in keys:
        name, _, version = key.partition("@")
        metric = get_metric(conn, name, int(version or 1))
        if metric is not None:
            found.append(metric)
    return found


def begin_immediate(conn: sqlite3.Connection) -> None:
    """串行化批次写入：切换与状态更新放在同一个独占事务里。"""
    conn.execute("BEGIN IMMEDIATE")

