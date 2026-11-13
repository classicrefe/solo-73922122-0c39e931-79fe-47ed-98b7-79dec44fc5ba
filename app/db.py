"""SQLite 数据访问层。单文件库，表：records / baselines / previews。"""

import json
import os
import sqlite3
import threading
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "survey.db"))

# 所有写操作与确认合并共用同一把锁，保证"检查-应用"之间不被插入
LOCK = threading.RLock()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def init_db(seed_csv: str | None = None):
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with LOCK, connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                id      TEXT PRIMARY KEY,
                quadrat TEXT NOT NULL,
                species TEXT NOT NULL,
                count   INTEGER NOT NULL CHECK (count >= 0)
            );
            CREATE TABLE IF NOT EXISTS baselines (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                created_at TEXT NOT NULL,
                data       TEXT NOT NULL          -- JSON: {id: record}
            );
            CREATE TABLE IF NOT EXISTS previews (
                token       TEXT PRIMARY KEY,
                baseline_id INTEGER NOT NULL,
                created_at  TEXT NOT NULL,
                status      TEXT NOT NULL,        -- pending/applied/discarded
                message     TEXT NOT NULL DEFAULT '',
                revision    TEXT NOT NULL,        -- JSON: {id: record}
                snapshot    TEXT NOT NULL,        -- JSON: 预览时涉及记录的当前值 {id: record|null}
                plan        TEXT NOT NULL         -- JSON: compute_plan 的结果
            );
            """
        )
        empty = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"] == 0
    if empty and seed_csv and os.path.exists(seed_csv):
        from .csvio import parse_csv

        with open(seed_csv, encoding="utf-8") as f:
            seed = parse_csv(f.read())
        with LOCK, connect() as conn:
            for rid, r in seed.items():
                conn.execute(
                    "INSERT OR IGNORE INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                    (rid, r["quadrat"], r["species"], r["count"]),
                )


# ---------- 当前记录 ----------

def get_records() -> dict:
    with connect() as conn:
        rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
    return {row["id"]: {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]} for row in rows}


def get_records_by_ids(ids) -> dict:
    """返回 {id: record|None}，不存在的 ID 值为 None。"""
    out = {}
    with connect() as conn:
        for rid in ids:
            row = conn.execute("SELECT * FROM records WHERE id = ?", (rid,)).fetchone()
            out[rid] = (
                {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]}
                if row
                else None
            )
    return out


def add_record(rid, quadrat, species, count):
    with LOCK, connect() as conn:
        conn.execute(
            "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
            (rid, quadrat, species, count),
        )


def update_record(rid, quadrat, species, count) -> bool:
    with LOCK, connect() as conn:
        cur = conn.execute(
            "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
            (quadrat, species, count, rid),
        )
        return cur.rowcount > 0


def delete_record(rid) -> bool:
    with LOCK, connect() as conn:
        cur = conn.execute("DELETE FROM records WHERE id=?", (rid,))
        return cur.rowcount > 0


# ---------- 基线 ----------

def save_baseline(name: str, data: dict) -> int:
    with LOCK, connect() as conn:
        cur = conn.execute(
            "INSERT INTO baselines (name, created_at, data) VALUES (?,?,?)",
            (name, now(), json.dumps(data, ensure_ascii=False)),
        )
        return cur.lastrowid


def list_baselines() -> list:
    with connect() as conn:
        rows = conn.execute("SELECT id, name, created_at, data FROM baselines ORDER BY id DESC").fetchall()
    return [
        {"id": r["id"], "name": r["name"], "created_at": r["created_at"], "size": len(json.loads(r["data"]))}
        for r in rows
    ]


def get_baseline(bid: int):
    with connect() as conn:
        row = conn.execute("SELECT * FROM baselines WHERE id=?", (bid,)).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": row["name"],
        "created_at": row["created_at"],
        "data": json.loads(row["data"]),
    }


# ---------- 合并预览 ----------

def save_preview(token, baseline_id, revision, snapshot, plan):
    with LOCK, connect() as conn:
        conn.execute(
            "INSERT INTO previews (token, baseline_id, created_at, status, message, revision, snapshot, plan)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                token,
                baseline_id,
                now(),
                "pending",
                "",
                json.dumps(revision, ensure_ascii=False),
                json.dumps(snapshot, ensure_ascii=False),
                json.dumps(plan, ensure_ascii=False),
            ),
        )


def get_preview(token):
    with connect() as conn:
        row = conn.execute("SELECT * FROM previews WHERE token=?", (token,)).fetchone()
    if not row:
        return None
    return {
        "token": row["token"],
        "baseline_id": row["baseline_id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "message": row["message"],
        "revision": json.loads(row["revision"]),
        "snapshot": json.loads(row["snapshot"]),
        "plan": json.loads(row["plan"]),
    }


def list_previews() -> list:
    with connect() as conn:
        rows = conn.execute(
            "SELECT token, baseline_id, created_at, status, message FROM previews ORDER BY created_at DESC, token DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def update_preview_plan(token, snapshot, plan, message):
    """过期预览被重新比较后，用新计划与新快照替换旧内容。"""
    with LOCK, connect() as conn:
        conn.execute(
            "UPDATE previews SET snapshot=?, plan=?, message=?, status='pending', created_at=? WHERE token=?",
            (json.dumps(snapshot, ensure_ascii=False), json.dumps(plan, ensure_ascii=False), message, now(), token),
        )


def set_preview_status(token, status, message=""):
    with LOCK, connect() as conn:
        conn.execute("UPDATE previews SET status=?, message=? WHERE token=?", (status, message, token))


def apply_actions(actions) -> None:
    """单事务应用最终动作；任何失败整体回滚，当前数据不变。"""
    with LOCK, connect() as conn:
        try:
            conn.execute("BEGIN")
            for a in actions:
                if a["action"] == "insert":
                    r = a["record"]
                    conn.execute(
                        "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                        (a["id"], r["quadrat"], r["species"], r["count"]),
                    )
                elif a["action"] == "update":
                    r = a["to"]
                    conn.execute(
                        "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
                        (r["quadrat"], r["species"], r["count"], a["id"]),
                    )
                elif a["action"] == "delete":
                    conn.execute("DELETE FROM records WHERE id=?", (a["id"],))
                else:
                    raise ValueError(f"未知动作：{a['action']}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
