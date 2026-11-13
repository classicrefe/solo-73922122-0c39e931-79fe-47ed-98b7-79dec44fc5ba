"""SQLite 数据访问层。单文件库，表：records / baselines / previews。

并发策略：
- WAL 日志模式：写入持有写锁期间，其它连接仍可读到已提交记录（读写不互斥）。
- 每个连接设置 ``busy_timeout``（默认 5000ms）：抢写锁时在 SQLite 内部忙等，
  超时后抛 :class:`LockTimeout`，由 Web 层转成可重试的 503。
- 所有写操作走 ``BEGIN IMMEDIATE``，在事务起点就取得写锁；
  "核对快照 + 应用动作" 在同一个事务里完成，替代旧的进程级互斥锁。
- 连接随用随开、用完即关，不在请求之间复用。
"""

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime

from .merge import build_final_actions, compute_plan

DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(__file__), "..", "survey.db")
)
# 写锁忙等上限：需求固定 5 秒
BUSY_TIMEOUT_MS = int(os.environ.get("BUSY_TIMEOUT_MS", "5000"))

# SQLite 扩展错误码：SQLITE_BUSY=5，SQLITE_LOCKED=6
_BUSY_CODES = frozenset({5, 6})


class LockTimeout(sqlite3.OperationalError):
    """等待 SQLite 写锁超过 busy_timeout 仍未获得。Web 层映射为 503。"""


class NotFoundError(Exception):
    """请求的行不存在（例如基线已被删除）。"""


def is_busy(exc: BaseException) -> bool:
    """判断异常是否为 SQLite 写锁忙/锁冲突（含忙等待超时）。"""
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(code, int):
        return code in _BUSY_CODES
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


def connect() -> sqlite3.Connection:
    """打开一个短生命周期连接，带 5 秒忙等待与行工厂。"""
    conn = sqlite3.connect(
        DB_PATH,
        timeout=BUSY_TIMEOUT_MS / 1000.0,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def transaction():
    """写事务上下文：BEGIN IMMEDIATE 起点即抢写锁。

    - 抢锁忙等 5 秒仍失败 -> 回滚并抛 :class:`LockTimeout`（不留任何写入）。
    - 其它异常 -> 回滚后原样抛出。
    - 正常退出 -> 提交；连接始终关闭。
    """
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except sqlite3.OperationalError as exc:
        conn.rollback()
        if is_busy(exc):
            raise LockTimeout(str(exc)) from exc
        raise
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


_SCHEMA = """
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


def init_db(seed_csv: str | None = None):
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    # journal_mode 是数据库文件级持久属性，只需在初始化时切换一次
    conn = connect()
    try:
        mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if str(mode).lower() != "wal":  # pragma: no cover - 文件系统不支持 WAL 时给出明确报错
            raise RuntimeError(f"SQLite 无法切换到 WAL 模式（实际：{mode}）")
        conn.executescript(_SCHEMA)
        empty = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"] == 0
    finally:
        conn.close()

    if empty and seed_csv and os.path.exists(seed_csv):
        from .csvio import parse_csv

        with open(seed_csv, encoding="utf-8") as f:
            seed = parse_csv(f.read())
        with transaction() as conn:
            for rid, r in seed.items():
                conn.execute(
                    "INSERT OR IGNORE INTO records (id, quadrat, species, count) "
                    "VALUES (?,?,?,?)",
                    (rid, r["quadrat"], r["species"], r["count"]),
                )


# ---------- 行解码 ----------


def _record(row: sqlite3.Row) -> dict:
    return {
        "quadrat": row["quadrat"],
        "species": row["species"],
        "count": row["count"],
    }


def _all_records(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
    return {row["id"]: _record(row) for row in rows}


def _records_by_ids(conn: sqlite3.Connection, ids) -> dict:
    """返回 {id: record|None}，不存在的 ID 值为 None。"""
    out = {rid: None for rid in ids}
    for rid in ids:
        row = conn.execute(
            "SELECT * FROM records WHERE id = ?", (rid,)
        ).fetchone()
        if row:
            out[rid] = _record(row)
    return out


def _baseline_row(conn: sqlite3.Connection, bid: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM baselines WHERE id=?", (bid,)).fetchone()
    if not row:
        raise NotFoundError(f"基线 {bid} 不存在")
    return row


# ---------- 当前记录（只读，WAL 下不被写锁阻塞） ----------


def get_records() -> dict:
    conn = connect()
    try:
        return _all_records(conn)
    finally:
        conn.close()


# ---------- 记录维护（写事务，锁超时 -> LockTimeout -> 503） ----------


def add_record(rid, quadrat, species, count):
    with transaction() as conn:
        try:
            conn.execute(
                "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                (rid, quadrat, species, count),
            )
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"记录 {rid} 已存在") from exc


def update_record(rid, quadrat, species, count) -> bool:
    with transaction() as conn:
        cur = conn.execute(
            "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
            (quadrat, species, count, rid),
        )
        return cur.rowcount > 0


def delete_record(rid) -> bool:
    with transaction() as conn:
        cur = conn.execute("DELETE FROM records WHERE id=?", (rid,))
        return cur.rowcount > 0


# ---------- 基线 ----------


def save_baseline(name: str, data: dict) -> int:
    with transaction() as conn:
        cur = conn.execute(
            "INSERT INTO baselines (name, created_at, data) VALUES (?,?,?)",
            (name, now(), json.dumps(data, ensure_ascii=False)),
        )
        return cur.lastrowid


def list_baselines() -> list:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT id, name, created_at, data FROM baselines ORDER BY id DESC"
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "created_at": r["created_at"],
            "size": len(json.loads(r["data"])),
        }
        for r in rows
    ]


def get_baseline(bid: int):
    conn = connect()
    try:
        row = conn.execute(
            "SELECT * FROM baselines WHERE id=?", (bid,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": row["name"],
        "created_at": row["created_at"],
        "data": json.loads(row["data"]),
    }


# ---------- 合并预览 ----------


def _decode_preview(row: sqlite3.Row) -> dict:
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


def create_preview(baseline_id: int, revision: dict) -> str:
    """在单个写事务内完成三方比较并落库预览，返回预览 token。

    抢不到写锁（忙等 5 秒超时）时抛 :class:`LockTimeout`，事务回滚，
    不会留下任何预览行。基线不存在抛 :class:`NotFoundError`。
    """
    with transaction() as conn:
        row = _baseline_row(conn, baseline_id)
        base_data = json.loads(row["data"])
        current = _all_records(conn)
        plan = compute_plan(base_data, revision, current)
        snapshot = _records_by_ids(conn, plan["involved"])
        token = uuid.uuid4().hex[:12]
        conn.execute(
            "INSERT INTO previews (token, baseline_id, created_at, status, message, "
            "revision, snapshot, plan) VALUES (?,?,?,?,?,?,?,?)",
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
        return token


def get_preview(token: str):
    conn = connect()
    try:
        row = conn.execute(
            "SELECT * FROM previews WHERE token=?", (token,)
        ).fetchone()
    finally:
        conn.close()
    return _decode_preview(row) if row else None


def list_previews() -> list:
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT token, baseline_id, created_at, status, message "
            "FROM previews ORDER BY created_at DESC, token DESC"
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def count_previews() -> int:
    conn = connect()
    try:
        return conn.execute("SELECT COUNT(*) AS n FROM previews").fetchone()["n"]
    finally:
        conn.close()


def set_preview_status(token, status, message=""):
    with transaction() as conn:
        conn.execute(
            "UPDATE previews SET status=?, message=? WHERE token=?",
            (status, message, token),
        )


def _apply(conn: sqlite3.Connection, actions) -> None:
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


def confirm_preview(token: str, resolutions: dict) -> dict:
    """单事务完成"过期核对 → 必要时重算 → 整批应用"。

    返回 outcome：
    - applied：已提交，带 count；
    - stale：快照过期，已在同事务内按最新记录重算并替换预览，需重新核对；
    - invalid：冲突未裁决全，无写入；
    - already / missing：预览已处理或不存在。
    锁冲突一律抛 :class:`LockTimeout`（回滚，数据不变）。
    """
    with transaction() as conn:
        row = conn.execute(
            "SELECT * FROM previews WHERE token=?", (token,)
        ).fetchone()
        if not row:
            return {"outcome": "missing"}
        pv = _decode_preview(row)
        if pv["status"] != "pending":
            return {"outcome": "already", "status": pv["status"]}

        fresh = _records_by_ids(conn, pv["plan"]["involved"])
        if fresh != pv["snapshot"]:
            base_row = _baseline_row(conn, pv["baseline_id"])
            base_data = json.loads(base_row["data"])
            current = _all_records(conn)
            plan = compute_plan(base_data, pv["revision"], current)
            snapshot = _records_by_ids(conn, plan["involved"])
            conn.execute(
                "UPDATE previews SET snapshot=?, plan=?, message=?, "
                "status='pending', created_at=? WHERE token=?",
                (
                    json.dumps(snapshot, ensure_ascii=False),
                    json.dumps(plan, ensure_ascii=False),
                    "拟改记录在预览后又被修改或删除，旧预览已作废并按最新当前记录重新比较，请重新核对。",
                    now(),
                    token,
                ),
            )
            return {"outcome": "stale"}

        try:
            actions = build_final_actions(pv["plan"], resolutions)
        except ValueError as exc:
            return {"outcome": "invalid", "message": str(exc)}

        _apply(conn, actions)
        conn.execute(
            "UPDATE previews SET status='applied', message=? WHERE token=?",
            (f"已应用 {len(actions)} 项离线改动", token),
        )
        return {"outcome": "applied", "count": len(actions)}
