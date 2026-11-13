"""受控演示：写锁持有、并发上传、任务取消。

仅当 DEMO_ENABLED != "0" 时挂载到 /demo 下。所有演练在进程内直接驱动
:mod:`app.jobs` 与 SQLite，不依赖浏览器连接数限制，结果确定、可重复。
"""

import asyncio
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from . import db
from .csvio import dump_csv
from .jobs import runner
from .main import templates

router = APIRouter(prefix="/demo", tags=["demo"])

SELF_URL = os.environ.get("SELF_URL", "http://127.0.0.1:8000")
DEMO_LOCK_SECONDS = float(os.environ.get("DEMO_LOCK_SECONDS", "8"))


# ---------------- 写锁持有器（独立连接，不经过连接池） ----------------


class LockHolder:
    """用独立 SQLite 连接持有数据库写锁，用于模拟长事务占用。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._conn = None
        self._thread = None
        self._stop = threading.Event()
        self.seconds = 0.0
        self.started_at = None
        self.holding = False

    def acquire(self, seconds: float) -> None:
        with self._lock:
            if self.holding:
                raise RuntimeError("已有写锁演示在进行中")
            conn = sqlite3.connect(db.DB_PATH, check_same_thread=False)
            conn.execute("PRAGMA busy_timeout = 0")
            try:
                conn.execute("BEGIN IMMEDIATE")
                # 写一行临时标记再回滚，确保确实拿到的是写锁
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS _demo_lock_marker (id INTEGER PRIMARY KEY)"
                )
                conn.execute("DELETE FROM _demo_lock_marker")
                conn.execute("INSERT INTO _demo_lock_marker (id) VALUES (1)")
            except BaseException:
                conn.rollback()
                conn.close()
                raise
            self._conn = conn
            self._stop.clear()
            self.seconds = seconds
            self.started_at = time.monotonic()
            self.holding = True
            self._thread = threading.Thread(
                target=self._hold, args=(seconds,), daemon=True, name="demo-lock-holder"
            )
            self._thread.start()

    def _hold(self, seconds: float) -> None:
        try:
            self._stop.wait(seconds)
        finally:
            self.release()

    def release(self) -> bool:
        with self._lock:
            conn, self._conn = self._conn, None
            if conn is None:
                return False
            try:
                conn.rollback()  # 标记表不落任何内容
            finally:
                conn.close()
            self.holding = False
            self._stop.set()
            return True

    def status(self) -> dict:
        remaining = None
        if self.holding and self.started_at is not None:
            remaining = max(0.0, round(self.seconds - (time.monotonic() - self.started_at), 1))
        return {
            "holding": self.holding,
            "seconds": self.seconds,
            "remaining_seconds": remaining,
        }


holder = LockHolder()


# ---------------- 演示页 ----------------


@router.get("", response_class=HTMLResponse)
def demo_page(request: Request, msg: str = ""):
    return templates.TemplateResponse(
        request,
        "demo.html",
        {
            "request": request,
            "workers": runner.workers,
            "holder": holder.status(),
            "jobs": runner.snapshot()[:20],
            "busy_timeout": db.BUSY_TIMEOUT_MS,
            "msg": msg,
        },
    )


# ---------------- 持锁 ----------------


@router.post("/lock/hold")
def lock_hold(seconds: str = Form(str(DEMO_LOCK_SECONDS))):
    try:
        secs = float(seconds)
        if not 1 <= secs <= 60:
            raise ValueError
    except ValueError:
        return RedirectResponse("/demo?msg=" + urllib.parse.quote("持锁秒数需在 1~60 之间"), status_code=303)
    try:
        holder.acquire(secs)
    except Exception as exc:  # 已有持锁或抢锁失败
        return RedirectResponse("/demo?msg=" + urllib.parse.quote(f"持锁失败：{exc}"), status_code=303)
    return RedirectResponse(
        "/demo?msg=" + urllib.parse.quote(f"已持有 SQLite 写锁 {secs:g} 秒，期间可触发上传观察 503"),
        status_code=303,
    )


@router.post("/lock/release")
def lock_release():
    ok = holder.release()
    return RedirectResponse(
        "/demo?msg=" + urllib.parse.quote("写锁已提前释放" if ok else "当前没有持中的演示锁"),
        status_code=303,
    )


# ---------------- 读延迟探针 ----------------


def _probe_once(path: str) -> tuple[int, float]:
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(SELF_URL + path, timeout=3) as resp:
            resp.read(1024)
            return resp.status, (time.monotonic() - t0) * 1000
    except Exception as exc:  # 探针失败也记录，便于报告暴露问题
        return getattr(exc, "code", 0), (time.monotonic() - t0) * 1000


class ReadProbe(threading.Thread):
    """并发上传期间持续采样首页与 CSV 导出的读取延迟。"""

    def __init__(self, interval: float = 0.1):
        super().__init__(daemon=True, name="demo-read-probe")
        self.interval = interval
        self._stop = threading.Event()
        self.samples: list[dict] = []

    def run(self) -> None:
        while not self._stop.wait(self.interval):
            for path, label in (("/", "首页"), ("/export.csv", "CSV导出")):
                status, ms = _probe_once(path)
                self.samples.append({"label": label, "status": status, "ms": round(ms, 1)})

    def stop(self) -> list[dict]:
        self._stop.set()
        return self.samples


# ---------------- 公共工具 ----------------


def _ensure_baseline() -> int:
    baselines = db.list_baselines()
    if baselines:
        return baselines[0]["id"]
    return db.save_baseline(f"演示基线（{datetime.now().isoformat(timespec='seconds')}）", db.get_records())


def _revision_bytes(baseline_id: int, idx: int) -> bytes:
    baseline = db.get_baseline(baseline_id)
    # 复制基线内容并微调一条记录的株数，保证每次都是有效且不同的修订
    rows = list(baseline["data"].items())
    first_id, first = rows[0]
    first = dict(first)
    first["count"] = (first["count"] + idx) % 1_000_000
    data = dict(baseline["data"])
    data[first_id] = first
    return dump_csv(data).encode("utf-8")


async def _run_uploads(n: int, baseline_id: int) -> list:
    jobs = []
    for i in range(n):
        job = await runner.submit(
            _demo_process,
            baseline_id,
            _revision_bytes(baseline_id, i),
            label=f"demo-{i + 1:02d}.csv",
        )
        jobs.append(job)
    # 等待全部结束
    while any(not j.finished for j in jobs):
        await asyncio.sleep(0.05)
    return jobs


def _demo_process(baseline_id: int, raw: bytes) -> str:
    # 与正式上传完全相同的处理函数，保证演练路径即生产路径
    from .main import process_upload

    return process_upload(baseline_id, raw)


DEMO_TASK_DELAY = float(os.environ.get("DEMO_TASK_DELAY", "0.25"))


def _demo_process_slow(baseline_id: int, raw: bytes) -> str:
    """演示专用：人为放慢任务，使排队/运行态稳定可观察（取消语义更易复现）。"""
    time.sleep(DEMO_TASK_DELAY)
    return _demo_process(baseline_id, raw)


def _classify_jobs(jobs: list) -> dict:
    buckets = {"success": [], "locked": [], "cancelled": [], "error": []}
    for j in jobs:
        if j.state == "done":
            buckets["success"].append(j)
        elif j.state == "cancelled":
            buckets["cancelled"].append(j)
        elif j.error_type == "locked":
            buckets["locked"].append(j)
        else:
            buckets["error"].append(j)
    return buckets


def _summarize_latency(samples: list[dict]) -> dict:
    out = {}
    for label in ("首页", "CSV导出"):
        vals = sorted(s["ms"] for s in samples if s["label"] == label)
        if not vals:
            out[label] = None
            continue
        out[label] = {
            "n": len(vals),
            "max": round(vals[-1], 1),
            "p95": round(vals[min(len(vals) - 1, int(len(vals) * 0.95))], 1),
            "ok": all(s["status"] == 200 and s["ms"] < 1000 for s in samples if s["label"] == label),
        }
    return out


# ---------------- 演练 A：40 并发上传 + 读延迟 ----------------


@router.post("/burst")
async def demo_burst(request: Request, n: str = Form("40")):
    try:
        total = max(1, min(int(n), 80))
    except ValueError:
        total = 40
    baseline_id = _ensure_baseline()
    previews_before = db.count_previews()

    probe = ReadProbe()
    t0 = time.monotonic()
    probe.start()
    jobs = await _run_uploads(total, baseline_id)
    wall = round((time.monotonic() - t0) * 1000, 1)
    samples = probe.stop()
    buckets = _classify_jobs(jobs)
    latency = _summarize_latency(samples)
    previews_after = db.count_previews()

    checks = [
        (
            f"{total} 份并发上传全部完成（成功 {len(buckets['success'])}、"
            f"锁超时 {len(buckets['locked'])}、取消 {len(buckets['cancelled'])}、错误 {len(buckets['error'])}）",
            len(jobs) == total,
        ),
        (
            "每份成功上传恰好生成 1 条预览，无多余残留",
            previews_after - previews_before == len(buckets["success"]),
        ),
        (
            "并发期间首页读取始终 < 1 秒（WAL 读不被写锁阻塞）",
            latency["首页"] is not None and latency["首页"]["ok"],
        ),
        (
            "并发期间 CSV 导出读取始终 < 1 秒",
            latency["CSV导出"] is not None and latency["CSV导出"]["ok"],
        ),
        (
            "排队任务不占用页面查询线程（探针全部返回 200）",
            all(s["status"] == 200 for s in samples),
        ),
    ]
    return templates.TemplateResponse(
        request,
        "demo_report.html",
        {
            "request": request,
            "title": f"演练 A：{total} 份并发上传",
            "wall_ms": wall,
            "workers": runner.workers,
            "buckets": buckets,
            "latency": latency,
            "samples": samples,
            "checks": checks,
            "jobs": [j for j in jobs],
            "kind": "burst",
        },
    )


# ---------------- 演练 B：持锁期间上传 -> 503，解锁后可重传 ----------------


@router.post("/timeout")
async def demo_timeout(request: Request, n: str = Form("6")):
    try:
        total = max(1, min(int(n), 10))
    except ValueError:
        total = 6
    baseline_id = _ensure_baseline()
    previews_before = db.count_previews()

    if not holder.status()["holding"]:
        # 覆盖全部 total 个任务的忙等窗口：每批 workers 个、每个忙等 5 秒
        batches = (total + runner.workers - 1) // runner.workers
        need = batches * (db.BUSY_TIMEOUT_MS / 1000.0) + 2
        holder.acquire(max(DEMO_LOCK_SECONDS + 2, need))
    t0 = time.monotonic()
    jobs = await _run_uploads(total, baseline_id)
    wall = round((time.monotonic() - t0) * 1000, 1)
    buckets = _classify_jobs(jobs)
    previews_mid = db.count_previews()

    # 解锁后重新上传同一份修订，应当成功
    holder.release()
    await asyncio.sleep(0.2)  # 等锁真正释放
    retry_jobs = await _run_uploads(1, baseline_id)
    retry = retry_jobs[0]
    previews_after = db.count_previews()

    checks = [
        (
            f"持锁期间 {total} 份上传在 5 秒忙等后全部得到可重试的锁超时（503 语义）",
            len(buckets["locked"]) == total and not buckets["success"],
        ),
        ("锁超时未留下任何预览行", previews_mid == previews_before),
        (
            "每份上传的忙等耗时约为 5 秒（busy_timeout=5000ms）",
            all(4800 <= (j.elapsed_ms() or 0) <= 6500 for j in buckets["locked"]),
        ),
        ("解锁后重新上传同一份修订成功并生成预览", retry.state == "done"),
        ("重试恰好补回 1 条预览", previews_after - previews_mid == 1),
    ]
    return templates.TemplateResponse(
        request,
        "demo_report.html",
        {
            "request": request,
            "title": "演练 B：写锁忙等 5 秒 → 503 → 解锁重传成功",
            "wall_ms": wall,
            "workers": runner.workers,
            "buckets": buckets,
            "latency": None,
            "samples": [],
            "checks": checks,
            "jobs": [j for j in jobs] + [retry],
            "kind": "timeout",
        },
    )


# ---------------- 演练 C：取消语义 ----------------


@router.post("/cancel")
async def demo_cancel(request: Request):
    baseline_id = _ensure_baseline()
    report = []

    # C1：提交超过 worker 数的任务，立即取消尚未开始的
    n = runner.workers + 4
    jobs = []
    for i in range(n):
        jobs.append(
            await runner.submit(
                _demo_process_slow,
                baseline_id,
                _revision_bytes(baseline_id, 100 + i),
                label=f"cancel-{i + 1}.csv",
            )
        )
    await asyncio.sleep(0.15)  # 让前 workers 个进入运行态，其余排队
    queued = [j for j in jobs if j.state == "queued"]
    target = queued[0]
    first_ok = target.cancel()
    second_ok = target.cancel()  # 重复取消必须幂等
    while any(not j.finished for j in jobs):
        await asyncio.sleep(0.05)

    previews = db.list_previews()
    target_token = None
    for j in jobs:
        if j is target and j.state == "done":
            target_token = j.result
    c1 = [
        (f"提交 {n} 个任务（{runner.workers} 个执行名额），确有任务处于排队态", bool(queued)),
        ("排队任务首次取消成功", first_ok is True),
        ("对同一任务重复取消不再生效（不提前回收任何资源）", second_ok is False),
        ("被取消任务终态为 cancelled 且从未运行", target.state == "cancelled" and target.started_at is None),
        ("被取消任务没有产生预览", target.result is None and not any(p["token"] == target_token for p in previews)),
        (
            "其余任务全部正常完成并生成预览（执行名额最终全部归还）",
            all(j.state == "done" for j in jobs if j is not target),
        ),
    ]
    report.append(("C1 排队任务取消 / 重复取消", c1))

    # C2：对运行中的任务取消 -> 不生效，任务完成提交并释放名额
    running_job = await runner.submit(
        _demo_process_slow, baseline_id, _revision_bytes(baseline_id, 200), label="cancel-running.csv"
    )
    await asyncio.sleep(0.05)  # 慢任务保证此刻仍在运行态
    cancel_running_ok = running_job.cancel()
    while not running_job.finished:
        await asyncio.sleep(0.05)
    c2 = [
        ("运行中的任务取消被拒绝（已开始的数据库工作须完成）", cancel_running_ok is False),
        ("运行任务最终正常完成（提交或回滚后关闭连接、归还名额）", running_job.state == "done"),
        ("完成后再取消仍是幂等的 no-op", running_job.cancel() is False),
    ]
    report.append(("C2 运行中任务不可提前回收", c2))

    # C3：持锁 + 取消排队任务；解锁后队列中剩余任务恢复处理
    holder.acquire(DEMO_LOCK_SECONDS + 2)
    locked_jobs = []
    for i in range(runner.workers + 2):
        locked_jobs.append(
            await runner.submit(
                _demo_process,
                baseline_id,
                _revision_bytes(baseline_id, 300 + i),
                label=f"lock-cancel-{i + 1}.csv",
            )
        )
    await asyncio.sleep(0.2)
    queued2 = [j for j in locked_jobs if j.state == "queued"]
    q = queued2[0]
    q.cancel()
    holder.release()
    while any(not j.finished for j in locked_jobs):
        await asyncio.sleep(0.05)
    c3 = [
        ("持锁期间仍有排队任务可被取消", bool(queued2) and q.state == "cancelled"),
        (
            "解锁后其余任务完成：忙等者超时或成功，但被取消者永不执行",
            all(j.state in ("done", "error", "cancelled") for j in locked_jobs)
            and q.started_at is None,
        ),
        ("执行名额全部归还，无活动任务残留", runner.active_count() == 0),
    ]
    report.append(("C3 持锁 + 取消 + 解锁恢复", c3))

    checks = [(text, ok) for _, group in report for text, ok in group]
    return templates.TemplateResponse(
        request,
        "demo_report.html",
        {
            "request": request,
            "title": "演练 C：取消语义（排队可取消 / 运行中不提前回收 / 重复取消幂等）",
            "wall_ms": None,
            "workers": runner.workers,
            "buckets": None,
            "latency": None,
            "samples": [],
            "checks": checks,
            "jobs": [],
            "report_groups": report,
            "kind": "cancel",
        },
    )
