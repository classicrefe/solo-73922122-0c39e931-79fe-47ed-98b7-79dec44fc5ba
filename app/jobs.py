"""上传任务的有界执行层。

设计要点（对应修复要求）：

1. **专用、有界的线程池**：上传处理只在 ``UPLOAD_WORKERS``（默认 2）个线程里跑，
   超出容量的任务在 *本模块的队列* 中排队——FastAPI/anyio 处理页面查询的共享
   线程池不被占用，首页与 CSV 导出始终有线程可用。
2. **排队任务可取消**：尚未开始的任务调用 :meth:`JobHandle.cancel` 立即转为
   cancelled，永远不会执行，因此不会迟到生成预览；已开始的任务 cancel 返回
   False，必须等它把数据库事务提交或回滚、关闭连接后才自然结束并释放名额，
   重复取消不会提前回收任何资源。
3. **线程与事件循环桥接**：路由协程把任务提交到本池后 await 一个 asyncio 事件，
   等待期间事件循环线程保持空闲，可继续服务其它页面请求并感知客户端断连。
"""

import asyncio
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from . import db

UPLOAD_WORKERS = int(os.environ.get("UPLOAD_WORKERS", "2"))
# 注册表容量上限，防止异常情况下任务无限堆积（队列本身随 ThreadPoolExecutor 无界，
# 这里只限制同时保留的句柄数，已结束的句柄会被淘汰）。
MAX_TRACKED_JOBS = int(os.environ.get("MAX_TRACKED_JOBS", "200"))


class UploadError(Exception):
    """上传内容本身的问题（解析失败、基线不存在等），与锁竞争无关。"""


# 任务终态
DONE = "done"
ERROR = "error"
CANCELLED = "cancelled"
# 非终态
QUEUED = "queued"
RUNNING = "running"
_TERMINAL = frozenset({DONE, ERROR, CANCELLED})


@dataclass
class JobHandle:
    id: str
    label: str
    created_at: float
    state: str = QUEUED
    result: Any = None
    error: Optional[BaseException] = None
    error_type: str = ""  # "" / "locked" / "input"
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    _future: Any = None
    _event: Optional[asyncio.Event] = None
    _loop: Optional[asyncio.AbstractEventLoop] = None
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def finished(self) -> bool:
        return self.state in _TERMINAL

    def current_state(self) -> str:
        """线程安全的状态快照（状态由工作线程写、事件循环线程读）。"""
        with self._lock:
            return self.state

    def elapsed_ms(self) -> Optional[float]:
        if self.started_at is None or self.finished_at is None:
            return None
        return round((self.finished_at - self.started_at) * 1000, 1)

    def queued_ms(self) -> Optional[float]:
        end = self.started_at or self.finished_at or time.monotonic()
        return round((end - self.created_at) * 1000, 1)

    def cancel(self) -> bool:
        """取消任务。

        - 仍在排队：取消成功，状态转 cancelled 并唤醒等待者，任务体不会执行；
        - 已开始运行：返回 False，任务继续直到事务提交/回滚、连接关闭；
        - 已结束（含重复取消）：返回 False，状态保持不变。
        """
        with self._lock:
            if self.state != QUEUED or self._future is None:
                return False
            if not self._future.cancel():
                # 已经被线程池取走，只能让它跑完
                return False
            self.state = CANCELLED
            self.finished_at = time.monotonic()
        self._signal()
        return True

    def _signal(self) -> None:
        if self._event is None or self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._event.set)


class JobRunner:
    """有界线程池 + 任务注册表。进程内单例（见模块末尾）。"""

    def __init__(self, workers: int):
        self._pool = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="upload-worker"
        )
        self._registry: dict[str, JobHandle] = {}
        self._reg_lock = threading.Lock()
        self._workers = workers

    @property
    def workers(self) -> int:
        return self._workers

    async def submit(
        self, func: Callable, *args, label: str = ""
    ) -> JobHandle:
        """从事件循环线程提交一个上传任务。"""
        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        job = JobHandle(
            id=uuid.uuid4().hex[:12],
            label=label,
            created_at=time.monotonic(),
            _event=event,
            _loop=loop,
        )
        with self._reg_lock:
            # 淘汰已结束的旧句柄，保证注册表有界
            if len(self._registry) >= MAX_TRACKED_JOBS:
                stale = [
                    jid
                    for jid, j in self._registry.items()
                    if j.finished
                ]
                for jid in stale:
                    self._registry.pop(jid, None)
                if len(self._registry) >= MAX_TRACKED_JOBS:
                    raise UploadError("上传任务过多，请稍后再试")
            self._registry[job.id] = job
        job._future = self._pool.submit(self._run, job, func, args)
        return job

    def get(self, job_id: str) -> Optional[JobHandle]:
        with self._reg_lock:
            return self._registry.get(job_id)

    def snapshot(self) -> list[dict]:
        with self._reg_lock:
            jobs = list(self._registry.values())
        return [
            {
                "id": j.id,
                "label": j.label,
                "state": j.state,
                "error_type": j.error_type,
                "queued_ms": j.queued_ms(),
                "elapsed_ms": j.elapsed_ms(),
            }
            for j in sorted(jobs, key=lambda j: j.created_at, reverse=True)
        ]

    def active_count(self) -> int:
        with self._reg_lock:
            return sum(1 for j in self._registry.values() if not j.finished)

    def _run(self, job: JobHandle, func: Callable, args: tuple) -> None:
        # future.cancel() 成功时本函数根本不会被调用；走到这里说明任务已开始，
        # 任何取消请求都只能等它跑完——名额与连接都在 finally 语义内释放。
        with job._lock:
            if job.state == CANCELLED:  # 极端竞态兜底
                job._signal()
                return
            job.state = RUNNING
            job.started_at = time.monotonic()
        try:
            result = func(*args)
        except BaseException as exc:  # 线程内兜底，异常经句柄回传，不吞掉
            with job._lock:
                job.state = ERROR
                job.error = exc
                job.error_type = _classify(exc)
                job.finished_at = time.monotonic()
        else:
            with job._lock:
                job.state = DONE
                job.result = result
                job.finished_at = time.monotonic()
        finally:
            # 任务结束即归还线程名额；连接由 db 层的事务上下文保证已关闭
            job._signal()

    async def aclose(self) -> None:
        """应用关闭：先取消所有排队任务，再等在跑的数据库工作完成提交/回滚。"""
        with self._reg_lock:
            jobs = list(self._registry.values())
        for job in jobs:
            job.cancel()
        await asyncio.get_running_loop().run_in_executor(
            None, self._pool.shutdown, True
        )


def _classify(exc: BaseException) -> str:
    if isinstance(exc, db.LockTimeout):
        return "locked"  # -> 可重试 503
    if isinstance(exc, (db.NotFoundError, UploadError, ValueError)):
        return "input"
    return "internal"


# 进程级单例：上传执行资源有界，与 Web 的共享线程池完全隔离
runner = JobRunner(UPLOAD_WORKERS)
