"""植被调查离线修订合并网页：FastAPI 入口。

并发模型：
- 页面查询（首页、导出、预览）走 FastAPI 共享线程池，只做 WAL 只读短查询，
  不被写锁阻塞；
- /merge/upload 的重活（三方比较 + 写预览）提交到 jobs.runner 的专用有界线程池，
  路由协程仅等待，不占用页面查询线程；
- 写锁忙等 5 秒超时由 db.LockTimeout 统一转成带 Retry-After 的 503。
"""

import asyncio
import os
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
)
from fastapi.templating import Jinja2Templates

from . import db
from .csvio import dump_csv, parse_csv
from .jobs import UploadError, runner
from .merge import FIELD_LABELS, FIELDS

BASE_DIR = Path(__file__).resolve().parent
SEED_CSV = os.environ.get(
    "INITIAL_CSV", str(BASE_DIR.parent / "data" / "initial_records.csv")
)
# 503 Retry-After 建议值（秒），与 SQLite busy_timeout 对齐
RETRY_AFTER = int(os.environ.get("BUSY_TIMEOUT_MS", "5000")) // 1000
DEMO_ENABLED = os.environ.get("DEMO_ENABLED", "1") != "0"

app = FastAPI(title="植被调查离线修订合并")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals["FIELD_LABELS"] = FIELD_LABELS
templates.env.globals["DEMO_ENABLED"] = DEMO_ENABLED


@app.on_event("startup")
def startup():
    db.init_db(SEED_CSV)


@app.on_event("shutdown")
async def shutdown():
    await runner.aclose()


def redirect(url: str, msg: str = "") -> RedirectResponse:
    if msg:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}msg={quote(msg)}"
    return RedirectResponse(url, status_code=303)


def parse_count(raw: str) -> int:
    try:
        count = int(raw)
    except (TypeError, ValueError):
        raise ValueError("株数必须是非负整数")
    if count < 0:
        raise ValueError("株数必须是非负整数")
    return count


def is_api(request: Request) -> bool:
    return request.url.path.startswith("/api/")


@app.exception_handler(db.LockTimeout)
async def lock_timeout_handler(request: Request, exc: db.LockTimeout):
    """写锁忙等 5 秒仍未获得 -> 可重试 503，且不产生任何预览。"""
    headers = {"Retry-After": str(RETRY_AFTER)}
    if is_api(request):
        return JSONResponse(
            status_code=503,
            headers=headers,
            content={
                "detail": "数据库写锁忙，等待 5 秒仍未获取，请稍后重试上传",
                "retry_after": RETRY_AFTER,
            },
        )
    html = (
        '<!DOCTYPE html><html lang="zh-CN"><meta charset="utf-8">'
        '<title>503 服务暂时繁忙</title>'
        '<body style="font-family:sans-serif;padding:40px;line-height:1.8">'
        '<h2>503：数据库正被其它写入占用</h2>'
        '<p>本次上传已等待写锁 5 秒仍未获取，<b>未生成任何预览，数据未改变</b>。</p>'
        f'<p>请至少 {RETRY_AFTER} 秒后重新上传同一文件（该错误可安全重试）。</p>'
        '<p><a href="/">返回首页重新上传</a></p></body>'
    )
    return HTMLResponse(status_code=503, headers=headers, content=html)


# ---------------- 首页／当前记录 ----------------


@app.get("/", response_class=HTMLResponse)
def index(request: Request, msg: str = ""):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "records": db.get_records(),
            "baselines": db.list_baselines(),
            "previews": db.list_previews(),
            "msg": msg,
        },
    )


@app.post("/records/add")
def record_add(
    rid: str = Form(...),
    quadrat: str = Form(...),
    species: str = Form(...),
    count: str = Form(...),
):
    rid, quadrat, species = rid.strip(), quadrat.strip(), species.strip()
    try:
        n = parse_count(count)
        if not rid or not quadrat or not species:
            raise ValueError("ID、样方、物种均不能为空")
        db.add_record(rid, quadrat, species, n)
        return redirect("/", f"已新增记录 {rid}")
    except ValueError as e:
        return redirect("/", f"新增失败：{e}")


@app.post("/records/{rid}/update")
def record_update(
    rid: str,
    quadrat: str = Form(...),
    species: str = Form(...),
    count: str = Form(...),
):
    try:
        n = parse_count(count)
        if not quadrat.strip() or not species.strip():
            raise ValueError("样方、物种不能为空")
        if not db.update_record(rid, quadrat.strip(), species.strip(), n):
            raise ValueError(f"记录 {rid} 不存在")
        return redirect("/", f"已更新记录 {rid}")
    except ValueError as e:
        return redirect("/", f"更新失败：{e}")


@app.post("/records/{rid}/delete")
def record_delete(rid: str):
    if db.delete_record(rid):
        return redirect("/", f"已删除记录 {rid}")
    return redirect("/", f"记录 {rid} 不存在")


# ---------------- 导出 / 基线 ----------------


@app.get("/export.csv")
def export_current():
    return PlainTextResponse(
        dump_csv(db.get_records()),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=current_records.csv"},
    )


@app.post("/baselines")
def baseline_create(name: str = Form("")):
    records = db.get_records()
    bid = db.save_baseline(name.strip() or f"基线（{db.now()}）", records)
    return redirect(f"/baselines/{bid}", "基线已保存，可下载对应 CSV 交给外业队")


@app.get("/baselines/{bid}", response_class=HTMLResponse)
def baseline_detail(request: Request, bid: int, msg: str = ""):
    baseline = db.get_baseline(bid)
    if not baseline:
        return redirect("/", "基线不存在")
    return templates.TemplateResponse(
        request,
        "baseline.html",
        {"request": request, "baseline": baseline, "msg": msg},
    )


@app.get("/baselines/{bid}/csv")
def baseline_csv(bid: int):
    baseline = db.get_baseline(bid)
    if not baseline:
        return redirect("/", "基线不存在")
    return PlainTextResponse(
        dump_csv(baseline["data"]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=baseline_{bid}.csv"},
    )


# ---------------- 上传修订 / 合并预览（有界异步执行） ----------------


def process_upload(baseline_id: int, raw_bytes: bytes) -> str:
    """上传任务体：在专用上传线程内执行。

    解析与三方比较都在同一个 BEGIN IMMEDIATE 事务内完成并落库预览；
    任何失败（含锁超时）整体回滚，不留预览。返回预览 token。
    """
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise UploadError("修订文件必须是 UTF-8 编码的 CSV") from exc
    revision = parse_csv(text)  # ValueError -> UploadError 语义
    try:
        return db.create_preview(baseline_id, revision)
    except db.NotFoundError as exc:
        raise UploadError(str(exc)) from exc


async def _wait_job(request: Request, job):
    """等待上传任务结束，同时监听客户端断连（断连即尝试取消排队任务）。"""
    event = job._event
    disconnected = False
    while not job.finished:
        try:
            await asyncio.wait_for(event.wait(), timeout=0.2)
        except asyncio.TimeoutError:
            if not disconnected and await request.is_disconnected():
                disconnected = True
                job.cancel()  # 排队中则取消；运行中返回 False，继续等其完成
        finally:
            event.clear()
    return disconnected


def _job_error_response(request: Request, job):
    exc = job.error
    if job.error_type == "locked":
        # 交给全局 LockTimeout 处理器，保证与同步写路径行为一致
        raise db.LockTimeout(str(exc))
    if isinstance(exc, (UploadError, ValueError)):
        if is_api(request):
            return JSONResponse(status_code=400, content={"detail": str(exc)})
        return redirect("/", f"修订文件处理失败：{exc}")
    # 未知错误：返回 500（锁等待路径不会走到这里）
    detail = f"服务器内部错误：{type(exc).__name__}: {exc}"
    if is_api(request):
        return JSONResponse(status_code=500, content={"detail": detail})
    return HTMLResponse(status_code=500, content=f"<h2>500</h2><p>{detail}</p>")


@app.post("/merge/upload")
async def merge_upload(
    request: Request,
    baseline_id: int = Form(...),
    file: UploadFile = File(...),
):
    # 读取上传内容发生在请求协程内（受背压约束），之后的处理才进入有界线程池
    raw = await file.read()
    try:
        job = await runner.submit(
            process_upload, baseline_id, raw, label=file.filename or "revision.csv"
        )
    except UploadError as exc:
        return redirect("/", str(exc))

    disconnected = await _wait_job(request, job)
    state = job.current_state()

    if state == "cancelled":
        if is_api(request):
            return JSONResponse(
                status_code=409,
                content={"detail": "任务在开始处理前已取消，未生成预览"},
            )
        return redirect("/", "上传任务已取消，未生成预览")

    if state == "error":
        return _job_error_response(request, job)

    token = job.result
    if is_api(request):
        return JSONResponse(
            status_code=202,
            content={"job_id": job.id, "token": token, "preview_url": f"/previews/{token}"},
        )
    if disconnected:
        return redirect(
            "/",
            f"浏览器已断开连接，但上传已处理完成，预览 {token} 仍可打开查看",
        )
    return redirect(f"/previews/{token}")


# ---------------- 任务取消 / 查询（供演示页与脚本调用） ----------------


@app.post("/api/jobs/{job_id}/cancel")
async def job_cancel(job_id: str):
    job = runner.get(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"detail": "任务不存在或已清理"})
    ok = job.cancel()
    return JSONResponse(
        status_code=200,
        content={
            "job_id": job_id,
            "cancelled": ok,
            "state": job.state,
            "note": (
                "任务尚未开始，已取消，不会生成预览"
                if ok
                else "任务已开始或已结束：已开始的数据库工作会完成提交或回滚，不能提前回收"
            ),
        },
    )


@app.get("/api/jobs")
def job_list():
    return JSONResponse(
        status_code=200,
        content={"workers": runner.workers, "active": runner.active_count(), "jobs": runner.snapshot()},
    )


# ---------------- 预览 / 确认 / 放弃 ----------------


@app.get("/previews/{token}", response_class=HTMLResponse)
def preview_detail(request: Request, token: str, msg: str = ""):
    pv = db.get_preview(token)
    if not pv:
        return redirect("/", "预览不存在")
    baseline = db.get_baseline(pv["baseline_id"])
    return templates.TemplateResponse(
        request,
        "preview.html",
        {
            "request": request,
            "pv": pv,
            "baseline": baseline,
            "plan": pv["plan"],
            "FIELDS": FIELDS,
            "msg": msg,
        },
    )


@app.post("/previews/{token}/discard")
def preview_discard(token: str):
    pv = db.get_preview(token)
    if pv and pv["status"] == "pending":
        db.set_preview_status(token, "discarded", "已放弃")
    return redirect("/", "已放弃该合并预览")


def _collect_resolutions(form) -> dict:
    resolutions = {}
    # 键形如 c_{rid}_{field}（字段级冲突）或 c_{rid}__（整行冲突）
    for key, value in form.multi_items():
        if not key.startswith("c_") or value not in ("rev", "cur"):
            continue
        body = key[2:]
        if body.endswith("__"):
            resolutions[body[:-2]] = {"_": value}
        else:
            rid, _, field = body.partition("_")
            resolutions.setdefault(rid, {})[field] = value
    return resolutions


@app.post("/previews/{token}/confirm")
async def preview_confirm(token: str, request: Request):
    pv = db.get_preview(token)
    if not pv:
        return redirect("/", "预览不存在")
    if pv["status"] != "pending":
        return redirect("/", "该预览已处理，不能重复确认")

    form = await request.form()
    resolutions = _collect_resolutions(form)

    # 过期核对 + 裁决合成 + 整批应用在同一个写事务内完成；锁超时 -> 503
    outcome = await asyncio.get_running_loop().run_in_executor(
        None, db.confirm_preview, token, resolutions
    )
    kind = outcome["outcome"]
    if kind == "missing":
        return redirect("/", "预览不存在")
    if kind == "already":
        return redirect("/", "该预览已处理，不能重复确认")
    if kind == "stale":
        return redirect(f"/previews/{token}")
    if kind == "invalid":
        return redirect(f"/previews/{token}", outcome["message"])
    # applied 状态已在同一个写事务内与数据改动一起提交，无需再写一次
    n = outcome["count"]
    return redirect("/", f"合并完成：已应用 {n} 项离线改动")


# ---------------- 受控演示（持锁 / 并发 / 取消） ----------------

if DEMO_ENABLED:
    from .demo import router as demo_router

    app.include_router(demo_router)
