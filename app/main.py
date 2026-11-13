"""植被调查离线修订合并网页：FastAPI 入口。"""

import os
import uuid
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import db
from .csvio import dump_csv, parse_csv
from .merge import FIELD_LABELS, FIELDS, build_final_actions, compute_plan

BASE_DIR = Path(__file__).resolve().parent
SEED_CSV = os.environ.get("INITIAL_CSV", str(BASE_DIR.parent / "data" / "initial_records.csv"))

app = FastAPI(title="植被调查离线修订合并")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals["FIELD_LABELS"] = FIELD_LABELS


@app.on_event("startup")
def startup():
    db.init_db(SEED_CSV)


def redirect(url: str, msg: str = "") -> RedirectResponse:
    if msg:
        sep = "&" if "?" in url else "?"
        from urllib.parse import quote

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
def record_add(rid: str = Form(...), quadrat: str = Form(...), species: str = Form(...), count: str = Form(...)):
    rid, quadrat, species = rid.strip(), quadrat.strip(), species.strip()
    try:
        n = parse_count(count)
        if not rid or not quadrat or not species:
            raise ValueError("ID、样方、物种均不能为空")
        db.add_record(rid, quadrat, species, n)
        return redirect("/", f"已新增记录 {rid}")
    except Exception as e:
        return redirect("/", f"新增失败：{e}")


@app.post("/records/{rid}/update")
def record_update(rid: str, quadrat: str = Form(...), species: str = Form(...), count: str = Form(...)):
    try:
        n = parse_count(count)
        if not quadrat.strip() or not species.strip():
            raise ValueError("样方、物种不能为空")
        if not db.update_record(rid, quadrat.strip(), species.strip(), n):
            raise ValueError(f"记录 {rid} 不存在")
        return redirect("/", f"已更新记录 {rid}")
    except Exception as e:
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
        request, "baseline.html", {"request": request, "baseline": baseline, "msg": msg}
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


# ---------------- 上传修订 / 合并预览 ----------------


@app.post("/merge/upload")
async def merge_upload(baseline_id: int = Form(...), file: UploadFile = File(...)):
    baseline = db.get_baseline(baseline_id)
    if not baseline:
        return redirect("/", "所选基线不存在")
    raw = (await file.read()).decode("utf-8-sig")
    try:
        revision = parse_csv(raw)
    except ValueError as e:
        return redirect("/", f"修订文件解析失败：{e}")

    current = db.get_records()
    plan = compute_plan(baseline["data"], revision, current)
    snapshot = db.get_records_by_ids(plan["involved"])
    token = uuid.uuid4().hex[:12]
    db.save_preview(token, baseline_id, revision, snapshot, plan)
    return redirect(f"/previews/{token}")


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


@app.post("/previews/{token}/confirm")
async def preview_confirm(token: str, request: Request):
    pv = db.get_preview(token)
    if not pv:
        return redirect("/", "预览不存在")
    if pv["status"] != "pending":
        return redirect("/", "该预览已处理，不能重复确认")

    # 1) 过期检测：仅核对计划触及的记录；无关记录变化不影响确认
    fresh = db.get_records_by_ids(pv["plan"]["involved"])
    if fresh != pv["snapshot"]:
        baseline = db.get_baseline(pv["baseline_id"])
        current = db.get_records()
        plan = compute_plan(baseline["data"], pv["revision"], current)
        snapshot = db.get_records_by_ids(plan["involved"])
        db.update_preview_plan(
            token, snapshot, plan, "拟改记录在预览后又被修改或删除，旧预览已作废并按最新当前记录重新比较，请重新核对。"
        )
        return redirect(f"/previews/{token}")

    # 2) 汇总人工裁决
    form = await request.form()
    resolutions = {}
    for conf in pv["plan"]["conflicts"]:
        rid = conf["id"]
        if conf["ctype"] == "fields":
            sel = {}
            for fc in conf["fields"]:
                v = form.get(f"c_{rid}_{fc['field']}")
                if v in ("rev", "cur"):
                    sel[fc["field"]] = v
            if sel:
                resolutions[rid] = sel
        else:
            v = form.get(f"c_{rid}__")
            if v in ("rev", "cur"):
                resolutions[rid] = {"_": v}

    # 3) 合成最终动作并单事务应用；失败则当前数据不变
    try:
        actions = build_final_actions(pv["plan"], resolutions)
    except ValueError as e:
        return redirect(f"/previews/{token}", str(e))
    try:
        db.apply_actions(actions)
    except Exception as e:
        return redirect(f"/previews/{token}", f"应用失败，当前数据未改变：{e}")

    db.set_preview_status(token, "applied", f"已应用 {len(actions)} 项离线改动")
    return redirect("/", f"合并完成：已应用 {len(actions)} 项离线改动")
