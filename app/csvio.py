"""记录 CSV 的解析与导出。格式：id,quadrat,species,count（含表头）。"""

import csv
import io

HEADER = ["id", "quadrat", "species", "count"]


def parse_csv(text: str) -> dict:
    """解析修订 CSV，返回 {id: {quadrat, species, count}}。格式错误抛 ValueError。"""
    # 容忍 UTF-8 BOM 与 \r\n
    reader = csv.reader(io.StringIO(text.lstrip("﻿")))
    rows = [row for row in reader if any(cell.strip() for cell in row)]
    if not rows:
        raise ValueError("CSV 文件为空")

    header = [h.strip().lower() for h in rows[0]]
    if header != HEADER:
        raise ValueError(
            f"表头必须为 {','.join(HEADER)}，实际为：{','.join(rows[0])}"
        )

    records = {}
    errors = []
    for lineno, row in enumerate(rows[1:], start=2):
        if len(row) != 4:
            errors.append(f"第{lineno}行：应为4列，实际{len(row)}列")
            continue
        rid, quadrat, species, count_s = (c.strip() for c in row)
        if not rid:
            errors.append(f"第{lineno}行：ID 为空")
            continue
        if rid in records:
            errors.append(f"第{lineno}行：ID {rid} 重复")
            continue
        if not quadrat or not species:
            errors.append(f"第{lineno}行：样方/物种不能为空")
            continue
        try:
            count = int(count_s)
        except ValueError:
            errors.append(f"第{lineno}行：株数 {count_s!r} 不是整数")
            continue
        if count < 0:
            errors.append(f"第{lineno}行：株数不能为负数")
            continue
        records[rid] = {"quadrat": quadrat, "species": species, "count": count}

    if errors:
        raise ValueError("；".join(errors))
    return records


def dump_csv(records: dict) -> str:
    """把 {id: record} 导出为 CSV 文本（按 ID 排序）。"""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(HEADER)
    for rid in sorted(records):
        r = records[rid]
        writer.writerow([rid, r["quadrat"], r["species"], r["count"]])
    return buf.getvalue()
