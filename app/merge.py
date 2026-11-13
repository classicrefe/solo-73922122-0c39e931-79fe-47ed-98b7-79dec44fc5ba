"""三方向合并核心逻辑：基线(base) / 离线修订(rev) / 当前记录(cur)。

逐字段比较：
- 仅修订侧改动的字段 -> 自动采用修订值
- 仅当前侧改动的字段 -> 自动保留当前值
- 两侧改成相同值      -> 自动保留一份
- 两侧改成不同值 / 删除遇修改 / 同ID新增内容不同 -> 冲突，交人工选择
"""

FIELDS = ("quadrat", "species", "count")
FIELD_LABELS = {"quadrat": "样方", "species": "物种", "count": "株数"}


def compute_plan(base: dict, rev: dict, cur: dict) -> dict:
    """计算合并计划。base/rev/cur 均为 {id: {quadrat, species, count}}。

    返回 {"auto": [...], "conflicts": [...], "involved": [...]}
    involved 为计划触及的记录 ID（用于确认时的过期检测）。
    """
    auto = []
    conflicts = []
    involved = set()

    for rid in sorted(set(base) | set(rev) | set(cur)):
        b = base.get(rid)
        r = rev.get(rid)
        c = cur.get(rid)

        if b is not None and r is not None and c is not None:
            # 三方都存在：逐字段合并
            if r == b:
                continue  # 修订未改动该记录，保留当前
            merged = {}
            field_conflicts = []
            taken = {}  # 自动决定的字段来源，供预览展示
            for f in FIELDS:
                bv, rv, cv = b[f], r[f], c[f]
                r_changed = rv != bv
                c_changed = cv != bv
                if r_changed and c_changed and rv != cv:
                    field_conflicts.append(
                        {"field": f, "base": bv, "rev": rv, "cur": cv}
                    )
                elif r_changed:
                    merged[f] = rv
                    taken[f] = {"src": "rev", "base": bv, "value": rv}
                else:
                    merged[f] = cv
                    if c_changed:
                        taken[f] = {"src": "cur", "base": bv, "value": cv}
            if field_conflicts:
                conflicts.append(
                    {
                        "id": rid,
                        "ctype": "fields",
                        "base": b,
                        "rev": r,
                        "cur": c,
                        "merged": merged,  # 无冲突字段已合并好的部分
                        "fields": field_conflicts,
                    }
                )
                involved.add(rid)
            elif merged != c:
                auto.append(
                    {"action": "update", "id": rid, "from": c, "to": merged, "taken": taken}
                )
                involved.add(rid)

        elif b is not None and r is None and c is not None:
            # 修订中缺行 = 离线删除
            if c == b:
                auto.append({"action": "delete", "id": rid, "record": b})
                involved.add(rid)
            else:
                # 删除 vs 当前已修改 -> 冲突
                conflicts.append(
                    {"id": rid, "ctype": "delete_vs_modify", "base": b, "rev": None, "cur": c}
                )
                involved.add(rid)

        elif b is not None and r is not None and c is None:
            # 当前已删除；修订未改动则删除生效，改动过则冲突
            if r == b:
                continue
            conflicts.append(
                {"id": rid, "ctype": "modify_vs_delete", "base": b, "rev": r, "cur": None}
            )
            involved.add(rid)

        elif b is not None and r is None and c is None:
            continue  # 双方都删除，同一改动只留一份

        elif b is None and r is not None and c is None:
            # 纯离线新增
            auto.append({"action": "insert", "id": rid, "record": r})
            involved.add(rid)

        elif b is None and r is not None and c is not None:
            # 双方各自新增同一 ID
            if r == c:
                continue  # 内容相同，只留一份
            conflicts.append(
                {"id": rid, "ctype": "add_add", "base": None, "rev": r, "cur": c}
            )
            involved.add(rid)

        else:
            # 基线与修订都没有，仅当前新增 -> 不动
            continue

    return {"auto": auto, "conflicts": conflicts, "involved": sorted(involved)}


def build_final_actions(plan: dict, resolutions: dict) -> list:
    """把自动合并结果与人工冲突裁决合成为最终动作列表。

    resolutions: {record_id: {"_": "rev"|"cur"} 或 {field: "rev"|"cur", ...}}
    所有冲突必须全部裁决，否则抛 ValueError。
    """
    actions = [dict(a) for a in plan["auto"]]

    for conf in plan["conflicts"]:
        rid = conf["id"]
        ctype = conf["ctype"]
        choice = resolutions.get(rid)
        if not choice:
            raise ValueError(f"记录 {rid} 的冲突尚未裁决")

        if ctype == "fields":
            final = dict(conf["merged"])
            for fc in conf["fields"]:
                sel = choice.get(fc["field"])
                if sel not in ("rev", "cur"):
                    raise ValueError(f"记录 {rid} 字段 {fc['field']} 的冲突尚未裁决")
                final[fc["field"]] = fc[sel]
            if final != conf["cur"]:
                actions.append(
                    {"action": "update", "id": rid, "from": conf["cur"], "to": final}
                )

        elif ctype == "delete_vs_modify":
            # 修订删除 vs 当前修改：rev=删除，cur=保留当前值
            sel = choice.get("_")
            if sel == "rev":
                actions.append({"action": "delete", "id": rid})
            elif sel != "cur":
                raise ValueError(f"记录 {rid} 的冲突尚未裁决")

        elif ctype == "modify_vs_delete":
            # 修订修改 vs 当前删除：rev=按修订值恢复，cur=保持删除
            sel = choice.get("_")
            if sel == "rev":
                actions.append({"action": "insert", "id": rid, "record": conf["rev"]})
            elif sel != "cur":
                raise ValueError(f"记录 {rid} 的冲突尚未裁决")

        elif ctype == "add_add":
            # 同 ID 新增内容不同：rev=采用修订版本覆盖，cur=保留当前版本
            sel = choice.get("_")
            if sel == "rev":
                actions.append(
                    {"action": "update", "id": rid, "from": conf["cur"], "to": conf["rev"]}
                )
            elif sel != "cur":
                raise ValueError(f"记录 {rid} 的冲突尚未裁决")

    return actions
