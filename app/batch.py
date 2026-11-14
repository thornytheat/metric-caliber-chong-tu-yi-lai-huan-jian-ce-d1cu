"""整批候选版本：先按整批关系演练，再一次性切换。

为什么需要批次：
    候选版本逐个发布时，依赖解析仍按「当前已发布版本」进行。两个候选
    `a@2 -> metric:b`、`b@2 -> metric:a` 单独看都不成环，一起上线才成环；
    同理，裸引用重定向、跨候选的未发布依赖、同名异口径在整批语义下才完整。

本模块的两条规则：
1) 演练（rehearse）——把整批候选视为「同时已发布」构造一个批次世界，
   在批次世界里解析依赖、找环、分组口径、算传递下游；任何阻断项都拦住整批。
2) 切换（apply）——重新演练确认仍通过后，在同一个 SQLite 独占事务里把
   全部候选 published=true；任一步失败整体回滚，旧版本保持发布状态。
   同一 batch_id / idempotency_key 的重试复用同一条记录与同一组候选，
   不新增指标版本、不新增批次/明细记录。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3

from . import conflict, expr, gate, graph as graph_mod, registry, report, store

# 阻断规则码（演练/切换统一口径）
BLOCK_CANDIDATE_NOT_FOUND = "candidate_not_found"
BLOCK_EXPRESSION_INVALID = "expression_invalid"
BLOCK_UNPUBLISHED_REFERENCE = "unpublished_reference"
BLOCK_MISSING_REFERENCE = "missing_reference"
BLOCK_CYCLE_INTRODUCED = "cycle_introduced"
BLOCK_CYCLE_MEMBER = "cycle_member"
BLOCK_CALIBER_CONFLICT = "caliber_conflict"
BLOCK_CANDIDATE_CHANGED = "candidate_changed"
BLOCK_REHEARSAL_FAILED = "rehearsal_failed"
BLOCK_APPLY_FAILED = "apply_failed"

_CODE_PRIORITY = {
    BLOCK_CYCLE_INTRODUCED: 0,
    BLOCK_CYCLE_MEMBER: 1,
    "aggregation_changed": 2,
    "dimension_removed": 3,
    BLOCK_UNPUBLISHED_REFERENCE: 4,
    "requires_sync": 5,
}


class BatchError(ValueError):
    """批次入参非法（HTTP 400）。"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class BatchNotFound(BatchError):
    """批次不存在（HTTP 404）。"""


class BatchConflict(BatchError):
    """同一幂等键被用于不同候选集合（HTTP 409）。"""


# --------------------------------------------------------------------------- 工具


def _digest(payload: object) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def parse_candidate_key(raw: str) -> tuple[str, int]:
    text = str(raw or "").strip()
    name, sep, version = text.partition("@")
    name = name.strip()
    version = version.strip()
    if not sep or not name or not version.isdigit() or int(version) < 1:
        raise BatchError(f"候选版本格式必须是 name@version：{raw!r}")
    return name, int(version)


def canonical_candidates(raw_candidates: list[str]) -> list[str]:
    """规整、去重并保持输入顺序。"""
    keys: list[str] = []
    seen: set[str] = set()
    for raw in raw_candidates or []:
        name, version = parse_candidate_key(raw)
        key = graph_mod.node_key(name, version)
        if key not in seen:
            seen.add(key)
            keys.append(key)
    if not keys:
        raise BatchError("候选版本列表不能为空")
    return keys


def request_fingerprint(candidates: list[str]) -> str:
    return _digest({"candidates": sorted(candidates)})


def candidate_fingerprint(metrics: list[dict], candidates: list[str]) -> str:
    """候选版本自身的口径内容指纹：用于切换前确认「演练的就是要上线的」。

    只比口径内容（表达式/聚合/维度），不比 published —— 别的流程先把同口径
    版本发布出去，不应让本批被判成「内容被改了」。
    """
    index = registry.index_by_key(metrics)
    rows = []
    for key in sorted(candidates):
        name, version = graph_mod.split_key(key)
        metric = index.get((name, version))
        if metric is None:
            rows.append([key, None])
            continue
        rows.append(
            [
                key,
                str(metric.get("expression", "")).strip(),
                str(metric.get("aggregation", "")).strip(),
                sorted(str(dim) for dim in (metric.get("dimensions") or [])),
            ]
        )
    return _digest(rows)


def registry_fingerprint(metrics: list[dict]) -> str:
    payload = [
        [
            metric["name"],
            int(metric["version"]),
            str(metric.get("expression", "")).strip(),
            str(metric.get("aggregation", "")).strip(),
            sorted(str(dim) for dim in (metric.get("dimensions") or [])),
            bool(metric.get("published")),
        ]
        for metric in sorted(metrics, key=lambda item: (item["name"], int(item["version"])))
    ]
    return _digest(payload)


def _baseline_of(metrics: list[dict], name: str, version: int) -> dict | None:
    """候选版本对应的已发布基线（同名、版本不同、最新一个已发布版本）。"""
    candidates = [
        metric
        for metric in registry.versions_of(metrics, name)
        if metric["published"] and int(metric["version"]) != int(version)
    ]
    return candidates[-1] if candidates else None


def _changes(baseline: dict | None, metric: dict) -> dict:
    if baseline is None:
        # 全新指标名：没有可比基线，不算「聚合变更」，也不存在被移除的维度
        return {
            "baseline_key": None,
            "expression_changed": True,
            "aggregation_changed": False,
            "removed_dims": [],
            "added_dims": list(metric.get("dimensions") or []),
            "old_aggregation": None,
            "new_aggregation": str(metric.get("aggregation", "")),
        }
    old_dims = list(baseline.get("dimensions") or [])
    new_dims = list(metric.get("dimensions") or [])
    return {
        "baseline_key": graph_mod.node_key(baseline["name"], baseline["version"]),
        "expression_changed": registry.normalized_of(baseline)
        != registry.normalized_of(metric),
        "aggregation_changed": str(baseline.get("aggregation", ""))
        != str(metric.get("aggregation", "")),
        "removed_dims": [dim for dim in old_dims if dim not in new_dims],
        "added_dims": [dim for dim in new_dims if dim not in old_dims],
        "old_aggregation": str(baseline.get("aggregation", "")),
        "new_aggregation": str(metric.get("aggregation", "")),
    }


def _blocker(code: str, detail: str, **extra) -> dict:
    payload = {"code": code, "detail": detail}
    payload.update(extra)
    return payload


# --------------------------------------------------------------------------- 演练


def rehearse(
    metrics: list[dict],
    candidates: list[str],
    previous_failures: list[dict] | None = None,
) -> dict:
    """按整批关系演练：候选视为同时已发布，列出冲突/环/受影响下游与阻断项。"""
    metrics = [registry.with_normalized(metric) for metric in metrics]
    index = registry.index_by_key(metrics)
    keys = canonical_candidates(candidates)

    pending: list[str] = []
    already: list[str] = []
    missing: list[str] = []
    for key in keys:
        name, version = graph_mod.split_key(key)
        metric = index.get((name, version))
        if metric is None:
            missing.append(key)
        elif metric["published"]:
            already.append(key)
        else:
            pending.append(key)

    pending_set = set(pending)
    all_set = set(keys)

    # 批次世界：只把本批「待发布」候选临时视为已发布，其他保持原状。
    world = []
    for metric in metrics:
        key = graph_mod.node_key(metric["name"], metric["version"])
        if key in pending_set:
            metric = {**metric, "published": True}
        world.append(metric)

    world_graph = graph_mod.build_graph(world)
    base_graph = graph_mod.build_graph(metrics)
    world_cycles = graph_mod.find_cycles(world_graph)
    base_cycle_sigs = {
        frozenset(cycle["path"][:-1]) for cycle in graph_mod.find_cycles(base_graph)
    }
    world_cycle_members = graph_mod.cycle_nodes(world_graph)

    blockers: list[dict] = []
    for key in missing:
        blockers.append(
            _blocker(
                BLOCK_CANDIDATE_NOT_FOUND,
                f"候选版本 {key} 尚未登记，批次不能凭空新增版本",
                key=key,
            )
        )

    # ------------------------------------------------------------------ 逐候选分析
    per_candidate: dict[str, dict] = {}
    for key in keys:
        name, version = graph_mod.split_key(key)
        metric = index.get((name, version))
        if metric is None:
            per_candidate[key] = {
                "key": key,
                "name": name,
                "version": version,
                "status": "missing",
                "expression": None,
                "normalized": None,
                "downstream": {},
                "unpublished_refs": [],
                "missing_refs": [],
                "cycle_paths": [],
                "cycle_new": False,
                "item_blockers": [
                    _blocker(
                        BLOCK_CANDIDATE_NOT_FOUND,
                        f"候选版本 {key} 尚未登记",
                        key=key,
                    )
                ],
                "changes": _changes(None, {"dimensions": [], "aggregation": ""}),
                "baseline": None,
            }
            continue

        baseline = _baseline_of(metrics, name, version)
        changes = _changes(baseline, metric)

        # 候选自身的引用解析（批次世界语义）
        refs_unpublished: list[str] = []
        refs_missing: list[str] = []
        analysis = gate.reference_findings(metric, world)
        for finding in analysis:
            if finding["published"] is False:
                refs_unpublished.append(finding["target"])
            elif finding["reason"] in (
                "version_not_found",
                "metric_not_found",
                "no_published_version",
            ):
                refs_missing.append(finding["target"])

        cycle_paths = [
            cycle["path"] for cycle in world_cycles if key in cycle["path"][:-1]
        ]
        cycle_new = any(
            frozenset(path[:-1]) not in base_cycle_sigs for path in cycle_paths
        )
        # 候选本身不在环里、但依赖了环内指标，同样无法确定取数顺序
        cycle_upstream = sorted(
            {
                item["key"]
                for item in graph_mod.upstream(world_graph, key)
            }
            & world_cycle_members
        )

        item_blockers: list[dict] = []
        for path in cycle_paths:
            code = BLOCK_CYCLE_INTRODUCED if cycle_new else BLOCK_CYCLE_MEMBER
            item_blockers.append(
                _blocker(
                    code,
                    f"{key} 在批次世界里处于依赖环：" + " → ".join(path),
                    key=key,
                )
            )
        if cycle_upstream and not cycle_paths:
            item_blockers.append(
                _blocker(
                    BLOCK_CYCLE_MEMBER,
                    f"{key} 依赖了环内指标 {('、'.join(cycle_upstream))}，无法确定取数顺序",
                    key=key,
                )
            )
        for target in refs_unpublished:
            item_blockers.append(
                _blocker(
                    BLOCK_UNPUBLISHED_REFERENCE,
                    f"{key} 依赖 {target}，该版本不在本批且尚未发布",
                    key=key,
                )
            )
        for target in refs_missing:
            item_blockers.append(
                _blocker(
                    BLOCK_MISSING_REFERENCE,
                    f"{key} 引用了不存在或没有已发布版本的 {target}",
                    key=key,
                )
            )

        downstream = {
            item["key"]: item
            for item in graph_mod.downstream(world_graph, key)
        }

        per_candidate[key] = {
            "key": key,
            "name": name,
            "version": version,
            "status": "already_published" if key in already else "pending",
            "expression": metric["expression"],
            "normalized": registry.normalized_of(metric),
            "aggregation": metric.get("aggregation", ""),
            "dimensions": list(metric.get("dimensions") or []),
            "published_before": bool(metric["published"]),
            "baseline": (
                {
                    "key": changes["baseline_key"],
                    "expression": baseline["expression"],
                    "normalized": registry.normalized_of(baseline),
                    "aggregation": baseline.get("aggregation", ""),
                    "dimensions": list(baseline.get("dimensions") or []),
                }
                if baseline
                else None
            ),
            "changes": changes,
            "cycle_paths": cycle_paths,
            "cycle_new": cycle_new,
            "cycle_upstream": cycle_upstream,
            "unpublished_refs": refs_unpublished,
            "missing_refs": refs_missing,
            "downstream": downstream,
            "item_blockers": item_blockers,
        }

    # 阻断项里也包含候选自身口径非法（演练世界会记录 unresolved）
    for key in pending:
        name, version = graph_mod.split_key(key)
        metric = index.get((name, version))
        analysis = expr.analyze_expression(metric["expression"]) if metric else {"ok": False}
        if metric is not None and not analysis.get("ok", False):
            per_candidate[key]["item_blockers"].append(
                _blocker(
                    BLOCK_EXPRESSION_INVALID,
                    f"{key} 表达式无法解析",
                    key=key,
                )
            )

    # ------------------------------------------------------------------ 同名异口径
    # 只看本批真正要切换的候选；已发布候选不参与口径阻断
    touched_names = {graph_mod.split_key(key)[0] for key in pending}
    conflicts: list[dict] = []
    for name in sorted(touched_names):
        groups = conflict.caliber_groups(world, name)
        if len(groups) <= 1:
            continue
        baseline = None
        for metric in registry.versions_of(metrics, name):
            if metric["published"]:
                baseline = metric
        conflict_entry = {
            "name": name,
            "variant_count": len(groups),
            "groups": groups,
            "differences": conflict.difference_summary(groups),
            "summary": (
                f"{name} 在批次世界里归一化后仍有 {len(groups)} 组不同口径"
            ),
            "candidates": [key for key in keys if graph_mod.split_key(key)[0] == name],
        }
        conflicts.append(conflict_entry)

        baseline_normalized = registry.normalized_of(baseline) if baseline else None
        for key in pending:
            if graph_mod.split_key(key)[0] != name:
                continue
            candidate = per_candidate.get(key)
            if candidate is None:
                continue
            if baseline_normalized is None or candidate["normalized"] != baseline_normalized:
                blocker = _blocker(
                    BLOCK_CALIBER_CONFLICT,
                    (
                        f"{name} 在批次世界里归一化后为 {len(groups)} 组不同口径；"
                        f"{key} 口径 {candidate['normalized']} 与"
                        f"已发布基线 "
                        f"{('口径 ' + baseline_normalized) if baseline_normalized else '（无已发布基线）'}"
                        f"不一致"
                    ),
                    key=key,
                    name=name,
                )
                candidate["item_blockers"].append(blocker)

    for key in pending:
        blockers.extend(per_candidate[key]["item_blockers"])

    # ------------------------------------------------------------------ 受影响下游
    def _candidate_summary(key: str) -> dict:
        candidate = per_candidate[key]
        changes = candidate["changes"]
        cycle_blocked = bool(candidate["cycle_paths"]) or bool(candidate["cycle_upstream"])
        cycle_reason = (
            "候选在批次世界里处于依赖环，下游无法确定取数顺序"
            if candidate["cycle_paths"]
            else (
                "候选依赖了环内指标，下游无法取数"
                if candidate["cycle_upstream"]
                else ""
            )
        )
        return {
            "key": key,
            "removed_dims": changes["removed_dims"],
            "aggregation_changed": changes["aggregation_changed"],
            "aggregation": (
                changes["old_aggregation"] or "",
                changes["new_aggregation"] or "",
            ),
            "cycle_blocked": cycle_blocked,
            "cycle_reason": cycle_reason,
            "unpublished_refs": candidate["unpublished_refs"],
        }

    external: dict[str, dict] = {}
    internal_affected: dict[str, dict] = {}
    for key in pending:
        candidate = per_candidate[key]
        for target_key, item in candidate["downstream"].items():
            summary = _candidate_summary(key)
            if target_key in all_set:
                # 同批候选之间的依赖：单独判定，breaking 也要拦住整批
                target = per_candidate.get(target_key) or {}
                entry = internal_affected.get(target_key)
                if entry is None:
                    entry = {
                        "key": target_key,
                        "name": target.get("name", graph_mod.split_key(target_key)[0]),
                        "version": target.get("version", graph_mod.split_key(target_key)[1]),
                        "expression": target.get("expression") or "",
                        "normalized": target.get("normalized") or "",
                        "dimensions": list(target.get("dimensions") or []),
                        "owner": "",
                        "published": False,
                        "depth": item["depth"],
                        "in_batch": True,
                        "affected_by": [],
                    }
                    internal_affected[target_key] = entry
                else:
                    entry["depth"] = min(entry["depth"], item["depth"])
                verdict = gate._verdict(
                    entry=entry,
                    removed_dims=summary["removed_dims"],
                    aggregation_changed=summary["aggregation_changed"],
                    aggregation=summary["aggregation"],
                    cycle_blocked=summary["cycle_blocked"],
                    cycle_reason=summary["cycle_reason"],
                    unpublished_refs=summary["unpublished_refs"],
                )
                entry["affected_by"].append(
                    {
                        "candidate": key,
                        "code": verdict["code"],
                        "reason": verdict["reason"],
                        "evidence": verdict["evidence"],
                    }
                )
                continue

            target_name, target_version = graph_mod.split_key(target_key)
            entry = external.get(target_key)
            if entry is None:
                metric = world_graph.nodes.get(target_key, {})
                entry = {
                    "key": target_key,
                    "name": metric.get("name", target_name),
                    "version": metric.get("version", target_version),
                    "expression": metric.get("expression", ""),
                    "normalized": registry.normalized_of(metric),
                    "dimensions": list(metric.get("dimensions") or []),
                    "owner": metric.get("owner", ""),
                    "published": bool(metric.get("published")),
                    "depth": item["depth"],
                    "in_batch": False,
                    "affected_by": [],
                }
                external[target_key] = entry
            else:
                entry["depth"] = min(entry["depth"], item["depth"])

            verdict = gate._verdict(
                entry=entry,
                removed_dims=summary["removed_dims"],
                aggregation_changed=summary["aggregation_changed"],
                aggregation=summary["aggregation"],
                cycle_blocked=summary["cycle_blocked"],
                cycle_reason=summary["cycle_reason"],
                unpublished_refs=summary["unpublished_refs"],
            )
            entry["affected_by"].append(
                {
                    "candidate": key,
                    "code": verdict["code"],
                    "reason": verdict["reason"],
                    "evidence": verdict["evidence"],
                }
            )

    def _finalize(entries: dict[str, dict]) -> list[dict]:
        rows: list[dict] = []
        for entry in entries.values():
            codes = [item["code"] for item in entry["affected_by"]]
            entry["code"] = min(codes, key=lambda code: _CODE_PRIORITY.get(code, 99))
            entry["reason"] = next(
                item["reason"]
                for item in entry["affected_by"]
                if item["code"] == entry["code"]
            )
            rows.append(entry)
        rows.sort(key=lambda item: (item["depth"], item["key"]))
        return rows

    downstream = _finalize(external)
    internal_affected_rows = _finalize(internal_affected)
    for entry in internal_affected_rows:
        if entry["code"] == "requires_sync":
            continue
        target = per_candidate.get(entry["key"]) or {}
        if target.get("cycle_paths"):
            # 环本身已经作为阻断项列出，不再重复报「同批下游 breaking」
            continue
        blockers.append(
            _blocker(
                "internal_breaking",
                (
                    f"同批候选 {entry['key']} 是 {entry['affected_by'][0]['candidate']} "
                    f"的下游，判定为 {entry['code']}：{entry['reason']}"
                ),
                key=entry["key"],
                name=entry["name"],
            )
        )

    # 同批候选之间的直接依赖（批次内部关系），含裸引用在批次世界里的重定向
    internal = []
    for key in pending:
        metric = index.get(graph_mod.split_key(key))
        analysis = expr.analyze_expression(metric["expression"]) if metric else {"ok": False}
        for ref in (analysis.get("metrics") or []):
            ref_name = ref["name"]
            if ref["version"] is not None:
                target = graph_mod.node_key(ref_name, ref["version"])
                if target in pending_set:
                    internal.append({"from": key, "to": target, "kind": "explicit"})
                continue
            resolved = world_graph.deps.get(key, [])
            for target in resolved:
                target_name, _ = graph_mod.split_key(target)
                if target_name != ref_name:
                    continue
                if target in pending_set:
                    internal.append(
                        {
                            "from": key,
                            "to": target,
                            "kind": "bare_reference",
                            "detail": (
                                f"裸引用 metric:{ref_name} 在批次世界里解析到 "
                                f"{target}（不是原先的已发布版本）"
                            ),
                        }
                    )
    internal.sort(key=lambda item: (item["from"], item["to"], item["kind"]))

    # 逐候选的下游/breaking 统计与证据（含同批内部受影响）
    for key in keys:
        candidate = per_candidate[key]
        related = [entry for entry in downstream if any(
            item["candidate"] == key for item in entry["affected_by"]
        )]
        internal_related = [
            entry for entry in internal_affected_rows if any(
                item["candidate"] == key for item in entry["affected_by"]
            )
        ]
        candidate["affected_downstream"] = [entry["key"] for entry in related]
        candidate["affected_internal"] = [entry["key"] for entry in internal_related]
        candidate["breaking"] = [
            entry["key"]
            for entry in related + internal_related
            if entry["code"] != "requires_sync"
        ]
        candidate["breaking_count"] = len(candidate["breaking"])
        candidate["downstream_count"] = len(related) + len(internal_related)
        candidate.pop("downstream", None)

    blockers = _dedupe_blockers(blockers)
    breaking_count = sum(1 for entry in downstream if entry["code"] != "requires_sync")
    requires_sync_count = len(downstream) - breaking_count

    # 失败后的重试：历史失败保留在同一记录里，不新增行
    failures = [dict(attempt) for attempt in (previous_failures or [])]

    notes: list[str] = []
    if missing:
        notes.append("候选中有未登记版本：" + "、".join(missing))
    if already:
        notes.append("候选已在发布状态（重试时为幂等无操作）：" + "、".join(already))
    if conflicts:
        notes.append(f"批次世界里 {len(conflicts)} 个指标名存在同名异口径")
    cycle_list = [
        {
            "path": cycle["path"],
            "length": cycle["length"],
            "names": cycle["names"],
            "candidates": [
                key for key in keys if key in cycle["path"][:-1]
            ],
            "is_new": frozenset(cycle["path"][:-1]) not in base_cycle_sigs,
        }
        for cycle in world_cycles
        if any(key in cycle["path"][:-1] for key in keys)
    ]
    if cycle_list:
        notes.append(f"批次世界里 {len(cycle_list)} 条环涉及候选版本")
    notes.append(
        f"传递受影响下游 {len(downstream)} 个，其中 breaking {breaking_count} 个"
    )

    return {
        "ready": not blockers,
        "blockers": blockers,
        "candidates": [
            {
                "key": candidate["key"],
                "name": candidate["name"],
                "version": candidate["version"],
                "status": candidate["status"],
            }
            for candidate in per_candidate.values()
        ],
        "items": [per_candidate[key] for key in keys],
        "conflicts": conflicts,
        "cycles": cycle_list,
        "downstream": downstream,
        "downstream_count": len(downstream),
        "internal_affected": internal_affected_rows,
        "internal_breaking": [
            entry for entry in internal_affected_rows if entry["code"] != "requires_sync"
        ],
        "breaking_count": breaking_count,
        "requires_sync_count": requires_sync_count,
        "internal_dependencies": internal,
        "candidate_fingerprint": candidate_fingerprint(metrics, keys),
        "registry_fingerprint": registry_fingerprint(metrics),
        "cycle_members_world": sorted(world_cycle_members),
        "failures": failures,
        "notes": notes,
    }


def _dedupe_blockers(blockers: list[dict]) -> list[dict]:
    seen: set[tuple] = set()
    result: list[dict] = []
    for item in blockers:
        signature = (item.get("code"), item.get("key"), item.get("name"), item.get("detail"))
        if signature in seen:
            continue
        seen.add(signature)
        result.append(item)
    return result


# --------------------------------------------------------------------------- 编排：创建 / 重演练 / 切换


def _item_rows(rehearsal: dict, update_status: str | None = None) -> list[dict]:
    rows = []
    for item in rehearsal.get("items", []):
        evidence = dict(item)
        status = evidence.get("status", "pending")
        if update_status and status == "pending":
            status = update_status
        rows.append({"key": item["key"], "status": status, "evidence": evidence})
    return rows


def _verdict_text(rehearsal: dict) -> str:
    if rehearsal["ready"]:
        return "演练通过"
    return f"演练阻断 {len(rehearsal['blockers'])} 项"


def _payload(conn: sqlite3.Connection, batch: dict, *, replayed: bool = False) -> dict:
    rehearsal = batch["rehearsal"]
    return {
        "batch_id": batch["batch_id"],
        "idempotency_key": batch["idempotency_key"],
        "status": batch["status"],
        "ready": bool(rehearsal.get("ready")),
        "replayed": replayed,
        "fingerprint": batch["fingerprint"],
        "candidates": batch["candidates"],
        "created_at": batch["created_at"],
        "updated_at": batch["updated_at"],
        "applied_at": batch["applied_at"],
        "can_apply": batch["status"] in ("rehearsed", "failed", "blocked"),
        "rehearsal": rehearsal,
        "result": batch["result"],
        "items": store.batch_items(conn, batch["batch_id"]),
    }


def _summary(batch: dict) -> dict:
    rehearsal = batch["rehearsal"]
    return {
        "batch_id": batch["batch_id"],
        "idempotency_key": batch["idempotency_key"],
        "status": batch["status"],
        "ready": bool(rehearsal.get("ready")),
        "candidates": batch["candidates"],
        "candidate_count": len(batch["candidates"]),
        "blocker_count": len(rehearsal.get("blockers", [])),
        "downstream_count": rehearsal.get("downstream_count", 0),
        "breaking_count": rehearsal.get("breaking_count", 0),
        "outcome": batch["result"].get("outcome"),
        "attempts": int(batch["result"].get("attempts", 0)),
        "created_at": batch["created_at"],
        "updated_at": batch["updated_at"],
        "applied_at": batch["applied_at"],
    }


def create_batch(
    metrics: list[dict],
    raw_candidates: list[str],
    idempotency_key: str | None,
    note: str = "",
) -> dict:
    """创建批次并立即演练。相同幂等键重放返回原记录，不新增任何行。"""
    keys = canonical_candidates(raw_candidates)
    idem = (idempotency_key or "").strip() or report.new_id("idem")
    fp = request_fingerprint(keys)
    now = report.now_iso()

    with store.session() as conn:
        store.init_schema(conn)
        existing = store.get_batch_by_idempotency(conn, idem)
        if existing is not None:
            if existing["request_fingerprint"] != fp:
                raise BatchConflict(
                    f"幂等键 {idem} 已用于不同的候选集合，不能复用"
                )
            return _payload(conn, existing, replayed=True)

        rehearsal = rehearse(metrics, keys)
        rehearsal["note"] = note
        batch = {
            "batch_id": report.new_id("bat"),
            "idempotency_key": idem,
            "request_fingerprint": fp,
            "status": "rehearsed" if rehearsal["ready"] else "blocked",
            "candidates": keys,
            "rehearsal": rehearsal,
            "result": {
                "outcome": "rehearsed" if rehearsal["ready"] else "blocked",
                "blockers": rehearsal["blockers"],
                "attempts": 0,
                "failures": [],
            },
            "fingerprint": _digest(
                {
                    "idempotency_key": idem,
                    "candidates": keys,
                    "candidate_fingerprint": rehearsal["candidate_fingerprint"],
                }
            ),
        }
        try:
            store.insert_batch(conn, batch, now)
        except sqlite3.IntegrityError:
            # 并发下同一幂等键被另一个请求先写入：复用那条记录
            conn.rollback()
            existing = store.get_batch_by_idempotency(conn, idem)
            if existing is not None and existing["request_fingerprint"] == fp:
                return _payload(conn, existing, replayed=True)
            raise BatchConflict(f"幂等键 {idem} 并发冲突")
        store.replace_batch_items(conn, batch["batch_id"], _item_rows(rehearsal))
        store.add_event(
            conn,
            "batch_rehearsal",
            f"批次 {batch['batch_id']} 演练：候选 {len(keys)} 个，{_verdict_text(rehearsal)}，"
            f"受影响下游 {rehearsal['downstream_count']} 个（breaking {rehearsal['breaking_count']}）",
            {
                "batch_id": batch["batch_id"],
                "candidates": keys,
                "ready": rehearsal["ready"],
                "blockers": rehearsal["blockers"],
            },
            now,
        )
        batch["created_at"] = now
        batch["updated_at"] = now
        batch["applied_at"] = None
        return _payload(conn, batch)


def rehearse_batch(batch_id: str, metrics: list[dict]) -> dict:
    """对同一条批次记录重演练（原地更新，不新增记录/版本）。"""
    now = report.now_iso()
    with store.session() as conn:
        store.init_schema(conn)
        batch = store.get_batch(conn, batch_id)
        if batch is None:
            raise BatchNotFound(f"批次 {batch_id} 不存在")
        if batch["status"] == "applied":
            return _payload(conn, batch, replayed=True)

        rehearsal = rehearse(
            metrics,
            batch["candidates"],
            previous_failures=batch["result"].get("failures", []),
        )
        rehearsal["note"] = batch["rehearsal"].get("note", "")
        status = "rehearsed" if rehearsal["ready"] else "blocked"
        result = dict(batch["result"])
        result.update(
            {
                "outcome": "rehearsed" if rehearsal["ready"] else "blocked",
                "blockers": rehearsal["blockers"],
            }
        )
        store.update_batch(
            conn, batch_id, status=status, rehearsal=rehearsal, result=result, now=now
        )
        store.replace_batch_items(
            conn, batch_id, _item_rows(rehearsal)
        )
        store.add_event(
            conn,
            "batch_rehearsal",
            (
                f"批次 {batch_id} 重演练：{_verdict_text(rehearsal)}"
            ),
            {
                "batch_id": batch_id,
                "ready": rehearsal["ready"],
                "blockers": rehearsal["blockers"],
            },
            now,
        )
        refreshed = store.get_batch(conn, batch_id)
        return _payload(conn, refreshed)


class _ApplyFailure(Exception):
    def __init__(self, code: str, detail: str, blockers: list[dict] | None = None) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.blockers = blockers or []


def apply_batch(batch_id: str, metrics: list[dict] | None = None) -> dict:
    """一次性切换：全部候选在同一事务内置为已发布；失败整体回滚。

    `metrics` 仅作为兼容入参；真正的演练/校验在 BEGIN IMMEDIATE 之后
    从库里重新读取，保证切换依据与提交内容来自同一快照。
    """
    now = report.now_iso()
    with store.session() as conn:
        store.init_schema(conn)
        if store.get_batch(conn, batch_id) is None:
            raise BatchNotFound(f"批次 {batch_id} 不存在")
        store.begin_immediate(conn)
        batch = store.get_batch(conn, batch_id)
        if batch["status"] == "applied":
            return _payload(conn, batch, replayed=True)

        # 事务内重读注册表：演练、指纹校验与切换共用同一份快照
        current = [registry.with_normalized(metric) for metric in store.all_metrics(conn)]
        candidates = batch["candidates"]
        pinned = batch["rehearsal"].get("candidate_fingerprint")
        fresh = rehearse(
            current,
            candidates,
            previous_failures=batch["result"].get("failures", []),
        )
        result = dict(batch["result"])
        result["attempts"] = int(result.get("attempts", 0)) + 1
        result["attempted_at"] = now

        if fresh["candidate_fingerprint"] != pinned:
            # 候选口径在演练后被改动：不能把没演练过的内容切上线
            conn.rollback()
            return _record_failure(
                conn,
                batch,
                now,
                result=result,
                status="blocked",
                code=BLOCK_CANDIDATE_CHANGED,
                detail="候选版本在演练后被改动，请先重演练再切换",
                rehearsal=fresh,
                rolled_back=False,
            )

        if not fresh["ready"]:
            conn.rollback()
            return _record_failure(
                conn,
                batch,
                now,
                result=result,
                status="blocked",
                code=BLOCK_REHEARSAL_FAILED,
                detail="切换前重演练未通过，整批保持不变（未写入任何发布状态）",
                rehearsal=fresh,
                rolled_back=False,
            )

        try:
            for key in candidates:
                name, version = graph_mod.split_key(key)
                store.set_published(conn, name, version, True, now)
            # 提交前自检：整批必须全部切换成功，否则回滚
            for key in candidates:
                name, version = graph_mod.split_key(key)
                metric = store.get_metric(conn, name, version)
                if metric is None or not metric["published"]:
                    raise _ApplyFailure(
                        BLOCK_APPLY_FAILED,
                        f"{key} 切换后仍不是已发布状态，整批回滚",
                    )
        except Exception as exc:  # noqa: BLE001 - 任何失败都必须回滚并留痕
            conn.rollback()
            code = getattr(exc, "code", BLOCK_APPLY_FAILED)
            return _record_failure(
                conn,
                batch,
                now,
                result=result,
                status="failed",
                code=code,
                detail=str(exc),
                rehearsal=fresh,
                rolled_back=True,
            )

        result.update(
            {
                "outcome": "applied",
                "applied": candidates,
                "blockers": [],
                "candidate_fingerprint": fresh["candidate_fingerprint"],
                "rehearsal_fingerprint": fresh["registry_fingerprint"],
            }
        )
        result.pop("failure", None)
        result["rolled_back"] = False
        store.update_batch(
            conn,
            batch["batch_id"],
            status="applied",
            rehearsal=fresh,
            result=result,
            applied_at=now,
            now=now,
        )
        store.replace_batch_items(
            conn, batch["batch_id"], _item_rows(fresh, update_status="published")
        )
        store.add_event(
            conn,
            "batch_applied",
            f"批次 {batch['batch_id']} 一次性切换 {len(candidates)} 个候选版本："
            + "、".join(candidates),
            {"batch_id": batch["batch_id"], "candidates": candidates},
            now,
        )
        updated = store.get_batch(conn, batch["batch_id"])
        return _payload(conn, updated)


def _record_failure(
    conn: sqlite3.Connection,
    batch: dict,
    now: str,
    *,
    result: dict,
    status: str,
    code: str,
    detail: str,
    rehearsal: dict,
    rolled_back: bool,
) -> dict:
    """切换未完成；把结论与逐项依据写进同一条批次记录。"""
    failures = list(result.get("failures", []))
    failures.append(
        {
            "attempt": int(result.get("attempts", 0)),
            "at": now,
            "code": code,
            "detail": detail,
        }
    )
    result.update(
        {
            "outcome": "failed" if rolled_back else "blocked",
            "failure": {"code": code, "detail": detail},
            "blockers": rehearsal["blockers"],
            "failures": failures,
            "rolled_back": rolled_back,
            "candidate_fingerprint": rehearsal["candidate_fingerprint"],
        }
    )
    previous_status = batch["status"]
    previous_code = (batch["result"].get("failure") or {}).get("code")
    store.update_batch(
        conn,
        batch["batch_id"],
        status=status,
        rehearsal=rehearsal,
        result=result,
        now=now,
    )
    store.replace_batch_items(
        conn,
        batch["batch_id"],
        _item_rows(rehearsal, update_status="rolled_back" if rolled_back else "blocked"),
    )
    # 重试不新增流水：只有状态或失败原因发生变化时才追加一条审计记录
    if previous_status != status or previous_code != code:
        store.add_event(
            conn,
            "batch_failed" if rolled_back else "batch_blocked",
            (
                f"批次 {batch['batch_id']} 切换失败（{code}）：{detail}；旧版本保持发布状态"
                if rolled_back
                else f"批次 {batch['batch_id']} 切换被拒绝（{code}）：{detail}"
            ),
            {
                "batch_id": batch["batch_id"],
                "code": code,
                "detail": detail,
                "candidates": batch["candidates"],
                "attempts": int(result.get("attempts", 0)),
            },
            now,
        )
    conn.commit()
    updated = store.get_batch(conn, batch["batch_id"])
    return _payload(conn, updated)
