"""批次发布：整批演练（冲突/环/下游）、一次性切换、失败回滚、幂等重试、逐项依据。"""

from __future__ import annotations

import pytest

from app import batch, registry, settings, store


def _unpublished(name: str, version: int, expression: str, **extra) -> dict:
    payload = {
        "name": name,
        "version": version,
        "expression": expression,
        "aggregation": extra.pop("aggregation", "sum"),
        "dimensions": extra.pop("dimensions", ["dt", "channel"]),
        "owner": extra.pop("owner", "指标体系组"),
        "published": False,
    }
    payload.update(extra)
    return registry.normalize_metric(payload)


def _register(metric: dict) -> None:
    with store.session() as conn:
        store.init_schema(conn)
        store.insert_metric(conn, metric, "2026-11-14 10:00:00")


def _reset(metrics: list[dict]) -> None:
    with store.session() as conn:
        store.init_schema(conn)
        conn.execute("DELETE FROM release_batch_items")
        conn.execute("DELETE FROM release_batches")
        conn.execute("DELETE FROM metrics")
        for metric in metrics:
            store.insert_metric(conn, metric, "2026-11-14 09:00:00")


@pytest.fixture()
def batch_db(metrics):
    """每个用例重建一份干净的注册表（批次表留空）。"""
    _reset(metrics)
    yield metrics
    _reset(metrics)


def _published_keys() -> set[str]:
    with store.session() as conn:
        return {
            f"{metric['name']}@{metric['version']}"
            for metric in store.all_metrics(conn)
            if metric["published"]
        }


def _counts() -> dict:
    with store.session() as conn:
        store.init_schema(conn)
        return {
            "batches": conn.execute("SELECT COUNT(*) c FROM release_batches").fetchone()["c"],
            "items": conn.execute("SELECT COUNT(*) c FROM release_batch_items").fetchone()["c"],
            "metrics": conn.execute("SELECT COUNT(*) c FROM metrics").fetchone()["c"],
        }


# --------------------------------------------------------------------------- 演练


def test_batch_only_cycle_is_detected(metrics, batch_db):
    """两个候选单独看都不成环，作为一批同时上线才成环。"""
    loop_x = _unpublished("loop_x", 1, "metric:loop_y")
    loop_y = _unpublished("loop_y", 1, "metric:loop_x")
    for metric in (loop_x, loop_y):
        _register(metric)
    world = metrics + [loop_x, loop_y]

    # 单独演练其中一个（另一个未发布）→ 引用不可解析阻断，但不会提前成环
    single = batch.rehearse(world, ["loop_x@1"])
    assert single["ready"] is False
    assert {item["code"] for item in single["blockers"]} == {"missing_reference"}
    assert single["cycles"] == []

    together = batch.rehearse(world, ["loop_x@1", "loop_y@1"])
    assert together["ready"] is False
    assert {item["code"] for item in together["blockers"]} == {"cycle_introduced"}
    assert together["cycles"][0]["path"] == ["loop_x@1", "loop_y@1", "loop_x@1"]
    assert together["cycles"][0]["is_new"] is True
    assert together["internal_dependencies"][0]["kind"] == "bare_reference"


def test_batch_detects_caliber_conflict_that_requires_whole_batch(metrics, batch_db):
    """补充候选后同名指标在批次世界里出现两套口径，必须整批拦住。"""
    fix = _unpublished(
        "gmv_daily", 2, "metric:order_amount - metric:refund_amount", dimensions=["dt"]
    )
    _register(fix)
    rehearsal = batch.rehearse(metrics + [fix], ["gmv_daily@2"])
    # gmv_daily@1 已发布且口径一致，本批不含同名冲突版本
    assert rehearsal["ready"] is True

    variant = _unpublished("gmv_daily", 3, "metric:pay_amount", dimensions=["dt"])
    _register(variant)
    rehearsal = batch.rehearse(metrics + [fix, variant], ["gmv_daily@2", "gmv_daily@3"])
    codes = {item["code"] for item in rehearsal["blockers"]}
    assert "caliber_conflict" in codes
    conflict = next(item for item in rehearsal["conflicts"] if item["name"] == "gmv_daily")
    assert conflict["variant_count"] >= 2
    assert set(conflict["candidates"]) == {"gmv_daily@2", "gmv_daily@3"}


def test_rehearsal_reports_affected_downstream_and_evidence(metrics, batch_db):
    fix = _unpublished(
        "gmv_daily", 2, "metric:order_amount - metric:refund_amount", dimensions=["dt"]
    )
    _register(fix)
    rehearsal = batch.rehearse(metrics + [fix], ["gmv_daily@2"])
    assert rehearsal["ready"] is True
    # gmv_7d 按 channel 分组，候选移除 channel → breaking
    downstream = {item["key"]: item for item in rehearsal["downstream"]}
    assert set(downstream) == {
        "arpu@1",
        "gmv_7d@1",
        "gmv_index@1",
        "pay_gmv_share@1",
        "uv_per_gmv@1",
        "revenue@1",
    }
    assert downstream["gmv_7d@1"]["code"] == "dimension_removed"
    assert downstream["gmv_7d@1"]["affected_by"][0]["candidate"] == "gmv_daily@2"
    item = rehearsal["items"][0]
    assert item["changes"]["removed_dims"] == ["channel"]
    assert item["breaking_count"] == 2
    assert item["downstream_count"] == 6
    # 逐项依据里保留基线、归一化表达式与维度
    assert item["baseline"]["key"] == "gmv_daily@1"
    assert item["normalized"] == "metric:order_amount-metric:refund_amount"
    assert item["downstream"] if "downstream" in item else True


def test_internal_breaking_between_candidates_blocks_batch(metrics, batch_db):
    """同批候选 A 移除维度，而同批候选 B 依赖该维度：整批必须拦住。"""
    base_upstream = _unpublished("stage_gmv", 1, "metric:order_amount", dimensions=["dt", "channel"])
    base_upstream["published"] = True
    base_downstream = _unpublished("stage_ratio", 1, "metric:stage_gmv", dimensions=["dt", "channel"])
    base_downstream["published"] = True
    upstream = _unpublished("stage_gmv", 2, "metric:order_amount", dimensions=["dt"])
    downstream = _unpublished("stage_ratio", 2, "metric:stage_gmv", dimensions=["dt", "channel"])
    for metric in (base_upstream, base_downstream, upstream, downstream):
        _register(metric)
    rehearsal = batch.rehearse(
        metrics + [base_upstream, base_downstream, upstream, downstream],
        ["stage_gmv@2", "stage_ratio@2"],
    )
    assert rehearsal["ready"] is False
    codes = {item["code"] for item in rehearsal["blockers"]}
    assert "internal_breaking" in codes
    affected = {item["key"]: item for item in rehearsal["internal_affected"]}
    assert affected["stage_ratio@2"]["code"] == "dimension_removed"


def test_candidate_must_already_exist(metrics, batch_db):
    rehearsal = batch.rehearse(metrics, ["not_registered@1"])
    assert rehearsal["ready"] is False
    assert {item["code"] for item in rehearsal["blockers"]} == {"candidate_not_found"}


# --------------------------------------------------------------------------- 切换


def test_apply_switches_whole_batch(metrics, batch_db):
    fix = _unpublished("gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    other = _unpublished("new_atomic", 1, "metric:order_amount")
    for metric in (fix, other):
        _register(metric)
    created = batch.create_batch(metrics + [fix, other], ["gmv_daily@2", "new_atomic@1"], "ok-1")
    assert created["status"] == "rehearsed"

    applied = batch.apply_batch(created["batch_id"])
    assert applied["status"] == "applied"
    assert applied["result"]["outcome"] == "applied"
    assert {"gmv_daily@2", "new_atomic@1"} <= _published_keys()
    assert applied["applied_at"]


def test_apply_failure_rolls_back_every_candidate(metrics, batch_db, monkeypatch):
    fix = _unpublished("gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    other = _unpublished("new_atomic", 1, "metric:order_amount")
    for metric in (fix, other):
        _register(metric)
    before = _published_keys()
    created = batch.create_batch(metrics + [fix, other], ["gmv_daily@2", "new_atomic@1"], "fail-1")

    original = store.set_published

    def flaky(conn, name, version, published, now):
        if name == "new_atomic":
            raise RuntimeError("模拟写入失败")
        return original(conn, name, version, published, now)

    monkeypatch.setattr(store, "set_published", flaky)
    failed = batch.apply_batch(created["batch_id"])

    assert failed["status"] == "failed"
    assert failed["result"]["outcome"] == "failed"
    assert failed["result"]["rolled_back"] is True
    assert failed["result"]["failure"]["code"] == "apply_failed"
    # 整批回滚：没有任何候选被切换
    assert _published_keys() == before
    assert {"gmv_daily@2", "new_atomic@1"}.isdisjoint(_published_keys())
    # 失败记录与逐项依据仍在同一批次里
    assert failed["batch_id"] == created["batch_id"]
    assert {item["key"] for item in failed["items"]} == {"gmv_daily@2", "new_atomic@1"}
    assert all(item["status"] == "rolled_back" for item in failed["items"])
    assert failed["rehearsal"]["downstream_count"] >= 1

    monkeypatch.setattr(store, "set_published", original)
    retried = batch.apply_batch(created["batch_id"])
    assert retried["status"] == "applied"
    assert retried["result"]["attempts"] == 2
    assert retried["result"]["failures"][0]["code"] == "apply_failed"
    assert {"gmv_daily@2", "new_atomic@1"} <= _published_keys()
    # 重试复用同一记录，不新增批次/明细/指标
    counts = _counts()
    assert counts["batches"] == 1
    assert counts["items"] == 2
    assert counts["metrics"] == len(batch_db) + 2


def test_retry_is_idempotent(metrics, batch_db):
    fix = _unpublished("gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    _register(fix)
    created = batch.create_batch(metrics + [fix], ["gmv_daily@2"], "idem-42")
    again = batch.create_batch(metrics + [fix], ["gmv_daily@2"], "idem-42")
    assert again["batch_id"] == created["batch_id"]
    assert again["replayed"] is True
    first_apply = batch.apply_batch(created["batch_id"])
    second_apply = batch.apply_batch(created["batch_id"])
    assert first_apply["status"] == second_apply["status"] == "applied"
    assert second_apply["replayed"] is True
    counts = _counts()
    assert counts["batches"] == 1
    assert counts["items"] == 1
    assert counts["metrics"] == len(batch_db) + 1


def test_idempotency_key_reuse_with_other_candidates_is_rejected(metrics, batch_db):
    fix = _unpublished("gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    _register(fix)
    batch.create_batch(metrics + [fix], ["gmv_daily@2"], "shared-key")
    with pytest.raises(batch.BatchConflict):
        batch.create_batch(metrics + [fix], ["uv@2", "gmv_daily@2"], "shared-key")


def test_apply_refuses_changed_candidate(metrics, batch_db):
    fix = _unpublished("gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    _register(fix)
    created = batch.create_batch(metrics + [fix], ["gmv_daily@2"], "drift-1")

    # 演练后候选被改了口径：切换必须要求先重演练
    with store.session() as conn:
        store.init_schema(conn)
        changed = dict(fix)
        changed["expression"] = "metric:pay_amount"
        store.upsert_metric(conn, changed, "2026-11-14 11:00:00")

    blocked = batch.apply_batch(created["batch_id"])
    assert blocked["status"] == "blocked"
    assert blocked["result"]["outcome"] == "blocked"
    assert blocked["result"]["failure"]["code"] == "candidate_changed"
    assert "gmv_daily@2" not in _published_keys()

    rehearsal = batch.rehearse_batch(created["batch_id"], registry.load_sample(settings.SAMPLE_PATH)["metrics"] + [changed])
    assert rehearsal["status"] in ("rehearsed", "blocked")


def test_failed_batch_keeps_old_versions_published(metrics, batch_db, monkeypatch):
    fix = _unpublished("gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    _register(fix)
    created = batch.create_batch(metrics + [fix], ["gmv_daily@2"], "keep-old")

    def boom(conn, name, version, published, now):
        raise RuntimeError("事务中途失败")

    monkeypatch.setattr(store, "set_published", boom)
    failed = batch.apply_batch(created["batch_id"])
    assert failed["status"] == "failed"
    assert "gmv_daily@1" in _published_keys()
    assert "gmv_daily@2" not in _published_keys()
