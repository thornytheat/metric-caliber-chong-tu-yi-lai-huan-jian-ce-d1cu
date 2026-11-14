"""批次发布 HTTP 层：演练、详情、切换、幂等重放、失败回滚与错误码。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app import store
from app.main import app


@pytest.fixture()
def client(metrics):
    # 每个用例重建一份种子注册表，批次表留空
    with store.session() as conn:
        store.init_schema(conn)
        conn.execute("DELETE FROM release_batch_items")
        conn.execute("DELETE FROM release_batches")
        conn.execute("DELETE FROM metrics")
        for metric in metrics:
            store.insert_metric(conn, metric, "2026-11-14 09:00:00")
    with TestClient(app) as test_client:
        yield test_client


def _register(client, name, version, expression, dimensions=("dt", "channel"), aggregation="sum"):
    response = client.post(
        "/api/registry",
        json={
            "name": name,
            "version": version,
            "expression": expression,
            "aggregation": aggregation,
            "dimensions": list(dimensions),
            "owner": "指标体系组",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_batches_page(client):
    response = client.get("/batches")
    assert response.status_code == 200
    assert "批次发布" in response.text
    assert "整批关系" in response.text


def test_create_batch_rehearses_without_switching(client):
    _register(client, "gmv_daily", 2, "metric:order_amount - metric:refund_amount", dimensions=["dt"])
    created = client.post(
        "/api/batches",
        json={"candidates": ["gmv_daily@2"], "idempotency_key": "k-success"},
    )
    assert created.status_code == 201
    body = created.json()
    assert body["status"] == "rehearsed"
    assert body["ready"] is True
    assert body["rehearsal"]["downstream_count"] == 6
    assert body["rehearsal"]["breaking_count"] == 2
    # 演练不切换发布状态
    published = {item["key"] for item in client.get("/api/metrics").json()["metrics"] if item["published"]}
    assert "gmv_daily@2" not in published


def test_batch_blocked_by_caliber_conflict_returns_409_on_apply(client):
    created = client.post(
        "/api/batches", json={"candidates": ["gmv@3"], "idempotency_key": "k-gmv3"}
    ).json()
    assert created["status"] == "blocked"
    assert {item["code"] for item in created["rehearsal"]["blockers"]} == {"caliber_conflict"}

    applied = client.post(f"/api/batches/{created['batch_id']}/apply")
    assert applied.status_code == 409
    assert applied.json()["status"] == "blocked"
    assert applied.json()["result"]["rolled_back"] is False
    # 旧版本仍在
    assert "gmv@3" not in {
        item["key"] for item in client.get("/api/metrics").json()["metrics"] if item["published"]
    }


def test_apply_switches_all_candidates(client):
    _register(client, "gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    _register(client, "new_atomic", 1, "metric:order_amount")
    created = client.post(
        "/api/batches",
        json={"candidates": ["gmv_daily@2", "new_atomic@1"], "idempotency_key": "k-apply"},
    ).json()
    applied = client.post(f"/api/batches/{created['batch_id']}/apply")
    assert applied.status_code == 200
    body = applied.json()
    assert body["status"] == "applied"
    published = {item["key"] for item in client.get("/api/metrics").json()["metrics"] if item["published"]}
    assert {"gmv_daily@2", "new_atomic@1"} <= published


def test_idempotent_replay_does_not_add_records(client):
    _register(client, "gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    payload = {"candidates": ["gmv_daily@2"], "idempotency_key": "k-replay"}
    first = client.post("/api/batches", json=payload).json()
    second = client.post("/api/batches", json=payload).json()
    assert second["replayed"] is True
    assert second["batch_id"] == first["batch_id"]

    client.post(f"/api/batches/{first['batch_id']}/apply")
    again = client.post(f"/api/batches/{first['batch_id']}/apply")
    assert again.status_code == 200
    assert again.json()["replayed"] is True

    with store.session() as conn:
        batches = conn.execute("SELECT COUNT(*) c FROM release_batches").fetchone()["c"]
        items = conn.execute("SELECT COUNT(*) c FROM release_batch_items").fetchone()["c"]
        metrics = conn.execute("SELECT COUNT(*) c FROM metrics").fetchone()["c"]
    assert batches == 1
    assert items == 1
    assert metrics == 25  # 24 seed + 1 登记


def test_idempotency_key_conflict_returns_409(client):
    _register(client, "gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    client.post("/api/batches", json={"candidates": ["gmv_daily@2"], "idempotency_key": "k-clash"})
    response = client.post(
        "/api/batches", json={"candidates": ["uv@2"], "idempotency_key": "k-clash"}
    )
    assert response.status_code == 409


def test_batch_detail_and_history(client):
    _register(client, "gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    created = client.post(
        "/api/batches", json={"candidates": ["gmv_daily@2"], "idempotency_key": "k-detail"}
    ).json()
    detail = client.get(f"/api/batches/{created['batch_id']}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["items"][0]["key"] == "gmv_daily@2"
    assert body["items"][0]["evidence"]["baseline"]["key"] == "gmv_daily@1"
    assert body["rehearsal"]["internal_dependencies"] == []

    listing = client.get("/api/batches").json()
    assert listing["count"] == 1
    assert listing["batches"][0]["batch_id"] == created["batch_id"]

    page = client.get(f"/batches?batch_id={created['batch_id']}")
    assert page.status_code == 200
    assert created["batch_id"] in page.text
    assert "逐项依据" in page.text


def test_batch_detail_missing_returns_404(client):
    assert client.get("/api/batches/bat_missing").status_code == 404


def test_rehearse_endpoint_reuses_record(client):
    _register(client, "gmv_daily", 2, "metric:order_amount - metric:refund_amount")
    created = client.post(
        "/api/batches", json={"candidates": ["gmv_daily@2"], "idempotency_key": "k-rerun"}
    ).json()
    response = client.post(f"/api/batches/{created['batch_id']}/rehearse")
    assert response.status_code == 200
    body = response.json()
    assert body["batch_id"] == created["batch_id"]
    with store.session() as conn:
        assert conn.execute("SELECT COUNT(*) c FROM release_batches").fetchone()["c"] == 1


def test_empty_candidates_returns_400(client):
    response = client.post("/api/batches", json={"candidates": []})
    assert response.status_code == 400


def test_bad_candidate_format_returns_400(client):
    response = client.post("/api/batches", json={"candidates": ["gmv_daily"]})
    assert response.status_code == 400
