"""快照导出接口测试（进程内 TestClient）。

覆盖：冻结一致性、幂等、空数据集、权限差异、脱敏策略/权限变更、
失败不留半成品与原地重试。并发与重启在 test_snapshot_live.py 中验证。
"""

from __future__ import annotations

import json
import threading

from tests.conftest import EXTERNAL_HEADERS, FULL_HEADERS, seed_minimal_dataset, wait_status


def _request_ready(client, dataset_id: int, headers: dict, extra_headers: dict | None = None) -> dict:
    resp = client.post(
        f"/api/v1/datasets/{dataset_id}/snapshot",
        json={"dataset_id": dataset_id},
        headers={**headers, **(extra_headers or {})},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    status = wait_status(client, body["snapshot"]["id"], headers)
    assert status["status"] == "ready", status
    return status


def _download(client, snapshot_id: int, headers: dict) -> dict:
    resp = client.get(f"/api/v1/exports/{snapshot_id}/download", headers=headers)
    assert resp.status_code == 200, resp.text
    assert resp.headers["X-Snapshot-Content-SHA256"]
    return json.loads(resp.content)


def test_snapshot_contains_members_annotations_summary_and_manifest(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]

    status = _request_ready(client, ds_id, FULL_HEADERS)
    payload = _download(client, status["id"], FULL_HEADERS)

    assert payload["export_type"] == "dataset_snapshot"
    assert payload["schema_version"] == "1.0"
    assert payload["dataset"]["id"] == ds_id
    assert payload["related_resources"]["robot_model"]["name"] == "RM-X1"

    members = payload["members"]
    assert len(members) == 2
    op_ids = {m["operation"]["id"] for m in members}
    assert op_ids == {op["id"] for op in seed["operations"]}

    annotated = [m for m in members if m["annotation"] is not None]
    assert len(annotated) == 1
    ann = annotated[0]["annotation"]
    assert ann["failure_category"] == "感知异常"

    summary = payload["quality_summary"]
    assert summary["total_items"] == 2
    assert summary["annotated_count"] == 1
    assert summary["failure_count"] == 1
    assert summary["success_count"] == 0
    assert summary["annotation_complete_rate"] == 0.5

    manifest = payload["manifest"]
    assert [s["operation_data_id"] for s in manifest["sources"]] == sorted(op_ids)
    assert manifest["source_counts"] == {"operations": 2, "annotations": 1}
    assert manifest["dataset_ref"]["dataset_id"] == ds_id

    # 内容摘要（canonical digest）真实覆盖负载
    digest_field = payload.pop("content_digest")
    from app.services.snapshot import canonical_digest

    assert digest_field == canonical_digest(payload)


def test_repeated_requests_return_same_snapshot_and_identical_bytes(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]

    first = client.post(f"/api/v1/datasets/{ds_id}/snapshot", json={}, headers=EXTERNAL_HEADERS).json()
    first_status = wait_status(client, first["snapshot"]["id"], EXTERNAL_HEADERS)

    second = client.post(f"/api/v1/datasets/{ds_id}/snapshot", json={}, headers=EXTERNAL_HEADERS).json()
    assert second["reused"] is True
    assert second["snapshot"]["id"] == first_status["id"]

    third = client.post(f"/api/v1/datasets/{ds_id}/snapshot", json={}, headers=EXTERNAL_HEADERS).json()
    assert third["snapshot"]["id"] == first_status["id"]

    bytes_a = client.get(f"/api/v1/exports/{first_status['id']}/download", headers=EXTERNAL_HEADERS).content
    bytes_b = client.get(f"/api/v1/exports/{first_status['id']}/download", headers=EXTERNAL_HEADERS).content
    assert bytes_a == bytes_b


def test_parallel_identical_requests_build_single_snapshot(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]
    ids: list[int] = []
    errors: list[Exception] = []

    def fire():
        try:
            r = client.post(f"/api/v1/datasets/{ds_id}/snapshot", json={}, headers=EXTERNAL_HEADERS)
            ids.append(r.json()["snapshot"]["id"])
        except Exception as exc:  # pragma: no cover - 仅用于暴露并发问题
            errors.append(exc)

    threads = [threading.Thread(target=fire) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    assert len(set(ids)) == 1, ids
    status = wait_status(client, ids[0], EXTERNAL_HEADERS)
    assert status["status"] == "ready"


def test_snapshot_is_frozen_during_export_despite_concurrent_modifications(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]
    original_serial = "SN-SECRET-001"

    # 后台落盘延迟 800ms，制造“导出过程中业务数据变化”的窗口
    resp = client.post(
        f"/api/v1/datasets/{ds_id}/snapshot",
        json={},
        headers={**EXTERNAL_HEADERS, "X-Export-Delay-Ms": "800"},
    )
    snapshot_id = resp.json()["snapshot"]["id"]

    # 响应已返回即代表冻结完成；在后台写文件期间进行并发修改
    model_id = seed["model"]["id"]
    scene_id = seed["scene"]["id"]
    skill_id = seed["skill"]["id"]
    new_op = client.post(
        "/api/v1/operations",
        json={
            "robot_model_id": model_id,
            "scene_id": scene_id,
            "skill_id": skill_id,
            "robot_serial": "SN-LATE-999",
            "motion_trajectory": {},
            "perception_records": {},
            "timestamp_start": "2026-03-01T00:00:00+00:00",
            "timestamp_end": "2026-03-01T00:01:00+00:00",
        },
        headers=FULL_HEADERS,
    ).json()
    add_resp = client.post(
        f"/api/v1/datasets/{ds_id}/items",
        json={"operation_data_ids": [new_op["id"]]},
        headers=FULL_HEADERS,
    )
    assert add_resp.status_code == 200
    client.put(
        f"/api/v1/operations/{seed['operations'][0]['id']}",
        json={"robot_serial": "SN-CHANGED-AFTER-FREEZE"},
        headers=FULL_HEADERS,
    )

    status = wait_status(client, snapshot_id, EXTERNAL_HEADERS)
    frozen = _download(client, status["id"], EXTERNAL_HEADERS)

    # 快照仍然是冻结时刻的两条成员，且包含旧序列值（脱敏后为占位符）
    assert frozen["quality_summary"]["total_items"] == 2
    serials = {m["operation"]["id"]: m["operation"]["robot_serial"] for m in frozen["members"]}
    assert all(value == "[REDACTED]" for value in serials.values())

    # 修改完成后再次请求：数据指纹变化 -> 新快照版本，包含 3 条成员
    second = _request_ready(client, ds_id, EXTERNAL_HEADERS)
    assert second["id"] != snapshot_id
    assert second["data_fingerprint"] != status["data_fingerprint"]
    updated = _download(client, second["id"], EXTERNAL_HEADERS)
    assert updated["quality_summary"]["total_items"] == 3

    # 老快照仍可下载，内容不变
    again = _download(client, status["id"], EXTERNAL_HEADERS)
    assert again["quality_summary"]["total_items"] == 2


def test_empty_dataset_produces_valid_ready_snapshot(client):
    seed = seed_minimal_dataset(client)
    empty_ds = client.post(
        "/api/v1/datasets",
        json={
            "name": "空数据集",
            "robot_model_id": seed["model"]["id"],
            "scene_id": seed["scene"]["id"],
            "skill_id": seed["skill"]["id"],
            "owner_team": "合规组",
        },
        headers=FULL_HEADERS,
    ).json()

    status = _request_ready(client, empty_ds["id"], EXTERNAL_HEADERS)
    payload = _download(client, status["id"], EXTERNAL_HEADERS)
    assert payload["members"] == []
    assert payload["manifest"]["sources"] == []
    summary = payload["quality_summary"]
    assert summary["total_items"] == 0
    assert summary["annotated_count"] == 0
    assert summary["annotation_complete_rate"] == 0.0
    assert summary["average_quality_score"] is None
    assert payload["content_digest"].startswith("sha256:")


def test_permission_levels_produce_different_redaction_and_isolation(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]

    restricted_status = _request_ready(client, ds_id, EXTERNAL_HEADERS)
    restricted = _download(client, restricted_status["id"], EXTERNAL_HEADERS)

    assert restricted["redaction"]["applied"] is True
    first_member = restricted["members"][0]["operation"]
    assert first_member["robot_serial"] == "[REDACTED]"
    assert first_member["motion_trajectory"]["serial_number"] == "[REDACTED]"
    ann = restricted["members"][0]["annotation"]
    assert ann["failure_description"] == "[REDACTED]"
    # 结构化分类字段保留
    assert ann["failure_category"] == "感知异常"
    assert restricted["redaction"]["redacted_field_count"] >= 3

    full_status = _request_ready(client, ds_id, FULL_HEADERS)
    full = _download(client, full_status["id"], FULL_HEADERS)
    assert full["redaction"]["applied"] is False
    raw_first = full["members"][0]["operation"]
    assert raw_first["robot_serial"] == "SN-SECRET-001"
    assert raw_first["motion_trajectory"]["serial_number"] == "TRAJ-SN-9"
    assert full["members"][0]["annotation"]["failure_description"].startswith("自由文本")

    # 权限差异形成不同快照版本
    assert restricted_status["id"] != full_status["id"]
    assert restricted_status["permission_level"] == "restricted"
    assert full_status["permission_level"] == "full"

    # 外部调用方无权查看/下载合规组快照
    assert client.get(f"/api/v1/exports/{full_status['id']}", headers=EXTERNAL_HEADERS).status_code == 403
    assert client.get(
        f"/api/v1/exports/{full_status['id']}/download", headers=EXTERNAL_HEADERS
    ).status_code == 403


def test_missing_or_unknown_caller_is_rejected(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]
    assert client.post(f"/api/v1/datasets/{ds_id}/snapshot", json={}).status_code == 401
    assert client.post(
        f"/api/v1/datasets/{ds_id}/snapshot", json={}, headers={"X-Caller-Key": "nobody"}
    ).status_code == 401


def test_redaction_policy_change_creates_new_snapshot_version(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]

    first = _request_ready(client, ds_id, EXTERNAL_HEADERS)
    first_payload = _download(client, first["id"], EXTERNAL_HEADERS)
    # 默认策略不移除 contact_person
    assert first_payload["dataset"]["contact_person"] is None  # seed 未设置联系人
    assert first["policy_revision"] == 1

    # 内容相同的策略发布应被拒绝
    same = client.post(
        "/api/v1/redaction-policies",
        json={"name": "重复策略", "created_by": "测试"},
        headers=FULL_HEADERS,
    )
    assert same.status_code == 400

    # 发布新策略：额外把 contact_person 作为自由文本脱敏
    new_policy = client.post(
        "/api/v1/redaction-policies",
        json={
            "name": "外部复核v2",
            "text_field_keys": [
                "failure_description", "review_notes", "description", "descriptions",
                "notes", "note", "comment", "comments", "remark", "remarks",
                "text", "free_text", "message", "contact_person",
            ],
            "created_by": "合规经理",
        },
        headers=FULL_HEADERS,
    )
    assert new_policy.status_code == 200, new_policy.text
    assert new_policy.json()["revision"] == 2

    second = _request_ready(client, ds_id, EXTERNAL_HEADERS)
    assert second["id"] != first["id"]
    assert second["policy_revision"] == 2
    assert second["policy_fingerprint"] != first["policy_fingerprint"]
    second_payload = _download(client, second["id"], EXTERNAL_HEADERS)
    assert second_payload["redaction"]["policy_revision"] == 2

    # 老快照仍按策略 v1 提供
    old = _download(client, first["id"], EXTERNAL_HEADERS)
    assert old["redaction"]["policy_revision"] == 1

    # 外部调用方无权发布策略
    forbidden = client.post(
        "/api/v1/redaction-policies",
        json={"name": "x", "text_field_keys": ["z"]},
        headers=EXTERNAL_HEADERS,
    )
    assert forbidden.status_code == 403


def test_caller_permission_change_creates_new_snapshot_version(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]

    create = client.post(
        "/api/v1/export-callers",
        json={"caller_key": "vendor-a", "display_name": "供应商A", "permission_level": "restricted"},
        headers=FULL_HEADERS,
    )
    assert create.status_code == 200, create.text
    vendor_headers = {"X-Caller-Key": "vendor-a"}

    before = _request_ready(client, ds_id, vendor_headers)
    assert before["permission_level"] == "restricted"

    promoted = client.patch(
        "/api/v1/export-callers/vendor-a",
        json={"permission_level": "full"},
        headers=FULL_HEADERS,
    )
    assert promoted.status_code == 200
    assert promoted.json()["permission_level"] == "full"

    after = _request_ready(client, ds_id, vendor_headers)
    assert after["id"] != before["id"]
    assert after["permission_level"] == "full"
    payload = _download(client, after["id"], vendor_headers)
    assert payload["members"][0]["operation"]["robot_serial"] == "SN-SECRET-001"

    # 老快照保持 restricted 视图不变
    old = _download(client, before["id"], vendor_headers)
    assert old["redaction"]["applied"] is True


def test_dataset_version_bump_creates_new_snapshot(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]

    first = _request_ready(client, ds_id, EXTERNAL_HEADERS)
    version_resp = client.put(
        f"/api/v1/datasets/{ds_id}",
        json={"version": "1.1"},
        headers=FULL_HEADERS,
    )
    assert version_resp.status_code == 200
    second = _request_ready(client, ds_id, EXTERNAL_HEADERS)
    assert second["id"] != first["id"]
    assert second["snapshot_key"] != first["snapshot_key"]


def test_failed_export_leaves_no_artifact_and_retry_reuses_same_id(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]

    resp = client.post(
        f"/api/v1/datasets/{ds_id}/snapshot",
        json={},
        headers={**FULL_HEADERS, "X-Export-Fail": "1"},
    )
    snapshot_id = resp.json()["snapshot"]["id"]
    failed = wait_status(client, snapshot_id, FULL_HEADERS)
    assert failed["status"] == "failed"
    assert failed["fail_reason"]
    assert failed["attempt_count"] == 1
    assert failed["download_url"] is None

    # 失败状态不可下载，不留本快照的半成品文件
    assert client.get(f"/api/v1/exports/{snapshot_id}/download", headers=FULL_HEADERS).status_code == 409
    import os

    from app.config import settings

    expected_file = f"snapshot_{snapshot_id}_{failed['snapshot_key'][:12]}.json"
    assert expected_file not in os.listdir(settings.EXPORT_DIR_ABS)
    # 临时文件也不应残留
    assert not [f for f in os.listdir(settings.EXPORT_DIR_ABS) if f.endswith(".tmp")]

    # 列表接口可按状态过滤到失败任务
    listed = client.get("/api/v1/exports", params={"status": "failed"}, headers=FULL_HEADERS).json()
    assert any(item["id"] == snapshot_id and item["status"] == "failed" for item in listed)

    # 相同请求重试：原地复活，使用同一快照ID
    retry = client.post(f"/api/v1/datasets/{ds_id}/snapshot", json={}, headers=FULL_HEADERS).json()
    assert retry["reused"] is False
    assert retry["snapshot"]["id"] == snapshot_id
    ready = wait_status(client, snapshot_id, FULL_HEADERS)
    assert ready["status"] == "ready"
    # 共两次尝试：一次失败、一次成功
    assert ready["attempt_count"] == 2

    payload = _download(client, snapshot_id, FULL_HEADERS)
    assert payload["quality_summary"]["total_items"] == 2

    # 再重复请求直接复用 ready
    again = client.post(f"/api/v1/datasets/{ds_id}/snapshot", json={}, headers=FULL_HEADERS).json()
    assert again["reused"] is True
    assert again["snapshot"]["id"] == snapshot_id


def test_list_endpoint_scopes_results_to_caller(client):
    seed = seed_minimal_dataset(client)
    ds_id = seed["dataset"]["id"]
    _request_ready(client, ds_id, EXTERNAL_HEADERS)
    _request_ready(client, ds_id, FULL_HEADERS)

    external_list = client.get("/api/v1/exports", headers=EXTERNAL_HEADERS).json()
    assert all(item["caller_key"] == "external-review" for item in external_list)
    assert len(external_list) == 1

    full_list = client.get("/api/v1/exports", headers=FULL_HEADERS).json()
    assert {item["caller_key"] for item in full_list} >= {"external-review", "compliance-team"}
