"""快照导出本地接口测试。

覆盖：
1. 并发修改：导出进行中修改成员/标注/序列号，快照保持冻结；
2. 空数据集：零成员快照可完成、可下载、摘要正确；
3. 权限差异：owner/reviewer/external 脱敏不同，指纹不同，越权收窄放开被拒；
4. 失败重试：构建失败无半成品、状态 failed，重试后完成；
5. 重启一致性：重启恢复 preparing 任务，下载内容与摘要保持一致。
"""

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.database import SessionLocal
from app.models import (
    Annotation,
    Dataset,
    DatasetItem,
    DatasetVersion,
    OperationData,
    RobotModel,
    Scene,
    Skill,
    SnapshotExport,
)
from app.services import snapshot_export as svc
from main import app

API = "/api/v1"
UTC = timezone.utc


# ---------------------------------------------------------------------------
# 数据工厂
# ---------------------------------------------------------------------------

def _make_resources(session, n_ops=3, with_annotations=True, published=True):
    model = RobotModel(name="RM-TEST", manufacturer="RealMotion")
    scene = Scene(name="测试场景", category="测试")
    skill = Skill(name="测试技能", category="测试")
    session.add_all([model, scene, skill])
    session.flush()

    start = datetime(2026, 1, 1, tzinfo=UTC)
    ops = []
    for i in range(n_ops):
        op = OperationData(
            robot_model_id=model.id,
            scene_id=scene.id,
            skill_id=skill.id,
            robot_serial=f"RM65A-2026-{i:03d}",
            motion_trajectory={"waypoints": [{"x": i, "y": 0, "z": 0}]},
            perception_records={
                "camera_serial": f"CAM-{i}",
                # 落在嵌套 JSON 中的设备序列号字段，也要被脱敏。
                "sensor": {"serial_number": f"SNS-{i}", "ok": True},
            },
            grasp_result={"attempted": True, "success": i != 0},
            timestamp_start=start + timedelta(minutes=i),
            timestamp_end=start + timedelta(minutes=i + 1),
            duration_ms=60000,
            quality_score=0.8 + i * 0.05,
            completeness_score=0.9,
            data_grade="A" if i else "B",
        )
        session.add(op)
        ops.append(op)
    session.flush()

    annotations = []
    if with_annotations:
        for i, op in enumerate(ops):
            ann = Annotation(
                operation_data_id=op.id,
                is_success=(i != 0),
                failure_category="感知异常" if i == 0 else None,
                failure_subcategory="视觉识别失败" if i == 0 else None,
                failure_description=(
                    f"现场粉尘严重，设备编号 RM65A-2026-{i:03d} 镜头被遮挡"
                    if i == 0 else None
                ),
                annotator="张工",
                review_status="approved",
                reviewer="复核-甲",
                review_notes=f"复核意见自由文本 {i}：标注无误",
                annotation_quality_score=0.9,
            )
            session.add(ann)
            annotations.append(ann)

    dataset = Dataset(
        name="测试数据集",
        description="用于快照导出测试",
        version="1.0",
        robot_model_id=model.id,
        scene_id=scene.id,
        skill_id=skill.id,
        owner_team="精密装配组",
        review_status="approved" if published else "draft",
        is_published=published,
        published_at=datetime.now(UTC) if published else None,
        current_version=1,
        total_items=n_ops,
    )
    session.add(dataset)
    session.flush()
    for op in ops:
        session.add(DatasetItem(dataset_id=dataset.id, operation_data_id=op.id))
    session.add(
        DatasetVersion(
            dataset_id=dataset.id,
            version_number=1,
            version_label="1.0",
            total_items=n_ops,
            success_count=max(n_ops - 1, 0),
            failure_count=1 if n_ops else 0,
            annotation_complete_rate=1.0 if n_ops else 0.0,
            average_quality_score=0.85,
            data_grade="A",
        )
    )
    session.commit()
    return {
        "model_id": model.id,
        "scene_id": scene.id,
        "skill_id": skill.id,
        "dataset_id": dataset.id,
        "op_ids": [op.id for op in ops],
    }


def wait_for_terminal(client, export_id, timeout=10.0):
    deadline = time.time() + timeout
    while True:
        resp = client.get(f"{API}/snapshot-exports/{export_id}")
        assert resp.status_code == 200
        body = resp.json()
        if body["status"] in ("completed", "failed"):
            return body
        if time.time() > deadline:
            raise AssertionError(f"快照 {export_id} 长时间未结束: {body['status']}")
        time.sleep(0.01)


def request_export(client, **overrides):
    payload = {
        "dataset_id": overrides.pop("dataset_id"),
        "requestor_team": overrides.pop("requestor_team", "外部复核团队"),
        "requestor_role": overrides.pop("requestor_role", "external"),
    }
    payload.update(overrides)
    resp = client.post(f"{API}/snapshot-exports", json=payload)
    return resp


def download(client, export_id):
    resp = client.get(f"{API}/snapshot-exports/{export_id}/download")
    assert resp.status_code == 200, resp.text
    return resp, json.loads(resp.content.decode("utf-8"))


def verify_digest(doc):
    assert svc.content_digest(doc) == doc["manifest"]["content_sha256"]


# ---------------------------------------------------------------------------
# 1. 并发修改：导出期间数据变化不影响快照，重复请求幂等
# ---------------------------------------------------------------------------

def test_export_is_frozen_against_concurrent_modifications(client, db, export_dir):
    ids = _make_resources(db, n_ops=3)

    resp = request_export(client, dataset_id=ids["dataset_id"])
    assert resp.status_code == 201, resp.text
    export_id = resp.json()["id"]

    # 导出后台任务运行的同时，并发修改底层数据：
    # 改序列号、改标注自由文本、新增成员、给新成员加标注。
    new_op = OperationData(
        robot_model_id=ids["model_id"],
        scene_id=ids["scene_id"],
        skill_id=ids["skill_id"],
        robot_serial="RM65A-NEW-999",
        motion_trajectory={"waypoints": []},
        perception_records={},
        grasp_result=None,
        timestamp_start=datetime(2026, 2, 1, tzinfo=UTC),
        timestamp_end=datetime(2026, 2, 1, minute=1, tzinfo=UTC),
    )
    db.add(new_op)
    db.flush()
    db.add(DatasetItem(dataset_id=ids["dataset_id"], operation_data_id=new_op.id))
    db.query(OperationData).filter(OperationData.id == ids["op_ids"][0]).update(
        {OperationData.robot_serial: "CHANGED-SERIAL"}
    )
    db.query(Annotation).filter(
        Annotation.operation_data_id == ids["op_ids"][0]
    ).update({Annotation.failure_description: "被篡改后的自由文本"})
    db.commit()

    body = wait_for_terminal(client, export_id)
    assert body["status"] == "completed"

    resp, doc = download(client, export_id)
    verify_digest(doc)
    first_bytes = resp.content

    assert doc["quality_summary"]["total_items"] == 3
    members = doc["members"]
    assert len(members) == 3
    assert len(doc["manifest"]["sources"]) == 3
    # 冻结时的序列号与文本（经脱敏）保留，改动没有泄漏进快照。
    first_member = next(m for m in members if m["operation_data_id"] == ids["op_ids"][0])
    assert first_member["robot_serial"] == "[REDACTED]"
    assert first_member["annotation"]["failure_description"] == "[REDACTED]"

    # 相同请求重复执行：复用同一快照。
    again = request_export(client, dataset_id=ids["dataset_id"])
    assert again.status_code == 200
    assert again.json()["id"] == export_id
    assert again.json()["reused"] is True

    # 即使数据集继续变化，下载内容逐字节一致。
    db.query(OperationData).filter(OperationData.id == ids["op_ids"][1]).update(
        {OperationData.robot_serial: "CHANGED-AGAIN"}
    )
    db.commit()
    resp2, doc2 = download(client, export_id)
    assert resp2.content == first_bytes
    assert doc2["manifest"]["content_sha256"] == doc["manifest"]["content_sha256"]
    assert resp2.headers["X-Content-SHA256"] == doc["manifest"]["content_sha256"]


def test_concurrent_identical_requests_create_single_snapshot(client, db):
    ids = _make_resources(db, n_ops=2)

    results = []
    errors = []

    def fire():
        try:
            results.append(request_export(client, dataset_id=ids["dataset_id"]))
        except Exception as exc:  # pragma: no cover - 测试诊断
            errors.append(exc)

    threads = [threading.Thread(target=fire) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    returned_ids = {r.json()["id"] for r in results}
    assert len(returned_ids) == 1
    # 恰好一个 201（新建），其余 200（复用竞态胜者）。
    statuses = sorted(r.status_code for r in results)
    assert statuses[0] == 200
    assert statuses[-1] == 201

    export_id = returned_ids.pop()
    body = wait_for_terminal(client, export_id)
    assert body["status"] == "completed"
    session = SessionLocal()
    try:
        assert session.query(SnapshotExport).count() == 1
    finally:
        session.close()


def test_preparing_status_is_observable_then_completed(client, db):
    # 人为拖住构建，确保能观察到 preparing 状态。
    original = svc.build_snapshot_payload
    barrier = threading.Event()

    def slow_build(record):
        barrier.wait(timeout=5)
        return original(record)

    svc.build_snapshot_payload = slow_build
    try:
        ids = _make_resources(db, n_ops=1)
        resp = request_export(client, dataset_id=ids["dataset_id"], requestor_role="owner")
        export_id = resp.json()["id"]
        status = client.get(f"{API}/snapshot-exports/{export_id}").json()
        assert status["status"] == "preparing"
        # 准备中不可下载。
        dl = client.get(f"{API}/snapshot-exports/{export_id}/download")
        assert dl.status_code == 409
        barrier.set()
        body = wait_for_terminal(client, export_id)
        assert body["status"] == "completed"
    finally:
        svc.build_snapshot_payload = original
        barrier.set()


# ---------------------------------------------------------------------------
# 2. 空数据集
# ---------------------------------------------------------------------------

def test_empty_dataset_export_completes(client, db, export_dir):
    ids = _make_resources(db, n_ops=0, with_annotations=False)
    resp = request_export(client, dataset_id=ids["dataset_id"])
    assert resp.status_code == 201
    body = wait_for_terminal(client, resp.json()["id"])
    assert body["status"] == "completed"
    assert body["item_count"] == 0

    _, doc = download(client, body["id"])
    verify_digest(doc)
    assert doc["members"] == []
    assert doc["manifest"]["sources"] == []
    summary = doc["quality_summary"]
    assert summary["total_items"] == 0
    assert summary["annotation_complete_rate"] == 0.0
    assert summary["success_count"] == 0
    assert summary["failure_count"] == 0
    assert summary["average_quality_score"] is None


# ---------------------------------------------------------------------------
# 3. 权限差异
# ---------------------------------------------------------------------------

def test_permission_profiles_produce_different_snapshots(client, db, export_dir):
    ids = _make_resources(db, n_ops=2)
    exported = {}

    for role, team in [
        ("owner", "精密装配组"),
        ("reviewer", "内部复核组"),
        ("external", "外部复核团队"),
    ]:
        resp = request_export(
            client, dataset_id=ids["dataset_id"], requestor_role=role, requestor_team=team
        )
        assert resp.status_code == 201, resp.text
        body = wait_for_terminal(client, resp.json()["id"])
        assert body["status"] == "completed"
        exported[role] = body["id"]

    # 三种权限画像指纹互不相同，形成三个不同快照版本。
    fingerprints = {
        client.get(f"{API}/snapshot-exports/{eid}").json()["request_fingerprint"]
        for eid in exported.values()
    }
    assert len(fingerprints) == 3

    _, owner_doc = download(client, exported["owner"])
    _, reviewer_doc = download(client, exported["reviewer"])
    _, external_doc = download(client, exported["external"])

    owner_member = owner_doc["members"][0]
    reviewer_member = reviewer_doc["members"][0]
    external_member = external_doc["members"][0]

    # owner：序列号与自由文本均可见。
    assert owner_member["robot_serial"] == "RM65A-2026-000"
    assert owner_member["perception_records"]["sensor"]["serial_number"] == "SNS-0"
    assert "粉尘" in owner_member["annotation"]["failure_description"]
    assert "复核意见" in owner_member["annotation"]["review_notes"]

    # reviewer：序列号脱敏，自由文本保留，但文本中夹带的序列号字面值同样被清除。
    assert reviewer_member["robot_serial"] == "[REDACTED]"
    assert reviewer_member["perception_records"]["sensor"]["serial_number"] == "[REDACTED]"
    assert "粉尘" in reviewer_member["annotation"]["failure_description"]
    assert "RM65A-2026-000" not in reviewer_member["annotation"]["failure_description"]
    assert "复核意见" in reviewer_member["annotation"]["review_notes"]

    # external：序列号与自由文本全部脱敏。
    assert external_member["robot_serial"] == "[REDACTED]"
    assert external_member["perception_records"]["sensor"]["serial_number"] == "[REDACTED]"
    assert external_member["annotation"]["failure_description"] == "[REDACTED]"
    assert external_member["annotation"]["review_notes"] == "[REDACTED]"
    # 非敏感的分类字段保留，外部团队仍可复核。
    assert external_member["annotation"]["failure_category"] == "感知异常"
    assert external_member["annotation"]["is_success"] is False

    # 脱敏计数进入清单。
    redaction = external_doc["manifest"]["redaction_summary"]
    assert redaction["device_serial_fields_redacted"] >= 2
    assert redaction["free_text_fields_redacted"] >= 2

    # 权限画像记录在清单中。
    assert external_doc["manifest"]["generated_for"]["requestor_role"] == "external"
    assert (
        external_doc["manifest"]["generated_for"]["permission_profile"][
            "view_device_serial"
        ]
        is False
    )


def test_external_cannot_export_unpublished_and_cannot_escalate(client, db):
    ids = _make_resources(db, n_ops=1, published=False)

    forbidden = request_export(client, dataset_id=ids["dataset_id"])
    assert forbidden.status_code == 403

    # owner 可以导出未发布数据集。
    ok = request_export(
        client, dataset_id=ids["dataset_id"], requestor_role="owner",
        requestor_team="精密装配组"
    )
    assert ok.status_code == 201

    # 越权放开脱敏被拒绝（external 默认不可见序列号）。
    escalate = request_export(
        client, dataset_id=ids["dataset_id"],
        permission_overrides={"view_device_serial": True},
    )
    assert escalate.status_code == 400

    # 未知数据集返回 404。
    assert request_export(client, dataset_id=9999).status_code == 404


def test_permission_narration_creates_new_version(client, db):
    ids = _make_resources(db, n_ops=1, published=True)
    base = request_export(client, dataset_id=ids["dataset_id"])
    narrowed = request_export(
        client,
        dataset_id=ids["dataset_id"],
        permission_overrides={"view_free_text": False, "view_device_serial": False},
    )
    # external 默认两项均为 False，覆盖等价 → 复用。
    assert narrowed.json()["id"] == base.json()["id"]

    # reviewer 收窄自由文本后，指纹与默认 reviewer 不同。
    r1 = request_export(
        client, dataset_id=ids["dataset_id"], requestor_role="reviewer",
        requestor_team="内部复核组"
    )
    r2 = request_export(
        client, dataset_id=ids["dataset_id"], requestor_role="reviewer",
        requestor_team="内部复核组",
        permission_overrides={"view_free_text": False},
    )
    assert r1.json()["id"] != r2.json()["id"]
    wait_for_terminal(client, r1.json()["id"])
    body2 = wait_for_terminal(client, r2.json()["id"])
    _, doc2 = download(client, body2["id"])
    assert doc2["members"][0]["annotation"]["review_notes"] == "[REDACTED]"


def test_masking_policy_change_forces_new_version(client, db, monkeypatch):
    ids = _make_resources(db, n_ops=1)
    first = request_export(client, dataset_id=ids["dataset_id"])
    first_id = first.json()["id"]
    wait_for_terminal(client, first_id)

    # 脱敏策略版本提升后，相同请求形成新版本。
    monkeypatch.setattr(svc, "MASKING_POLICY_VERSION", "2027-01-01")
    second = request_export(client, dataset_id=ids["dataset_id"])
    assert second.status_code == 201
    assert second.json()["reused"] is False
    assert second.json()["id"] != first_id
    body = wait_for_terminal(client, second.json()["id"])
    assert body["masking_policy_version"] == "2027-01-01"


# ---------------------------------------------------------------------------
# 4. 失败重试
# ---------------------------------------------------------------------------

def test_failure_leaves_no_artifact_and_retry_succeeds(client, db, export_dir, monkeypatch):
    ids = _make_resources(db, n_ops=2)

    calls = {"count": 0}
    real_write = svc.write_atomic

    def flaky_write(record_id, payload):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("磁盘已满（模拟）")
        return real_write(record_id, payload)

    monkeypatch.setattr(svc, "write_atomic", flaky_write)

    resp = request_export(client, dataset_id=ids["dataset_id"])
    export_id = resp.json()["id"]
    body = wait_for_terminal(client, export_id)
    assert body["status"] == "failed"
    assert "磁盘已满" in (body["error_message"] or "")
    assert body["content_sha256"] is None

    # 失败状态下不可下载。
    dl = client.get(f"{API}/snapshot-exports/{export_id}/download")
    assert dl.status_code == 409

    # 导出目录里没有半成品：无最终文件，.tmp 下也无残留。
    assert not os.path.exists(os.path.join(export_dir, f"snapshot_{export_id}.json"))
    tmp_dir = os.path.join(export_dir, ".tmp")
    if os.path.isdir(tmp_dir):
        assert not [f for f in os.listdir(tmp_dir) if f.endswith(".tmp")]

    # 列表接口可过滤出失败任务。
    listed = client.get(f"{API}/snapshot-exports", params={"status": "failed"}).json()
    assert any(item["id"] == export_id for item in listed)

    # 恢复写盘后重试：仍是同一条逻辑快照，attempts 递增。
    monkeypatch.undo()
    retry = client.post(f"{API}/snapshot-exports/{export_id}/retry")
    assert retry.status_code == 200
    assert retry.json()["attempts"] == 2
    body2 = wait_for_terminal(client, export_id)
    assert body2["status"] == "completed"

    _, doc = download(client, export_id)
    verify_digest(doc)
    assert len(doc["members"]) == 2

    # 已完成任务不允许再重试。
    again = client.post(f"{API}/snapshot-exports/{export_id}/retry")
    assert again.status_code == 400


def test_retry_unknown_export_404(client, db):
    assert client.post(f"{API}/snapshot-exports/9999/retry").status_code == 404
    assert client.get(f"{API}/snapshot-exports/9999").status_code == 404


# ---------------------------------------------------------------------------
# 5. 重启后的下载一致性
# ---------------------------------------------------------------------------

def test_restart_recovers_preparing_and_preserves_content(client, db, export_dir):
    ids = _make_resources(db, n_ops=3)
    resp = request_export(client, dataset_id=ids["dataset_id"])
    export_id = resp.json()["id"]
    body = wait_for_terminal(client, export_id)
    assert body["status"] == "completed"
    original_sha = body["content_sha256"]

    artifact = os.path.join(export_dir, f"snapshot_{export_id}.json")
    with open(artifact, "rb") as fh:
        original_bytes = fh.read()

    # 模拟进程在构建中途崩溃：记录回到 preparing、产物缺失、.tmp 留有残文件。
    session = SessionLocal()
    try:
        record = (
            session.query(SnapshotExport)
            .filter(SnapshotExport.id == export_id)
            .one()
        )
        record.status = SnapshotExport.STATUS_PREPARING
        record.artifact_path = None
        record.content_sha256 = None
        session.commit()
    finally:
        session.close()
    os.remove(artifact)
    tmp_dir = os.path.join(export_dir, ".tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    leftover = os.path.join(tmp_dir, f"snapshot_{export_id}.999.tmp")
    with open(leftover, "w") as fh:
        fh.write('{"half": ')

    # 新进程启动：startup 钩子执行恢复。
    with TestClient(app) as new_client:
        recovered = wait_for_terminal(new_client, export_id)
    assert recovered["status"] == "completed"
    assert recovered["content_sha256"] == original_sha
    assert not os.path.exists(leftover)

    with open(artifact, "rb") as fh:
        recovered_bytes = fh.read()
    assert recovered_bytes == original_bytes

    # 重启后仍可正常下载并通过摘要核验。
    resp, doc = download(client, export_id)
    verify_digest(doc)
    assert doc["manifest"]["content_sha256"] == original_sha
