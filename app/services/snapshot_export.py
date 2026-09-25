"""合规快照导出服务。

职责：
1. 按调用方角色解析数据权限（内部 owner / 内部 reviewer / 外部复核团队）；
2. 在请求事务内冻结指定版本数据集的成员、标注与质量摘要；
3. 按权限脱敏（设备序列号、标注与复核自由文本），并产出脱敏清单；
4. 生成内容摘要（SHA-256）与来源清单（manifest）；
5. 原子写盘：先写 .tmp/ 临时文件，校验摘要后再改名，失败不留半成品；
6. 后台线程构建，进程重启后恢复未完成任务，失败可重试（尝试号计入指纹外）。

相同请求（数据集版本 + 权限画像 + 脱敏策略版本 + 产物结构版本）的指纹
相同，永远复用同一条快照；权限或脱敏策略变化后指纹变化，必然形成新版本。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.config import settings
from app.models import (
    Annotation,
    Dataset,
    DatasetItem,
    DatasetVersion,
    OperationData,
    SnapshotExport,
)

# ---------------------------------------------------------------------------
# 权限与脱敏策略
# ---------------------------------------------------------------------------

MASK_TOKEN = "[REDACTED]"

# 脱敏策略版本：字段规则或默认角色权限发生变化时必须提升该版本，
# 提升后旧请求的指纹不再匹配，会自动形成新快照。
MASKING_POLICY_VERSION = "2026-09-01"

# 设备序列号以外、散落在 JSON 结构中的设备标识字段。
_SERIAL_JSON_KEYS = {"robot_serial", "serial_number", "device_serial", "serial"}


def _is_serial_key(key: str) -> bool:
    return (
        key in _SERIAL_JSON_KEYS
        or key.endswith("_serial")
        or key.endswith("_serial_number")
    )

# 标注/复核中的自由文本字段。
_FREE_TEXT_COLUMNS = ("failure_description", "review_notes")

# 默认角色权限画像。键固定且有序，保证 JSON 序列化确定性。
#   view_device_serial  是否可见设备序列号
#   view_free_text      是否可见标注/复核自由文本
#   scope               可见范围：owner 全部 / external 仅已发布数据集
DEFAULT_ROLE_PERMISSIONS: Dict[str, Dict[str, Any]] = {
    "owner": {
        "view_device_serial": True,
        "view_free_text": True,
        "scope": "owner",
    },
    "reviewer": {
        "view_device_serial": False,
        "view_free_text": True,
        "scope": "internal",
    },
    "external": {
        "view_device_serial": False,
        "view_free_text": False,
        "scope": "published_only",
    },
}


def resolve_permissions(
    role: str,
    permission_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """解析调用方权限画像。

    role 取 owner/reviewer/external；permission_overrides 仅允许收窄
    （把 True 改成 False），不允许越权放开，避免调用方通过自定义参数提权。
    """
    if role not in DEFAULT_ROLE_PERMISSIONS:
        raise ValueError(f"未知调用方角色: {role}")
    profile = dict(DEFAULT_ROLE_PERMISSIONS[role])
    if permission_overrides:
        for key in ("view_device_serial", "view_free_text"):
            if key in permission_overrides:
                value = bool(permission_overrides[key])
                if value and not profile[key]:
                    raise ValueError(f"角色 {role} 不允许开启 {key}")
                profile[key] = value
        if "scope" in permission_overrides:
            raise ValueError("数据可见范围不可由调用方指定")
    return {
        "role": role,
        "view_device_serial": profile["view_device_serial"],
        "view_free_text": profile["view_free_text"],
        "scope": profile["scope"],
    }


def _redact_serial_in_json(value: Any, counters: Dict[str, int]) -> Any:
    """递归移除 JSON 结构中的设备序列号字段。"""
    if isinstance(value, dict):
        cleaned = {}
        for key, val in value.items():
            if _is_serial_key(key) and val not in (None, ""):
                cleaned[key] = MASK_TOKEN
                counters["device_serial_fields"] += 1
            else:
                cleaned[key] = _redact_serial_in_json(val, counters)
        return cleaned
    if isinstance(value, list):
        return [_redact_serial_in_json(item, counters) for item in value]
    return value


def _as_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else value


def _build_member_record(op: OperationData, hide_serial: bool, counters: Dict[str, int]) -> Dict[str, Any]:
    record: Dict[str, Any] = {
        "operation_data_id": op.operation_data_id,
        "robot_model_id": op.robot_model_id,
        "scene_id": op.scene_id,
        "skill_id": op.skill_id,
        "robot_serial": op.robot_serial,
        "timestamp_start": _as_iso(op.timestamp_start),
        "timestamp_end": _as_iso(op.timestamp_end),
        "duration_ms": op.duration_ms,
        "quality_score": op.quality_score,
        "completeness_score": op.completeness_score,
        "data_grade": op.data_grade,
        "motion_trajectory": op.motion_trajectory,
        "perception_records": op.perception_records,
        "grasp_result": op.grasp_result,
        "environment_conditions": op.environment_conditions,
        "hardware_status": op.hardware_status,
    }
    if hide_serial:
        if record["robot_serial"] not in (None, ""):
            record["robot_serial"] = MASK_TOKEN
            counters["device_serial_fields"] += 1
        for field in (
            "motion_trajectory",
            "perception_records",
            "grasp_result",
            "environment_conditions",
            "hardware_status",
        ):
            record[field] = _redact_serial_in_json(record[field], counters)
    return record


def _build_annotation_record(
    ann: Optional[Annotation],
    hide_free_text: bool,
    counters: Dict[str, int],
    serial_literals: Optional[set] = None,
) -> Optional[Dict[str, Any]]:
    if ann is None:
        return None
    serial_literals = serial_literals or set()
    record: Dict[str, Any] = {
        "operation_data_id": ann.operation_data_id,
        "is_success": ann.is_success,
        "failure_category": ann.failure_category,
        "failure_subcategory": ann.failure_subcategory,
        "failure_description": ann.failure_description,
        "annotator": ann.annotator,
        "annotation_time": _as_iso(ann.annotation_time),
        "review_status": ann.review_status,
        "reviewer": ann.reviewer,
        "review_notes": ann.review_notes,
        "annotation_quality_score": ann.annotation_quality_score,
    }
    if hide_free_text:
        for field in _FREE_TEXT_COLUMNS:
            if record[field] not in (None, ""):
                record[field] = MASK_TOKEN
                counters["free_text_fields"] += 1
    elif serial_literals:
        # 文本对该调用方可见，但其中出现的设备序列号字面值仍须移除。
        for field in _FREE_TEXT_COLUMNS:
            value = record[field]
            if isinstance(value, str):
                scrubbed, hits = _scrub_serial_literals(value, serial_literals)
                if hits:
                    record[field] = scrubbed
                    counters["device_serial_fields"] += hits
    return record


def _scrub_serial_literals(text: str, serials: set) -> Tuple[str, int]:
    hits = 0
    for serial in sorted(serials, key=len, reverse=True):
        if serial and serial in text:
            count = text.count(serial)
            text = text.replace(serial, MASK_TOKEN)
            hits += count
    return text, hits


def _collect_serial_literals(value: Any, sink: set) -> None:
    """收集冻结结构中出现过的所有序列号字面值（含嵌套 JSON 字段）。"""
    if isinstance(value, dict):
        for key, val in value.items():
            if _is_serial_key(key) and isinstance(val, str) and val:
                sink.add(val)
            else:
                _collect_serial_literals(val, sink)
    elif isinstance(value, list):
        for item in value:
            _collect_serial_literals(item, sink)


def compute_fingerprint(
    dataset_id: int,
    dataset_version_id: Optional[int],
    permission_profile: Dict[str, Any],
    masking_policy_version: str,
    artifact_schema_version: str,
) -> str:
    """相同输入必然得到相同指纹；任何权限/策略变化都会改变指纹。"""
    payload = {
        "dataset_id": dataset_id,
        "dataset_version_id": dataset_version_id,
        "permission_profile": permission_profile,
        "masking_policy_version": masking_policy_version,
        "artifact_schema_version": artifact_schema_version,
    }
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# 冻结与快照构建
# ---------------------------------------------------------------------------

def freeze_version_sources(db: Session, dataset: Dataset, version: Optional[DatasetVersion]) -> Tuple[List[int], Dict[str, Any]]:
    """在当前事务内读取并冻结数据集某版本的成员与标注。

    成员以 DatasetItem 当前内容为准（数据集版本只保存统计摘要，未保存逐成员
    明细），调用方通过先冻结数据集（停止增删成员）再导出保证版本一致；
    读取发生在导出创建事务内，整段读取期间的状态被一次性固化。
    """
    items = (
        db.query(DatasetItem)
        .filter(DatasetItem.dataset_id == dataset.id)
        .order_by(DatasetItem.operation_data_id)
        .all()
    )
    operation_ids = [item.operation_data_id for item in items]

    operations = (
        db.query(OperationData)
        .filter(OperationData.id.in_(operation_ids))
        .order_by(OperationData.id)
        .all()
        if operation_ids
        else []
    )
    operations_by_id = {op.id: op for op in operations}

    annotations = (
        db.query(Annotation)
        .filter(Annotation.operation_data_id.in_(operation_ids))
        .all()
        if operation_ids
        else []
    )
    annotations_by_op = {a.operation_data_id: a for a in annotations}

    members = []
    for op_id in operation_ids:
        op = operations_by_id.get(op_id)
        if op is None:
            # 成员在冻结瞬间已被删除：记录来源缺口而不是静默丢弃。
            members.append({"operation_data_id": op_id, "missing": True})
            continue
        members.append(_freeze_operation(op))

    annotation_records = []
    for op_id in operation_ids:
        ann = annotations_by_op.get(op_id)
        if ann is None:
            continue
        annotation_records.append(_freeze_annotation(ann))

    frozen = {
        "dataset": {
            "id": dataset.id,
            "name": dataset.name,
            "description": dataset.description,
            "owner_team": dataset.owner_team,
            "review_status": dataset.review_status,
            "is_published": bool(dataset.is_published),
            "version_label": dataset.version,
            "current_version": dataset.current_version,
        },
        "version": (
            {
                "id": version.id,
                "version_number": version.version_number,
                "version_label": version.version_label,
                "total_items": version.total_items,
                "success_count": version.success_count,
                "failure_count": version.failure_count,
                "annotation_complete_rate": version.annotation_complete_rate,
                "average_quality_score": version.average_quality_score,
                "data_grade": version.data_grade,
                "created_at": version.created_at.isoformat() if version.created_at else None,
            }
            if version is not None
            else None
        ),
        "members": members,
        "annotations": annotation_records,
        "frozen_at": utcnow().isoformat(),
    }
    return operation_ids, frozen


def _freeze_operation(op: OperationData) -> Dict[str, Any]:
    return {
        "operation_data_id": op.id,
        "robot_model_id": op.robot_model_id,
        "scene_id": op.scene_id,
        "skill_id": op.skill_id,
        "robot_serial": op.robot_serial,
        "motion_trajectory": op.motion_trajectory,
        "perception_records": op.perception_records,
        "grasp_result": op.grasp_result,
        "timestamp_start": op.timestamp_start.isoformat() if op.timestamp_start else None,
        "timestamp_end": op.timestamp_end.isoformat() if op.timestamp_end else None,
        "duration_ms": op.duration_ms,
        "environment_conditions": op.environment_conditions,
        "hardware_status": op.hardware_status,
        "quality_score": op.quality_score,
        "completeness_score": op.completeness_score,
        "data_grade": op.data_grade,
    }


def _freeze_annotation(ann: Annotation) -> Dict[str, Any]:
    return {
        "annotation_id": ann.id,
        "operation_data_id": ann.operation_data_id,
        "is_success": ann.is_success,
        "failure_category": ann.failure_category,
        "failure_subcategory": ann.failure_subcategory,
        "failure_description": ann.failure_description,
        "annotator": ann.annotator,
        "annotation_time": _as_iso(ann.annotation_time),
        "review_status": ann.review_status,
        "reviewer": ann.reviewer,
        "review_notes": ann.review_notes,
        "annotation_quality_score": ann.annotation_quality_score,
    }


def build_snapshot_payload(record: SnapshotExport) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """把冻结的原始来源按权限画像脱敏，组装最终快照文档（含质量摘要）。"""
    permissions = record.permission_profile
    hide_serial = not permissions["view_device_serial"]
    hide_free_text = not permissions["view_free_text"]
    counters = {"device_serial_fields": 0, "free_text_fields": 0}

    sources = record.frozen_sources
    members_out: List[Dict[str, Any]] = []
    annotations_by_op: Dict[int, Dict[str, Any]] = {}

    # 收集冻结数据中的全部序列号字面值，用于从可见自由文本中二次清除。
    serial_literals: set = set()
    for raw in sources["members"]:
        _collect_serial_literals(raw, serial_literals)

    for raw in sources["members"]:
        if raw.get("missing"):
            members_out.append(
                {"operation_data_id": raw["operation_data_id"], "status": "missing_at_freeze"}
            )
            continue
        op_view = _obj_like(raw)
        members_out.append(_build_member_record(op_view, hide_serial, counters))

    for raw_ann in sources["annotations"]:
        ann_view = _obj_like(raw_ann)
        rendered = _build_annotation_record(
            ann_view, hide_free_text, counters, serial_literals
        )
        if rendered is not None:
            annotations_by_op[rendered["operation_data_id"]] = rendered

    # 成员与标注按相同顺序合并，便于外部团队逐项复核。
    for member in members_out:
        op_id = member.get("operation_data_id")
        member["annotation"] = annotations_by_op.get(op_id)

    # 质量摘要基于冻结数据重新计算，不依赖可能已漂移的实时表。
    total = len(members_out)
    present = [m for m in members_out if m.get("status") != "missing_at_freeze"]
    success_count = sum(
        1 for m in members_out
        if (m.get("annotation") or {}).get("is_success") is True
    )
    failure_count = sum(
        1 for m in members_out
        if (a := m.get("annotation")) is not None and a.get("is_success") is False
    )
    annotated = sum(1 for m in members_out if m.get("annotation") is not None)
    quality_scores = [
        m["quality_score"] for m in present if m.get("quality_score") is not None
    ]
    average_quality = (
        round(sum(quality_scores) / len(quality_scores), 6)
        if quality_scores else None
    )
    grade_distribution: Dict[str, int] = {}
    for m in present:
        grade = m.get("data_grade") or "未分级"
        grade_distribution[grade] = grade_distribution.get(grade, 0) + 1

    quality_summary = {
        "total_items": total,
        "present_items": len(present),
        "annotated_items": annotated,
        "annotation_complete_rate": round(annotated / total, 6) if total else 0.0,
        "success_count": success_count,
        "failure_count": failure_count,
        "average_quality_score": average_quality,
        "grade_distribution": grade_distribution,
    }

    present_ids = {
        m["operation_data_id"] for m in members_out if m.get("status") != "missing_at_freeze"
    }
    manifest = {
        "snapshot_export_id": record.id,
        "dataset": sources["dataset"],
        "version": sources["version"],
        "generated_for": {
            "requestor_team": record.requestor_team,
            "requestor_role": record.requestor_role,
            "permission_profile": permissions,
        },
        "masking_policy_version": record.masking_policy_version,
        "artifact_schema_version": record.artifact_schema_version,
        "frozen_at": sources["frozen_at"],
        "sources": [
            {
                "operation_data_id": op_id,
                "table": "operation_data",
                "present": op_id in present_ids,
            }
            for op_id in record.frozen_operation_ids
        ],
        "redaction_summary": {
            "device_serial_fields_redacted": counters["device_serial_fields"],
            "free_text_fields_redacted": counters["free_text_fields"],
            "mask_token": MASK_TOKEN,
        },
    }

    payload = {
        "manifest": manifest,
        "quality_summary": quality_summary,
        "members": members_out,
    }
    return payload, counters


class _obj_like:
    """让脱敏函数同时接受 ORM 对象与冻结字典。"""

    def __init__(self, data: Dict[str, Any]):
        self.__dict__.update(data)


def canonical_json(payload: Dict[str, Any]) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def content_digest(payload: Dict[str, Any]) -> str:
    """内容摘要：对除 manifest.content_sha256 外的整个文档取摘要。

    摘要字段自身不能参与哈希（自引用无解），因此下载方核验时同样剔除该字段。
    """
    without_hash = dict(payload)
    manifest = dict(without_hash.get("manifest") or {})
    manifest.pop("content_sha256", None)
    without_hash["manifest"] = manifest
    return hashlib.sha256(canonical_json(without_hash)).hexdigest()


def artifact_paths(record_id: int) -> Tuple[str, str, str]:
    base = os.path.abspath(settings.SNAPSHOT_EXPORT_DIR)
    final = os.path.join(base, f"snapshot_{record_id}.json")
    tmp_dir = os.path.join(base, ".tmp")
    tmp = os.path.join(tmp_dir, f"snapshot_{record_id}.{os.getpid()}.tmp")
    return final, tmp_dir, tmp


def write_atomic(record_id: int, payload: Dict[str, Any]) -> Tuple[str, int, str]:
    """原子写盘并回读校验摘要；任何失败都不会留下最终文件。"""
    final, tmp_dir, tmp = artifact_paths(record_id)
    os.makedirs(tmp_dir, exist_ok=True)
    try:
        digest = content_digest(payload)
        data = canonical_json(payload)
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        # 回读校验，防止半成品落盘。
        with open(tmp, "rb") as fh:
            written = fh.read()
        reloaded = json.loads(written.decode("utf-8"))
        if content_digest(reloaded) != digest:
            raise RuntimeError("快照临时文件回读校验失败")
        os.replace(tmp, final)
        return final, len(data), digest
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# 后台执行
# ---------------------------------------------------------------------------

_worker_lock = threading.Lock()


def build_export(record_id: int, session_factory) -> None:
    """构建单条快照（在线程中执行）。失败时落库 failed 状态，不留半成品。"""
    db: Session = session_factory()
    try:
        record = db.query(SnapshotExport).filter(SnapshotExport.id == record_id).first()
        if record is None or record.status != SnapshotExport.STATUS_PREPARING:
            return

        payload, counters = build_snapshot_payload(record)

        # 先算出内容摘要写入清单，再以最终文档原子落盘；摘要不覆盖自身字段。
        digest = content_digest(payload)
        payload["manifest"]["content_sha256"] = digest

        path, size, digest = write_atomic(record.id, payload)

        summary = payload["quality_summary"]
        record.status = SnapshotExport.STATUS_COMPLETED
        record.content_sha256 = digest
        record.artifact_path = path
        record.artifact_size = size
        record.success_count = summary["success_count"]
        record.failure_count = summary["failure_count"]
        record.annotation_complete_rate = summary["annotation_complete_rate"]
        record.average_quality_score = summary["average_quality_score"]
        record.grade_distribution = summary["grade_distribution"]
        record.redaction_summary = payload["manifest"]["redaction_summary"]
        record.completed_at = utcnow()
        record.error_message = None
        db.commit()
    except Exception as exc:  # noqa: BLE001 - 任何构建失败都要进入 failed 状态
        db.rollback()
        record = db.query(SnapshotExport).filter(SnapshotExport.id == record_id).first()
        if record is not None:
            record.status = SnapshotExport.STATUS_FAILED
            record.error_message = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"[:4000]
            record.artifact_path = None
            db.commit()
    finally:
        db.close()


def start_build(record_id: int, session_factory) -> None:
    thread = threading.Thread(
        target=build_export,
        args=(record_id, session_factory),
        name=f"snapshot-export-{record_id}",
        daemon=True,
    )
    thread.start()


def recover_preparing(session_factory) -> int:
    """进程重启后恢复：preparing 任务重新入队。

    由于最终文件只通过原子改名产生，重启前没有任何可下载半成品；
    .tmp 下的残留文件直接清理。
    """
    with _worker_lock:
        db: Session = session_factory()
        try:
            stale = (
                db.query(SnapshotExport)
                .filter(SnapshotExport.status == SnapshotExport.STATUS_PREPARING)
                .all()
            )
            ids = [r.id for r in stale]
        finally:
            db.close()

    base = os.path.abspath(settings.SNAPSHOT_EXPORT_DIR)
    tmp_dir = os.path.join(base, ".tmp")
    if os.path.isdir(tmp_dir):
        for name in os.listdir(tmp_dir):
            if name.endswith(".tmp"):
                try:
                    os.remove(os.path.join(tmp_dir, name))
                except OSError:
                    pass

    for record_id in ids:
        start_build(record_id, session_factory)
    return len(ids)
