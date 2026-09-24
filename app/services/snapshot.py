"""服务端快照导出。

职责：
- 请求到达时在同一事务内冻结成员列表并物化成员、标注与质量摘要，
  导出期间业务数据的变化不会进入快照文件；
- 以“数据集版本 + 调用方权限 + 脱敏策略指纹”构成幂等键，
  相同请求返回同一快照，权限或策略变化必然形成新版本；
- 后台线程仅负责序列化与原子落盘（临时文件 fsync 后 os.replace），
  失败只留下 failed 状态，不留下可下载的半成品；
- 服务重启时把中断的 preparing 快照标记为 failed，可重新发起重试。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models import (
    Annotation,
    ApiCaller,
    Dataset,
    DatasetItem,
    ExportSnapshot,
    OperationData,
    RedactionPolicy,
    RobotModel,
    Scene,
    Skill,
)
from app.services.redaction import RedactionPolicyConfig, apply_redaction

STATUS_PREPARING = "preparing"
STATUS_READY = "ready"
STATUS_FAILED = "failed"

SNAPSHOT_SCHEMA_VERSION = "1.0"

# 同一进程内，快照的“查找/冻结/物化”临界区串行化，
# 保证并发的相同请求只有一个构建者；物化本身是只读短事务。
_GLOBAL_LOCK = threading.RLock()

# 存活的后台构建线程，便于测试与优雅关停时等待落盘结束
_WORKERS: set[threading.Thread] = set()
_WORKERS_LOCK = threading.Lock()


def wait_for_snapshot_workers(timeout: float | None = None) -> None:
    """等待所有后台快照构建线程结束（主要用于测试隔离与关停）。"""
    with _WORKERS_LOCK:
        workers = list(_WORKERS)
    for worker in workers:
        worker.join(timeout=timeout)


class SnapshotRequestError(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat()


def policy_fingerprint(config: RedactionPolicyConfig) -> str:
    canonical = json.dumps(
        config.to_canonical_dict(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def make_snapshot_key(
    dataset_id: int,
    version_number: Optional[int],
    version_label: Optional[str],
    data_fingerprint: str,
    caller_key: str,
    permission_level: str,
    policy_revision: Optional[int],
    policy_fp: str,
) -> str:
    identity = {
        "dataset_id": dataset_id,
        "dataset_version_number": version_number,
        "dataset_version_label": version_label,
        "data_fingerprint": data_fingerprint,
        "caller_key": caller_key,
        "permission_level": permission_level,
        "policy_revision": policy_revision,
        "policy_fingerprint": policy_fp,
    }
    raw = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def canonical_digest(payload: dict[str, Any]) -> str:
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 启动引导：默认调用方、默认策略、中断恢复
# ---------------------------------------------------------------------------

def bootstrap_exports() -> None:
    os.makedirs(settings.EXPORT_DIR_ABS, exist_ok=True)

    # 清理上次进程崩溃可能残留的临时文件
    for name in os.listdir(settings.EXPORT_DIR_ABS):
        if name.endswith(".tmp") and name.startswith(".snapshot-"):
            try:
                os.unlink(os.path.join(settings.EXPORT_DIR_ABS, name))
            except OSError:
                pass

    db = SessionLocal()
    try:
        latest = (
            db.query(RedactionPolicy)
            .order_by(RedactionPolicy.revision.desc())
            .first()
        )
        if latest is None:
            config = RedactionPolicyConfig.from_dict(None)
            db.add(RedactionPolicy(
                revision=1,
                name="内置默认脱敏策略",
                config_json=config.to_canonical_dict(),
                policy_fingerprint=policy_fingerprint(config),
                is_current=True,
                created_by="system",
            ))
        elif not latest.is_current:
            latest.is_current = True

        if db.query(ApiCaller).count() == 0:
            db.add_all([
                ApiCaller(
                    caller_key="compliance-team",
                    display_name="合规审核组（数据所有方）",
                    permission_level="full",
                ),
                ApiCaller(
                    caller_key="external-review",
                    display_name="外部复核团队",
                    permission_level="restricted",
                ),
            ])

        # 进程重启：不可能还有活着的后台构建线程，中断任务标记失败
        interrupted = (
            db.query(ExportSnapshot)
            .filter(ExportSnapshot.status == STATUS_PREPARING)
            .all()
        )
        for row in interrupted:
            row.status = STATUS_FAILED
            row.fail_reason = "服务重启导致导出中断，请重新发起导出"

        db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 调用方与策略管理
# ---------------------------------------------------------------------------

def get_current_policy(db: Session) -> RedactionPolicy:
    policy = (
        db.query(RedactionPolicy)
        .filter(RedactionPolicy.is_current == True)  # noqa: E712
        .order_by(RedactionPolicy.revision.desc())
        .first()
    )
    if policy is None:
        raise SnapshotRequestError(500, "当前没有可用的脱敏策略")
    return policy


def list_callers(db: Session) -> list[ApiCaller]:
    return db.query(ApiCaller).order_by(ApiCaller.id.asc()).all()


def create_caller(
    db: Session,
    caller_key: str,
    display_name: str,
    permission_level: str,
) -> ApiCaller:
    if permission_level not in ("full", "restricted"):
        raise SnapshotRequestError(400, "permission_level 仅支持 full 或 restricted")
    if db.query(ApiCaller).filter(ApiCaller.caller_key == caller_key).first():
        raise SnapshotRequestError(400, "调用方标识已存在")
    caller = ApiCaller(
        caller_key=caller_key,
        display_name=display_name,
        permission_level=permission_level,
    )
    db.add(caller)
    db.commit()
    db.refresh(caller)
    return caller


def update_caller(
    db: Session,
    caller_key: str,
    permission_level: Optional[str] = None,
    is_active: Optional[bool] = None,
    display_name: Optional[str] = None,
) -> ApiCaller:
    caller = db.query(ApiCaller).filter(ApiCaller.caller_key == caller_key).first()
    if caller is None:
        raise SnapshotRequestError(404, "调用方不存在")
    if permission_level is not None:
        if permission_level not in ("full", "restricted"):
            raise SnapshotRequestError(400, "permission_level 仅支持 full 或 restricted")
        caller.permission_level = permission_level
    if is_active is not None:
        caller.is_active = is_active
    if display_name is not None:
        caller.display_name = display_name
    db.commit()
    db.refresh(caller)
    return caller


def list_policies(db: Session) -> list[RedactionPolicy]:
    return db.query(RedactionPolicy).order_by(RedactionPolicy.revision.desc()).all()


def publish_policy(
    db: Session,
    name: str,
    serial_field_keys: Optional[list[str]],
    text_field_keys: Optional[list[str]],
    created_by: Optional[str],
) -> RedactionPolicy:
    current = get_current_policy(db)
    current_config = RedactionPolicyConfig.from_dict(current.config_json)

    merged = {
        "serial_field_keys": (
            serial_field_keys
            if serial_field_keys is not None
            else sorted(current_config.serial_field_keys)
        ),
        "text_field_keys": (
            text_field_keys
            if text_field_keys is not None
            else sorted(current_config.text_field_keys)
        ),
    }
    config = RedactionPolicyConfig.from_dict(merged)
    fp = policy_fingerprint(config)
    if fp == current.policy_fingerprint:
        raise SnapshotRequestError(400, "新策略内容与当前版本一致，无需发布")

    current.is_current = False
    policy = RedactionPolicy(
        revision=current.revision + 1,
        name=name,
        config_json=config.to_canonical_dict(),
        policy_fingerprint=fp,
        is_current=True,
        created_by=created_by,
    )
    db.add(policy)
    db.commit()
    db.refresh(policy)
    return policy


# ---------------------------------------------------------------------------
# 冻结与物化
# ---------------------------------------------------------------------------

def _operation_dict(op: OperationData) -> dict[str, Any]:
    return {
        "id": op.id,
        "robot_model_id": op.robot_model_id,
        "scene_id": op.scene_id,
        "skill_id": op.skill_id,
        "robot_serial": op.robot_serial,
        "timestamp_start": _iso(op.timestamp_start),
        "timestamp_end": _iso(op.timestamp_end),
        "duration_ms": op.duration_ms,
        "motion_trajectory": op.motion_trajectory,
        "perception_records": op.perception_records,
        "grasp_result": op.grasp_result,
        "environment_conditions": op.environment_conditions,
        "hardware_status": op.hardware_status,
        "quality_score": op.quality_score,
        "completeness_score": op.completeness_score,
        "data_grade": op.data_grade,
        "created_at": _iso(op.created_at),
    }


def _annotation_dict(ann: Annotation) -> dict[str, Any]:
    return {
        "id": ann.id,
        "operation_data_id": ann.operation_data_id,
        "is_success": ann.is_success,
        "failure_category": ann.failure_category,
        "failure_subcategory": ann.failure_subcategory,
        "failure_description": ann.failure_description,
        "annotator": ann.annotator,
        "annotation_time": _iso(ann.annotation_time),
        "review_status": ann.review_status,
        "reviewer": ann.reviewer,
        "review_notes": ann.review_notes,
        "annotation_quality_score": ann.annotation_quality_score,
        "created_at": _iso(ann.created_at),
        "updated_at": _iso(ann.updated_at),
    }


def _freeze_operation_ids(db: Session, dataset_id: int) -> list[int]:
    rows = (
        db.query(DatasetItem.operation_data_id)
        .filter(DatasetItem.dataset_id == dataset_id)
        .order_by(DatasetItem.operation_data_id.asc())
        .all()
    )
    return [row[0] for row in rows]


def _annotation_fingerprint_view(ann: Optional[Annotation]) -> Optional[dict[str, Any]]:
    if ann is None:
        return None
    return {
        "is_success": ann.is_success,
        "failure_category": ann.failure_category,
        "failure_subcategory": ann.failure_subcategory,
        "failure_description": ann.failure_description,
        "annotator": ann.annotator,
        "annotation_time": _iso(ann.annotation_time),
        "review_status": ann.review_status,
        "reviewer": ann.reviewer,
        "review_notes": ann.review_notes,
        "annotation_quality_score": ann.annotation_quality_score,
        "updated_at": _iso(ann.updated_at),
    }


def compute_data_fingerprint(
    db: Session,
    dataset: Dataset,
    op_ids: list[int],
) -> str:
    """对冻结时刻的成员与标注内容取指纹。

    成员增删、作业记录或标注的任何字段变化都会改变指纹，
    从而即使数据集版本号未变，相同请求也会形成新快照版本。
    """
    view: list[dict[str, Any]] = []
    for op_id in op_ids:
        op = db.get(OperationData, op_id)
        if op is None:
            continue
        ann = (
            db.query(Annotation)
            .filter(Annotation.operation_data_id == op_id)
            .first()
        )
        view.append({
            "operation": {
                "id": op.id,
                "robot_serial": op.robot_serial,
                "timestamp_start": _iso(op.timestamp_start),
                "timestamp_end": _iso(op.timestamp_end),
                "duration_ms": op.duration_ms,
                "motion_trajectory": op.motion_trajectory,
                "perception_records": op.perception_records,
                "grasp_result": op.grasp_result,
                "environment_conditions": op.environment_conditions,
                "hardware_status": op.hardware_status,
                "quality_score": op.quality_score,
                "completeness_score": op.completeness_score,
                "data_grade": op.data_grade,
            },
            "annotation": _annotation_fingerprint_view(ann),
        })

    envelope = {
        "dataset": {
            "id": dataset.id,
            "version": dataset.version,
            "current_version": dataset.current_version,
            "review_status": dataset.review_status,
        },
        "members": view,
    }
    raw = json.dumps(
        envelope,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _build_payload(
    db: Session,
    dataset: Dataset,
    op_ids: list[int],
    as_of: datetime,
    caller: ApiCaller,
    policy: RedactionPolicy,
    policy_config: RedactionPolicyConfig,
) -> tuple[dict[str, Any], int, int]:
    """在调用方事务内读取一致快照并组装导出负载，返回 (负载, 标注数, 脱敏命中数)。"""
    robot_model = db.get(RobotModel, dataset.robot_model_id)
    scene = db.get(Scene, dataset.scene_id)
    skill = db.get(Skill, dataset.skill_id) if dataset.skill_id else None

    members: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    quality_scores: list[float] = []
    completeness_scores: list[float] = []
    grade_distribution: dict[str, int] = {}
    success_count = 0
    failure_count = 0

    for op_id in op_ids:
        op = db.get(OperationData, op_id)
        if op is None:
            # 冻结后极端情况下记录被删除：跳过且不进入来源清单
            continue
        ann = (
            db.query(Annotation)
            .filter(Annotation.operation_data_id == op_id)
            .first()
        )

        members.append({
            "operation": _operation_dict(op),
            "annotation": _annotation_dict(ann) if ann else None,
        })
        sources.append({
            "operation_data_id": op.id,
            "annotation_id": ann.id if ann else None,
        })

        if op.quality_score is not None:
            quality_scores.append(op.quality_score)
        if op.completeness_score is not None:
            completeness_scores.append(op.completeness_score)
        grade = op.data_grade or "未分级"
        grade_distribution[grade] = grade_distribution.get(grade, 0) + 1

        if ann is not None:
            if ann.is_success:
                success_count += 1
            else:
                failure_count += 1

    total_items = len(members)
    annotated_count = success_count + failure_count

    payload: dict[str, Any] = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "export_type": "dataset_snapshot",
        "frozen_at": _iso(as_of),
        "dataset": {
            "id": dataset.id,
            "name": dataset.name,
            "description": dataset.description,
            "version": dataset.version,
            "current_version": dataset.current_version,
            "review_status": dataset.review_status,
            "owner_team": dataset.owner_team,
            "contact_person": dataset.contact_person,
            "tags": dataset.tags,
            "license_info": dataset.license_info,
        },
        "related_resources": {
            "robot_model": (
                {"id": robot_model.id, "name": robot_model.name,
                 "manufacturer": robot_model.manufacturer,
                 "description": robot_model.description}
                if robot_model else None
            ),
            "scene": (
                {"id": scene.id, "name": scene.name,
                 "category": scene.category, "description": scene.description}
                if scene else None
            ),
            "skill": (
                {"id": skill.id, "name": skill.name,
                 "category": skill.category, "description": skill.description}
                if skill else None
            ),
        },
        "caller": {
            "caller_key": caller.caller_key,
            "permission_level": caller.permission_level,
        },
        "redaction": {
            "applied": caller.permission_level == "restricted",
            "policy_revision": policy.revision,
            "policy_fingerprint": policy.policy_fingerprint,
            "serial_field_keys": sorted(policy_config.serial_field_keys),
            "text_field_keys": sorted(policy_config.text_field_keys),
            "redacted_field_count": 0,
        },
        "quality_summary": {
            "total_items": total_items,
            "annotated_count": annotated_count,
            "annotation_complete_rate": (
                round(annotated_count / total_items, 6) if total_items else 0.0
            ),
            "success_count": success_count,
            "failure_count": failure_count,
            "average_quality_score": (
                round(sum(quality_scores) / len(quality_scores), 6)
                if quality_scores else None
            ),
            "average_completeness_score": (
                round(sum(completeness_scores) / len(completeness_scores), 6)
                if completeness_scores else None
            ),
            "grade_distribution": grade_distribution,
        },
        "members": members,
        "manifest": {
            "generated_by": settings.APP_NAME,
            "service_version": settings.APP_VERSION,
            "frozen_query": (
                "SELECT operation_data_id FROM dataset_items "
                f"WHERE dataset_id = {dataset.id} ORDER BY operation_data_id ASC"
            ),
            "dataset_ref": {
                "dataset_id": dataset.id,
                "dataset_version_number": dataset.current_version,
                "dataset_version_label": dataset.version,
            },
            "sources": sources,
            "source_counts": {
                "operations": total_items,
                "annotations": annotated_count,
            },
        },
    }

    redacted_field_count = 0
    if caller.permission_level == "restricted":
        result = apply_redaction(payload, policy_config)
        payload = result.data
        redacted_field_count = result.redacted_field_count
        payload["redaction"]["redacted_field_count"] = redacted_field_count

    # 内容摘要覆盖除摘要字段外的完整负载（含脱敏结果）
    payload["content_digest"] = canonical_digest(payload)
    return payload, annotated_count, redacted_field_count


# ---------------------------------------------------------------------------
# 快照请求入口与后台构建
# ---------------------------------------------------------------------------

def _file_names(snapshot_id: int, key: str) -> tuple[str, str]:
    name = f"snapshot_{snapshot_id}_{key[:12]}.json"
    return name, os.path.join(settings.EXPORT_DIR_ABS, name)


def _status_dict(row: ExportSnapshot) -> dict[str, Any]:
    return {
        "id": row.id,
        "snapshot_key": row.snapshot_key,
        "dataset_id": row.dataset_id,
        "dataset_version_number": row.dataset_version_number,
        "as_of": _iso(row.as_of),
        "caller_key": row.caller_key,
        "permission_level": row.permission_level,
        "policy_revision": row.policy_revision,
        "policy_fingerprint": row.policy_fingerprint,
        "data_fingerprint": row.data_fingerprint,
        "status": row.status,
        "fail_reason": row.fail_reason,
        "attempt_count": row.attempt_count,
        "item_count": row.item_count,
        "annotation_count": row.annotation_count,
        "redacted_field_count": row.redacted_field_count,
        "file_name": row.file_name,
        "file_size": row.file_size,
        "content_sha256": row.content_sha256,
        "content_digest": row.content_digest,
        "created_at": _iso(row.created_at),
        "prepared_at": _iso(row.prepared_at),
        "completed_at": _iso(row.completed_at),
        "download_url": (
            f"/api/v1/exports/{row.id}/download" if row.status == STATUS_READY else None
        ),
    }


def request_snapshot(
    dataset_id: int,
    caller_key: str,
    *,
    inject_failure: bool = False,
    delay_ms: int = 0,
) -> tuple[dict[str, Any], bool]:
    """发起或复用快照请求，返回 (状态字典, 是否复用已有快照)。"""
    with _GLOBAL_LOCK:
        db = SessionLocal()
        started_worker = False
        reused = False
        try:
            caller = (
                db.query(ApiCaller)
                .filter(ApiCaller.caller_key == caller_key)
                .first()
            )
            if caller is None:
                raise SnapshotRequestError(401, "调用方未登记，请通过 X-Caller-Key 提供有效标识")
            if not caller.is_active:
                raise SnapshotRequestError(403, "调用方已停用")

            dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
            if dataset is None:
                raise SnapshotRequestError(404, "数据集不存在")

            policy = get_current_policy(db)
            policy_config = RedactionPolicyConfig.from_dict(policy.config_json)

            # 先冻结成员列表，再对冻结内容取指纹：整个过程持锁，
            # 保证并发修改不会混入快照。
            as_of = datetime.now(timezone.utc)
            op_ids = _freeze_operation_ids(db, dataset.id)
            data_fp = compute_data_fingerprint(db, dataset, op_ids)

            key = make_snapshot_key(
                dataset_id=dataset.id,
                version_number=dataset.current_version,
                version_label=dataset.version,
                data_fingerprint=data_fp,
                caller_key=caller.caller_key,
                permission_level=caller.permission_level,
                policy_revision=policy.revision,
                policy_fp=policy.policy_fingerprint,
            )

            row = (
                db.query(ExportSnapshot)
                .filter(ExportSnapshot.snapshot_key == key)
                .first()
            )

            if row is not None and row.status in (STATUS_READY, STATUS_PREPARING):
                return _status_dict(row), True

            if row is None:
                row = ExportSnapshot(
                    snapshot_key=key,
                    dataset_id=dataset.id,
                    dataset_version_number=dataset.current_version,
                    as_of=as_of,
                    caller_id=caller.id,
                    caller_key=caller.caller_key,
                    permission_level=caller.permission_level,
                    policy_revision=policy.revision,
                    policy_fingerprint=policy.policy_fingerprint,
                    data_fingerprint=data_fp,
                    status=STATUS_PREPARING,
                    frozen_operation_ids=op_ids,
                )
                db.add(row)
                db.flush()
            else:
                # 同键失败记录：原地重试，保持同一快照标识
                row.status = STATUS_PREPARING
                row.fail_reason = None
                row.file_path = None
                row.file_name = None
                row.file_size = None
                row.content_sha256 = None
                row.content_digest = None
                row.prepared_at = None
                row.completed_at = None
                row.dataset_version_number = dataset.current_version
                row.policy_revision = policy.revision
                row.data_fingerprint = data_fp
                row.as_of = as_of
                row.frozen_operation_ids = op_ids

            # 关键步骤：在释放锁、返回响应前完成全部读取与脱敏，
            # 此后业务数据再变化也只会影响数据库，不影响本快照。
            payload, annotation_count, redacted_count = _build_payload(
                db, dataset, op_ids, as_of, caller, policy, policy_config
            )
            row.item_count = len(op_ids)
            row.annotation_count = annotation_count
            row.redacted_field_count = redacted_count

            db.commit()
            snapshot_id = row.id
            status = _status_dict(row)
            started_worker = True
        finally:
            db.close()

    if started_worker:
        def _tracked_target() -> None:
            try:
                _build_file_worker(
                    snapshot_id=snapshot_id,
                    payload=payload,
                    inject_failure=inject_failure,
                    delay_ms=delay_ms,
                )
            finally:
                with _WORKERS_LOCK:
                    _WORKERS.discard(worker)

        worker = threading.Thread(
            target=_tracked_target,
            daemon=True,
            name=f"snapshot-export-{snapshot_id}",
        )
        with _WORKERS_LOCK:
            _WORKERS.add(worker)
        worker.start()

    return status, reused


def _build_file_worker(
    snapshot_id: int,
    payload: dict[str, Any],
    inject_failure: bool,
    delay_ms: int,
) -> None:
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)

    db = SessionLocal()
    tmp_path: Optional[str] = None
    try:
        row = db.get(ExportSnapshot, snapshot_id)
        if row is None or row.status != STATUS_PREPARING:
            return

        row.attempt_count += 1

        if inject_failure:
            raise RuntimeError("导出被注入故障（X-Export-Fail），用于验证失败处理")

        file_bytes = (
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n"
        ).encode("utf-8")
        sha = hashlib.sha256(file_bytes).hexdigest()

        file_name, final_path = _file_names(row.id, row.snapshot_key)
        tmp_path = os.path.join(
            settings.EXPORT_DIR_ABS,
            f".snapshot-{row.id}-{uuid.uuid4().hex}.tmp",
        )
        with open(tmp_path, "wb") as fh:
            fh.write(file_bytes)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, final_path)
        tmp_path = None

        row.status = STATUS_READY
        row.fail_reason = None
        row.file_path = final_path
        row.file_name = file_name
        row.file_size = len(file_bytes)
        row.content_sha256 = sha
        row.content_digest = payload.get("content_digest")
        finished = datetime.now(timezone.utc)
        row.prepared_at = finished
        row.completed_at = finished
        db.commit()
    except Exception as exc:  # 失败落库，绝不留下半成品文件
        db.rollback()
        try:
            failed_row = db.get(ExportSnapshot, snapshot_id)
            if failed_row is not None and failed_row.status == STATUS_PREPARING:
                failed_row.status = STATUS_FAILED
                failed_row.fail_reason = str(exc)
                failed_row.attempt_count = max(failed_row.attempt_count, 1)
                db.commit()
        except Exception:
            db.rollback()
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    finally:
        db.close()


# ---------------------------------------------------------------------------
# 查询与下载
# ---------------------------------------------------------------------------

def list_snapshots(
    db: Session,
    dataset_id: Optional[int] = None,
    caller_key: Optional[str] = None,
    status_filter: Optional[str] = None,
) -> list[ExportSnapshot]:
    query = db.query(ExportSnapshot)
    if dataset_id is not None:
        query = query.filter(ExportSnapshot.dataset_id == dataset_id)
    if caller_key is not None:
        query = query.filter(ExportSnapshot.caller_key == caller_key)
    if status_filter is not None:
        query = query.filter(ExportSnapshot.status == status_filter)
    return query.order_by(ExportSnapshot.id.desc()).all()


def get_snapshot(db: Session, snapshot_id: int) -> ExportSnapshot:
    row = db.get(ExportSnapshot, snapshot_id)
    if row is None:
        raise SnapshotRequestError(404, "快照不存在")
    return row


def resolve_download(db: Session, snapshot_id: int) -> tuple[ExportSnapshot, str, bytes]:
    """返回 (快照行, 文件名, 文件字节)；状态未就绪或被篡改时给出明确错误。"""
    row = get_snapshot(db, snapshot_id)
    if row.status == STATUS_PREPARING:
        raise SnapshotRequestError(409, "快照仍在准备中，请稍后查询状态")
    if row.status == STATUS_FAILED:
        raise SnapshotRequestError(409, f"快照导出失败：{row.fail_reason or '未知原因'}；可重新发起导出")
    if not row.file_path or not os.path.exists(row.file_path):
        raise SnapshotRequestError(409, "快照产物文件丢失，请重新发起导出")

    with open(row.file_path, "rb") as fh:
        content = fh.read()
    actual_sha = hashlib.sha256(content).hexdigest()
    if actual_sha != row.content_sha256:
        raise SnapshotRequestError(409, "快照文件校验失败，内容可能已被篡改")
    return row, row.file_name or f"snapshot_{row.id}.json", content
