"""快照导出接口。

- POST   /snapshot-exports            请求冻结并导出（相同请求幂等复用）
- GET    /snapshot-exports            查询导出列表（可按团队/数据集/状态过滤）
- GET    /snapshot-exports/{id}       查询单条状态：preparing/completed/failed
- GET    /snapshot-exports/{id}/download   下载已完成快照（带摘要校验）
- POST   /snapshot-exports/{id}/retry     失败重试
"""

import json
import os
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.database import SessionLocal, get_db
from app.models import Dataset, DatasetVersion, SnapshotExport
from app.schemas.snapshot import SnapshotExportCreateRequest, SnapshotExportResponse
from app.services import snapshot_export as svc

router = APIRouter()


def _get_dataset_or_404(db: Session, dataset_id: int) -> Dataset:
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if dataset is None:
        raise HTTPException(status_code=404, detail="数据集不存在")
    return dataset


def _get_version_or_400(db: Session, dataset: Dataset, version_id: Optional[int]):
    if version_id is None:
        version = (
            db.query(DatasetVersion)
            .filter(
                DatasetVersion.dataset_id == dataset.id,
                DatasetVersion.version_number == dataset.current_version,
            )
            .first()
        )
        return version
    version = (
        db.query(DatasetVersion)
        .filter(
            DatasetVersion.id == version_id,
            DatasetVersion.dataset_id == dataset.id,
        )
        .first()
    )
    if version is None:
        raise HTTPException(status_code=400, detail="指定的数据集版本不存在或不属于该数据集")
    return version


def _serialize(record: SnapshotExport, reused: bool = False) -> SnapshotExportResponse:
    resp = SnapshotExportResponse.model_validate(record)
    resp.reused = reused
    return resp


@router.post(
    "/snapshot-exports",
    response_model=SnapshotExportResponse,
    tags=["合规快照导出"],
)
def create_snapshot_export(
    req: SnapshotExportCreateRequest,
    response: Response,
    db: Session = Depends(get_db),
):
    """请求生成快照。相同请求（版本+权限+脱敏策略）重复执行返回同一条快照。"""
    try:
        permissions = svc.resolve_permissions(req.requestor_role, req.permission_overrides)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    dataset = _get_dataset_or_404(db, req.dataset_id)
    if permissions["scope"] == "published_only" and not dataset.is_published:
        raise HTTPException(status_code=403, detail="外部复核团队仅可导出已发布数据集")

    version = _get_version_or_400(db, dataset, req.dataset_version_id)

    fingerprint = svc.compute_fingerprint(
        dataset_id=dataset.id,
        dataset_version_id=version.id if version is not None else None,
        permission_profile=permissions,
        masking_policy_version=svc.MASKING_POLICY_VERSION,
        artifact_schema_version=settings.SNAPSHOT_ARTIFACT_VERSION,
    )

    existing = (
        db.query(SnapshotExport)
        .filter(SnapshotExport.request_fingerprint == fingerprint)
        .first()
    )
    if existing is not None:
        # 完成的快照原样返回；准备中/失败的快照也不重复创建，
        # 失败的快照由调用方走重试接口。
        response.status_code = 200
        return _serialize(existing, reused=True)

    # 在请求事务内一次性冻结成员与标注，提交后即与后续数据变更隔离。
    operation_ids, frozen = svc.freeze_version_sources(db, dataset, version)

    record = SnapshotExport(
        dataset_id=dataset.id,
        dataset_version_id=version.id if version is not None else None,
        dataset_version_label=version.version_label if version is not None else dataset.version,
        requestor_team=req.requestor_team,
        requestor_role=req.requestor_role,
        permission_profile=permissions,
        masking_policy_version=svc.MASKING_POLICY_VERSION,
        artifact_schema_version=settings.SNAPSHOT_ARTIFACT_VERSION,
        request_fingerprint=fingerprint,
        status=SnapshotExport.STATUS_PREPARING,
        frozen_operation_ids=operation_ids,
        frozen_sources=frozen,
        item_count=len(operation_ids),
        attempts=1,
        created_at=svc.utcnow(),
    )
    db.add(record)
    response.status_code = 201
    try:
        db.commit()
    except IntegrityError:
        # 并发请求竞态：另一请求已插入相同指纹，复用先提交者。
        db.rollback()
        winner = (
            db.query(SnapshotExport)
            .filter(SnapshotExport.request_fingerprint == fingerprint)
            .first()
        )
        if winner is None:  # pragma: no cover - 理论上不可达
            raise HTTPException(status_code=500, detail="快照创建竞态处理失败，请重试")
        response.status_code = 200
        return _serialize(winner, reused=True)
    db.refresh(record)

    svc.start_build(record.id, SessionLocal)
    return _serialize(record)


@router.get(
    "/snapshot-exports",
    response_model=List[SnapshotExportResponse],
    tags=["合规快照导出"],
)
def list_snapshot_exports(
    dataset_id: Optional[int] = Query(None, description="按数据集过滤"),
    requestor_team: Optional[str] = Query(None, description="按调用方团队过滤"),
    status: Optional[str] = Query(
        None, description="按状态过滤：preparing/completed/failed"
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    if status is not None and status not in {
        SnapshotExport.STATUS_PREPARING,
        SnapshotExport.STATUS_COMPLETED,
        SnapshotExport.STATUS_FAILED,
    }:
        raise HTTPException(status_code=400, detail="无效的快照状态")
    query = db.query(SnapshotExport)
    if dataset_id is not None:
        query = query.filter(SnapshotExport.dataset_id == dataset_id)
    if requestor_team:
        query = query.filter(SnapshotExport.requestor_team == requestor_team)
    if status:
        query = query.filter(SnapshotExport.status == status)
    return (
        query.order_by(SnapshotExport.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )


def _get_record_or_404(db: Session, export_id: int) -> SnapshotExport:
    record = db.query(SnapshotExport).filter(SnapshotExport.id == export_id).first()
    if record is None:
        raise HTTPException(status_code=404, detail="快照不存在")
    return record


@router.get(
    "/snapshot-exports/{export_id}",
    response_model=SnapshotExportResponse,
    tags=["合规快照导出"],
)
def get_snapshot_export(export_id: int, db: Session = Depends(get_db)):
    """查询快照状态：preparing（准备中）/ completed（已完成）/ failed（失败）。"""
    return _serialize(_get_record_or_404(db, export_id))


@router.get("/snapshot-exports/{export_id}/download", tags=["合规快照导出"])
def download_snapshot_export(export_id: int, db: Session = Depends(get_db)):
    record = _get_record_or_404(db, export_id)
    if record.status == SnapshotExport.STATUS_PREPARING:
        raise HTTPException(status_code=409, detail="快照仍在准备中，暂不可下载")
    if record.status == SnapshotExport.STATUS_FAILED:
        raise HTTPException(status_code=409, detail="快照生成失败，无产物可下载，请重试")
    if not record.artifact_path or not os.path.isfile(record.artifact_path):
        raise HTTPException(status_code=410, detail="快照产物文件缺失，请重试生成")

    # 下载前重新核验摘要与大小，确保对外提供的文件与记录一致。
    with open(record.artifact_path, "rb") as fh:
        data = fh.read()
    try:
        actual_digest = svc.content_digest(json.loads(data.decode("utf-8")))
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(status_code=500, detail="快照文件内容无法解析")
    if actual_digest != record.content_sha256:
        raise HTTPException(status_code=500, detail="快照文件摘要校验失败")
    if record.artifact_size is not None and len(data) != record.artifact_size:
        raise HTTPException(status_code=500, detail="快照文件大小校验失败")

    filename = (
        f"dataset_{record.dataset_id}_snapshot_{record.id}.json"
    )
    return FileResponse(
        record.artifact_path,
        media_type="application/json",
        filename=filename,
        headers={
            "X-Snapshot-Id": str(record.id),
            "X-Content-SHA256": record.content_sha256 or "",
        },
    )


@router.post(
    "/snapshot-exports/{export_id}/retry",
    response_model=SnapshotExportResponse,
    tags=["合规快照导出"],
)
def retry_snapshot_export(export_id: int, db: Session = Depends(get_db)):
    """失败任务重试。请求指纹不变（仍为同一逻辑快照），attempts 递增。"""
    record = _get_record_or_404(db, export_id)
    artifact_missing = bool(record.artifact_path) and not os.path.isfile(record.artifact_path)
    if record.status != SnapshotExport.STATUS_FAILED and not artifact_missing:
        raise HTTPException(status_code=400, detail="仅失败状态或产物缺失的快照可以重试")

    if record.artifact_path and os.path.isfile(record.artifact_path):
        try:
            os.remove(record.artifact_path)
        except OSError:
            pass
    record.status = SnapshotExport.STATUS_PREPARING
    record.error_message = None
    record.artifact_path = None
    record.content_sha256 = None
    record.artifact_size = None
    record.attempts = (record.attempts or 0) + 1
    db.commit()
    db.refresh(record)

    svc.start_build(record.id, SessionLocal)
    return _serialize(record)
