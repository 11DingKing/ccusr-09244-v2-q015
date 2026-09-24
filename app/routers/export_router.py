"""数据集快照导出接口。

鉴权约定（本地接口）：所有请求通过 X-Caller-Key 头标识调用方，
服务端按调用方登记的权限档位决定脱敏范围。
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import ApiCaller
from app.schemas.export_schema import (
    CallerCreate,
    CallerResponse,
    CallerUpdate,
    RedactionPolicyPublish,
    RedactionPolicyResponse,
    SnapshotRequest,
    SnapshotRequestResponse,
    SnapshotStatus,
)
from app.services import snapshot as snapshot_service

router = APIRouter()


def require_caller(
    db: Session = Depends(get_db),
    x_caller_key: Optional[str] = Header(None, alias="X-Caller-Key"),
) -> ApiCaller:
    if not x_caller_key:
        raise HTTPException(status_code=401, detail="缺少调用方标识头 X-Caller-Key")
    caller = db.query(ApiCaller).filter(ApiCaller.caller_key == x_caller_key).first()
    if caller is None:
        raise HTTPException(status_code=401, detail="调用方未登记")
    if not caller.is_active:
        raise HTTPException(status_code=403, detail="调用方已停用")
    return caller


@router.post(
    "/datasets/{dataset_id}/snapshot",
    response_model=SnapshotRequestResponse,
    tags=["快照导出"],
)
def request_dataset_snapshot(
    dataset_id: int,
    payload: Optional[SnapshotRequest] = None,
    db: Session = Depends(get_db),
    caller: ApiCaller = Depends(require_caller),
    x_export_fail: Optional[str] = Header(None, alias="X-Export-Fail"),
    x_export_delay_ms: Optional[int] = Header(None, alias="X-Export-Delay-Ms"),
):
    """冻结指定版本的成员、标注与质量摘要并生成快照。

    相同请求（数据集版本 + 调用方权限 + 脱敏策略均不变）重复执行
    返回同一快照；任一要素变化都会形成新版本。
    """
    inject_failure = bool(
        settings.EXPORT_FAULT_INJECTION
        and x_export_fail
        and x_export_fail.lower() in ("1", "true", "yes")
    )
    delay_ms = x_export_delay_ms if settings.EXPORT_FAULT_INJECTION and x_export_delay_ms else 0
    delay_ms = max(0, min(delay_ms or 0, 10000))

    try:
        status, reused = snapshot_service.request_snapshot(
            dataset_id=dataset_id,
            caller_key=caller.caller_key,
            inject_failure=inject_failure,
            delay_ms=delay_ms,
        )
    except snapshot_service.SnapshotRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)

    return SnapshotRequestResponse(
        message=("复用已有快照" if reused else "快照已开始准备"),
        reused=reused,
        snapshot=SnapshotStatus(**status),
    )


@router.get(
    "/exports",
    response_model=List[SnapshotStatus],
    tags=["快照导出"],
)
def list_snapshots(
    dataset_id: Optional[int] = Query(None, description="按数据集过滤"),
    status_filter: Optional[str] = Query(None, alias="status", description="preparing/ready/failed"),
    db: Session = Depends(get_db),
    caller: ApiCaller = Depends(require_caller),
):
    """列出调用方可见的快照；普通调用方只能看到自己的，full 可指定 caller_key 查全部。"""
    caller_key_filter: Optional[str] = caller.caller_key
    if caller.permission_level == "full":
        caller_key_filter = None  # 合规人员可查看全部快照
    rows = snapshot_service.list_snapshots(
        db,
        dataset_id=dataset_id,
        caller_key=caller_key_filter,
        status_filter=status_filter,
    )
    return [SnapshotStatus(**snapshot_service._status_dict(row)) for row in rows]


@router.get(
    "/exports/{snapshot_id}",
    response_model=SnapshotStatus,
    tags=["快照导出"],
)
def get_snapshot_status(
    snapshot_id: int,
    db: Session = Depends(get_db),
    caller: ApiCaller = Depends(require_caller),
):
    """查询快照准备、完成或失败状态。"""
    try:
        row = snapshot_service.get_snapshot(db, snapshot_id)
    except snapshot_service.SnapshotRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    if caller.permission_level != "full" and row.caller_key != caller.caller_key:
        raise HTTPException(status_code=403, detail="无权查看该调用方的快照")
    return SnapshotStatus(**snapshot_service._status_dict(row))


@router.get("/exports/{snapshot_id}/download", tags=["快照导出"])
def download_snapshot(
    snapshot_id: int,
    db: Session = Depends(get_db),
    caller: ApiCaller = Depends(require_caller),
):
    """下载已完成的快照文件；准备中/失败/文件缺失均不会返回半成品。"""
    try:
        row = snapshot_service.get_snapshot(db, snapshot_id)
        if caller.permission_level != "full" and row.caller_key != caller.caller_key:
            raise HTTPException(status_code=403, detail="无权下载该调用方的快照")
        _row, file_name, content = snapshot_service.resolve_download(db, snapshot_id)
    except snapshot_service.SnapshotRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)

    return Response(
        content=content,
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "X-Snapshot-Content-SHA256": row.content_sha256 or "",
        },
    )


# ---------------------------------------------------------------------------
# 调用方与脱敏策略管理（full 权限的合规人员使用）
# ---------------------------------------------------------------------------

def require_full_caller(caller: ApiCaller = Depends(require_caller)) -> ApiCaller:
    if caller.permission_level != "full":
        raise HTTPException(status_code=403, detail="仅 full 权限的合规人员可执行该操作")
    return caller


@router.get("/export-callers", response_model=List[CallerResponse], tags=["快照-调用方管理"])
def list_callers(
    db: Session = Depends(get_db),
    _: ApiCaller = Depends(require_full_caller),
):
    return snapshot_service.list_callers(db)


@router.post("/export-callers", response_model=CallerResponse, tags=["快照-调用方管理"])
def create_caller(
    req: CallerCreate,
    db: Session = Depends(get_db),
    _: ApiCaller = Depends(require_full_caller),
):
    try:
        return snapshot_service.create_caller(
            db, req.caller_key, req.display_name, req.permission_level
        )
    except snapshot_service.SnapshotRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.patch("/export-callers/{caller_key}", response_model=CallerResponse, tags=["快照-调用方管理"])
def update_caller(
    caller_key: str,
    req: CallerUpdate,
    db: Session = Depends(get_db),
    _: ApiCaller = Depends(require_full_caller),
):
    try:
        return snapshot_service.update_caller(
            db,
            caller_key,
            permission_level=req.permission_level,
            is_active=req.is_active,
            display_name=req.display_name,
        )
    except snapshot_service.SnapshotRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.get(
    "/redaction-policies",
    response_model=List[RedactionPolicyResponse],
    tags=["快照-脱敏策略"],
)
def list_policies(
    db: Session = Depends(get_db),
    _: ApiCaller = Depends(require_full_caller),
):
    return snapshot_service.list_policies(db)


@router.post(
    "/redaction-policies",
    response_model=RedactionPolicyResponse,
    tags=["快照-脱敏策略"],
)
def publish_policy(
    req: RedactionPolicyPublish,
    db: Session = Depends(get_db),
    _: ApiCaller = Depends(require_full_caller),
):
    """发布新的脱敏策略版本；内容真正变化才会生成新版本。"""
    try:
        return snapshot_service.publish_policy(
            db,
            name=req.name,
            serial_field_keys=req.serial_field_keys,
            text_field_keys=req.text_field_keys,
            created_by=req.created_by,
        )
    except snapshot_service.SnapshotRequestError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
