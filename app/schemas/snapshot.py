from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SnapshotExportCreateRequest(BaseModel):
    dataset_id: int = Field(..., description="要导出的数据集ID")
    dataset_version_id: Optional[int] = Field(
        None, description="数据集版本ID；不传则导出当前版本"
    )
    requestor_team: str = Field(..., max_length=100, description="调用方团队")
    requestor_role: str = Field(
        ..., max_length=50, description="调用方角色：owner/reviewer/external"
    )
    # 仅允许在角色默认权限之上进一步收窄（True -> False），不允许提权。
    permission_overrides: Optional[Dict[str, Any]] = Field(
        None, description="可选的权限收窄项，如 view_device_serial=false"
    )


class SnapshotExportResponse(BaseModel):
    id: int
    dataset_id: int
    dataset_version_id: Optional[int] = None
    dataset_version_label: Optional[str] = None
    requestor_team: str
    requestor_role: str
    permission_profile: Dict[str, Any]
    masking_policy_version: str
    artifact_schema_version: str
    request_fingerprint: str
    status: str
    item_count: int
    success_count: int
    failure_count: int
    annotation_complete_rate: float
    average_quality_score: Optional[float] = None
    grade_distribution: Optional[Dict[str, int]] = None
    redaction_summary: Optional[Dict[str, Any]] = None
    content_sha256: Optional[str] = None
    artifact_size: Optional[int] = None
    error_message: Optional[str] = None
    attempts: int
    created_at: datetime
    completed_at: Optional[datetime] = None
    reused: bool = Field(False, description="本次请求是否复用了既有快照")

    class Config:
        from_attributes = True
