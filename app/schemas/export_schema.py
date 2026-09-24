from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class SnapshotRequest(BaseModel):
    # 以路径参数 dataset_id 为准；请求体仅为便于附加未来参数而保留
    dataset_id: Optional[int] = Field(None, description="要冻结导出的数据集ID（以路径参数为准）")


class SnapshotStatus(BaseModel):
    id: int
    snapshot_key: str
    dataset_id: int
    dataset_version_number: Optional[int] = None
    as_of: Optional[str] = None
    caller_key: str
    permission_level: str
    policy_revision: Optional[int] = None
    policy_fingerprint: str
    data_fingerprint: Optional[str] = None
    status: str = Field(..., description="preparing / ready / failed")
    fail_reason: Optional[str] = None
    attempt_count: int = 0
    item_count: int = 0
    annotation_count: int = 0
    redacted_field_count: int = 0
    file_name: Optional[str] = None
    file_size: Optional[int] = None
    content_sha256: Optional[str] = None
    content_digest: Optional[str] = None
    created_at: Optional[str] = None
    prepared_at: Optional[str] = None
    completed_at: Optional[str] = None
    download_url: Optional[str] = None


class SnapshotRequestResponse(BaseModel):
    message: str
    reused: bool = Field(..., description="是否复用了已有的准备中/已完成快照")
    snapshot: SnapshotStatus


class CallerCreate(BaseModel):
    caller_key: str = Field(..., max_length=100, description="调用方唯一标识")
    display_name: str = Field(..., max_length=100)
    permission_level: str = Field("restricted", description="full 或 restricted")


class CallerUpdate(BaseModel):
    permission_level: Optional[str] = None
    is_active: Optional[bool] = None
    display_name: Optional[str] = Field(None, max_length=100)


class CallerResponse(BaseModel):
    id: int
    caller_key: str
    display_name: str
    permission_level: str
    is_active: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class RedactionPolicyResponse(BaseModel):
    id: int
    revision: int
    name: str
    config_json: dict
    policy_fingerprint: str
    is_current: bool
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class RedactionPolicyPublish(BaseModel):
    name: str = Field(..., max_length=100, description="新版本策略名称")
    serial_field_keys: Optional[List[str]] = Field(
        None, description="设备序列类字段名（完全替换）；省略则沿用当前版本"
    )
    text_field_keys: Optional[List[str]] = Field(
        None, description="自由文本类字段名（完全替换）；省略则沿用当前版本"
    )
    created_by: Optional[str] = Field(None, max_length=100)
