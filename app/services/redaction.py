"""快照导出脱敏：按调用方权限与脱敏策略移除敏感字段。

设备序列（robot_serial 及嵌套结构中的 serial/serial_number 等）与
自由文本（failure_description、review_notes、description、notes 等）
按字段名递归识别；策略以配置版本化，配置变化会改变策略指纹，
从而迫使相同请求形成新的快照版本。
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Any


REDACTION_TOKEN = "[REDACTED]"

# revision=1 的内置默认脱敏策略
DEFAULT_SERIAL_KEYS = [
    "robot_serial",
    "serial",
    "serial_number",
    "serial_no",
    "device_serial",
    "device_sn",
    "sn",
]
DEFAULT_TEXT_KEYS = [
    "failure_description",
    "review_notes",
    "description",
    "descriptions",
    "notes",
    "note",
    "comment",
    "comments",
    "remark",
    "remarks",
    "text",
    "free_text",
    "message",
]

DEFAULT_POLICY_CONFIG: dict[str, list[str]] = {
    "serial_field_keys": DEFAULT_SERIAL_KEYS,
    "text_field_keys": DEFAULT_TEXT_KEYS,
}


def _normalize_key(key: Any) -> str:
    # camelCase / PascalCase 转为下划线形式，再统一小写与空白/连字符
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", str(key).strip())
    snake = re.sub(r"[\s\-]+", "_", snake)
    return snake.lower()


@dataclass(frozen=True)
class RedactionPolicyConfig:
    """某次导出实际使用的脱敏配置（不可变）。"""

    serial_field_keys: frozenset[str]
    text_field_keys: frozenset[str]

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> "RedactionPolicyConfig":
        raw = raw or {}
        serial = {_normalize_key(k) for k in raw.get("serial_field_keys", DEFAULT_SERIAL_KEYS)}
        text = {_normalize_key(k) for k in raw.get("text_field_keys", DEFAULT_TEXT_KEYS)}
        return cls(serial_field_keys=frozenset(serial), text_field_keys=frozenset(text))

    def to_canonical_dict(self) -> dict[str, list[str]]:
        return {
            "serial_field_keys": sorted(self.serial_field_keys),
            "text_field_keys": sorted(self.text_field_keys),
        }


@dataclass
class RedactionResult:
    data: Any
    redacted_field_count: int


def redact_value(
    value: Any,
    config: RedactionPolicyConfig,
    counter: list[int] | None = None,
) -> Any:
    """递归复制并移除敏感字段。counter 为单元素列表用于跨递归累计命中数。"""
    if counter is None:
        counter = [0]

    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            normalized = _normalize_key(key)
            if normalized in config.serial_field_keys or normalized in config.text_field_keys:
                # 只对实际承载信息的值计数；空值移除不算脱敏命中
                if item not in (None, "", [], {}):
                    counter[0] += 1
                cleaned[key] = REDACTION_TOKEN
            else:
                cleaned[key] = redact_value(item, config, counter)
        return cleaned

    if isinstance(value, (list, tuple)):
        return [redact_value(item, config, counter) for item in value]

    return value


def apply_redaction(payload: Any, config: RedactionPolicyConfig) -> RedactionResult:
    """对已组装的导出负载执行脱敏，返回深拷贝后的新对象与命中字段数。"""
    counter = [0]
    cleaned = redact_value(copy.deepcopy(payload), config, counter)
    return RedactionResult(data=cleaned, redacted_field_count=counter[0])
