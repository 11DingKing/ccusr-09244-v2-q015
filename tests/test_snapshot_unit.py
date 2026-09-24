"""脱敏与快照指纹的单元测试。"""

from __future__ import annotations

from app.services.redaction import (
    DEFAULT_POLICY_CONFIG,
    REDACTION_TOKEN,
    RedactionPolicyConfig,
    apply_redaction,
)
from app.services.snapshot import (
    canonical_digest,
    make_snapshot_key,
    policy_fingerprint,
)


def test_redaction_removes_serial_everywhere_recursively():
    payload = {
        "robot_serial": "SN-001",
        "motion_trajectory": {"serial_number": "TRAJ-1", "nested": [{"deviceSn": "X"}]},
        "keep": 1,
        "list": [{"serialNo": "Y", "ok": True}, "plain"],
    }
    config = RedactionPolicyConfig.from_dict(DEFAULT_POLICY_CONFIG)
    result = apply_redaction(payload, config)

    assert result.data["robot_serial"] == REDACTION_TOKEN
    assert result.data["motion_trajectory"]["serial_number"] == REDACTION_TOKEN
    assert result.data["motion_trajectory"]["nested"][0]["deviceSn"] == REDACTION_TOKEN
    assert result.data["list"][0]["serialNo"] == REDACTION_TOKEN
    # 非敏感字段原样保留
    assert result.data["keep"] == 1
    assert result.data["list"][0]["ok"] is True
    assert result.data["list"][1] == "plain"
    assert result.redacted_field_count == 4
    # 输入不被修改
    assert payload["robot_serial"] == "SN-001"


def test_redaction_removes_free_text():
    payload = {
        "annotation": {
            "failure_description": "自由文本描述",
            "review_notes": "复核备注",
            "failure_category": "感知异常",
        },
        "description": "数据集描述",
        "notes": "一些说明",
        "name": "结构化名称不应被脱敏",
    }
    config = RedactionPolicyConfig.from_dict(DEFAULT_POLICY_CONFIG)
    result = apply_redaction(payload, config)

    ann = result.data["annotation"]
    assert ann["failure_description"] == REDACTION_TOKEN
    assert ann["review_notes"] == REDACTION_TOKEN
    assert ann["failure_category"] == "感知异常"
    assert result.data["description"] == REDACTION_TOKEN
    assert result.data["notes"] == REDACTION_TOKEN
    assert result.data["name"] == "结构化名称不应被脱敏"
    assert result.redacted_field_count == 4


def test_empty_and_null_fields_are_still_masked_but_not_counted():
    config = RedactionPolicyConfig.from_dict(DEFAULT_POLICY_CONFIG)
    result = apply_redaction({"robot_serial": None, "notes": ""}, config)
    assert result.data == {"robot_serial": REDACTION_TOKEN, "notes": REDACTION_TOKEN}
    assert result.redacted_field_count == 0


def test_policy_fingerprint_changes_with_config():
    base = RedactionPolicyConfig.from_dict(None)
    changed = RedactionPolicyConfig.from_dict(
        {"serial_field_keys": ["robot_serial"], "text_field_keys": ["notes"]}
    )
    assert policy_fingerprint(base) != policy_fingerprint(changed)

    # 顺序不同但内容相同 -> 指纹一致（规范化排序）
    reordered = RedactionPolicyConfig.from_dict(
        {"text_field_keys": ["notes"], "serial_field_keys": ["robot_serial"]}
    )
    changed_again = RedactionPolicyConfig.from_dict(
        {"serial_field_keys": ["robot_serial"], "text_field_keys": ["notes"]}
    )
    assert policy_fingerprint(reordered) == policy_fingerprint(changed_again)


def test_snapshot_key_versions_on_every_dimension():
    common = dict(
        dataset_id=1,
        version_number=3,
        version_label="1.2",
        data_fingerprint="datafp",
        caller_key="external-review",
        permission_level="restricted",
        policy_revision=1,
        policy_fp="pfp",
    )
    base_key = make_snapshot_key(**common)

    assert make_snapshot_key(**common) == base_key  # 完全相同 -> 同快照

    # 数据集版本变化
    changed = dict(common, version_number=4, version_label="1.3")
    assert make_snapshot_key(**changed) != base_key

    # 冻结数据内容变化
    assert make_snapshot_key(**dict(common, data_fingerprint="other")) != base_key

    # 调用方变化
    assert make_snapshot_key(**dict(common, caller_key="other-team")) != base_key

    # 权限档位变化
    assert make_snapshot_key(**dict(common, permission_level="full")) != base_key

    # 脱敏策略修订或指纹变化
    assert make_snapshot_key(**dict(common, policy_revision=2)) != base_key
    assert make_snapshot_key(**dict(common, policy_fp="newfp")) != base_key


def test_canonical_digest_is_order_insensitive_and_changes_with_content():
    a = {"x": 1, "y": [1, 2]}
    b = {"y": [1, 2], "x": 1}
    c = {"x": 1, "y": [1, 3]}
    assert canonical_digest(a) == canonical_digest(b)
    assert canonical_digest(a).startswith("sha256:")
    assert canonical_digest(a) != canonical_digest(c)
