"""真实本地 HTTP 服务（uvicorn 子进程）测试。

验证 TestClient 无法覆盖的两件事：
- 多个并发 HTTP 请求下的快照幂等与冻结一致性；
- 服务进程重启后，已完成快照仍可逐字节下载，
  且重启时处于 preparing 的任务被标记为失败、可重试。
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import time

import httpx

from tests.conftest import EXTERNAL_HEADERS, FULL_HEADERS

API = "/api/v1"


def _seed(api: httpx.Client) -> dict:
    model = api.post(
        f"{API}/robot-models",
        json={"name": "RM-LIVE", "manufacturer": "Acme"},
        headers=FULL_HEADERS,
    ).json()
    scene = api.post(
        f"{API}/scenes",
        json={"name": "LIVE场景", "category": "测试"},
        headers=FULL_HEADERS,
    ).json()
    skill = api.post(
        f"{API}/skills",
        json={"name": "LIVE技能", "category": "测试"},
        headers=FULL_HEADERS,
    ).json()

    ops = []
    for idx, serial in enumerate(["LIVE-SN-1", "LIVE-SN-2", "LIVE-SN-3"]):
        op = api.post(
            f"{API}/operations",
            json={
                "robot_model_id": model["id"],
                "scene_id": scene["id"],
                "skill_id": skill["id"],
                "robot_serial": serial,
                "motion_trajectory": {"serial_number": f"TRAJ-{idx}"},
                "perception_records": {},
                "timestamp_start": f"2026-01-0{idx + 1}T00:00:00+00:00",
                "timestamp_end": f"2026-01-0{idx + 1}T00:01:00+00:00",
            },
            headers=FULL_HEADERS,
        ).json()
        ops.append(op)

    api.post(
        f"{API}/annotations",
        json={
            "operation_data_id": ops[0]["id"],
            "is_success": False,
            "failure_category": "硬件故障",
            "failure_description": "通信总线日志含敏感序列号",
        },
        headers=FULL_HEADERS,
    )

    dataset = api.post(
        f"{API}/datasets",
        json={
            "name": "LIVE数据集",
            "robot_model_id": model["id"],
            "scene_id": scene["id"],
            "skill_id": skill["id"],
            "owner_team": "合规组",
            "operation_data_ids": [op["id"] for op in ops],
        },
        headers=FULL_HEADERS,
    ).json()
    return {"model": model, "scene": scene, "skill": skill, "ops": ops, "dataset": dataset}


def _wait(api: httpx.Client, snapshot_id: int, headers: dict, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = api.get(f"{API}/exports/{snapshot_id}", headers=headers).json()
        if body["status"] != "preparing":
            return body
        time.sleep(0.05)
    raise AssertionError("快照超时未完成")


def _request(api: httpx.Client, dataset_id: int, headers: dict, extra: dict | None = None) -> dict:
    return api.post(
        f"{API}/datasets/{dataset_id}/snapshot",
        json={},
        headers={**headers, **(extra or {})},
    ).json()


def test_concurrent_requests_during_modifications_are_consistent(live_server):
    with live_server.client() as api:
        seed = _seed(api)
        ds_id = seed["dataset"]["id"]

        # 阶段一：无修改时，10 个完全相同的并发请求只能构建一个快照
        first_ids: list[int] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            futs = [pool.submit(lambda: first_ids.append(
                _request(api, ds_id, EXTERNAL_HEADERS)["snapshot"]["id"]
            )) for _ in range(10)]
            for fut in futs:
                fut.result()
        assert len(set(first_ids)) == 1, first_ids
        baseline_id = first_ids[0]
        baseline = _wait(api, baseline_id, EXTERNAL_HEADERS)
        assert baseline["status"] == "ready"
        baseline_bytes = api.get(
            f"{API}/exports/{baseline_id}/download", headers=EXTERNAL_HEADERS
        ).content

        # 阶段二：每轮“3 个并发相同请求 + 1 次业务修改”交替进行
        def export_three(out: list[int]) -> None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as inner:
                futs = [
                    inner.submit(lambda: out.append(
                        _request(api, ds_id, EXTERNAL_HEADERS)["snapshot"]["id"]
                    ))
                    for _ in range(3)
                ]
                for fut in futs:
                    fut.result()

        def add_member(idx: int) -> None:
            new_op = api.post(
                f"{API}/operations",
                json={
                    "robot_model_id": seed["model"]["id"],
                    "scene_id": seed["scene"]["id"],
                    "skill_id": seed["skill"]["id"],
                    "robot_serial": f"RACER-{idx}",
                    "motion_trajectory": {},
                    "perception_records": {},
                    "timestamp_start": f"2026-02-0{idx + 1}T00:00:00+00:00",
                    "timestamp_end": f"2026-02-0{idx + 1}T00:01:00+00:00",
                },
                headers=FULL_HEADERS,
            ).json()
            api.post(
                f"{API}/datasets/{ds_id}/items",
                json={"operation_data_ids": [new_op["id"]]},
                headers=FULL_HEADERS,
            )

        all_round_ids: list[int] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            for idx in range(4):
                round_ids: list[int] = []
                done = [pool.submit(export_three, round_ids), pool.submit(add_member, idx)]
                for fut in done:
                    fut.result()
                all_round_ids.extend(round_ids)

        # 每个快照都必须完成、内部自洽、外部视角完全脱敏
        seen: dict[int, dict] = {}
        for sid in {baseline_id, *all_round_ids}:
            status = _wait(api, sid, EXTERNAL_HEADERS)
            assert status["status"] == "ready", (sid, status)
            payload = json.loads(
                api.get(f"{API}/exports/{sid}/download", headers=EXTERNAL_HEADERS).content
            )
            members = payload["members"]
            # 成员、摘要、来源清单三者数量一致 —— 冻结原子性
            assert len(members) == payload["quality_summary"]["total_items"]
            assert len(members) == len(payload["manifest"]["sources"])
            raw = json.dumps(payload, ensure_ascii=False)
            assert "LIVE-SN-" not in raw and "RACER-" not in raw
            assert "[REDACTED]" in raw
            seen[sid] = payload

        # 至少存在两个不同的冻结状态版本（初始 3 条与增长后的版本）
        counts = sorted({p["quality_summary"]["total_items"] for p in seen.values()})
        assert counts[0] == 3 and counts[-1] >= 4, counts

        # 修改前的基准快照不受后续并发修改影响，逐字节不变
        assert api.get(
            f"{API}/exports/{baseline_id}/download", headers=EXTERNAL_HEADERS
        ).content == baseline_bytes


def test_snapshot_download_is_byte_identical_after_restart(live_server):
    with live_server.client() as api:
        seed = _seed(api)
        ds_id = seed["dataset"]["id"]
        external = _request(api, ds_id, EXTERNAL_HEADERS)
        ext_sid = external["snapshot"]["id"]
        ext_status = _wait(api, ext_sid, EXTERNAL_HEADERS)

        full = _request(api, ds_id, FULL_HEADERS)
        full_sid = full["snapshot"]["id"]
        _wait(api, full_sid, FULL_HEADERS)

        ext_bytes = api.get(f"{API}/exports/{ext_sid}/download", headers=EXTERNAL_HEADERS).content
        full_bytes = api.get(f"{API}/exports/{full_sid}/download", headers=FULL_HEADERS).content

    # 重启服务（同一数据库与导出目录）
    live_server.stop()
    live_server.start()

    with live_server.client() as api:
        health = api.get("/health")
        assert health.status_code == 200

        ext_status_after = api.get(f"{API}/exports/{ext_sid}", headers=EXTERNAL_HEADERS).json()
        assert ext_status_after["status"] == "ready"
        ext_bytes_after = api.get(
            f"{API}/exports/{ext_sid}/download", headers=EXTERNAL_HEADERS
        ).content
        assert ext_bytes_after == ext_bytes
        assert ext_status_after["content_sha256"] == ext_status["content_sha256"]

        full_bytes_after = api.get(
            f"{API}/exports/{full_sid}/download", headers=FULL_HEADERS
        ).content
        assert full_bytes_after == full_bytes

        # 脱敏结果在重启后仍然成立
        payload = json.loads(ext_bytes_after)
        assert all(
            m["operation"]["robot_serial"] == "[REDACTED]" for m in payload["members"]
        )

        # 相同请求在重启后仍然幂等：返回同一快照
        again = _request(api, seed["dataset"]["id"], EXTERNAL_HEADERS)
        assert again["reused"] is True
        assert again["snapshot"]["id"] == ext_sid


def test_preparing_snapshot_marked_failed_on_restart_and_can_retry(live_server):
    with live_server.client() as api:
        seed = _seed(api)
        ds_id = seed["dataset"]["id"]

        # 为外部调用方发起一个“落盘极慢”的导出
        body = _request(
            api,
            ds_id,
            EXTERNAL_HEADERS,
            extra={"X-Export-Delay-Ms": "10000"},
        )
        sid = body["snapshot"]["id"]
        preparing = api.get(f"{API}/exports/{sid}", headers=EXTERNAL_HEADERS).json()
        assert preparing["status"] == "preparing"

    # 在后台落盘完成前强制重启
    live_server.stop()
    live_server.start()

    with live_server.client() as api:
        after = api.get(f"{API}/exports/{sid}", headers=EXTERNAL_HEADERS).json()
        assert after["status"] == "failed"
        assert "重启" in after["fail_reason"]
        assert api.get(f"{API}/exports/{sid}/download", headers=EXTERNAL_HEADERS).status_code == 409

        # 目录里没有可下载的残留临时文件
        leftovers = [
            f for f in os.listdir(live_server.export_dir)
            if f.endswith(".tmp") or f.startswith(f"snapshot_{sid}_")
        ]
        assert leftovers == [], leftovers

        # 重新发起相同请求：原地重试成功
        retry = _request(api, ds_id, EXTERNAL_HEADERS)
        assert retry["snapshot"]["id"] == sid
        ready = _wait(api, sid, EXTERNAL_HEADERS)
        assert ready["status"] == "ready"
        payload = json.loads(
            api.get(f"{API}/exports/{sid}/download", headers=EXTERNAL_HEADERS).content
        )
        assert payload["quality_summary"]["total_items"] == 3
