"""快照导出测试公共夹具。

- 进程内测试使用 TestClient 与临时数据库/导出目录；
- 重启与真实并发测试通过 uvicorn 子进程启动本地 HTTP 服务。
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx
import pytest

# 必须在导入应用模块前完成环境变量设置
_TMP_ROOT = tempfile.mkdtemp(prefix="snapshot-tests-")
_DB_PATH = os.path.join(_TMP_ROOT, "test.db")
_EXPORT_DIR = os.path.join(_TMP_ROOT, "exports")
os.makedirs(_EXPORT_DIR, exist_ok=True)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["EXPORT_DIR"] = _EXPORT_DIR
os.environ["EXPORT_FAULT_INJECTION"] = "true"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

FULL_HEADERS = {"X-Caller-Key": "compliance-team"}
EXTERNAL_HEADERS = {"X-Caller-Key": "external-review"}


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _reset_database() -> None:
    from app.database import Base, engine
    from app.services.snapshot import bootstrap_exports

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    bootstrap_exports()


@pytest.fixture
def client():
    """进程内 FastAPI 客户端，每个用例重建数据库与导出目录。"""
    shutil.rmtree(_EXPORT_DIR, ignore_errors=True)
    os.makedirs(_EXPORT_DIR, exist_ok=True)
    _reset_database()

    from main import app
    from fastapi.testclient import TestClient
    from app.services.snapshot import wait_for_snapshot_workers

    with TestClient(app) as test_client:
        yield test_client

    # 等待带延迟的后台导出线程结束，防止守护线程跨用例写文件/数据库
    wait_for_snapshot_workers(timeout=15)


def seed_minimal_dataset(api, headers=None, with_annotation: bool = True):
    """通过接口创建机型/场景/技能/作业/标注/数据集，返回各资源ID。"""
    headers = headers or FULL_HEADERS
    model = api.post(
        "/api/v1/robot-models",
        json={"name": "RM-X1", "manufacturer": "Acme", "description": "测试机型"},
        headers=headers,
    ).json()
    scene = api.post(
        "/api/v1/scenes",
        json={"name": "测试场景", "category": "测试", "description": "场景自由文本"},
        headers=headers,
    ).json()
    skill = api.post(
        "/api/v1/skills",
        json={"name": "测试技能", "category": "测试"},
        headers=headers,
    ).json()

    op_one = api.post(
        "/api/v1/operations",
        json={
            "robot_model_id": model["id"],
            "scene_id": scene["id"],
            "skill_id": skill["id"],
            "robot_serial": "SN-SECRET-001",
            "motion_trajectory": {"serial_number": "TRAJ-SN-9", "points": [1, 2, 3]},
            "perception_records": {"camera": "cam-a"},
            "grasp_result": {"success": False},
            "timestamp_start": "2026-01-01T00:00:00+00:00",
            "timestamp_end": "2026-01-01T00:01:00+00:00",
            "duration_ms": 60000,
        },
        headers=headers,
    ).json()
    op_two = api.post(
        "/api/v1/operations",
        json={
            "robot_model_id": model["id"],
            "scene_id": scene["id"],
            "skill_id": skill["id"],
            "robot_serial": "SN-SECRET-002",
            "motion_trajectory": {},
            "perception_records": {},
            "timestamp_start": "2026-01-02T00:00:00+00:00",
            "timestamp_end": "2026-01-02T00:01:00+00:00",
        },
        headers=headers,
    ).json()

    if with_annotation:
        api.post(
            "/api/v1/annotations",
            json={
                "operation_data_id": op_one["id"],
                "is_success": False,
                "failure_category": "感知异常",
                "failure_subcategory": "视觉识别失败",
                "failure_description": "自由文本：镜头被遮挡，操作员李四",
                "annotator": "张工",
            },
            headers=headers,
        )

    dataset = api.post(
        "/api/v1/datasets",
        json={
            "name": "复核数据集",
            "description": "用于外部复核",
            "robot_model_id": model["id"],
            "scene_id": scene["id"],
            "skill_id": skill["id"],
            "owner_team": "合规组",
            "operation_data_ids": [op_one["id"], op_two["id"]],
        },
        headers=headers,
    ).json()

    return {
        "model": model,
        "scene": scene,
        "skill": skill,
        "operations": [op_one, op_two],
        "dataset": dataset,
    }


def wait_status(api, snapshot_id: int, headers=None, timeout: float = 10.0) -> dict:
    headers = headers or EXTERNAL_HEADERS
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = api.get(f"/api/v1/exports/{snapshot_id}", headers=headers).json()
        if body["status"] != "preparing":
            return body
        time.sleep(0.02)
    raise AssertionError(f"快照 {snapshot_id} 超时仍未完成")


class LiveServer:
    """运行在子进程中的真实 uvicorn 服务，可反复重启以验证持久化。"""

    def __init__(self, workdir: Path):
        self.workdir = workdir
        self.db_path = workdir / "live.db"
        self.export_dir = workdir / "exports"
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.proc: subprocess.Popen | None = None

    def _env(self) -> dict:
        env = os.environ.copy()
        env["DATABASE_URL"] = f"sqlite:///{self.db_path}"
        env["EXPORT_DIR"] = str(self.export_dir)
        env["EXPORT_FAULT_INJECTION"] = "true"
        env["PYTHONPATH"] = f"{PROJECT_ROOT}{os.pathsep}{env.get('PYTHONPATH', '')}"
        return env

    def start(self) -> None:
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
                "--log-level",
                "warning",
            ],
            cwd=str(PROJECT_ROOT),
            env=self._env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                output = self.proc.stdout.read().decode() if self.proc.stdout else ""
                raise AssertionError(f"服务进程提前退出：\n{output}")
            try:
                resp = httpx.get(f"{self.base_url}/health", timeout=1.0)
                if resp.status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.1)
        raise AssertionError("服务启动超时")

    def stop(self) -> None:
        if self.proc is None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        self.proc = None

    def client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=10.0)


@pytest.fixture
def live_server(tmp_path):
    server = LiveServer(tmp_path)
    server.start()
    try:
        yield server
    finally:
        server.stop()
