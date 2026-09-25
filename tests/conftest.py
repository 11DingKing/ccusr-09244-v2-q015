import os
import tempfile

# 必须在导入任何 app 模块之前配置临时数据库与快照目录。
_TMP = tempfile.mkdtemp(prefix="snapshot-test-")
_DB_PATH = os.path.join(_TMP, "test_robot_data.db")
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["SNAPSHOT_EXPORT_DIR"] = os.path.join(_TMP, "exports")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.database import Base, SessionLocal, engine  # noqa: E402
from main import app  # noqa: E402


@pytest.fixture()
def db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def client(db):
    # 以 context manager 方式进入会触发 startup（快照恢复），对空库无副作用。
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def export_dir(tmp_path):
    # 每个测试使用独立产物目录，避免数据库重建后 ID 复用导致文件互相覆盖。
    path = str(tmp_path / "exports")
    os.makedirs(path, exist_ok=True)
    from app.config import settings
    old = settings.SNAPSHOT_EXPORT_DIR
    settings.SNAPSHOT_EXPORT_DIR = path
    try:
        yield path
    finally:
        settings.SNAPSHOT_EXPORT_DIR = old
