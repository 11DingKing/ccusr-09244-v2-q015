import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Robot Data Pipeline Backend"
    APP_VERSION: str = "1.0.0"
    DATABASE_URL: str = "sqlite:///./robot_data.db"
    API_V1_PREFIX: str = "/api/v1"

    # 快照导出文件的落盘目录；失败任务的临时文件只存在于同目录的 .tmp/ 下，
    # 完成的文件通过原子改名进入该目录，因此外部永远看不到半成品。
    SNAPSHOT_EXPORT_DIR: str = os.environ.get(
        "SNAPSHOT_EXPORT_DIR", os.path.join(os.getcwd(), "data", "snapshot_exports")
    )
    # 快照产物格式版本，结构发生不兼容变化时提升该值以强制生成新版本。
    SNAPSHOT_ARTIFACT_VERSION: str = "1.0"

    class Config:
        env_file = ".env"


settings = Settings()
