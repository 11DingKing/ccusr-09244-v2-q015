import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "Robot Data Pipeline Backend"
    APP_VERSION: str = "1.0.0"
    DATABASE_URL: str = "sqlite:///./robot_data.db"
    API_V1_PREFIX: str = "/api/v1"

    # 快照导出文件存放目录（相对路径基于进程工作目录解析）
    EXPORT_DIR: str = "./exports"
    # 是否允许通过请求头注入失败/延迟（仅用于本地接口测试，生产部署应通过环境变量关闭）
    EXPORT_FAULT_INJECTION: bool = True

    class Config:
        env_file = ".env"

    @property
    def EXPORT_DIR_ABS(self) -> str:
        return os.path.abspath(self.EXPORT_DIR)


settings = Settings()
