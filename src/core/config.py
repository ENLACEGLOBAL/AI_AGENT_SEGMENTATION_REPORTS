# src/core/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    # --- DB ---
    DB_ENGINE: str = "mysql"
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = ""
    DB_NAME: str = "default"

    # --- JWT ---
    JWT_SECRET: str
    JWT_ALGORITHM: str

    # --- TEMP ---
    TEMP_FOLDER: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def SOURCE_DATABASE_URL(self) -> str:
        engine = os.getenv("SRC_DB_ENGINE", self.DB_ENGINE)
        host = os.getenv("SRC_DB_HOST", self.DB_HOST)
        port = int(os.getenv("SRC_DB_PORT", self.DB_PORT))
        user = os.getenv("SRC_DB_USER", self.DB_USER)
        password = os.getenv("SRC_DB_PASSWORD", self.DB_PASSWORD)
        name = os.getenv("SRC_DB_NAME", self.DB_NAME)
        return f"{engine}+pymysql://{user}:{password}@{host}:{port}/{name}"

    @property
    def TARGET_DATABASE_URL(self) -> str:
        engine = os.getenv("TGT_DB_ENGINE", self.DB_ENGINE)
        host = os.getenv("TGT_DB_HOST", self.DB_HOST)
        port = int(os.getenv("TGT_DB_PORT", self.DB_PORT))
        user = os.getenv("TGT_DB_USER", self.DB_USER)
        password = os.getenv("TGT_DB_PASSWORD", self.DB_PASSWORD)
        name = os.getenv("TGT_DB_NAME", self.DB_NAME)
        return f"{engine}+pymysql://{user}:{password}@{host}:{port}/{name}"

settings = Settings()
