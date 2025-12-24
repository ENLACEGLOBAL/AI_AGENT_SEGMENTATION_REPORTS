# src/core/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
import os

class Settings(BaseSettings):
    # --- DB ---
    DB_ENGINE_FORM: str = "mysql"
    DB_HOST_FORM: str = "localhost"
    DB_PORT_FORM: int = 3306
    DB_USER_FORM: str = "root"
    DB_PASSWORD_FORM: str = ""
    DB_NAME_FORM: str = "default"

    # --- JWT ---
    JWT_SECRET: str
    JWT_ALGORITHM: str

    # --- TEMP ---
    TEMP_FOLDER: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def SOURCE_DATABASE_URL(self) -> str:
        engine = os.getenv("SRC_DB_ENGINE_FORM", self.DB_ENGINE_FORM)
        host = os.getenv("SRC_DB_HOST_FORM", self.DB_HOST_FORM)
        port = int(os.getenv("SRC_DB_PORT_FORM", self.DB_PORT_FORM))
        user = os.getenv("SRC_DB_USER_FORM", self.DB_USER_FORM)
        password = os.getenv("SRC_DB_PASSWORD_FORM", self.DB_PASSWORD_FORM)
        name = os.getenv("SRC_DB_NAME_FORM", self.DB_NAME_FORM)
        return f"{engine}+pymysql://{user}:{password}@{host}:{port}/{name}"

    @property
    def TARGET_DATABASE_URL(self) -> str:
        engine = os.getenv("TGT_DB_ENGINE_FORM", self.DB_ENGINE_FORM)
        host = os.getenv("TGT_DB_HOST_FORM", self.DB_HOST_FORM)
        port = int(os.getenv("TGT_DB_PORT_FORM", self.DB_PORT_FORM))
        user = os.getenv("TGT_DB_USER_FORM", self.DB_USER_FORM)
        password = os.getenv("TGT_DB_PASSWORD_FORM", self.DB_PASSWORD_FORM)
        name = os.getenv("TGT_DB_NAME_FORM", self.DB_NAME_FORM)
        return f"{engine}+pymysql://{user}:{password}@{host}:{port}/{name}"

settings = Settings()
