from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from urllib.parse import quote_plus
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    # --- DB (Valores por defecto) ---
    DB_ENGINE: str = "mysql"
    DB_HOST: str = "localhost"
    DB_PORT: int = 3306
    DB_USER: str = "root"
    DB_PASSWORD: str = ""
    DB_NAME: str = "default"

    # --- SRC DB (Agregamos estas para que Pydantic no las ignore) ---
    SRC_DB_ENGINE: Optional[str] = None
    SRC_DB_HOST: Optional[str] = None
    SRC_DB_PORT: Optional[int] = None
    SRC_DB_USER: Optional[str] = None
    SRC_DB_PASSWORD: Optional[str] = None
    SRC_DB_NAME: Optional[str] = None

    # --- TGT DB (Agregamos estas también) ---
    TGT_DB_ENGINE: Optional[str] = None
    TGT_DB_HOST: Optional[str] = None
    TGT_DB_PORT: Optional[int] = None
    TGT_DB_USER: Optional[str] = None
    TGT_DB_PASSWORD: Optional[str] = None
    TGT_DB_NAME: Optional[str] = None

    # --- JWT ---
    JWT_SECRET: str
    JWT_ALGORITHM: str

    # --- TEMP ---
    TEMP_FOLDER: str

    # --- STORAGE CONFIG ---
    FILESYSTEM_CLOUD: str = "aws"

    # --- AWS S3 (SRC) ---
    AWS_SRC_ACCESS_KEY_ID: str = ""
    AWS_SRC_SECRET_ACCESS_KEY: str = ""
    AWS_SRC_REGION: str = "us-east-1"
    AWS_SRC_BUCKET: str = ""

    # --- MINIO ---
    MINIO_ACCESS_KEY_ID: str = ""
    MINIO_SECRET_ACCESS_KEY: str = ""
    MINIO_DEFAULT_REGION: str = "us-east-1"
    MINIO_BUCKET: str = ""
    MINIO_ENDPOINT: str = ""
    MINIO_USE_PATH_STYLE_ENDPOINT: bool = True

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def STORAGE_CONFIG(self):
        """Returns the active storage configuration based on FILESYSTEM_CLOUD."""
        if self.FILESYSTEM_CLOUD.lower() == "minio":
            return {
                "type": "minio",
                "access_key": self.MINIO_ACCESS_KEY_ID,
                "secret_key": self.MINIO_SECRET_ACCESS_KEY,
                "region": self.MINIO_DEFAULT_REGION,
                "bucket": self.MINIO_BUCKET,
                "endpoint_url": self.MINIO_ENDPOINT
            }
        else:
            return {
                "type": "aws",
                "access_key": self.AWS_SRC_ACCESS_KEY_ID,
                "secret_key": self.AWS_SRC_SECRET_ACCESS_KEY,
                "region": self.AWS_SRC_REGION,
                "bucket": self.AWS_SRC_BUCKET,
                "endpoint_url": None
            }

    @property
    def SOURCE_DATABASE_URL(self) -> str:
        # Usamos directamente "self" porque Pydantic ya cargó los datos
        engine = self.SRC_DB_ENGINE or self.DB_ENGINE
        host = self.SRC_DB_HOST or self.DB_HOST
        port = self.SRC_DB_PORT or self.DB_PORT
        user = quote_plus(self.SRC_DB_USER or self.DB_USER)
        password = quote_plus(self.SRC_DB_PASSWORD or self.DB_PASSWORD)
        name = self.SRC_DB_NAME or self.DB_NAME

        return f"{engine}+mysqlconnector://{user}:{password}@{host}:{port}/{name}"

    @property
    def TARGET_DATABASE_URL(self) -> str:
        engine = self.TGT_DB_ENGINE or self.DB_ENGINE
        host = self.TGT_DB_HOST or self.DB_HOST
        port = self.TGT_DB_PORT or self.DB_PORT
        user = quote_plus(self.TGT_DB_USER or self.DB_USER)
        password = quote_plus(self.TGT_DB_PASSWORD or self.DB_PASSWORD)
        name = self.TGT_DB_NAME or self.DB_NAME

        return f"{engine}+mysqlconnector://{user}:{password}@{host}:{port}/{name}"


settings = Settings()
