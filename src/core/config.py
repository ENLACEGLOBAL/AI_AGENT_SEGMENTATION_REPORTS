# src/core/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from urllib.parse import quote_plus

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

    # --- STORAGE CONFIG ---
    FILESYSTEM_CLOUD: str = "aws" # 'aws' or 'minio'

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
            # Default to AWS
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
        engine = os.getenv("SRC_DB_ENGINE", self.DB_ENGINE)
        host = os.getenv("SRC_DB_HOST", self.DB_HOST)
        port = int(os.getenv("SRC_DB_PORT", self.DB_PORT))
        user = quote_plus(os.getenv("SRC_DB_USER", self.DB_USER))
        password = quote_plus(os.getenv("SRC_DB_PASSWORD", self.DB_PASSWORD))
        name = os.getenv("SRC_DB_NAME", self.DB_NAME)
        # Use mysql-connector-python for better authentication support (auth_gssapi_client fix)
        return f"{engine}+mysqlconnector://{user}:{password}@{host}:{port}/{name}"

    @property
    def TARGET_DATABASE_URL(self) -> str:
        engine = os.getenv("TGT_DB_ENGINE", self.DB_ENGINE)
        host = os.getenv("TGT_DB_HOST", self.DB_HOST)
        port = int(os.getenv("TGT_DB_PORT", self.DB_PORT))
        user = quote_plus(os.getenv("TGT_DB_USER", self.DB_USER))
        password = quote_plus(os.getenv("TGT_DB_PASSWORD", self.DB_PASSWORD))
        name = os.getenv("TGT_DB_NAME", self.DB_NAME)
        # Use mysql-connector-python for better authentication support
        return f"{engine}+mysqlconnector://{user}:{password}@{host}:{port}/{name}"

settings = Settings()
