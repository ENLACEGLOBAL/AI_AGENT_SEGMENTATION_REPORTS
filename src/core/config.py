# src/core/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # --- DB ---
    DB_ENGINE: str
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str

    # --- JWT ---
    JWT_SECRET: str
    JWT_ALGORITHM: str

    # --- TEMP ---
    TEMP_FOLDER: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def DATABASE_URL(self) -> str:
        """
        Construye automáticamente la cadena de conexión MariaDB.
        Ejemplo: mariadb+pymysql://user:pass@host:port/dbname
        """
        return (
            f"{self.DB_ENGINE}+pymysql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

settings = Settings()
