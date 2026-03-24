from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from urllib.parse import quote_plus

# 👇 1. Agregamos esto para que lea el .env cuando corres localmente
from dotenv import load_dotenv

load_dotenv()


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

    # 🟢 --- CORREO MAILGUN ---
    MAILGUN_SMTP_SERVER: str = "smtp.mailgun.org"
    MAILGUN_SMTP_PORT: int = 587
    MAILGUN_SMTP_LOGIN: str = ""
    MAILGUN_SMTP_PASSWORD: str = ""
    MAIL_FROM: str = "Auditoría Riesgos 365 <reportes@tudominio.com>"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @property
    def SOURCE_DATABASE_URL(self) -> str:
        # 👇 2. Le quitamos el sufijo "_FORM" a la búsqueda para que coincida con tu .env
        engine = os.getenv("SRC_DB_ENGINE", self.DB_ENGINE_FORM)
        host = os.getenv("SRC_DB_HOST", self.DB_HOST_FORM)
        port = int(os.getenv("SRC_DB_PORT", self.DB_PORT_FORM))
        user = os.getenv("SRC_DB_USER", self.DB_USER_FORM)
        password = os.getenv("SRC_DB_PASSWORD", self.DB_PASSWORD_FORM)
        name = os.getenv("SRC_DB_NAME", self.DB_NAME_FORM)

        # 🟢 CORRECCIÓN: Limpiar comillas literales inyectadas por Docker antes de codificar
        if password:
            password = password.strip('"').strip("'")
            password = quote_plus(password)
        else:
            password = ""

        return f"{engine}+pymysql://{user}:{password}@{host}:{port}/{name}"

    @property
    def TARGET_DATABASE_URL(self) -> str:
        # 👇 3. Lo mismo aquí: buscar TGT_DB_NAME, no TGT_DB_NAME_FORM
        engine = os.getenv("TGT_DB_ENGINE", self.DB_ENGINE_FORM)
        host = os.getenv("TGT_DB_HOST", self.DB_HOST_FORM)
        port = int(os.getenv("TGT_DB_PORT", self.DB_PORT_FORM))
        user = os.getenv("TGT_DB_USER", self.DB_USER_FORM)
        password = os.getenv("TGT_DB_PASSWORD", self.DB_PASSWORD_FORM)
        name = os.getenv("TGT_DB_NAME", self.DB_NAME_FORM)

        # 🟢 CORRECCIÓN: Limpiar comillas literales inyectadas por Docker antes de codificar
        if password:
            password = password.strip('"').strip("'")
            password = quote_plus(password)
        else:
            password = ""

        return f"{engine}+pymysql://{user}:{password}@{host}:{port}/{name}"