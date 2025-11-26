# src/core/security.py
import jwt
from datetime import datetime, timedelta
from src.core.config import settings

def generar_jwt(payload: dict, minutos: int = 10) -> str:
    """
    Genera un JWT firmado con expiración.
    """
    to_encode = payload.copy()
    to_encode["exp"] = datetime.utcnow() + timedelta(minutes=minutos)

    token = jwt.encode(
        to_encode,
        settings.JWT_SECRET,
        algorithm=settings.JWT_ALGORITHM
    )

    return token

