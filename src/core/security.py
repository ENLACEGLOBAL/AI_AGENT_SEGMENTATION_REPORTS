# src/core/security.py
import jwt
from datetime import datetime, timedelta
from src.core.config import settings
from fastapi import Header, HTTPException
from typing import Optional

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

def verify_jwt(token: str) -> dict:
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

def require_jwt(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization: Bearer token")
    token = authorization.split(" ", 1)[1]
    return verify_jwt(token)

