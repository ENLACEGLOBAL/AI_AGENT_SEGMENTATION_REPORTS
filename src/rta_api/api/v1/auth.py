from fastapi import APIRouter, Query
from src.core.security import generar_jwt

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

@router.post("/token")
def issue_token(subject: str = Query("php-client"), minutes: int = Query(30)):
    payload = {"sub": subject, "scope": ["analytics", "geo", "reports", "ml"]}
    return {"jwt": generar_jwt(payload, minutes)}
