from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from src.db.base import SessionLocal
from src.services.report_orchestrator import report_orchestrator
from src.core.security import require_jwt

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/pdf")
def generate_pdf(empresa_id: int = Query(...), tipo_contraparte: str = Query("cliente"), db: Session = Depends(get_db), claims: dict = Depends(require_jwt)):
    return report_orchestrator.generate_pdf(empresa_id, tipo_contraparte, db)
