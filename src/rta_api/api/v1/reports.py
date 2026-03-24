from fastapi import APIRouter, Body, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from src.db.base import TargetSessionLocal
from src.services.report_orchestrator import report_orchestrator
from src.core.security import require_jwt

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])

def get_db():
    db = TargetSessionLocal()
    try:
        yield db
    finally:
        db.close()


class PdfRequest(BaseModel):
    empresa_id: int = 1
    tipo_contraparte: str = "cliente"
    fecha: str | None = None
    monto_min: float | None = None
    empresa_nombre: str | None = None


@router.post("/pdf")
def generate_pdf(
    empresa_id: int = Query(...),
    tipo_contraparte: str = Query("cliente"),
    fecha: str | None = Query(None),
    monto_min: float | None = Query(None),
    db: Session = Depends(get_db),
    claims: dict = Depends(require_jwt),
):
    return report_orchestrator.generate_pdf(empresa_id, tipo_contraparte, db, fecha=fecha, monto_min=monto_min)


@router.post("/pdf/request")
def generate_pdf_from_request(
    payload: PdfRequest = Body(...),
    db: Session = Depends(get_db),
    claims: dict = Depends(require_jwt),
):
    return report_orchestrator.generate_pdf(
        payload.empresa_id,
        payload.tipo_contraparte,
        db,
        fecha=payload.fecha,
        monto_min=payload.monto_min,
        company_name=payload.empresa_nombre,
    )

@router.post("/html")
def generate_html(empresa_id: int = Query(...), db: Session = Depends(get_db), claims: dict = Depends(require_jwt)):
    return report_orchestrator.generate_html(empresa_id, db)

