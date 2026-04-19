from fastapi import APIRouter, Body, Depends, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional, Dict
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


# 🟢 Modelo actualizado para soportar filtros complejos y comentarios
class PdfRequest(BaseModel):
    empresa_id: int
    tipo_contraparte: str = "Universo General"
    filtros_pdf: Optional[Dict] = None  # Aquí vendrán fecha_desde, monto_min, etc.
    oficial_conclusion: Optional[str] = None
    refresh_data: bool = False
    empresa_nombre: Optional[str] = None
    validez_dd: int = 1


@router.post("/pdf")
def generate_pdf(
        empresa_id: int = Query(...),
        tipo_contraparte: str = Query("Universo General"),
        fecha: Optional[str] = Query(None),
        monto_min: Optional[float] = Query(None),
        validez_dd: int = Query(1),
        refresh: bool = Query(False),
        db: Session = Depends(get_db),
        claims: dict = Depends(require_jwt),
):
    """Endpoint simple vía Query Params (opcional)"""
    filtros = {"fecha_desde": fecha, "monto_min": monto_min} if fecha or monto_min else None

    return report_orchestrator.generate_pdf(
        empresa_id=empresa_id,
        tipo_contraparte=tipo_contraparte,
        db=db,
        filtros_pdf=filtros,
        validez_dd=validez_dd,
        refresh_data=refresh
    )


@router.post("/pdf/request")
def generate_pdf_from_request(
        payload: PdfRequest = Body(...),
        db: Session = Depends(get_db),
        claims: dict = Depends(require_jwt),
):
    """Endpoint principal que usará Laravel (Body JSON)"""

    # 🟢 Mapeamos el payload del request a la nueva lógica del orquestador
    result = report_orchestrator.generate_pdf(
        empresa_id=payload.empresa_id,
        db=db,
        filtros_pdf=payload.filtros_pdf,
        oficial_conclusion=payload.oficial_conclusion,
        refresh_data=payload.refresh_data,
        tipo_contraparte=payload.tipo_contraparte,
        company_name=payload.empresa_nombre,
        validez_dd=payload.validez_dd
    )

    # Manejo de error si el orquestador falla
    if result.get("status") == "error":
        raise HTTPException(status_code=500, detail=result.get("message"))

    return result


@router.post("/html")
def generate_html(
        empresa_id: int = Query(...),
        db: Session = Depends(get_db),
        claims: dict = Depends(require_jwt)
):
    return report_orchestrator.generate_html(empresa_id, db)