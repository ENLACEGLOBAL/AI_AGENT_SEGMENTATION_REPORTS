# src/rta_api/api/v1/cruces.py
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from typing import Optional

from src.db.base import TargetSessionLocal
from src.services.report_orchestrator import report_orchestrator
from src.core.security import require_jwt

router = APIRouter(prefix="/api/v1/cruces", tags=["cruces"])


def get_db():
    db = TargetSessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/process-batch")
def process_batch_analytics(
        empresa_id: int,
        background_tasks: BackgroundTasks,
        validez_dd: int = Query(1), # 👈 NUEVO
        db: Session = Depends(get_db),
        # claims: dict = Depends(require_jwt)
):
    """
    MOMENTO 1 / 4: Generación forzada (Batch)
    Llama al orquestador con refresh_data=True para asegurar que se procese la DB.
    """
    result = report_orchestrator.generate_json(
        empresa_id=empresa_id,
        db=db,
        refresh_data=True,
        validez_dd=validez_dd # 👈 NUEVO
    )

    if result.get("status") == "error":
        return result

    return {
        "status": "success",
        "message": "Analítica recalculada exitosamente (Momento 4)",
        "empresa_id": empresa_id
    }


@router.get("/analytics")
def get_cruces_analytics(
        empresa_id: int = Query(...),
        fecha: str | None = Query(None),
        monto_min: float | None = Query(None),
        validez_dd: int = Query(1), # 👈 NUEVO (Clave para que la tabla en pantalla muestre lo correcto)
        refresh: bool = Query(False),
        db: Session = Depends(get_db),
        claims: dict = Depends(require_jwt)
):
    """
    MOMENTO 2: Obtener JSON (Caché o Filtrado)
    Si refresh=False y no hay filtros, el orquestador devolverá el JSON de S3/DB analítica.
    """
    return report_orchestrator.generate_json(
        empresa_id=empresa_id,
        db=db,
        fecha=fecha,
        monto_min=monto_min,
        refresh_data=refresh,
        validez_dd=validez_dd # 👈 NUEVO
    )


@router.get("/dashboard", response_class=HTMLResponse)
def get_cruces_dashboard(
        empresa_id: int = Query(...),
        validez_dd: int = Query(1), # 👈 NUEVO
        db: Session = Depends(get_db),
        claims: dict = Depends(require_jwt)
):
    """
    Retorna la data para el dashboard usando el orquestador (Momento 2).
    """
    result = report_orchestrator.generate_json(
        empresa_id=empresa_id,
        db=db,
        validez_dd=validez_dd # 👈 NUEVO
    )

    if result.get("status") != "success":
        return HTMLResponse(content=f"<h1>Error</h1><p>{result.get('message')}</p>", status_code=500)

    return HTMLResponse(content="<html><body>Dashboard listo en JSON</body></html>")


@router.get("/export-json")
def export_cruces_json(
        empresa_id: int = Query(...),
        validez_dd: int = Query(1), # 👈 NUEVO
        db: Session = Depends(get_db),
        claims: dict = Depends(require_jwt)
):
    """
    Exportación rápida consumiendo el Orquestador (Momento 2).
    """
    return report_orchestrator.generate_json(
        empresa_id=empresa_id,
        db=db,
        validez_dd=validez_dd # 👈 NUEVO
    )