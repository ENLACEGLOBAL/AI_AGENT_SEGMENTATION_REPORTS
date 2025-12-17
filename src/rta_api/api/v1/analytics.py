# src/rta_api/api/v1/analytics.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.db.base import TargetSessionLocal
from src.db.repositories.riesgo_repo import RiesgoRepository
from src.domain.services.analytics_service import AnalyticsService
from src.core.security import generar_jwt

router = APIRouter(prefix="/analytics")

def get_db():
    db = TargetSessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/resumen")
def obtener_resumen(db: Session = Depends(get_db)):
    repo = RiesgoRepository()
    service = AnalyticsService()

    riesgos = repo.get_all(db)
    analitica = service.procesar_riesgos(riesgos)

    token = generar_jwt(analitica)

    return {"jwt": token}
