from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from src.db.base import TargetSessionLocal, SourceSessionLocal
from src.services.sector_analytics_service import sector_analytics_service
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.services.sector_analytics_service import cipher_suite
from src.core.security import require_jwt, generar_jwt
from src.services.map_image_service import map_image_service
from src.services.report_orchestrator import report_orchestrator
from src.db.repositories.html_report_repo import HtmlReportRepository

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

def get_db():
    db = TargetSessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_source_db():
    db = SourceSessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/sector-ubicacion")
def sector_ubicacion(empresa_id: int = Query(...), db: Session = Depends(get_source_db), claims: dict = Depends(require_jwt)):
    result = sector_analytics_service.generate_analytics_json(None, empresa_id, db)
    try:
        payload = result.get("data", result)
        signed = generar_jwt(payload, minutos=10)
        result["jwt"] = signed
    except Exception:
        pass
    return result

@router.get("/chart-image")
def chart_image(empresa_id: int = Query(...), db: Session = Depends(get_db), claims: dict = Depends(require_jwt)):
    src_db = SourceSessionLocal()
    repo = GeneratedReportRepository()
    rec = repo.get_latest_by_company(src_db, empresa_id)
    if not rec:
        return {"status": "error", "message": "No report found for company"}
    try:
        path = cipher_suite.decrypt(rec.file_path.encode()).decode()
        import base64
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        return {"status": "success", "empresa_id": empresa_id, "image_base64": f"data:image/png;base64,{b64}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        src_db.close()

@router.get("/latest")
def latest(empresa_id: int = Query(...), db: Session = Depends(get_source_db), claims: dict = Depends(require_jwt)):
    res = sector_analytics_service.generate_analytics_json(None, empresa_id, db)
    fatf = res.get('data', res).get('fatf_status', {})
    img = map_image_service.world_fatf_map(fatf)
    return {"status": "success", "empresa_id": empresa_id, "image_base64": img["base64"], "path": img["path"]}

@router.get("/colombia-map")
def colombia_map(empresa_id: int = Query(...), db: Session = Depends(get_source_db), claims: dict = Depends(require_jwt)):
    res = sector_analytics_service.generate_analytics_json(None, empresa_id, db)
    points = res.get('data', res).get('mapa_colombia', [])
    img = map_image_service.colombia_empresa_map(points, empresa_id)
    return {"status": "success", "empresa_id": empresa_id, "image_base64": img["base64"], "path": img["path"]}

@router.get("/latest")
def latest(empresa_id: int = Query(...), db: Session = Depends(get_db), claims: dict = Depends(require_jwt)):
    src_db = SourceSessionLocal()
    repo = GeneratedReportRepository()
    rec = repo.get_latest_by_company(src_db, empresa_id)
    if not rec:
        return {"status": "error", "message": "No report found"}
    try:
        path = cipher_suite.decrypt(rec.file_path.encode()).decode()
        import json
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {"status": "success", "path": path, "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        src_db.close()

@router.post("/sector-ubicacion/html")
def sector_ubicacion_html(empresa_id: int = Query(...), claims: dict = Depends(require_jwt)):
    db = SourceSessionLocal()
    res = report_orchestrator.generate_html(empresa_id, db)
    db.close()
    return res
