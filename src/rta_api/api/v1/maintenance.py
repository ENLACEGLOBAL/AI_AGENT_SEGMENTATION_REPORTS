from fastapi import APIRouter, Query, Depends
from src.core.security import require_jwt
from src.services.purge_service import purge_analytics

router = APIRouter(prefix="/api/v1/maintenance", tags=["maintenance"])

@router.delete("/purge-analytics")
def purge(empresa_id: int | None = Query(None), retain: int = Query(3), claims: dict = Depends(require_jwt)):
    deleted = purge_analytics(empresa_id=empresa_id, retain=retain)
    return {"status": "success", "deleted": deleted, "retain": retain}

