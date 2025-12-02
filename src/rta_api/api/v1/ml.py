from fastapi import APIRouter, Depends
from src.ml_pipelines.train import run
from src.core.security import require_jwt

router = APIRouter(prefix="/api/v1/ml", tags=["ml"])

@router.post("/train")
def train(claims: dict = Depends(require_jwt)):
    return run()
