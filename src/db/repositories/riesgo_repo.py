# src/db/repositories/riesgo_repo.py
from sqlalchemy.orm import Session
from src.db.models.riesgo import RiesgoTransaccional

class RiesgoRepository:
    def get_all(self, db: Session):
        return db.query(RiesgoTransaccional).all()

    def get_by_region(self, db: Session, region: str):
        return db.query(RiesgoTransaccional).filter_by(region=region).all()
