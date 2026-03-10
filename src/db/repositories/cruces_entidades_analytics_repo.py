from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
import time
from src.db.models.cruces_entidades_analytics import CrucesEntidadesAnalytics

class CrucesEntidadesAnalyticsRepository:
    def create(self, db: Session, empresa_id: int, json_path: str, data_json: str = None) -> CrucesEntidadesAnalytics:
        retries = 3
        delay = 2
        while True:
            try:
                record = CrucesEntidadesAnalytics(
                    empresa_id=empresa_id,
                    json_path=json_path,
                    data_json=data_json
                )
                db.add(record)
                db.commit()
                db.refresh(record)
                return record
            except OperationalError:
                db.rollback()
                retries -= 1
                if retries <= 0:
                    raise
                time.sleep(delay)
                delay *= 2

    def get_latest(self, db: Session, empresa_id: int) -> CrucesEntidadesAnalytics:
        return (
            db.query(CrucesEntidadesAnalytics)
            .filter(CrucesEntidadesAnalytics.empresa_id == empresa_id)
            .order_by(CrucesEntidadesAnalytics.created_at.desc())
            .first()
        )
