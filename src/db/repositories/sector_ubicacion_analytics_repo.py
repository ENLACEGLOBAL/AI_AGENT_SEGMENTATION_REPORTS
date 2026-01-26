from sqlalchemy.orm import Session
from src.db.models.sector_ubicacion_analytics import SectorUbicacionAnalytics

class SectorUbicacionAnalyticsRepository:
    def create(self, db: Session, empresa_id: int, json_path: str) -> SectorUbicacionAnalytics:
        record = SectorUbicacionAnalytics(
            empresa_id=empresa_id,
            json_path=json_path
        )
        db.add(record)
        db.commit()
        db.refresh(record)
        return record

    def get_latest(self, db: Session, empresa_id: int) -> SectorUbicacionAnalytics:
        return (
            db.query(SectorUbicacionAnalytics)
            .filter(SectorUbicacionAnalytics.empresa_id == empresa_id)
            .order_by(SectorUbicacionAnalytics.created_at.desc())
            .first()
        )

    def update_data_json(self, db: Session, empresa_id: int, data: dict) -> bool:
        """Actualiza el registro más reciente con el JSON unificado"""
        try:
            import json
            record = self.get_latest(db, empresa_id)
            if record:
                record.data_json = json.dumps(data, ensure_ascii=False)
                db.commit()
                return True
            return False
        except Exception as e:
            print(f"❌ Error actualizando data_json en DB: {e}")
            db.rollback()
            return False
