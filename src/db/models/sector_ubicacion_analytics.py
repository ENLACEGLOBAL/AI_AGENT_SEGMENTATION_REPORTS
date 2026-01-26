from sqlalchemy import Column, Integer, String, DateTime, func, Text
from sqlalchemy.dialects.mysql import LONGTEXT
from src.db.base import Base

class SectorUbicacionAnalytics(Base):
    __tablename__ = "sector_ubicacion_analytics"

    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(Integer, index=True)
    json_path = Column(String(500))
    data_json = Column(LONGTEXT, nullable=True)  # Nuevo campo para JSON unificado
    created_at = Column(DateTime(timezone=True), server_default=func.now())
