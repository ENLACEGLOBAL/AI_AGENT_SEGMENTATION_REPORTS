from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.sql import func
from src.db.base import Base

class CrucesEntidadesAnalytics(Base):
    __tablename__ = "cruces_entidades_analytics"

    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(Integer, index=True)
    json_path = Column(String(500))
    data_json = Column(LONGTEXT, nullable=True) # Added for PHP controller compatibility
    created_at = Column(DateTime(timezone=True), server_default=func.now())
