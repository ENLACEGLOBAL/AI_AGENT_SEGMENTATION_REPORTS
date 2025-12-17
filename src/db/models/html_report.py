from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from src.db.base import Base

class HtmlReport(Base):
    __tablename__ = "html_reports"
    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(Integer, index=True, nullable=False)
    file_path = Column(String(500), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
