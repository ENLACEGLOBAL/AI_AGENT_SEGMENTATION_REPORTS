# src/db/models/generated_report.py
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from src.db.base import Base

class GeneratedReport(Base):
    __tablename__ = "generated_reports"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, nullable=True, index=True)
    file_path = Column(String(500), nullable=False)  # Encrypted path
    created_at = Column(DateTime(timezone=True), server_default=func.now())
