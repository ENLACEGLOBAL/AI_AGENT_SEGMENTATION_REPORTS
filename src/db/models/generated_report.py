# src/db/models/generated_report.py
from sqlalchemy import Column, Integer, String, DateTime, LargeBinary
from sqlalchemy.sql import func
from src.db.base import Base

class GeneratedReport(Base):
    __tablename__ = "generated_reports"

    id = Column(Integer, primary_key=True, index=True)
    company_id = Column(Integer, nullable=True, index=True)
    file_path = Column(String(500), nullable=True)  # Encrypted path (Optional now)
    pdf_content = Column(LargeBinary, nullable=True) # Binary content
    created_at = Column(DateTime(timezone=True), server_default=func.now())
