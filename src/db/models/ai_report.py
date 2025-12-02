from sqlalchemy import Column, Integer, String, Text, DateTime
from datetime import datetime
from src.db.base import Base


class AIReport(Base):
    __tablename__ = "ai_reports"

    id = Column(Integer, primary_key=True, index=True)
    empresa_id = Column(Integer, index=True)
    report_content = Column(Text)  # JSON string of the report
    report_type = Column(String(50), default="risk_analysis")
    created_at = Column(DateTime, default=datetime.utcnow)
