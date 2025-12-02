from typing import Dict, Any
from sqlalchemy.orm import Session
from src.services.sector_analytics_service import sector_analytics_service
from src.services.pdf_risk_report_service import pdf_risk_report_service

class ReportOrchestrator:
    def generate_pdf(self, empresa_id: int, tipo_contraparte: str, db: Session) -> Dict[str, Any]:
        analytics = sector_analytics_service.generate_analytics_json(None, empresa_id, db)
        if analytics.get("status") != "success":
            return analytics
        json_path = analytics.get("json_path")
        pdf = pdf_risk_report_service.generate_pdf_report(
            analytics_json_path=json_path,
            tipo_contraparte=tipo_contraparte
        )
        return {"analytics": analytics, "pdf": pdf}

report_orchestrator = ReportOrchestrator()
