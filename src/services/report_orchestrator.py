from typing import Dict, Any
from sqlalchemy.orm import Session
from src.services.sector_analytics_service import sector_analytics_service
from src.services.pdf_risk_report_service import pdf_risk_report_service
from src.services.local_ai_report_service import generate_html_report
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.db.base import TargetSessionLocal

class ReportOrchestrator:
    def __init__(self):
        self.repo = GeneratedReportRepository()
    
    def generate_pdf(self, empresa_id: int, tipo_contraparte: str, db: Session) -> Dict[str, Any]:
        tgt = TargetSessionLocal()
        try:
            analytics = sector_analytics_service.generate_analytics_json(None, empresa_id, tgt)
        finally:
            tgt.close()
        if analytics.get("status") != "success":
            return analytics
        json_path = analytics.get("json_path")
        pdf = pdf_risk_report_service.generate_pdf_report(
            analytics_json_path=json_path,
            tipo_contraparte=tipo_contraparte
        )
        return {"analytics": analytics, "pdf": pdf}
    
    def generate_html(self, empresa_id: int, db: Session) -> Dict[str, Any]:
        tgt = TargetSessionLocal()
        try:
            analytics = sector_analytics_service.generate_analytics_json(None, empresa_id, tgt)
        finally:
            tgt.close()
        if analytics.get("status") != "success":
            return analytics
        html = generate_html_report(analytics.get("data", {}))
        if html.get("status") != "success":
            return html
        encrypted = sector_analytics_service.encrypt_path(html["path"])
        self.repo.create_report(db, encrypted, empresa_id)
        return {"analytics": analytics, "html": {"path_encrypted": encrypted, "empresa_id": empresa_id}}

report_orchestrator = ReportOrchestrator()
