from typing import Dict, Any
from sqlalchemy.orm import Session
from src.services.sector_analytics_service import sector_analytics_service
from src.services.pdf_risk_report_service import pdf_risk_report_service
from src.services.local_ai_report_service import generate_html_report
from src.services.local_ai_report_service import local_ai_report_service
from src.services.sector_analytics_service import cipher_suite
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.db.repositories.html_report_repo import HtmlReportRepository
from src.db.base import TargetSessionLocal

class ReportOrchestrator:
    def __init__(self):
        self.repo = GeneratedReportRepository()
        self.html_repo = HtmlReportRepository()
    
    def generate_pdf(self, empresa_id: int, tipo_contraparte: str, db: Session) -> Dict[str, Any]:
        tgt = TargetSessionLocal()
        try:
            analytics = sector_analytics_service.generate_analytics_json(None, empresa_id, tgt)
        finally:
            tgt.close()
        if analytics.get("status") != "success":
            return analytics
        
        analytics_data = analytics.get("data")
        pdf = pdf_risk_report_service.generate_pdf_report(
            analytics_data=analytics_data,
            tipo_contraparte=tipo_contraparte
        )
        try:
            path = pdf.get("file")
            if isinstance(path, str) and path:
                self.repo.create_report(db, path, empresa_id)
        except Exception:
            pass
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
        self.html_repo.create(db, empresa_id, encrypted)
        return {"analytics": analytics, "html": {"path_encrypted": encrypted, "empresa_id": empresa_id}}
    
    def generate_json(self, empresa_id: int, db: Session) -> Dict[str, Any]:
        tgt = TargetSessionLocal()
        try:
            analytics = sector_analytics_service.generate_analytics_json(None, empresa_id, tgt)
        finally:
            tgt.close()
        if analytics.get("status") != "success":
            return analytics
        report = local_ai_report_service.generate_report(analytics.get("data", {}))
        if report.get("status") != "success":
            return report
        return {
            "status": "success",
            "empresa_id": empresa_id,
            "analytics": analytics.get("data", {}),
            "report": report.get("report", {})
        }

report_orchestrator = ReportOrchestrator()
