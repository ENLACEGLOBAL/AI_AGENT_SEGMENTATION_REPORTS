from typing import Dict, Any
from sqlalchemy.orm import Session
from src.services.cruces_analytics_service import cruces_analytics_service
from src.services.pdf_risk_report_service_v2 import pdf_risk_report_service
from src.services.local_ai_report_service import generate_html_report
from src.services.local_ai_report_service import local_ai_report_service
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.db.repositories.html_report_repo import HtmlReportRepository
from src.db.base import TargetSessionLocal, SourceSessionLocal

class ReportOrchestrator:
    def __init__(self):
        self.repo = GeneratedReportRepository()
        self.html_repo = HtmlReportRepository()
    
    def generate_pdf(
        self,
        empresa_id: int,
        tipo_contraparte: str,
        db: Session,
        fecha: str | None = None,
        monto_min: float | None = None,
        output_path: str | None = None,
        company_name: str | None = None,
    ) -> Dict[str, Any]:
        src = SourceSessionLocal()
        try:
            # Generate Cruces Entidades Analytics (This is now the primary data source)
            cruces_result = cruces_analytics_service.generate_cruces_analytics(src, empresa_id, fecha=fecha, monto_min=monto_min)
            
            if cruces_result.get("status") != "success":
                return cruces_result
            
            analytics_data = cruces_result.get("data", {})
            if isinstance(analytics_data, dict) and company_name:
                analytics_data["empresa_nombre"] = company_name
        finally:
            src.close()
        
        pdf = pdf_risk_report_service.generate_pdf_report(
            analytics_data=analytics_data,
            tipo_contraparte=tipo_contraparte,
            output_path=output_path
        )
        if pdf.get("status") != "success":
            return pdf
        try:
            path = pdf.get("file")
            if isinstance(path, str) and path:
                self.repo.create_report(db, path, empresa_id)
        except Exception:
            pass
        return {"analytics": analytics_data, "pdf": pdf}
    
    def generate_html(self, empresa_id: int, db: Session, fecha: str | None = None, monto_min: float | None = None) -> Dict[str, Any]:
        src = SourceSessionLocal()
        try:
            cruces_result = cruces_analytics_service.generate_cruces_analytics(src, empresa_id, fecha=fecha, monto_min=monto_min)
            if cruces_result.get("status") != "success":
                return cruces_result
            analytics_data = cruces_result.get("data", {})
        finally:
            src.close()
            
        html = generate_html_report(analytics_data)
        if html.get("status") != "success":
            return html
        # We use a simple encryption or just return path
        encrypted = str(empresa_id) # Simplified for now as requested to reduce redundancy
        self.html_repo.create(db, empresa_id, encrypted)
        return {"analytics": {"status": "success", "data": analytics_data}, "html": {"path_encrypted": encrypted, "empresa_id": empresa_id}}
    
    def generate_json(self, empresa_id: int, db: Session, fecha: str | None = None, monto_min: float | None = None) -> Dict[str, Any]:
        src = SourceSessionLocal()
        try:
            cruces_result = cruces_analytics_service.generate_cruces_analytics(src, empresa_id, fecha=fecha, monto_min=monto_min)
            if cruces_result.get("status") != "success":
                return cruces_result
            analytics_data = cruces_result.get("data", {})
        finally:
            src.close()
            
        report = local_ai_report_service.generate_report(analytics_data)
        if report.get("status") != "success":
            return report
        return {
            "status": "success",
            "empresa_id": empresa_id,
            "analytics": analytics_data,
            "report": report.get("report", {})
        }

report_orchestrator = ReportOrchestrator()
