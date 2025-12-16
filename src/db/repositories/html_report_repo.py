from sqlalchemy.orm import Session
from src.db.models.html_report import HtmlReport

class HtmlReportRepository:
    def create(self, db: Session, empresa_id: int, file_path: str) -> HtmlReport:
        rec = HtmlReport(empresa_id=empresa_id, file_path=file_path)
        db.add(rec)
        db.commit()
        db.refresh(rec)
        return rec
    def latest_by_company(self, db: Session, empresa_id: int) -> HtmlReport | None:
        return (
            db.query(HtmlReport)
            .filter(HtmlReport.empresa_id == empresa_id)
            .order_by(HtmlReport.id.desc())
            .first()
        )
