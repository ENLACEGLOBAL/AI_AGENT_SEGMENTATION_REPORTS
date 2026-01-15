# src/db/repositories/generated_report_repo.py
from sqlalchemy.orm import Session
from src.db.models.generated_report import GeneratedReport

class GeneratedReportRepository:
    def create_report(self, db: Session, file_path: str = None, company_id: int = None, pdf_content: bytes = None) -> GeneratedReport:
        db_report = GeneratedReport(
            file_path=file_path, 
            company_id=company_id,
            pdf_content=pdf_content
        )
        db.add(db_report)
        db.commit()
        db.refresh(db_report)
        return db_report

    def get_report(self, db: Session, report_id: int) -> GeneratedReport:
        return db.query(GeneratedReport).filter(GeneratedReport.id == report_id).first()

    def get_latest_by_company(self, db: Session, company_id: int) -> GeneratedReport:
        return (
            db.query(GeneratedReport)
            .filter(GeneratedReport.company_id == company_id)
            .order_by(GeneratedReport.id.desc())
            .first()
        )
