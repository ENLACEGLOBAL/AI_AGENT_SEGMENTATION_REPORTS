import json
import gzip
import io
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.services.cruces_analytics_service import cruces_analytics_service
from src.services.pdf_risk_report_service_v2 import pdf_risk_report_service
from src.services.local_ai_report_service import generate_html_report
from src.services.local_ai_report_service import local_ai_report_service
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.db.repositories.html_report_repo import HtmlReportRepository
from src.db.base import SourceSessionLocal
from src.services.s3_service import s3_service


class ReportOrchestrator:
    def __init__(self):
        self.repo = GeneratedReportRepository()
        self.html_repo = HtmlReportRepository()

    def generate_pdf(
            self,
            empresa_id: int,
            db: Session,
            filtros_pdf: Optional[Dict] = None,
            oficial_conclusion: Optional[str] = None,
            refresh_data: bool = False,
            tipo_contraparte: str = "Universo General",
            company_name: Optional[str] = None,
            output_path: Optional[str] = None
    ) -> Dict[str, Any]:

        src = SourceSessionLocal()
        analytics_data = None

        # Validamos si Laravel envió filtros reales para saber si guardar o no el reporte oficial
        hay_filtros = bool(filtros_pdf and any(filtros_pdf.values()))

        try:
            # --- MOMENTO 2: Reutilizar JSON existente (si no se pide refresh) ---
            if not refresh_data:
                query = text("""
                    SELECT data_json, json_path 
                    FROM cruces_entidades_analytics 
                    WHERE empresa_id = :eid 
                    ORDER BY created_at DESC LIMIT 1
                """)
                res = src.execute(query, {"eid": empresa_id}).fetchone()

                if res:
                    if res.data_json and res.data_json != "STORED_IN_DB":
                        analytics_data = json.loads(res.data_json)
                    elif res.json_path:
                        file_bytes = s3_service.download_file_bytes(res.json_path)
                        with gzip.GzipFile(fileobj=io.BytesIO(file_bytes), mode='rb') as f:
                            analytics_data = json.loads(f.read().decode('utf-8'))

                    if analytics_data:
                        print(f"♻️ PDF: Reutilizando JSON existente para empresa {empresa_id}")

            # --- MOMENTO 1 y 4: Generación desde cero ---
            if not analytics_data:
                print(f"⚙️ PDF: Procesando DB fresca para empresa {empresa_id}")

                # 🟢 LIMPIEZA DE FILTROS: Evitar el ValueError ('') en Pandas
                f_desde = None
                m_min = None
                if filtros_pdf:
                    val_f = filtros_pdf.get("fecha_desde")
                    if val_f and str(val_f).strip() != "":
                        f_desde = str(val_f).strip()

                    val_m = filtros_pdf.get("monto_min")
                    if val_m and str(val_m).strip() != "":
                        m_min = float(val_m)  # Lo convertimos a float seguro

                cruces_result = cruces_analytics_service.generate_cruces_analytics(
                    src, empresa_id, fecha=f_desde, monto_min=m_min
                )
                if cruces_result.get("status") != "success":
                    return cruces_result
                analytics_data = cruces_result.get("data", {})

            # Inyectar nombre si viene de PHP
            if isinstance(analytics_data, dict) and 'data' in analytics_data:
                analytics_data = analytics_data['data']

            if company_name:
                analytics_data["empresa_nombre"] = company_name

        finally:
            src.close()

        # 🟢 NORMALIZACIÓN PARA EL SCRIPT DE PDF (Mantener compatibilidad)
        filtros_normalizados = {
            "fecha_desde": str(filtros_pdf.get("fecha_desde") or "") if filtros_pdf else "",
            "fecha_hasta": str(filtros_pdf.get("fecha_hasta") or "") if filtros_pdf else "",
            "monto_min": str(filtros_pdf.get("monto_min") or "") if filtros_pdf else "",
            "monto_min_tx": float(filtros_pdf.get("monto_min_tx") or 0.0) if filtros_pdf else 0.0,
            "sin_dd": "true" if (
                        filtros_pdf and str(filtros_pdf.get("sin_dd", "")).lower() in ["true", "1"]) else "false",
            "con_cruces": "true" if (
                        filtros_pdf and str(filtros_pdf.get("con_cruces", "")).lower() in ["true", "1"]) else "false"
        }

        # --- GENERACIÓN DEL PDF ---
        pdf = pdf_risk_report_service.generate_pdf_report(
            analytics_data=analytics_data,
            tipo_contraparte=tipo_contraparte,
            filtros_pdf=filtros_normalizados,
            oficial_conclusion=oficial_conclusion,
            output_path=output_path
        )

        # --- Guardar en la tabla de reportes si es el reporte oficial (sin filtros) ---
        if pdf.get("status") == "success" and not hay_filtros:
            try:
                path = pdf.get("file")
                if isinstance(path, str) and path:
                    self.repo.create_report(db, path, empresa_id)
            except Exception as e:
                print(f"⚠️ Error registrando reporte en DB: {e}")

        return {"analytics": analytics_data, "pdf": pdf}

    def generate_json(
            self,
            empresa_id: int,
            db: Session,
            fecha: str | None = None,
            monto_min: float | None = None,
            refresh_data: bool = False
    ) -> Dict[str, Any]:

        src = SourceSessionLocal()
        analytics_data = None

        # 🟢 LIMPIEZA DE FILTROS (Por si llegan desde la URL del Dashboard)
        if fecha == "": fecha = None
        if monto_min == "": monto_min = None

        try:
            if not refresh_data and not fecha and not monto_min:
                query = text("""
                    SELECT data_json, json_path 
                    FROM cruces_entidades_analytics 
                    WHERE empresa_id = :eid 
                    ORDER BY created_at DESC LIMIT 1
                """)
                res = src.execute(query, {"eid": empresa_id}).fetchone()

                if res:
                    if res.data_json and res.data_json != "STORED_IN_DB":
                        analytics_data = json.loads(res.data_json)
                    elif res.json_path:
                        file_bytes = s3_service.download_file_bytes(res.json_path)
                        with gzip.GzipFile(fileobj=io.BytesIO(file_bytes), mode='rb') as f:
                            analytics_data = json.loads(f.read().decode('utf-8'))

                    if analytics_data:
                        print(f"⚡ Dashboard: Reutilizando caché para empresa {empresa_id}")

            if not analytics_data:
                print(f"⚙️ Dashboard: Generando analítica completa desde DB para {empresa_id}")
                cruces_result = cruces_analytics_service.generate_cruces_analytics(
                    src, empresa_id, fecha=fecha, monto_min=monto_min
                )
                if cruces_result.get("status") != "success":
                    return cruces_result
                analytics_data = cruces_result.get("data", {})

            if isinstance(analytics_data, dict) and 'data' in analytics_data:
                analytics_data = analytics_data['data']

        finally:
            src.close()

        report = local_ai_report_service.generate_report(analytics_data)

        return {
            "status": "success",
            "empresa_id": empresa_id,
            "analytics": analytics_data,
            "report": report.get("report", {}) if report.get("status") == "success" else {}
        }

    def generate_html(self, empresa_id: int, db: Session, fecha: str | None = None, monto_min: float | None = None) -> \
    Dict[str, Any]:
        src = SourceSessionLocal()
        try:
            cruces_result = cruces_analytics_service.generate_cruces_analytics(src, empresa_id, fecha=fecha,
                                                                               monto_min=monto_min)
            if cruces_result.get("status") != "success":
                return cruces_result
            analytics_data = cruces_result.get("data", {})
        finally:
            src.close()

        html = generate_html_report(analytics_data)
        if html.get("status") != "success":
            return html
        encrypted = str(empresa_id)
        self.html_repo.create(db, empresa_id, encrypted)
        return {"analytics": {"status": "success", "data": analytics_data},
                "html": {"path_encrypted": encrypted, "empresa_id": empresa_id}}


report_orchestrator = ReportOrchestrator()