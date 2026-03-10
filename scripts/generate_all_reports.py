import sys
import os
import logging
import argparse

# Add project root to python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.base import SourceSessionLocal, TargetSessionLocal
from src.db.models.cliente import Cliente
from src.services.report_orchestrator import report_orchestrator
from sqlalchemy import distinct

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_all(empresa_id: int | None = None, fecha: str | None = None, monto_min: float | None = None):
    db_source = SourceSessionLocal()

    try:
        # Get company IDs
        if empresa_id is not None:
            empresa_ids = [empresa_id]
            logger.info(f"Generating report only for company ID {empresa_id}")
        else:
            logger.info("Fetching distinct company IDs from 'clientes' table...")
            empresa_ids = db_source.query(distinct(Cliente.id_empresa)).all()
            empresa_ids = [eid[0] for eid in empresa_ids if eid[0] is not None]
            logger.info(f"Found {len(empresa_ids)} companies: {empresa_ids}")

        results = []

        for emp_id in empresa_ids:
            logger.info(f"--- Processing Company ID: {emp_id} ---")

            # Create a fresh SOURCE session for each report generation
            db_session = SourceSessionLocal()
            try:
                # Assuming 'cliente' is the default type we want
                res = report_orchestrator.generate_pdf(emp_id, "cliente", db_session, fecha=fecha, monto_min=monto_min)

                status = "SUCCESS" if res.get("pdf", {}).get("status") == "success" else "FAILED"
                file_path = res.get("pdf", {}).get("file", "N/A")

                results.append({
                    "id": emp_id,
                    "status": status,
                    "file": file_path
                })

                if status == "SUCCESS":
                    logger.info(f"✅ Generated PDF for {emp_id}: {file_path}")
                else:
                    logger.error(f"❌ Failed to generate PDF for {emp_id}: {res}")

            except Exception as e:
                logger.error(f"❌ Exception for {emp_id}: {e}")
                results.append({
                    "id": emp_id,
                    "status": "ERROR",
                    "error": str(e)
                })
            finally:
                db_session.close()

        # Summary
        logger.info("\n=== GENERATION SUMMARY ===")
        for r in results:
            logger.info(f"ID {r['id']}: {r['status']} - {r.get('file') or r.get('error')}")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        # Ensure db_source is closed if it wasn't already (e.g. if error occurred during fetching)
        try:
            db_source.close()
        except Exception:
            pass


def generate_id1(fecha: str | None = None, monto_min: float | None = None):
    """Genera específicamente el reporte para la empresa ID 1."""
    db_session = SourceSessionLocal()
    try:
        logger.info("--- Processing Company ID: 1 ---")
        res = report_orchestrator.generate_pdf(1, "cliente", db_session, fecha=fecha, monto_min=monto_min)

        status = "SUCCESS" if res.get("pdf", {}).get("status") == "success" else "FAILED"
        file_path = res.get("pdf", {}).get("file", "N/A")

        if status == "SUCCESS":
            logger.info(f"✅ Generated PDF for 1: {file_path}")
        else:
            logger.error(f"❌ Failed to generate PDF for 1: {res}")

    except Exception as e:
        logger.error(f"❌ Exception for 1: {e}")
    finally:
        db_session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generación de informes de riesgo")
    parser.add_argument("--id1", action="store_true", help="Generar específicamente para empresa ID 1")
    parser.add_argument("--empresa", type=int, help="ID de empresa a procesar (si no se indica, procesa todas)")
    parser.add_argument("--fecha", type=str, help="Fecha específica (YYYY-MM-DD) para filtrar transacciones")
    parser.add_argument("--monto-min", type=float, help="Monto mínimo para filtrar transacciones")
    # Permitir variables de entorno como fallback
    args = parser.parse_args()

    fecha_env = os.getenv("REPORT_FECHA")
    monto_env = os.getenv("REPORT_MONTO_MIN")
    fecha = args.fecha or (fecha_env if fecha_env else None)
    try:
        monto_min = args.monto_min if args.monto_min is not None else (float(monto_env) if monto_env else None)
    except Exception:
        monto_min = None

    if args.id1:
        generate_id1(fecha=fecha, monto_min=monto_min)
    else:
        generate_all(empresa_id=args.empresa, fecha=fecha, monto_min=monto_min)