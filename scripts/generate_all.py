import sys
import os
import logging
from typing import List

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.base import TargetSessionLocal, SourceSessionLocal
from src.services.cruces_analytics_service import CrucesAnalyticsService
from src.services.report_orchestrator import report_orchestrator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_all_reports():
    """
    Generates PDF risk reports for all active companies found in the database.
    """
    logger.info("🚀 Starting bulk report generation...")
    
    # 1. Get active companies
    source_db = SourceSessionLocal()
    try:
        service = CrucesAnalyticsService()
        active_companies: List[int] = service.get_active_companies(source_db)
        logger.info(f"📋 Found {len(active_companies)} active companies: {active_companies}")
    except Exception as e:
        logger.error(f"❌ Error fetching active companies: {e}")
        return
    finally:
        source_db.close()

    if not active_companies:
        logger.warning("⚠️ No active companies found.")
        return

    # 2. Generate report for each company
    success_count = 0
    error_count = 0

    for empresa_id in active_companies:
        logger.info(f"🔄 Processing Company ID: {empresa_id}...")
        
        # 2.1 Generate Cruces Analytics explicitly (Populates cruces_entidades_analytics)
        source_db_for_analytics = SourceSessionLocal()
        try:
            logger.info(f"   📊 Generating Cruces Analytics for Company {empresa_id}...")
            cruces_service = CrucesAnalyticsService()
            cruces_result = cruces_service.generate_cruces_analytics(source_db_for_analytics, empresa_id)
            if cruces_result.get("status") == "success":
                logger.info(f"   ✅ Cruces Analytics generated successfully")
            else:
                logger.warning(f"   ⚠️ Cruces Analytics issue: {cruces_result.get('message')}")
        except Exception as e:
            logger.error(f"   ❌ Error generating Cruces Analytics: {e}")
        finally:
            source_db_for_analytics.close()

        # 2.2 Generate PDF Report (Populates sector_ubicacion_analytics and generated_reports)
        target_db = TargetSessionLocal()
        try:
            # Generate for 'cliente' type by default
            result = report_orchestrator.generate_pdf(empresa_id, "cliente", target_db)
            
            pdf_status = result.get("pdf", {}).get("status")
            if pdf_status == "success":
                logger.info(f"   ✅ Report generated for Company {empresa_id}")
                success_count += 1
            else:
                msg = result.get("pdf", {}).get("message", "Unknown error")
                logger.error(f"   ❌ Failed to generate report for Company {empresa_id}: {msg}")
                error_count += 1
                
        except Exception as e:
            logger.error(f"   ❌ Exception processing Company {empresa_id}: {e}")
            error_count += 1
        finally:
            target_db.close()

    # 3. Summary
    logger.info("="*30)
    logger.info("🏁 Bulk Generation Complete")
    logger.info(f"✅ Success: {success_count}")
    logger.info(f"❌ Errors:  {error_count}")
    logger.info("="*30)

if __name__ == "__main__":
    generate_all_reports()
