import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.config import settings
from src.services.sector_analytics_service import sector_analytics_service
from src.db.base import Base

# Setup DB connection
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db = SessionLocal()

def test_hybrid_generation():
    print("🧪 Testing Hybrid Analytics Generation...")
    
    empresa_id = 38 # Known ID from CSV
    
    try:
        # Call the service directly
        result = sector_analytics_service.generate_analytics_json(
            df=None,
            empresa_id=empresa_id,
            db=db
        )
        
        if result['status'] == 'success':
            print("✅ Service call successful!")
            print(f"   JSON Path: {result['json_path']}")
            print(f"   Image Filename: {result['data']['image_filename']}")
            
            # Verify JSON content
            import json
            with open(result['json_path'], 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            print(f"   Keys in JSON: {list(data.keys())}")
            print(f"   KPIs: {data['kpis']}")
            
            # Verify enrichment (check if DB fields are present)
            # We can check if 'pais_riesgo' or similar fields are in the internal dataframe 
            # but here we only have the final JSON. 
            # The JSON 'mapa_colombia' or 'chart_data' might reflect it if we used those fields.
            # However, the service logic uses them to calculate risk/KPIs.
            
        else:
            print(f"❌ Service call failed: {result['message']}")
            
    except Exception as e:
        print(f"❌ Exception during test: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    test_hybrid_generation()
