# src/services/sector_analytics_service.py
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from cryptography.fernet import Fernet
import pandas as pd

from src.core.config import settings
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.analytics_modules.sector_ubicacion.sector_geo_analytics import SectorGeoAnalytics
from src.analytics_modules.sector_ubicacion.graph_generator import GraphGenerator
from src.services.map_image_service import map_image_service

# Encryption key (should be in environment variables in production)
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', Fernet.generate_key())
if isinstance(ENCRYPTION_KEY, str):
    ENCRYPTION_KEY = ENCRYPTION_KEY.encode()
cipher_suite = Fernet(ENCRYPTION_KEY)

# Directories
DATA_PROVISIONAL_DIR = "data_provisional"
GENERATED_IMAGES_DIR = "generated_images"
os.makedirs(DATA_PROVISIONAL_DIR, exist_ok=True)
os.makedirs(GENERATED_IMAGES_DIR, exist_ok=True)


class SectorAnalyticsService:
    """
    Service for generating sector-location analytics JSON with encrypted image paths.
    """
    
    def __init__(self):
        self.repo = GeneratedReportRepository()
    
    def encrypt_path(self, path: str) -> str:
        """Encrypt a file path."""
        return cipher_suite.encrypt(path.encode()).decode()
    
    def decrypt_path(self, encrypted_path: str) -> str:
        """Decrypt a file path."""
        return cipher_suite.decrypt(encrypted_path.encode()).decode()
    
    def generate_analytics_json(
        self, 
        df: Optional[pd.DataFrame], 
        empresa_id: int,
        db: Session
    ) -> Dict[str, Any]:
        """
        Generate comprehensive analytics JSON for a specific empresa.
        
        Args:
            df: DataFrame (Optional, ignored if fetching from DB/CSV)
            empresa_id: Company ID to filter by
            db: Database session
            
        Returns:
            Dictionary with analytics data and encrypted image path
        """
        try:
            # 1. Load Transaction Data from CSV
            csv_path = os.path.join(DATA_PROVISIONAL_DIR, "datos prueba clientes.csv")
            if not os.path.exists(csv_path):
                return {
                    "status": "error",
                    "message": f"CSV file not found at {csv_path}"
                }
            
            df_csv = pd.read_csv(csv_path)
            
            # Filter by empresa_id
            df_empresa = df_csv[df_csv['id_empresa'] == empresa_id].copy()
            
            if df_empresa.empty:
                return {
                    "status": "error",
                    "message": f"No data found for empresa_id {empresa_id} in CSV"
                }

            # 2. Load Reference Data from DB
            from src.db.models.reference_tables import HistoricoPaises, SegmentacionJurisdicciones, AuxiliarCiiu
            
            # Fetch all reference data
            df_paises = pd.read_sql(db.query(HistoricoPaises).statement, db.bind)
            df_jurisdicciones = pd.read_sql(db.query(SegmentacionJurisdicciones).statement, db.bind)
            df_ciiu = pd.read_sql(db.query(AuxiliarCiiu).statement, db.bind)
            
            # 3. Enrich Data - Drop conflicting columns first to avoid duplicates
            cols_to_drop = ['clasificacion', 'riesgo', 'calificacion', 'valor', 'categoria', 
                           'descripcion', 'valor_riesgo']
            df_empresa = df_empresa.drop(columns=[c for c in cols_to_drop if c in df_empresa.columns], errors='ignore')
            
            # Merge Pais info
            if not df_paises.empty:
                # Drop duplicates from reference table first
                df_paises_clean = df_paises[['pais', 'clasificacion', 'riesgo', 'calificacion']].drop_duplicates(subset=['pais'])
                
                df_empresa = df_empresa.merge(
                    df_paises_clean, 
                    on='pais', 
                    how='left'
                ).reset_index(drop=True)
                
                df_empresa = df_empresa.rename(columns={
                    'clasificacion': 'pais_clasificacion',
                    'riesgo': 'pais_riesgo',
                    'calificacion': 'pais_calificacion'
                })
            
            # Merge Jurisdiccion info
            if not df_jurisdicciones.empty:
                df_jurisdicciones = df_jurisdicciones.rename(columns={
                    'municipio': 'ciudad',
                    'valor_riesgo_jurisdicciones': 'valor'
                })
                
                if 'ciudad' in df_empresa.columns and 'departamento' in df_empresa.columns:
                    df_empresa = df_empresa.merge(
                        df_jurisdicciones[['ciudad', 'departamento', 'valor', 'categoria']], 
                        on=['ciudad', 'departamento'], 
                        how='left'
                    )
                    df_empresa = df_empresa.rename(columns={
                        'valor': 'valor_jurisdicciones',
                        'categoria': 'categoria_jurisdicciones'
                    })

            # Merge CIIU info
            if not df_ciiu.empty:
                df_empresa['ciiu'] = df_empresa['ciiu'].astype(str)
                df_ciiu['ciiu'] = df_ciiu['ciiu'].astype(str)
                df_ciiu = df_ciiu.rename(columns={'riesgo': 'categoria'})
                
                df_empresa = df_empresa.merge(
                    df_ciiu[['ciiu', 'descripcion', 'categoria', 'valor_riesgo']], 
                    on='ciiu', 
                    how='left'
                )
                df_empresa = df_empresa.rename(columns={
                    'descripcion': 'ciiu_descripcion',
                    'categoria': 'ciiu_categoria',
                    'valor_riesgo': 'ciiu_valor_riesgo'
                })

            # 4. Prepare for Analytics
            # Rename columns to match what SectorGeoAnalytics expects
            df_empresa = df_empresa.rename(columns={
                'num_id': 'id_contraparte',
                'valor_transaccion': 'monto',
                'orden_clasificacion_del_riesgo': 'riesgo'
            })
            
            # Ensure required columns exist
            df_empresa['empresa'] = df_empresa['id_empresa'].astype(str)
            if 'monto' not in df_empresa.columns:
                df_empresa['monto'] = 0
            if 'riesgo' not in df_empresa.columns:
                df_empresa['riesgo'] = 'ALTO'
            if 'lat' not in df_empresa.columns:
                df_empresa['lat'] = 4.5709
            if 'lon' not in df_empresa.columns:
                df_empresa['lon'] = -74.2973
            
            # Create FATF data from the DB source
            if not df_paises.empty:
                df_fatf = df_paises[['pais', 'clasificacion']].rename(columns={'clasificacion': 'estatus'})
            else:
                df_fatf = pd.DataFrame({
                    'pais': ['ALEMANIA', 'ESPAÑA', 'VENEZUELA', 'IRAN'],
                    'estatus': ['COOPERANTE', 'COOPERANTE', 'NO COOPERANTE', 'NO COOPERANTE']
                })
            
            # MERGE with transaction data to ensure we capture countries in the CSV
            # that might be missing from the DB reference table
            if 'pais' in df_empresa.columns and 'pais_clasificacion' in df_empresa.columns:
                unique_csv_paises = df_empresa[['pais', 'pais_clasificacion']].drop_duplicates().rename(columns={'pais_clasificacion': 'estatus'})
                unique_csv_paises = unique_csv_paises.dropna(subset=['estatus'])
                base = {}
                for _, r in df_fatf[['pais', 'estatus']].dropna().iterrows():
                    p = str(r['pais']).strip().upper()
                    base[p] = str(r['estatus']).strip().upper()
                for _, r in unique_csv_paises.iterrows():
                    p = str(r['pais']).strip().upper()
                    base[p] = str(r['estatus']).strip().upper()
                df_fatf = pd.DataFrame({'pais': list(base.keys()), 'estatus': list(base.values())})
            
            # Generate analytics
            analytics = SectorGeoAnalytics(df_empresa, df_fatf)
            kpis = analytics.get_kpis()
            mapa_colombia = analytics.get_mapa_colombia()
            fatf_status = analytics.get_fatf_status()

            tabla_cols = {
                'id_transaccion': ['id_transaccion', 'id_tx', 'tx_id'],
                'empresa': ['empresa', 'id_empresa'],
                'nit': ['nit', 'num_id', 'no_documento_de_identidad'],
                'ciiu': ['ciiu'],
                'actividad': ['actividad', 'ciiu_descripcion'],
                'departamento': ['departamento'],
                'monto': ['monto', 'valor_transaccion']
            }
            def pick(row, keys):
                for k in keys:
                    v = row.get(k)
                    if pd.notna(v):
                        return v
                return None
            df_tabla = df_empresa.copy()
            if 'riesgo' in df_tabla.columns:
                df_tabla = df_tabla[df_tabla['riesgo'].astype(str).str.upper() == 'ALTO']
            tabla = []
            for _, r in df_tabla.iterrows():
                tabla.append({
                    'id_transaccion': pick(r, tabla_cols['id_transaccion']),
                    'empresa': str(pick(r, tabla_cols['empresa']) or ''),
                    'nit': str(pick(r, tabla_cols['nit']) or ''),
                    'ciiu': str(pick(r, tabla_cols['ciiu']) or ''),
                    'actividad': str(pick(r, tabla_cols['actividad']) or ''),
                    'departamento': str(pick(r, tabla_cols['departamento']) or ''),
                    'monto': float(pick(r, tabla_cols['monto']) or 0)
                })
            
            # Generate graph and save image
            graph_gen = GraphGenerator(df_empresa)
            chart_dataset = graph_gen.get_donut_dataset()
            chart_base64 = graph_gen.get_donut_base64()
            
            # Save chart image
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            image_filename = f"chart_{empresa_id}_{timestamp}.png"
            image_path = os.path.join(GENERATED_IMAGES_DIR, image_filename)
            
            # Generate and save the chart
            import matplotlib.pyplot as plt
            plt.figure(figsize=(6, 6))
            plt.pie(
                chart_dataset['values'],
                labels=chart_dataset['labels'],
                autopct='%1.1f%%',
                startangle=90
            )
            plt.title(f'Distribución por Actividad - Empresa {empresa_id}')
            plt.tight_layout()
            plt.savefig(image_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            # Encrypt image path
            encrypted_image_path = self.encrypt_path(image_path)
            
            # Generate map images (base64)
            world_map = map_image_service.world_fatf_map(fatf_status)
            colombia_map = map_image_service.colombia_empresa_map(mapa_colombia, empresa_id)

            # Build comprehensive JSON
            analytics_data = {
                "empresa_id": empresa_id,
                "timestamp": timestamp,
                "kpis": kpis,
                "mapa_colombia": mapa_colombia,
                "chart_data": chart_dataset,
                "fatf_status": fatf_status,
                "tabla": tabla,
                "encrypted_image_path": encrypted_image_path,
                "image_filename": image_filename,
                "images": {
                    "chart_donut_base64": chart_base64,
                    "world_fatf_base64": world_map["base64"],
                    "colombia_empresa_base64": colombia_map["base64"],
                    "chart_donut_path": image_path,
                    "world_fatf_path": world_map["path"],
                    "colombia_empresa_path": colombia_map["path"]
                }
            }
            
            # Save JSON to data_provisional
            json_filename = f"analytics_{empresa_id}_{timestamp}.json"
            json_path = os.path.join(DATA_PROVISIONAL_DIR, json_filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(analytics_data, f, ensure_ascii=False, indent=2)
            
            # Save encrypted image path to database
            self.repo.create_report(db, encrypted_image_path, empresa_id)
            
            print(f"✅ Analytics generated: {json_path}")
            print(f"✅ Image saved: {image_path}")
            print(f"✅ Encrypted path stored in database")
            
            return {
                "status": "success",
                "json_path": json_path,
                "data": analytics_data
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"❌ Error generating analytics: {e}")
            return {
                "status": "error",
                "message": str(e)
            }


# Singleton instance
sector_analytics_service = SectorAnalyticsService()
