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
from src.db.repositories.sector_ubicacion_analytics_repo import SectorUbicacionAnalyticsRepository
from src.analytics_modules.sector_ubicacion.sector_geo_analytics import SectorGeoAnalytics
from src.analytics_modules.sector_ubicacion.graph_generator import GraphGenerator
from src.analytics_modules.cruces_entidades.cruces_graph_generator import CrucesGraphGenerator
from src.services.map_image_service import map_image_service
from src.services.s3_service import s3_service
from src.db.base import SourceSessionLocal

import hashlib
import base64

# Encryption Setup
# We use JWT_SECRET to derive the Fernet key for compatibility with PHP
# PHP Logic: if key is not 32 bytes, it hashes it (sha256) to get 32 bytes.
# Fernet Logic: requires 32 bytes base64-url-encoded.
JWT_SECRET = os.getenv('JWT_SECRET', 'super-secret')
# 1. Get 32 bytes raw key
raw_key = hashlib.sha256(JWT_SECRET.encode()).digest()
# 2. Encode to base64url for Fernet
fernet_key = base64.urlsafe_b64encode(raw_key)
cipher_suite = Fernet(fernet_key)

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
        self.sector_repo = SectorUbicacionAnalyticsRepository()
    
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
            # 1. Load Transaction Data from DB (clientes y proveedores)
            from src.db.models.cliente import Cliente
            from src.db.models.proveedor import Proveedor
            from src.db.models.empleado import Empleado
            from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
            
            # Cargar Clientes
            query_cli = db.query(Cliente).filter(Cliente.id_empresa == empresa_id)
            df_cli = pd.read_sql(query_cli.statement, db.bind)
            if not df_cli.empty:
                df_cli["tipo_contraparte"] = "cliente"
                
            # Cargar Proveedores
            query_pro = db.query(Proveedor).filter(Proveedor.id_empresa == empresa_id)
            df_pro = pd.read_sql(query_pro.statement, db.bind)
            if not df_pro.empty:
                df_pro["tipo_contraparte"] = "proveedor"

            # Cargar Empleados (Para Cruces)
            query_emp = db.query(Empleado).filter(Empleado.id_empresa == empresa_id)
            df_emp = pd.read_sql(query_emp.statement, db.bind)
            
            # Concatenar (Solo Clientes y Proveedores para Sector Analytics)
            dfs = []
            if not df_cli.empty:
                dfs.append(df_cli)
            if not df_pro.empty:
                dfs.append(df_pro)
                
            df_empresa = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            
            if df_empresa.empty:
                return {
                    "status": "error",
                    "message": f"No data found for empresa_id {empresa_id} in Database (clientes/proveedores)"
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
                
                if 'pais' in df_empresa.columns:
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
                else:
                    # If 'pais' column is missing, we can't enrich with country risk
                    # Assuming default or skipping
                    pass
            
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
            # Proveedores ID field
            if 'id_contraparte' not in df_empresa.columns and 'no_documento_de_identidad' in df_empresa.columns:
                df_empresa = df_empresa.rename(columns={'no_documento_de_identidad': 'id_contraparte'})
            
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
            if 'tipo_contraparte' not in df_empresa.columns:
                df_empresa['tipo_contraparte'] = ''
            
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
            # Limit table to top 100 transactions by amount to avoid UI lag, but show ALL risks
            if 'monto' in df_tabla.columns:
                df_tabla = df_tabla.sort_values(by='monto', ascending=False).head(100)
            else:
                df_tabla = df_tabla.head(100)
                
            tabla = []
            for _, r in df_tabla.iterrows():
                riesgo_val = str(r.get('riesgo', 'BAJO')).upper()
                r_class = 'danger' if riesgo_val == 'ALTO' else ('warning' if riesgo_val == 'MEDIO' else 'success')
                r_label = riesgo_val.capitalize()
                
                tabla.append({
                    'id': pick(r, tabla_cols['id_transaccion']),
                    'empresa': str(pick(r, tabla_cols['empresa']) or ''),
                    'nit': str(pick(r, tabla_cols['nit']) or ''),
                    'ciiu': str(pick(r, tabla_cols['ciiu']) or ''),
                    'actividad': str(pick(r, tabla_cols['actividad']) or ''),
                    'departamento': str(pick(r, tabla_cols['departamento']) or ''),
                    'monto': float(pick(r, tabla_cols['monto']) or 0),
                    'tipo_contraparte': str(r.get('tipo_contraparte', '')),
                    'riesgo_class': r_class,
                    'riesgo_label': r_label
                })
            
            graph_gen = GraphGenerator(df_empresa)
            chart_dataset = graph_gen.get_donut_dataset()
            chart_base64 = graph_gen.get_donut_base64()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            image_filename = f"chart_{empresa_id}_{timestamp}.png"
            s3_url = None
            try:
                img_bytes = None
                if isinstance(chart_base64, str) and chart_base64.startswith("data:image"):
                    img_bytes = base64.b64decode(chart_base64.split(",", 1)[1])
                if img_bytes:
                    s3_key = f"analytics_images/{image_filename}"
                    s3_url = s3_service.upload_file(img_bytes, s3_key, content_type="image/png")
                    if s3_url:
                        print(f"✅ Image uploaded to S3: {s3_url}")
                    else:
                        print("⚠️ S3 upload failed, image not stored.")
            except Exception as e:
                print(f"⚠️ Error uploading image to S3: {e}")

            final_image_path = s3_url if s3_url else "NO_IMAGE"
            encrypted_image_path = self.encrypt_path(final_image_path)
            
            # Generate map images (base64)
            world_map = map_image_service.world_fatf_map(fatf_status)
            colombia_map = map_image_service.colombia_empresa_map(mapa_colombia, empresa_id)

            # Calcular Distribución de Riesgo (Bajo, Medio, Alto)
            risk_counts = df_empresa['riesgo'].value_counts().to_dict()
            distribucion_riesgo = {
                "bajo": int(risk_counts.get('BAJO', 0) + risk_counts.get('bajo', 0)),
                "medio": int(risk_counts.get('MEDIO', 0) + risk_counts.get('medio', 0)),
                "alto": int(risk_counts.get('ALTO', 0) + risk_counts.get('alto', 0))
            }

            # ---------------------------
            # CRUCES ENTIDADES ANALYTICS
            # ---------------------------
            cruces_data = {}
            try:
                cruces_analytics = CrucesAnalytics(df_cli, df_pro, df_emp)
                cruces_analytics.procesar_datos()
                
                # Generate Graphs
                cruces_graph_gen = CrucesGraphGenerator(cruces_analytics)
                chart_types_b64 = cruces_graph_gen.generate_cross_types_chart()
                chart_heatmap_b64 = cruces_graph_gen.generate_cruces_heatmap_chart()

                cruces_data = {
                    "tabla": cruces_analytics.get_tabla_detalles(empresa_id),
                    "distribucion": cruces_analytics.get_distribucion_riesgo(),
                    "tipos": cruces_analytics.get_tipos_cruces(),
                    "kpis": cruces_analytics.get_kpis(),
                    "chart_types_base64": chart_types_b64,
                    "chart_heatmap_base64": chart_heatmap_b64
                }
            except Exception as e:
                print(f"⚠️ Error calculando cruces: {e}")
                cruces_data = {}

            # Build comprehensive JSON
            analytics_data = {
                "empresa_id": empresa_id,
                "timestamp": timestamp,
                "kpis": kpis,
                "distribucion_riesgo": distribucion_riesgo,
                "entidades_cruces": cruces_data,
                "mapa_colombia": mapa_colombia,
                "mapa_global": world_map.get("map_data", []), # Agregar data cruda para mapa global
                "chart_data": chart_dataset,
                "fatf_status": fatf_status,
                "transacciones": tabla,
                "encrypted_image_path": encrypted_image_path,
                "image_filename": image_filename,
                "images": {
                    "chart_donut_base64": chart_base64,
                    "world_fatf_base64": world_map["base64"],
                    "colombia_empresa_base64": colombia_map["base64"],
                    "chart_donut_path": final_image_path,
                    "world_fatf_path": world_map["path"],
                    "colombia_empresa_path": colombia_map["path"]
                }
            }
            
            # Save JSON to data_provisional -> REMOVED to avoid local storage redundancy
            # We use a placeholder for the DB record since the actual data is stored in the data_json column
            json_path = "STORED_IN_DB_S3"
            
            # Create a lightweight version for DB (remove heavy base64 strings)
            analytics_data_db = analytics_data.copy()
            if "images" in analytics_data_db:
                analytics_data_db["images"] = {
                    k: v for k, v in analytics_data_db["images"].items() 
                    if not k.endswith('base64')
                }
            
            src_db = SourceSessionLocal()
            try:
                self.repo.create_report(src_db, encrypted_image_path, empresa_id)
                self.sector_repo.create(src_db, empresa_id, json_path)
                # Explicitly update data_json with the lightweight version
                self.sector_repo.update_data_json(src_db, empresa_id, analytics_data_db)
                src_db.commit()
            finally:
                src_db.close()
            
            print(f"✅ Analytics generated (In-Memory/DB)")
            if s3_url:
                 print(f"✅ Image saved to S3: {s3_url}")
            else:
                 print(f"⚠️ Image not saved to S3 (Local save disabled)")
            print(f"✅ Encrypted path stored in database")
            
            return {
                "status": "success",
                "json_path": None, # No local file
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
