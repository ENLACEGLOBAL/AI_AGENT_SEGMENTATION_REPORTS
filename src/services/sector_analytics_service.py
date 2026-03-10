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
from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
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

    def fix_encoding_artifacts(self, text: Any) -> Any:
        """
        Fix common encoding artifacts found in the database (e.g., 'AA3' -> 'Ó').
        Handles specific mojibake patterns observed in 'clientes' and 'proveedores'.
        """
        if not isinstance(text, str):
            return text
        
        # 1. Global replacements for unambiguous patterns
        # 'AA3' -> 'Ó' (e.g., CONSTRUCCIAA3N -> CONSTRUCCIÓN)
        text = text.replace('AA3', 'Ó')
        # 'AAA' -> 'ÍA' (e.g., FERRETERAAA -> FERRETERÍA, INGENIERAAA -> INGENIERÍA)
        # Note: This also handles CRAAA -> CRÍA
        text = text.replace('AAA', 'ÍA')
        
        # 2. Specific word corrections (Observed in DB)
        corrections = {
            'DISEAAO': 'DISEÑO',
            'INFORMAATICA': 'INFORMÁTICA',
            'PERIFAARICOS': 'PERIFÉRICOS',
            'AGRAACOLA': 'AGRÍCOLA',
            'ARTAACULOS': 'ARTÍCULOS',
            'LAAQUIDOS': 'LÍQUIDOS',
            'VAAAS': 'VÍAS',
            'SAA3LIDOS': 'SÓLIDOS',
            'ALCOHAA3LICAS': 'ALCOHÓLICAS',
            'PAABLICA': 'PÚBLICA', # Assumed
            'ELAACTRICA': 'ELÉCTRICA', # Assumed
            'METAALICOS': 'METÁLICOS', # Assumed
        }
        
        for bad, good in corrections.items():
            text = text.replace(bad, good)
            
        return text

    
    def decrypt_path(self, encrypted_path: str) -> str:
        """Decrypt a file path."""
        return cipher_suite.decrypt(encrypted_path.encode()).decode()

    def _read_sql_with_retry(self, query, bind, params=None, max_retries=3):
        import time
        last_error = None
        for attempt in range(max_retries):
            try:
                return pd.read_sql(query, bind, params=params)
            except Exception as e:
                print(f"⚠️ SQL Read Attempt {attempt+1}/{max_retries} failed: {e}")
                last_error = e
                time.sleep(2) 
        
        print(f"❌ Failed to read SQL after {max_retries} attempts.")
        # Return empty dataframe to allow partial processing or let it crash?
        # Better to raise to avoid silent data loss, but the current logic handles empty checks.
        if last_error:
            raise last_error
        return pd.DataFrame()
    
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
            # Use raw SQL to ensure all columns (including risk/normalized data) are fetched
            from sqlalchemy import text
            
            # Cargar Clientes
            query_cli = text("SELECT * FROM clientes WHERE id_empresa = :empresa_id")
            df_cli = self._read_sql_with_retry(query_cli, db.bind, params={'empresa_id': empresa_id})
            if not df_cli.empty:
                df_cli["tipo_contraparte"] = "cliente"
                
            # Cargar Proveedores
            query_pro = text("SELECT * FROM proveedores WHERE id_empresa = :empresa_id")
            df_pro = self._read_sql_with_retry(query_pro, db.bind, params={'empresa_id': empresa_id})
            if not df_pro.empty:
                df_pro["tipo_contraparte"] = "proveedor"

            # Cargar Empleados (Para Cruces)
            query_emp = text("SELECT * FROM empleados WHERE id_empresa = :empresa_id")
            df_emp = self._read_sql_with_retry(query_emp, db.bind, params={'empresa_id': empresa_id})
            
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

            # 2. Load Reference Data from DB - SKIPPED
            # The user confirmed that risk data is already normalized in the counterparty tables.
            # We trust the source data directly.
            
            # 3. Enrich Data - SKIPPED
            # We do NOT drop columns or re-merge with external tables.
            
            # 4. Prepare for Analytics
            # Rename columns to match what SectorGeoAnalytics expects
            # 1. Rename Client ID
            if 'num_id' in df_empresa.columns:
                df_empresa = df_empresa.rename(columns={'num_id': 'id_contraparte'})
            
            # 2. Merge Provider ID into id_contraparte
            if 'no_documento_de_identidad' in df_empresa.columns:
                if 'id_contraparte' in df_empresa.columns:
                    # Coalesce: fill NaN in id_contraparte with no_documento_de_identidad
                    df_empresa['id_contraparte'] = df_empresa['id_contraparte'].fillna(df_empresa['no_documento_de_identidad'])
                else:
                    # Just rename if id_contraparte doesn't exist yet
                    df_empresa = df_empresa.rename(columns={'no_documento_de_identidad': 'id_contraparte'})

            df_empresa = df_empresa.rename(columns={
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
            if 'tipo_contraparte' not in df_empresa.columns:
                df_empresa['tipo_contraparte'] = ''
            
            # Standardize missing values to "SIN_INFORMACION"
            cols_to_standardize = ['ciiu_descripcion', 'actividad', 'departamento', 'ciudad', 'ciiu', 'ciiu2', 'ciiu_categoria']
            
            # Additional logic to handle "NO IDENTIFICADO" or similar variants
            replace_values = ["NO IDENTIFICADO", "NO REPORTADO", "NAN", "nan", "None", "", "NO_REPORTA"]
            
            for col in cols_to_standardize:
                if col in df_empresa.columns:
                    # Fill NaN first
                    df_empresa[col] = df_empresa[col].fillna("SIN_INFORMACION")
                    # Replace strings
                    df_empresa[col] = df_empresa[col].astype(str).replace(replace_values, "SIN_INFORMACION")
                    # Also replace partial matches if strictly needed, but exact match is safer for now.
                    # Case insensitive check for 'nan' string
                    mask = df_empresa[col].astype(str).str.lower() == 'nan'
                    df_empresa.loc[mask, col] = "SIN_INFORMACION"

            # Fix Encoding Artifacts (Mojibake)
            cols_to_clean = ['ciiu_descripcion', 'actividad', 'nombre', 'razon_social', 'nombre_empresa', 'empresa', 'departamento', 'ciudad']
            for col in cols_to_clean:
                if col in df_empresa.columns:
                    df_empresa[col] = df_empresa[col].apply(self.fix_encoding_artifacts)


            
            # Create FATF data from the DB source (Derived from actual data)
            # Use pais_clasificacion if available
            df_fatf = pd.DataFrame(columns=['pais', 'estatus'])
            if 'pais' in df_empresa.columns and 'pais_clasificacion' in df_empresa.columns:
                unique_paises = df_empresa[['pais', 'pais_clasificacion']].drop_duplicates().rename(columns={'pais_clasificacion': 'estatus'})
                unique_paises = unique_paises.dropna(subset=['estatus'])
                # Clean up strings
                unique_paises['pais'] = unique_paises['pais'].astype(str).str.strip().str.upper()
                
                # Normalize estatus for Map Service (expects 'NO COOPERANTE' or 'COOPERANTE')
                def normalize_fatf_status(s):
                    s_upper = str(s).strip().upper()
                    if 'NO COOPERANTE' in s_upper or 'ALTO' in s_upper or 'HIGH' in s_upper:
                        return 'NO COOPERANTE'
                    return 'COOPERANTE'

                unique_paises['estatus'] = unique_paises['estatus'].apply(normalize_fatf_status)
                df_fatf = unique_paises.drop_duplicates(subset=['pais'])
            
            # Fallback if empty (shouldn't happen if data is normalized as claimed)
            if df_fatf.empty:
                df_fatf = pd.DataFrame({
                    'pais': ['ALEMANIA', 'ESPAÑA', 'VENEZUELA', 'IRAN'],
                    'estatus': ['COOPERANTE', 'COOPERANTE', 'NO COOPERANTE', 'NO COOPERANTE']
                })

            # Generate analytics
            analytics = SectorGeoAnalytics(df_empresa, df_fatf)
            kpis = analytics.get_kpis()
            mapa_colombia = analytics.get_mapa_colombia()
            fatf_status = analytics.get_fatf_status()

            tabla_cols = {
                'id_transaccion': ['id', 'id_transaccion', 'id_tx', 'tx_id'],
                'empresa': ['nombre', 'razon_social', 'nombre_empresa', 'empresa', 'id_empresa'],
                'nit': ['id_contraparte', 'nit', 'num_id', 'no_documento_de_identidad', 'identificacion'],
                'ciiu': ['ciiu', 'ciiu2', 'codigo_ciiu', 'actividad_economica'],
                'actividad': ['actividad', 'ciiu_descripcion', 'descripcion_actividad', 'detalle_transaccion'],
                'departamento': ['departamento', 'departamento_estandar', 'region_completa'],
                'monto': ['monto', 'valor_transaccion', 'valor']
            }
            def pick(row, keys):
                # Check normal columns
                for k in keys:
                    v = row.get(k)
                    if pd.notna(v):
                        return v
                
                # Check index if 'id' is requested
                if 'id' in keys:
                    # If row has a name and it looks like an ID (int)
                    if isinstance(row.name, (int, str)) and row.name:
                         return row.name
                
                return None

            df_tabla = df_empresa.copy()
            
            # Ensure 'id' column is available for the table if it's in the index
            if 'id' not in df_tabla.columns and df_tabla.index.name == 'id':
                 df_tabla = df_tabla.reset_index()
            elif 'id' not in df_tabla.columns and 'id_transaccion' in df_tabla.columns:
                 df_tabla['id'] = df_tabla['id_transaccion']

            # Limit table to top 100 transactions.
            # Prioritize High Risk transactions, then by amount.
            def get_risk_rank(row):
                # Check High Risk factors
                p_risk = str(row.get('pais_riesgo', '')).upper()
                j_risk = str(row.get('categoria_jurisdicciones', '')).upper()
                c_risk = str(row.get('ciiu_categoria', '')).upper()
                raw_risk = str(row.get('riesgo', '')).upper()
                
                if (p_risk in ['ALTO', 'HIGH'] or 
                    j_risk in ['ALTO', 'HIGH'] or
                    c_risk in ['ALTO', 'HIGH'] or
                    'ALTO' in raw_risk or 
                    'NO COOPERANTE' in p_risk or
                    raw_risk in ['5', '4']):
                    return 3
                
                if (p_risk in ['MEDIO', 'MEDIUM'] or 
                    j_risk in ['MEDIO', 'MEDIUM'] or
                    c_risk in ['MEDIO', 'MEDIUM'] or
                    'MEDIO' in raw_risk or
                    raw_risk in ['3']):
                    return 2
                
                return 1

            df_tabla['risk_rank'] = df_tabla.apply(get_risk_rank, axis=1)

            if 'monto' in df_tabla.columns:
                df_tabla = df_tabla.sort_values(by=['risk_rank', 'monto'], ascending=[False, False]).head(100)
            else:
                df_tabla = df_tabla.sort_values(by=['risk_rank'], ascending=[False]).head(100)
                
            tabla = []
            for _, r in df_tabla.iterrows():
                # Logic to determine descriptive risk label
                reasons = []
                
                # 1. Country Risk
                p_risk = str(r.get('pais_riesgo', '')).upper()
                p_class = str(r.get('pais_clasificacion', '')).upper()
                
                if p_risk in ['ALTO', 'HIGH']:
                    reasons.append(f"País Alto Riesgo")
                elif 'NO COOPERANTE' in p_class or 'NO-COOPERANTE' in p_class:
                    reasons.append(f"País No Cooperante")
                elif p_risk in ['MEDIO', 'MEDIUM']:
                    reasons.append(f"País Riesgo Medio")
                    
                # 2. Jurisdiction Risk
                j_risk = str(r.get('categoria_jurisdicciones', '')).upper()
                if j_risk in ['ALTO', 'HIGH']:
                    reasons.append(f"Jurisdicción Alto Riesgo")
                elif j_risk in ['MEDIO', 'MEDIUM']:
                    reasons.append(f"Jurisdicción Riesgo Medio")

                # 3. Activity Risk
                c_risk = str(r.get('ciiu_categoria', '')).upper()
                if c_risk in ['ALTO', 'HIGH']:
                    reasons.append(f"Actividad Alto Riesgo")
                elif c_risk in ['MEDIO', 'MEDIUM']:
                    reasons.append(f"Actividad Riesgo Medio")
                
                # Construct Label
                if reasons:
                    r_label = ", ".join(reasons)
                else:
                    # Fallback if no specific risk factors found
                    # Check nivel_riesgo as fallback
                    nivel_riesgo = str(r.get('nivel_riesgo', '')).upper()
                    
                    raw_risk = str(r.get('riesgo', 'BAJO')).upper()
                    if raw_risk.isdigit():
                        val = int(raw_risk)
                        if val >= 5: r_label = "Alto Riesgo"
                        elif val >= 3: r_label = "Riesgo Moderado"
                        else: r_label = "Riesgo Aceptable"
                    elif raw_risk != 'NAN' and raw_risk != '':
                        if 'ALTO' in raw_risk or 'HIGH' in raw_risk:
                            r_label = "Alto Riesgo"
                        elif 'MEDIO' in raw_risk or 'MEDIUM' in raw_risk:
                            r_label = "Riesgo Moderado"
                        else:
                            r_label = "Riesgo Aceptable"
                    elif nivel_riesgo != 'NAN' and nivel_riesgo != '':
                         r_label = nivel_riesgo.capitalize()
                    else:
                        r_label = 'Riesgo Aceptable'

                # Determine CSS Class
                # Check descriptive risks first
                full_text = (r_label + " " + str(r.get('riesgo', ''))).upper()
                if "ALTO" in full_text or "HIGH" in full_text or "5" in full_text or "NO COOPERANTE" in full_text:
                    r_class = 'danger'
                elif "MEDIO" in full_text or "MEDIUM" in full_text or "4" in full_text or "3" in full_text or "MODERADO" in full_text:
                    r_class = 'warning'
                else:
                    r_class = 'success'
                
                # Retrieve ID and cast to string to avoid issues
                raw_id = pick(r, tabla_cols['id_transaccion'])
                id_str = str(raw_id) if raw_id is not None else ""

                tabla.append({
                    'id': id_str, # Add alias 'id' in case frontend/pdf uses it
                    'id_transaccion': id_str, # Ensure string
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
            # Fix: Handle numeric risk scores (5=Alto, 4=Alto, 3=Medio) and other risk columns
            d_bajo = 0
            d_medio = 0
            d_alto = 0
            
            for _, row in df_empresa.iterrows():
                # Check all risk factors to determine the effective risk for this transaction
                risks = []
                risks.append(str(row.get('riesgo', '')).upper())
                risks.append(str(row.get('pais_riesgo', '')).upper())
                risks.append(str(row.get('categoria_jurisdicciones', '')).upper())
                risks.append(str(row.get('ciiu_categoria', '')).upper())
                
                is_alto = False
                is_medio = False
                
                for r_val in risks:
                    if 'ALTO' in r_val or 'HIGH' in r_val or r_val in ['5', '4', 'NO COOPERANTE']:
                        is_alto = True
                        break # High risk overrides everything
                    if 'MEDIO' in r_val or 'MEDIUM' in r_val or r_val in ['3']:
                        is_medio = True
                
                if is_alto:
                    d_alto += 1
                elif is_medio:
                    d_medio += 1
                else:
                    d_bajo += 1

            distribucion_riesgo = {
                "bajo": int(d_bajo),
                "medio": int(d_medio),
                "alto": int(d_alto)
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
                "tabla": tabla,
                "transacciones": tabla, # Keep both for backward compatibility
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
            
            # RETRY LOGIC FOR DB SAVE (Handle ConnectionAbortedError/WinError 10053)
            import time
            max_retries = 3
            saved_successfully = False
            last_error = None
            
            for attempt in range(max_retries):
                src_db = SourceSessionLocal()
                try:
                    self.repo.create_report(src_db, encrypted_image_path, empresa_id)
                    self.sector_repo.create(src_db, empresa_id, json_path)
                    # Explicitly update data_json with the lightweight version
                    self.sector_repo.update_data_json(src_db, empresa_id, analytics_data_db)
                    src_db.commit()
                    saved_successfully = True
                    break
                except Exception as e:
                    src_db.rollback()
                    last_error = e
                    print(f"⚠️ DB Save Attempt {attempt+1}/{max_retries} failed: {e}")
                    time.sleep(1)
                finally:
                    src_db.close()
            
            if not saved_successfully:
                print(f"❌ Failed to save analytics to DB after {max_retries} attempts.")
                if last_error:
                    raise last_error

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
