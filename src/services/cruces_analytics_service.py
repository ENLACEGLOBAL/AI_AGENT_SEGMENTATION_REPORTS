# src/services/cruces_analytics_service.py
"""
Servicio orquestador para análisis de cruces de entidades
Lee datos desde BD (no CSV) y genera JSON + gráficos
"""
import json
import os
from datetime import datetime, date
from typing import Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd

from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
from src.analytics_modules.cruces_entidades.cruces_graph_generator import CrucesGraphGenerator
# Models are no longer needed for data loading as we use Raw SQL
# from src.db.models.cliente import Cliente
# from src.db.models.proveedor import Proveedor
# from src.db.models.empleado import Empleado
from src.db.repositories.cruces_entidades_analytics_repo import CrucesEntidadesAnalyticsRepository
from src.db.base import SourceSessionLocal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.core.config2 import settings as form_settings


# Directories
DATA_PROVISIONAL_DIR = "data_provisional"
GENERATED_IMAGES_DIR = "generated_images"
os.makedirs(DATA_PROVISIONAL_DIR, exist_ok=True)
os.makedirs(GENERATED_IMAGES_DIR, exist_ok=True)


class CrucesAnalyticsService:
    """
    Servicio principal para análisis de cruces de entidades.
    Lee datos desde BD, procesa y genera analytics JSON.
    """
    
    def __init__(self):
        self.repo = CrucesEntidadesAnalyticsRepository()

    

    def _load_formularios_from_db(
        self, 
        db: Session, 
        empresa_id: Optional[int] = None,
        forms_url: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Carga formularios desde la BD de formularios (base2) con auto-detección de tabla/columnas.
        """
        # Build engine for Forms DB with driver fallback
        forms_url = forms_url or form_settings.SOURCE_DATABASE_URL
        def force_pymysql(url: str) -> str:
            u = url or ""
            u = u.replace("mysql+mysqlconnector", "mysql+pymysql")
            u = u.replace("+mysqlconnector", "+pymysql")
            if "mysql+pymysql" not in u and u.startswith("mysql://"):
                u = u.replace("mysql://", "mysql+pymysql://")
            return u
        def build_mysqlconnector(url: str) -> str:
            u = url or ""
            u = u.replace("mysql+pymysql", "mysql+mysqlconnector")
            if "mysql+mysqlconnector" not in u and u.startswith("mysql://"):
                u = u.replace("mysql://", "mysql+mysqlconnector://")
            sep = "&" if "?" in u else "?"
            if "auth_plugin=" not in u:
                u = f"{u}{sep}auth_plugin=mysql_native_password"
            return u
        try:
            url_conn = build_mysqlconnector(forms_url)
            engine = create_engine(url_conn, pool_pre_ping=True, future=True)
            local_forms = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)()
            local_forms.execute(text("SELECT 1"))
        except Exception:
            try:
                url_conn = force_pymysql(forms_url)
                engine = create_engine(url_conn, pool_pre_ping=True, future=True)
                local_forms = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)()
                local_forms.execute(text("SELECT 1"))
            except Exception as e2:
                print(f"   ⚠️ Error creando conexión a BD de formularios: {e2}")
                return pd.DataFrame(columns=['id_empresa','numero_id','fecha_registro','nombre_completo'])
        try:
            # Detect candidate table and columns
            info_sql = text("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()")
            dfc = pd.read_sql(info_sql, local_forms.bind)
            alias_empresa = ["id_empresa", "empresa_id", "company_id"]
            alias_id = ["numero_id", "num_id", "documento", "identificacion", "nit", "no_documento_de_identidad"]
            alias_fecha = ["fecha_registro", "created_at", "fecha", "timestamp"]
            alias_nombre = ["nombre_completo", "nombre", "razon_social"]
            table = None
            empresa_col = None
            id_col = None
            fecha_col = None
            nombre_col = None
            for tname in dfc["TABLE_NAME"].unique().tolist():
                cols = dfc[dfc["TABLE_NAME"] == tname]["COLUMN_NAME"].tolist()
                cand_emp = next((c for c in alias_empresa if c in cols), None)
                cand_id = next((c for c in alias_id if c in cols), None)
                if cand_emp and cand_id:
                    table = tname
                    empresa_col = cand_emp
                    id_col = cand_id
                    fecha_col = next((c for c in alias_fecha if c in cols), None)
                    nombre_col = next((c for c in alias_nombre if c in cols), None)
                    break
            if not table or not empresa_col or not id_col:
                print("   ⚠️ No se encontró tabla de formularios con columnas empresa/ID. Se omite análisis de formularios.")
                return pd.DataFrame(columns=['id_empresa','numero_id','fecha_registro','nombre_completo'])
            base_sql = f"SELECT {empresa_col} AS id_empresa, {id_col} AS numero_id"
            if fecha_col: base_sql += f", {fecha_col} AS fecha_registro"
            else: base_sql += ", NULL AS fecha_registro"
            if nombre_col: base_sql += f", {nombre_col} AS nombre_completo"
            else: base_sql += ", NULL AS nombre_completo"
            base_sql += f" FROM {table}"
            params = {}
            if empresa_id:
                base_sql += " WHERE {empresa_col} = :eid".format(empresa_col=empresa_col)
                params["eid"] = empresa_id
            df = pd.read_sql(text(base_sql), local_forms.bind, params=params)
            if df.empty:
                return pd.DataFrame(columns=['id_empresa','numero_id','fecha_registro','nombre_completo'])
            df['numero_id'] = df['numero_id'].astype(str).str.strip()
            df['fecha_registro'] = pd.to_datetime(df.get('fecha_registro'), errors='coerce')
            print(f"   ✅ Formularios cargados desde BD (forms): {len(df)} registros")
            return df
        except Exception as e:
            print(f"   ⚠️ Error cargando formularios: {e}")
            return pd.DataFrame(columns=['id_empresa','numero_id','fecha_registro','nombre_completo'])
        finally:
            local_forms.close()

    def get_active_companies(self, db: Session) -> list[int]:
        """Obtiene lista de IDs de empresas que tienen datos en la BD."""
        try:
            # Consultar IDs únicos de clientes usando SQL directo
            query = text("SELECT DISTINCT id_empresa FROM clientes")
            result = db.execute(query).fetchall()
            return [row[0] for row in result if row[0] is not None]
        except Exception as e:
            print(f"Error obteniendo empresas activas: {e}")
            return []

    def _load_data_from_db(self, db: Session, empresa_id: Optional[int] = None, fecha: Optional[str] = None, monto_min: Optional[float] = None) -> tuple:
        """
        Carga datos desde la base de datos usando SQL directo para obtener todas las columnas.
        Uses a FRESH connection to avoid timeouts/stale connections from previous steps.
        
        Args:
            db: Sesión de base de datos (Ignored in favor of fresh connection)
            empresa_id: Opcional, filtra por empresa específica
            fecha: Opcional, fecha específica (YYYY-MM-DD) para filtrar transacciones por día
            monto_min: Opcional, monto mínimo de transacción
            
        Returns:
            Tuple de (df_clientes, df_proveedores, df_empleados)
        """
        params = {}
        if empresa_id:
            params['empresa_id'] = empresa_id
            
        # Use fresh session for heavy data loading
        local_db = SourceSessionLocal()
        try:
            print(f"   🔌 Establishing fresh DB connection for Cruces Analytics...")
            
            # Optimización: Cargar solo columnas necesarias si el dataset es muy grande
            # Para la empresa 15, que es masiva, esto reduce drásticamente el consumo de memoria y red.
            # Columnas clave para el análisis de cruces:
            cols_base = [
                'id_empresa', 'id_contraparte', 'identificacion', 'numero_id', 'numero_documento', 'nit',
                'nombre', 'razon_social', 'nombre_completo', 'nombre_proveedor', 'nombre_cliente', 'nombre_empleado',
                'valor', 'valor_transaccion', 'monto', 'total', 'salario', 'sueldo',
                'fecha', 'fecha_transaccion', 'fecha_registro',
                'riesgo', 'nivel_riesgo', 'conteo_alto',
                'medio_pago', 'forma_pago', 'actividad', 'descripcion', 'concepto'
            ]
            
            def build_query(table, empresa_id):
                try:
                    # Detectamos columnas disponibles
                    probe = local_db.execute(text(f"SELECT * FROM {table} LIMIT 0"))
                    available_cols = list(probe.keys())
                    
                    # Filtramos solo las que nos interesan para optimizar
                    final_cols = []
                    for col in available_cols:
                        # Incluir si está en la lista base, empieza con risk_, o contiene id/fecha/nombre/valor
                        if col in cols_base or col.startswith('risk_') or 'id' in col or 'fecha' in col or 'nombre' in col or 'valor' in col:
                            final_cols.append(col)
                    
                    if not final_cols: 
                        return f"SELECT * FROM {table}"
                    
                    cols_str = ", ".join(final_cols)
                    query = f"SELECT {cols_str} FROM {table}"
                    if empresa_id:
                        query += f" WHERE id_empresa = {empresa_id}" # Direct injection for simplicity in this context, safe if empresa_id is int
                    return query
                except Exception as e:
                    print(f"⚠️ Error optimizando query para {table}: {e}")
                    q = f"SELECT * FROM {table}"
                    if empresa_id: q += f" WHERE id_empresa = {empresa_id}"
                    return q

            # 1. Cargar clientes
            sql_clientes = build_query("clientes", empresa_id)
            print(f"   📊 Query Clientes optimizado...")
            df_clientes = pd.read_sql(text(sql_clientes), local_db.bind)
            
            # 2. Cargar proveedores
            sql_proveedores = build_query("proveedores", empresa_id)
            df_proveedores = pd.read_sql(text(sql_proveedores), local_db.bind)
            
            # 3. Cargar empleados
            sql_empleados = build_query("empleados", empresa_id)
            df_empleados = pd.read_sql(text(sql_empleados), local_db.bind)

            # Aplicar filtros en memoria por falta de nombres de columnas estandarizados
            def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
                if df is None or df.empty:
                    return df
                df_f = df.copy()
                # Lista de posibles columnas de fecha
                date_cols = ['fecha_transaccion', 'fecha', 'created_at', 'fecha_registro', 'fecha_movimiento', 'date', 'timestamp', 'fecha_doc', 'fec_mov', 'fecha_corte', 'fecha_operacion', 'fec_doc', 'fecha_mvto']
                # Lista de posibles columnas de monto
                amount_cols = ['valor_transaccion', 'valor', 'monto', 'salario', 'valor_suma']
                # Normalizar fecha si se solicitó
                if fecha:
                    try:
                        fecha_dt = pd.to_datetime(fecha).date()
                        # Crear una máscara que sea True si alguna columna de fecha coincide con el día
                        mask_any = None
                        for c in date_cols:
                            if c in df_f.columns:
                                col_dt = pd.to_datetime(df_f[c], errors='coerce').dt.date
                                mask_c = (col_dt == fecha_dt)
                                mask_any = mask_c if mask_any is None else (mask_any | mask_c)
                        if mask_any is not None:
                            df_f = df_f[mask_any]
                    except Exception:
                        pass
                # Normalizar monto si se solicitó
                if monto_min is not None:
                    mask_amt = None
                    for c in amount_cols:
                        if c in df_f.columns:
                            col_num = pd.to_numeric(df_f[c], errors='coerce')
                            mask_c = col_num >= float(monto_min)
                            mask_amt = mask_c if mask_amt is None else (mask_amt | mask_c)
                    if mask_amt is not None:
                        df_f = df_f[mask_amt]
                return df_f

            df_clientes = apply_filters(df_clientes)
            df_proveedores = apply_filters(df_proveedores)
            df_empleados = apply_filters(df_empleados)

            return df_clientes, df_proveedores, df_empleados
            
        except Exception as e:
            print(f"   ❌ Error loading data from DB: {e}")
            raise e
        finally:
            local_db.close()
    
    @staticmethod
    def clean_nans(obj):
        if isinstance(obj, dict):
            return {k: CrucesAnalyticsService.clean_nans(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [CrucesAnalyticsService.clean_nans(v) for v in obj]
        elif isinstance(obj, float):
            return None if pd.isna(obj) else obj
        elif isinstance(obj, (pd.Timestamp, datetime, date)):
            try:
                return obj.isoformat()
            except Exception:
                return str(obj)
        else:
            # Convert numpy scalar types if present
            try:
                import numpy as np
                if isinstance(obj, (np.integer, np.floating)):
                    return obj.item()
            except Exception:
                pass
            return obj

    def generate_cruces_analytics(
        self, 
        db: Session, 
        empresa_id: Optional[int] = None,
        fecha: Optional[str] = None,
        monto_min: Optional[float] = None,
        forms_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Genera análisis completo de cruces de entidades.
        
        Args:
            db: Sesión de base de datos
            empresa_id: Opcional, filtra por empresa específica
            fecha: Opcional, fecha específica (YYYY-MM-DD) para filtrar transacciones por día
            monto_min: Opcional, monto mínimo de transacción
            
        Returns:
            Dictionary con status y datos de analytics
        """
        try:
            print(f"🔍 Cargando datos desde BD para cruces...")
            
            # 1. Cargar datos desde BD
            df_clientes, df_proveedores, df_empleados = self._load_data_from_db(db, empresa_id, fecha=fecha, monto_min=monto_min)
            
            if df_clientes.empty:
                return {
                    "status": "error",
                    "message": f"No se encontraron datos de clientes{' para empresa ' + str(empresa_id) if empresa_id else ''}"
                }
            
            print(f"   ✅ Clientes: {len(df_clientes)} registros")
            print(f"   ✅ Proveedores: {len(df_proveedores)} registros")
            print(f"   ✅ Empleados: {len(df_empleados)} registros")

            print(f"📋 Cargando formularios desde la BD...")
            df_formularios = self._load_formularios_from_db(db, empresa_id, forms_url=forms_url)
            
            # 2. Procesar con CrucesAnalytics
            print("🔄 Procesando cruces de entidades...")
            analytics = CrucesAnalytics(df_clientes=df_clientes, df_proveedores=df_proveedores, df_empleados=df_empleados, df_formularios=df_formularios)
            df_cruces = analytics.procesar_datos()
            
            if df_cruces.empty:
                return {
                    "status": "warning",
                    "message": "No se detectaron cruces de entidades con alto riesgo",
                    "data": {
                        "kpis": analytics.get_kpis(),
                        "cruces_detectados": []
                    }
                }
            
            print(f"   ✅ Cruces detectados: {len(df_cruces)}")
            
            # 3. Calcular KPIs y métricas
            kpis = analytics.get_kpis()
            distribucion_riesgo = analytics.get_distribucion_riesgo()
            tipos_cruces = analytics.get_tipos_cruces()
            distribucion_categorias = analytics.get_distribucion_categorias()
            top_empresas = analytics.get_top_empresas()
            tabla_detalles = analytics.get_tabla_detalles(empresa_id)
            estadisticas_formularios = analytics.get_estadisticas_formularios()
            missing_dd_report = analytics.get_missing_dd_report()
            total_transacciones = int((0 if df_clientes is None else len(df_clientes)) + (0 if df_proveedores is None else len(df_proveedores)) + (0 if df_empleados is None else len(df_empleados)))
            id_opts_cli = ["id_contraparte","no_documento_de_identidad","nit","identificacion","numero_documento","num_id","id","cedula"]
            id_opts_pro = ["id_contraparte","no_documento_de_identidad","nit_proveedor","nit","identificacion","numero_documento","num_id","id","cedula"]
            id_opts_emp = ["id_contraparte","no_documento_de_identidad","identificacion","numero_documento","num_id","id","cedula"]
            date_cols = ['fecha_transaccion','fecha','created_at','fecha_registro','fecha_movimiento','date','timestamp','fecha_doc','fec_mov','fecha_corte','fecha_operacion','fec_doc','fecha_mvto']
            amount_cols = ['valor_transaccion','valor','monto','salario','valor_suma']
            def norm_id(v):
                if v is None: return ""
                s = str(v).strip()
                if s == "": return ""
                return "".join(ch for ch in s if ch.isalnum()).upper()
            def pick_col(df, opts):
                for c in opts:
                    if c in df.columns: return c
                return None
            def pick_date(df):
                for c in date_cols:
                    if c in df.columns: return c
                return None
            def pick_amount(df):
                for c in amount_cols:
                    if c in df.columns: return c
                return None
            df_forms = analytics.df_formularios if analytics.df_formularios is not None else None
            forms_set = set()
            if df_forms is not None and not df_forms.empty:
                for _, r in df_forms.iterrows():
                    eid = int(r.get("id_empresa", 0) or 0)
                    nid = norm_id(r.get("numero_id"))
                    if eid and nid:
                        forms_set.add((eid, nid))
                        if len(nid) > 1:
                            base = nid[:-1]
                            if base:
                                forms_set.add((eid, base))
            missing_tx = []
            total_missing_tx = 0
            limit_missing = 10000
            def collect(df, tipo, id_opts):
                nonlocal total_missing_tx, missing_tx
                if df is None or df.empty: return
                id_col = pick_col(df, id_opts)
                dcol = pick_date(df)
                acol = pick_amount(df)
                # Name and location columns candidates
                name_opts = ['nombre', 'razon_social', 'nombre_cliente', 'nombre_proveedor', 'nombre_empleado', 'empresa', 'nombre_completo']
                loc_opts = ['municipio', 'ciudad', 'departamento', 'ubicacion', 'localizacion']
                def pick_name(r):
                    for c in name_opts:
                        if c in df.columns:
                            v = r.get(c)
                            if v and str(v).strip():
                                return str(v).strip()
                    return None
                def pick_loc(r):
                    vals = []
                    for c in loc_opts:
                        if c in df.columns:
                            v = r.get(c)
                            if v and str(v).strip():
                                vals.append(str(v).strip())
                    return ", ".join(vals) if vals else None
                for _, r in df.iterrows():
                    eid = int(r.get("id_empresa", 0) or 0)
                    sid = norm_id(r.get(id_col)) if id_col else ""
                    if not eid or not sid: continue
                    key = (eid, sid)
                    has = key in forms_set or (len(sid) > 1 and (eid, sid[:-1]) in forms_set)
                    if not has:
                        total_missing_tx += 1
                        if len(missing_tx) < limit_missing:
                            missing_tx.append({
                                "tipo": tipo,
                                "id_empresa": eid,
                                "id": sid,
                                "fecha": r.get(dcol) if dcol else None,
                                "monto": r.get(acol) if acol else None,
                                "nombre": pick_name(r),
                                "ubicacion": pick_loc(r)
                            })
            collect(df_clientes, "cliente", id_opts_cli)
            collect(df_proveedores, "proveedor", id_opts_pro)
            collect(df_empleados, "empleado", id_opts_emp)
            # Aggregate by contraparte, including transaction details
            entidades_sin_dd_map: Dict[tuple, Dict[str, Any]] = {}
            for row in missing_tx:
                key = (row["id_empresa"], row["id"])
                ent = entidades_sin_dd_map.get(key)
                if not ent:
                    entidades_sin_dd_map[key] = {
                        "id_empresa": row["id_empresa"],
                        "id_contraparte": row["id"],
                        "nombre": row.get("nombre") or "",
                        "ubicacion": row.get("ubicacion") or "",
                        "cliente_txs": [],
                        "proveedor_txs": [],
                        "empleado_txs": [],
                        "cliente_sum": 0.0,
                        "proveedor_sum": 0.0,
                        "empleado_sum": 0.0
                    }
                    ent = entidades_sin_dd_map[key]
                # Append transaction and accumulate amounts
                amt = pd.to_numeric(row.get("monto"), errors='coerce')
                val = float(amt) if not pd.isna(amt) else 0.0
                tx_item = {"fecha": row.get("fecha"), "monto": val}
                if row["tipo"] == "cliente":
                    ent["cliente_txs"].append(tx_item)
                    ent["cliente_sum"] += val
                elif row["tipo"] == "proveedor":
                    ent["proveedor_txs"].append(tx_item)
                    ent["proveedor_sum"] += val
                elif row["tipo"] == "empleado":
                    ent["empleado_txs"].append(tx_item)
                    ent["empleado_sum"] += val
            entidades_sin_dd = []
            for (_, _), ent in entidades_sin_dd_map.items():
                entidades_sin_dd.append({
                    "id": ent["id_contraparte"],
                    "empresa": ent["nombre"],
                    "ubicacion": ent["ubicacion"],
                    "id_contraparte": ent["id_contraparte"],
                    "id_empresa": ent["id_empresa"],
                    "cruces_count": 0,
                    "conteo_categorias": int((len(ent["cliente_txs"]) > 0) + (len(ent["proveedor_txs"]) > 0) + (len(ent["empleado_txs"]) > 0)),
                    "cliente": { "count": len(ent["cliente_txs"]), "amount": ent["cliente_sum"], "risk_class": "secondary", "risk_label": "N/A", "transacciones": ent["cliente_txs"] },
                    "proveedor": { "count": len(ent["proveedor_txs"]), "amount": ent["proveedor_sum"], "risk_class": "secondary", "risk_label": "N/A", "transacciones": ent["proveedor_txs"] },
                    "empleado": { "count": len(ent["empleado_txs"]), "amount": ent["empleado_sum"], "risk_class": "secondary", "risk_label": "N/A", "transacciones": ent["empleado_txs"] },
                    "risk_factors": {},
                    "riesgo_maximo": 0,
                    "dd": False,
                    "tiene_formulario": False
                })
            
            # 4. Generar gráficos
            print("📊 Generando gráficos...")
            graph_gen = CrucesGraphGenerator(analytics)
            charts = graph_gen.generate_all_charts()
            
            # 4.1 Anotar DD en tabla_detalles y construir dd_ids para el frontend
            dd_ids = []
            try:
                def extract_sid(entry: Dict[str, Any]) -> str:
                    for key in ["id_contraparte","id","nit","identificacion","nro_identificacion","documento","numero_documento","num_id","cedula"]:
                        v = entry.get(key)
                        if v:
                            s = norm_id(v)
                            if s: return s
                    return ""
                # Recorremos cada contraparte y marcamos dd=True si está en forms_set
                if isinstance(tabla_detalles, list):
                    for e in tabla_detalles:
                        sid = extract_sid(e)
                        if sid:
                            if (empresa_id, sid) in forms_set or (len(sid) > 1 and (empresa_id, sid[:-1]) in forms_set):
                                e["dd"] = True
                                dd_ids.append(sid)
                            else:
                                # mantener explícito cuando no tiene
                                e.setdefault("dd", False)
                # Unificar y limpiar dd_ids
                dd_ids = sorted(list(set(dd_ids)))
            except Exception as _:
                dd_ids = []

            # 5. Ensamblar payload completo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            analytics_data = {
                "generated_at": timestamp,
                "empresa_id": empresa_id,
                "filtros": {
                    "fecha": fecha,
                    "monto_min": monto_min
                },
                "kpis": kpis,
                "total_transacciones": total_transacciones,
                "distribucion_riesgo": distribucion_riesgo,
                "tipos_cruces": tipos_cruces,
                "distribucion_categorias": distribucion_categorias,
                "top_empresas": top_empresas,
                "tabla_detalles": tabla_detalles,
                "entidades_cruces": tabla_detalles, # Alias for PHP Controller compatibility
                "estadisticas_formularios": estadisticas_formularios,
                "faltantes_dd": missing_dd_report,
                "transacciones_sin_dd_total": int(total_missing_tx),
                "transacciones_sin_dd": missing_tx,
                "dd_ids": dd_ids,
                "entidades_sin_dd": entidades_sin_dd,
                "charts": charts
            }

            # Compaction: generar JSON más liviano para empresas con gran volumen
            try:
                compact_enabled = (os.getenv("COMPACT_JSON", "true").lower() in ("true", "1", "yes"))
                limit = int(os.getenv("JSON_LIMIT", "2000"))
                txn_limit = int(os.getenv("JSON_TXN_LIMIT", "100"))
                if compact_enabled:
                    def slice_with_meta(arr, key_name):
                        if not isinstance(arr, list):
                            return arr, { "total": 0, "limit": limit, "has_more": False }
                        total = len(arr)
                        sliced = arr[:limit]
                        meta = { "total": total, "limit": limit, "has_more": total > limit }
                        analytics_data[key_name + "_meta"] = meta
                        return sliced, meta
                    # Tablas principales
                    for k in ["tabla_detalles", "entidades_cruces", "entidades_sin_dd", "transacciones_sin_dd"]:
                        if k in analytics_data:
                            sliced, _ = slice_with_meta(analytics_data[k], k)
                            analytics_data[k] = sliced
                    # Limitar transacciones anidadas por contraparte
                    containers = analytics_data.get("entidades_cruces") or analytics_data.get("tabla_detalles") or []
                    if isinstance(containers, list):
                        for e in containers:
                            for tipo in ("cliente", "proveedor", "empleado"):
                                if isinstance(e.get(tipo), dict):
                                    for subk in ("transacciones", "transacciones_detalles"):
                                        if isinstance(e[tipo].get(subk), list):
                                            e[tipo][subk] = e[tipo][subk][:txn_limit]
                    analytics_data["payload_compact"] = True
                    analytics_data["compact_limits"] = { "limit": limit, "txn_limit": txn_limit }
            except Exception as _ce:
                # En caso de error, continuar sin compactar
                pass

            analytics_data = self.clean_nans(analytics_data)

            db_json_path = "STORED_IN_DB"
            src_db = SourceSessionLocal()
            try:
                import gzip, io
                from src.services.s3_service import s3_service
                json_str = json.dumps(analytics_data, ensure_ascii=False)
                payload = json_str.encode("utf-8")
                if len(payload) > 8 * 1024 * 1024:
                    buf = io.BytesIO()
                    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
                        f.write(payload)
                    gz = buf.getvalue()
                    key = f"analytics/analytics_{empresa_id}_{timestamp}.json.gz"
                    url = s3_service.upload_file(gz, key, content_type="application/gzip")
                    db_json_path = key if url else "JSON_TOO_LARGE"
                    self.repo.create(src_db, empresa_id, db_json_path, data_json=None)
                else:
                    self.repo.create(src_db, empresa_id, db_json_path, data_json=json_str)
            finally:
                src_db.close()

            print("✅ Analytics guardado en base de datos")
            
            return {
                "status": "success",
                "json_path": None,
                "data": analytics_data
            }
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"❌ Error generando analytics de cruces: {e}")
            return {
                "status": "error",
                "message": str(e)
            }


# Singleton instance
cruces_analytics_service = CrucesAnalyticsService()
