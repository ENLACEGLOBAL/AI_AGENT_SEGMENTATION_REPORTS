# src/services/cruces_analytics_service.py
"""
Servicio orquestador para análisis de cruces de entidades
Lee datos desde BD (no CSV) y genera JSON + gráficos
"""
import json
import math
import os
import re
from datetime import datetime, date
from typing import Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session
import pandas as pd
import polars as pl

from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
from src.analytics_modules.cruces_entidades.cruces_graph_generator import CrucesGraphGenerator
# Models are no longer needed for data loading as we use Raw SQL
# from src.db.models.cliente import Cliente
# from src.db.models.proveedor import Proveedor
# from src.db.models.empleado import Empleado
from src.db.repositories.cruces_entidades_analytics_repo import CrucesEntidadesAnalyticsRepository
from src.db.base import SourceSessionLocal, TargetSessionLocal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.core.config2 import settings as form_settings

# Directories
DATA_PROVISIONAL_DIR = "data_provisional"
GENERATED_IMAGES_DIR = "generated_images"
os.makedirs(DATA_PROVISIONAL_DIR, exist_ok=True)
os.makedirs(GENERATED_IMAGES_DIR, exist_ok=True)

# Cache de engines para la BD de formularios (base2), en vez de crear/destruir uno
# por cada llamada a _load_formularios_from_db (que se invoca 2 veces por reporte:
# vigencia actual + histórico completo). Reutilizar el engine conserva su pool de
# conexiones entre llamadas, evitando 2 handshakes TCP+auth nuevos por reporte.
_forms_engines: Dict[str, Any] = {}


def _force_pymysql_url(url: str) -> str:
    """config2 siempre debería dar un DSN pymysql; normalizamos por si acaso."""
    u = url or ""
    u = u.replace("mysql+mysqlconnector", "mysql+pymysql")
    u = u.replace("+mysqlconnector", "+pymysql")
    if "mysql+pymysql" not in u and u.startswith("mysql://"):
        u = u.replace("mysql://", "mysql+pymysql://")
    return u


def _get_forms_engine(forms_url: str):
    """Devuelve un engine cacheado (con pool) para la BD de formularios."""
    url_conn = _force_pymysql_url(forms_url)
    engine = _forms_engines.get(url_conn)
    if engine is None:
        engine = create_engine(
            url_conn, pool_pre_ping=True, pool_recycle=1800,
            pool_size=5, max_overflow=10, future=True
        )
        _forms_engines[url_conn] = engine
    return engine


class CrucesAnalyticsService:
    """
    Servicio principal para análisis de cruces de entidades.
    Lee datos desde BD, procesa y genera analytics JSON.
    """

    def __init__(self):
        self.repo = CrucesEntidadesAnalyticsRepository()

    @staticmethod
    def normalize_id(v: Any) -> str:
        """Limpia IDs: quita ceros a la izquierda, puntos y guiones para un match exacto."""
        if v is None or pd.isna(v):
            return ""
        s = str(v).strip().upper()
        # Deja solo letras y números, y quita los ceros a la izquierda
        s = re.sub(r'[^A-Z0-9]', '', s)
        return s.lstrip('0')

    def _load_formularios_from_db(
            self,
            db: Session,
            empresa_id: Optional[int] = None,
            forms_url: Optional[str] = None,
            validez_dd: int = 1,
            ignore_vigencia: bool = False
    ) -> pd.DataFrame:
        """
        Carga formularios desde la BD de formularios (base2) con auto-detección de tabla/columnas.
        """
        # Engine para Forms DB, cacheado a nivel de módulo (ver _get_forms_engine):
        # evita crear/destruir un engine nuevo en cada una de las 2 llamadas por reporte.
        forms_url = forms_url or form_settings.TARGET_DATABASE_URL

        try:
            engine = _get_forms_engine(forms_url)
            local_forms = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)()
            local_forms.execute(text("SELECT 1"))
        except Exception as e:
            print(f"   [WARN] Error creando conexión a BD de formularios: {e}")
            return pd.DataFrame(columns=['id_empresa', 'numero_id', 'fecha_registro', 'nombre_completo'])
        try:
            # [GREEN] FIX: Calculamos los días dinámicamente según la selección (1 o 2 años)
            dias_maximos = 730 if validez_dd == 2 else 365
            if ignore_vigencia:
                # Historial COMPLETO sin filtrar por vigencia respecto a hoy: se usa
                # únicamente para calcular si una transacción puntual cayó dentro de
                # la ventana de vigencia del formulario EN SU PROPIA FECHA (no en la de hoy).
                print("   [INFO] Buscando historial COMPLETO de Formularios DD (sin límite de vigencia respecto a hoy)...")
            else:
                print(f"   [INFO] Buscando Formularios DD con vigencia de hasta {dias_maximos} días...")

            base_sql = f"""
                                        SELECT
                                            id_empresa,
                                            numero_id,
                                            fecha_registro,
                                            nombre_completo
                                        FROM vista_info_forms_completo_new2
                                        WHERE anulado = 0
                                          {"" if ignore_vigencia else f"AND dias_transcurridos <= {dias_maximos}"}
                                    """
            params = {}
            if empresa_id:
                base_sql += " AND id_empresa = :eid"
                params["eid"] = empresa_id

            df = pd.read_sql(text(base_sql), local_forms.bind, params=params)

            if df.empty:
                return pd.DataFrame(columns=['id_empresa', 'numero_id', 'fecha_registro', 'nombre_completo'])

            # APLICAMOS NORMALIZACION EN LA CARGA
            if not df.empty and 'numero_id' in df.columns:
                df['numero_id'] = df['numero_id'].apply(self.normalize_id)
                df['fecha_registro'] = pd.to_datetime(df.get('fecha_registro'), errors='coerce')

            print(f"   [OK] Formularios cargados desde BD (tabla 'formularios'): {len(df)} registros")
            return df

        except Exception as e:
            print(f"   [WARN] Error cargando tabla 'formularios': {e}")
            return pd.DataFrame(columns=['id_empresa', 'numero_id', 'fecha_registro', 'nombre_completo'])
        finally:
            # No se llama a engine.dispose(): el engine está cacheado en
            # _forms_engines y se reutiliza en la siguiente llamada/reporte.
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

    @staticmethod
    def _apply_filters_polars(df: pd.DataFrame, fecha: Optional[str] = None,
                              monto_min: Optional[float] = None) -> pd.DataFrame:
        """Aplica filtros pesados con Polars y conserva el DataFrame Pandas original."""
        if df is None or df.empty or (fecha is None and monto_min is None):
            return df

        date_cols = ['fecha_transaccion', 'fecha', 'created_at', 'fecha_registro',
                     'fecha_movimiento', 'date', 'timestamp', 'fecha_doc', 'fec_mov',
                     'fecha_corte', 'fecha_operacion', 'fec_doc', 'fecha_mvto']
        amount_cols = ['valor_transaccion', 'valor', 'monto', 'salario', 'valor_suma']
        available_dates = [c for c in date_cols if c in df.columns]
        available_amounts = [c for c in amount_cols if c in df.columns]

        if fecha is not None and not available_dates:
            return df
        if monto_min is not None and not available_amounts:
            return df

        table = pl.DataFrame({
            '_row_id': list(range(len(df))),
            **{
                c: pl.Series(c, df[c].tolist(), strict=False)
                for c in set(available_dates + available_amounts)
            }
        })
        mask = None

        if fecha is not None:
            fecha_dt = pd.to_datetime(fecha).date()
            date_mask = None
            for column in available_dates:
                value = pl.col(column).cast(pl.String)
                current = (
                    (value.str.slice(0, 10) == str(fecha_dt)) |
                    (value.str.to_datetime(strict=False, exact=False).dt.date() == pl.lit(fecha_dt))
                )
                date_mask = current if date_mask is None else (date_mask | current)
            mask = date_mask

        if monto_min is not None:
            amount_mask = None
            for column in available_amounts:
                numeric = pl.col(column).cast(pl.Float64, strict=False).fill_nan(None)
                current = numeric.is_not_null() & (numeric >= float(monto_min))
                amount_mask = current if amount_mask is None else (amount_mask | current)
            mask = amount_mask if mask is None else (mask & amount_mask)

        selected_ids = table.filter(mask).get_column('_row_id').to_list()
        return df.iloc[selected_ids]

    def _load_data_from_db(self, db: Session, empresa_id: Optional[int] = None, fecha: Optional[str] = None,
                           monto_min: Optional[float] = None) -> tuple:
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
            print(f"   [DB] Establishing fresh DB connection for Cruces Analytics...")

            # Optimización: Cargar solo columnas necesarias si el dataset es muy grande
            # Para la empresa 15, que es masiva, esto reduce drásticamente el consumo de memoria y red.
            # Columnas clave para el análisis de cruces:
            cols_base = [
                'id_empresa', 'id_contraparte', 'identificacion', 'numero_id', 'num_id', 'no_documento_de_identidad', 'numero_documento', 'nit',
                'nombre', 'razon_social', 'nombre_completo', 'nombre_proveedor', 'nombre_cliente', 'nombre_empleado',
                'valor', 'valor_transaccion', 'monto', 'total', 'salario', 'sueldo',
                'fecha', 'fecha_transaccion', 'fecha_registro',
                'riesgo', 'nivel_riesgo', 'conteo_alto',
                'medio_pago', 'forma_pago', 'actividad', 'descripcion', 'concepto',
                'documento', 'no_documento_de_identidad', 'id_empleado', 'cedula', 'empleado',
                'ciiu_descripcion', 'concepto_pago', 'cat_concep_pago', 'cargo'
            ]

            def build_query(table, empresa_id):
                try:
                    # Detectamos columnas disponibles
                    probe = local_db.execute(text(f"SELECT * FROM {table} LIMIT 0"))
                    available_cols = list(probe.keys())
                    probe.close()  # [GREEN] CRÍTICO: Liberar el cursor inmediatamente para evitar bloqueos en MySQL

                    # Convertimos cols_base a un set para búsquedas ultra rápidas y exactas
                    cols_base_lower = set([c.lower() for c in cols_base])

                    final_cols = []
                    for col in available_cols:
                        col_lower = col.lower()

                        # [GREEN] Match EXACTO o prefijos conocidos. Nada de comodines abiertos.
                        if (col_lower in cols_base_lower or
                                col_lower.startswith('risk_') or
                                col_lower.startswith('categoria_') or
                                col_lower.startswith('criterio_') or
                                col_lower.startswith('pais_') or
                                'ciiu' in col_lower):
                            final_cols.append(f"`{col}`")  # [GREEN] Backticks protegen palabras reservadas de MySQL

                    # Si por alguna razón no encuentra nada, volvemos al default
                    if not final_cols:
                        q = f"SELECT * FROM {table}"
                        if empresa_id: q += f" WHERE id_empresa = {empresa_id}"
                        return q

                    cols_str = ", ".join(final_cols)
                    query = f"SELECT {cols_str} FROM {table}"
                    if empresa_id:
                        query += f" WHERE id_empresa = {empresa_id}"
                    return query

                except Exception as e:
                    print(f"[WARN] Error optimizando query para {table}: {e}")
                    q = f"SELECT * FROM {table}"
                    if empresa_id: q += f" WHERE id_empresa = {empresa_id}"
                    return q

            # 1. Cargar clientes
            sql_clientes = build_query("clientes", empresa_id)
            print(f"   [CHART] Query Clientes optimizado...")
            df_clientes = pd.read_sql(text(sql_clientes), local_db.bind)

            # 2. Cargar proveedores
            sql_proveedores = build_query("proveedores", empresa_id)
            df_proveedores = pd.read_sql(text(sql_proveedores), local_db.bind)

            # 3. Cargar empleados
            sql_empleados = build_query("empleados", empresa_id)
            df_empleados = pd.read_sql(text(sql_empleados), local_db.bind)

            # Aplicar filtros en Polars y conservar Pandas para el resto del pipeline.
            df_clientes = self._apply_filters_polars(df_clientes, fecha, monto_min)
            df_proveedores = self._apply_filters_polars(df_proveedores, fecha, monto_min)
            df_empleados = self._apply_filters_polars(df_empleados, fecha, monto_min)

            return df_clientes, df_proveedores, df_empleados

        except Exception as e:
            print(f"   [ERROR] Error loading data from DB: {e}")
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
            return obj if math.isfinite(obj) else None
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
            forms_url: Optional[str] = None,
            validez_dd: int = 1,
            full_detail: bool = False
    ) -> Dict[str, Any]:
        """
        Genera análisis completo de cruces de entidades.

        Args:
            db: Sesión de base de datos
            empresa_id: Opcional, filtra por empresa específica
            fecha: Opcional, fecha específica (YYYY-MM-DD) para filtrar transacciones por día
            monto_min: Opcional, monto mínimo de transacción
            full_detail: Si True, desactiva la compactación de JSON (JSON_TXN_LIMIT) sin
                importar el env var COMPACT_JSON. Usado por la generación de PDF, donde
                truncar transacciones silenciosamente no es aceptable para un reporte de
                cumplimiento.

        Returns:
            Dictionary con status y datos de analytics
        """
        try:
            print(">>> Cargando datos desde BD para cruces...")

            # 1. Cargar datos desde BD
            df_clientes, df_proveedores, df_empleados = self._load_data_from_db(db, empresa_id, fecha=fecha, monto_min=monto_min)

            if df_clientes.empty:
                return {
                    "status": "error",
                    "message": f"No se encontraron datos de clientes{' para empresa ' + str(empresa_id) if empresa_id else ''}"
                }

            print(f"   [OK] Clientes: {len(df_clientes)} registros")
            print(f"   [OK] Proveedores: {len(df_proveedores)} registros")
            print(f"   [OK] Empleados: {len(df_empleados)} registros")

            print(">>> Cargando formularios desde la BD...")
            df_formularios = self._load_formularios_from_db(db, empresa_id, forms_url=forms_url, validez_dd=validez_dd)

            # Historial completo de formularios (sin filtrar por vigencia respecto a HOY).
            # Se usa solo para poder indicar si una transacción concreta ocurrió
            # dentro de una ventana de DD vigente EN SU PROPIA FECHA, sin alterar
            # la clasificación "tiene/no tiene DD" que ya usa df_formularios.
            df_formularios_historial = self._load_formularios_from_db(
                db, empresa_id, forms_url=forms_url, validez_dd=validez_dd, ignore_vigencia=True
            )

            # 2. Procesar con CrucesAnalytics
            print("[PROC] Procesando cruces de entidades...")
            analytics = CrucesAnalytics(df_clientes=df_clientes, df_proveedores=df_proveedores,
                                        df_empleados=df_empleados, df_formularios=df_formularios)
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

            print(f"   [OK] Cruces detectados: {len(df_cruces)}")

            # 3. Calcular KPIs y métricas
            kpis = analytics.get_kpis()
            distribucion_riesgo = analytics.get_distribucion_riesgo()
            tipos_cruces = analytics.get_tipos_cruces()
            distribucion_categorias = analytics.get_distribucion_categorias()
            top_empresas = analytics.get_top_empresas()
            use_universo = (os.getenv("ANALYTICS_UNIVERSO", "false").lower() in ("true", "1", "yes"))
            tabla_detalles = analytics.get_tabla_detalles(empresa_id, usar_universo=use_universo)
            estadisticas_formularios = analytics.get_estadisticas_formularios()
            missing_dd_report = analytics.get_missing_dd_report()
            total_transacciones = int((0 if df_clientes is None else len(df_clientes)) + (
                0 if df_proveedores is None else len(df_proveedores)) + (
                                          0 if df_empleados is None else len(df_empleados)))
            id_opts_cli = ["num_id", "identificacion", "nit", "numero_documento", "id_contraparte"]
            id_opts_pro = ["no_documento_de_identidad", "identificacion", "nit", "numero_documento", "id_contraparte"]
            id_opts_emp = ["id_empleado", "identificacion", "documento", "numero_documento", "id_contraparte"]
            date_cols = ['fecha_transaccion', 'fecha', 'created_at', 'fecha_registro', 'fecha_movimiento', 'date',
                         'timestamp', 'fecha_doc', 'fec_mov', 'fecha_corte', 'fecha_operacion', 'fec_doc', 'fecha_mvto']
            amount_cols = ['valor_transaccion', 'valor', 'monto', 'salario', 'valor_suma']

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
                # Vectorized forms_set construction
                _tmp = df_forms[['id_empresa', 'numero_id']].dropna().copy()
                _tmp['id_empresa'] = _tmp['id_empresa'].astype(int)
                _tmp['numero_id_norm'] = _tmp['numero_id'].apply(self.normalize_id)
                _tmp = _tmp[_tmp['numero_id_norm'] != '']
                # Full ID pairs
                forms_set = set(zip(_tmp['id_empresa'], _tmp['numero_id_norm']))
                # Also add without DV for fuzzy matching
                _tmp_long = _tmp[_tmp['numero_id_norm'].str.len() > 5].copy()
                _tmp_long['numero_id_short'] = _tmp_long['numero_id_norm'].str[:-1]
                forms_set.update(zip(_tmp_long['id_empresa'], _tmp_long['numero_id_short']))
            # Claves precomputadas como strings para membership vectorizado (evita reconstruir el set en cada collect())
            forms_keys = {f"{e}_{s}" for e, s in forms_set}

            #  Vectorized DD vigencia lookup - replaces per-row compute_dd_vigencia
            dias_maximos_dd = 730 if validez_dd == 2 else 365

            def _build_dd_windows(df_forms: pd.DataFrame) -> pd.DataFrame:
                """Build a DataFrame of DD windows (empresa_id, id_normalize, inicio, fin) from form history."""
                if df_forms is None or df_forms.empty:
                    return pd.DataFrame(columns=['id_empresa', 'id_norm', 'inicio', 'fin'])
                df_w = df_forms[['id_empresa', 'numero_id', 'fecha_registro']].copy()
                df_w = df_w.dropna(subset=['fecha_registro', 'numero_id'])
                df_w['id_empresa'] = df_w['id_empresa'].astype(int)
                df_w['id_norm'] = df_w['numero_id'].apply(self.normalize_id)
                df_w = df_w[df_w['id_norm'] != '']
                df_w['inicio'] = pd.to_datetime(df_w['fecha_registro'])
                df_w['fin'] = df_w['inicio'] + pd.Timedelta(days=dias_maximos_dd)
                # Also add rows without last char (DV) for fuzzy matching
                df_w_dv = df_w[df_w['id_norm'].str.len() > 5].copy()
                df_w_dv['id_norm'] = df_w_dv['id_norm'].str[:-1]
                df_w = pd.concat([df_w[['id_empresa', 'id_norm', 'inicio', 'fin']], df_w_dv[['id_empresa', 'id_norm', 'inicio', 'fin']]], ignore_index=True)
                df_w = df_w.drop_duplicates(subset=['id_empresa', 'id_norm', 'inicio'])
                df_w = df_w.sort_values(['id_empresa', 'id_norm', 'inicio']).reset_index(drop=True)
                return df_w

            df_dd_windows = _build_dd_windows(df_formularios_historial)

            def compute_dd_vigencia_batch(df_tx: pd.DataFrame, id_col: str, date_col: str) -> pd.DataFrame:
                """Marca cada transaccion segun la ventana DD de su contraparte usando Polars de alto rendimiento."""
                if df_tx.empty or df_dd_windows.empty:
                    df_tx['dd_inicio'] = None
                    df_tx['dd_fin'] = None
                    df_tx['dd_vigente_en_fecha_transaccion'] = False
                    return df_tx

                try:
                    import numpy as np
                    df = df_tx.copy()
                    df['_row_idx'] = np.arange(len(df))
                    df['_eid'] = pd.to_numeric(df['id_empresa'], errors='coerce').fillna(0).astype(int) if 'id_empresa' in df.columns else 0
                    df['_sid'] = df[id_col].apply(self.normalize_id) if id_col else ''
                    df['_tx_fecha'] = pd.to_datetime(df[date_col], errors='coerce') if date_col else pd.NaT

                    # to_numpy(dtype=...) explícito en vez de `.values`: si numero_id vino con
                    # StringDtype de pandas, `.values` da un StringArray que pl.DataFrame
                    # rechaza, y `.astype(str)` sobre esa misma columna puede seguir devolviendo
                    # una StringArray en vez de un ndarray plano. Igual con fechas: `.values`
                    # puede quedar en una resolución (p. ej. 's') que Polars no acepta; solo
                    # 'D'/'ms'/'us'/'ns' son válidas, así que forzamos 'ns' explícitamente.
                    sid_plain = df['_sid'].astype(str).to_numpy(dtype=object)
                    tx_fecha_plain = df['_tx_fecha'].to_numpy(dtype='datetime64[ns]')
                    pl_tx = pl.DataFrame({
                        '_row_idx': df['_row_idx'].to_numpy(),
                        '_eid': df['_eid'].to_numpy() if isinstance(df['_eid'], pd.Series) else np.zeros(len(df), dtype=int),
                        '_sid': sid_plain,
                        '_sid_short': [s[:-1] if len(s) > 5 else s for s in sid_plain],
                        '_tx_fecha': tx_fecha_plain
                    })

                    pl_win = pl.DataFrame({
                        'w_eid': df_dd_windows['id_empresa'].to_numpy(dtype=int),
                        'w_sid': df_dd_windows['id_norm'].astype(str).to_numpy(dtype=object),
                        'w_inicio': pd.to_datetime(df_dd_windows['inicio']).to_numpy(dtype='datetime64[ns]'),
                        'w_fin': pd.to_datetime(df_dd_windows['fin']).to_numpy(dtype='datetime64[ns]')
                    })

                    # Join 1: Exact match on (eid, sid)
                    j1 = pl_tx.join(
                        pl_win,
                        left_on=['_eid', '_sid'],
                        right_on=['w_eid', 'w_sid'],
                        how='left'
                    ).with_columns([
                        (
                            pl.col('_tx_fecha').is_not_null() &
                            (pl.col('w_inicio') <= pl.col('_tx_fecha')) &
                            (pl.col('_tx_fecha') <= pl.col('w_fin'))
                        ).fill_null(False).alias('_en_ventana')
                    ])

                    # Join 2: Fuzzy match on (eid, sid_short)
                    j2 = pl_tx.join(
                        pl_win,
                        left_on=['_eid', '_sid_short'],
                        right_on=['w_eid', 'w_sid'],
                        how='left'
                    ).with_columns([
                        (
                            pl.col('_tx_fecha').is_not_null() &
                            (pl.col('w_inicio') <= pl.col('_tx_fecha')) &
                            (pl.col('_tx_fecha') <= pl.col('w_fin'))
                        ).fill_null(False).alias('_en_ventana')
                    ])

                    # group_by(maintain_order=True) conserva el orden de aparición del sort
                    # previo (ya ascendente por _row_idx), pero lo reordenamos explícito para
                    # no depender de ese detalle interno.
                    combined = pl.concat([j1, j2]).sort(
                        ['_row_idx', '_en_ventana', 'w_inicio'], descending=[False, True, True]
                    ).group_by('_row_idx', maintain_order=True).first().sort('_row_idx')

                    # Extracción nativa de Polars (sin combined.to_pandas()): to_pandas()
                    # requiere pyarrow, que no está instalado en este entorno.
                    df_tx['dd_inicio'] = combined.get_column('w_inicio').dt.strftime('%Y-%m-%d').to_list()
                    df_tx['dd_fin'] = combined.get_column('w_fin').dt.strftime('%Y-%m-%d').to_list()
                    df_tx['dd_vigente_en_fecha_transaccion'] = combined.get_column('_en_ventana').fill_null(False).to_list()
                    return df_tx
                except Exception as _pe:
                    print(f"   [WARN] Fallback a Pandas en compute_dd_vigencia_batch (id_col={id_col}, date_col={date_col}): {_pe}")
                    import traceback as _tb; _tb.print_exc()
                    df = df_tx.copy()
                    df['_tx_fecha'] = pd.to_datetime(df[date_col], errors='coerce') if date_col else pd.NaT
                    df['_eid'] = df['id_empresa'].astype(int) if 'id_empresa' in df.columns else 0
                    df['_sid'] = df[id_col].apply(self.normalize_id) if id_col else ''

                    # Fallback POR FILA: cada transacción se evalúa contra las ventanas
                    # DD de su propia contraparte y su propia fecha. Un groupby por
                    # (eid, sid) aquí colapsaría todas las transacciones de una misma
                    # contraparte al mismo resultado, filtrando transacciones fuera de
                    # ventana como si estuvieran cubiertas (o viceversa).
                    windows_by_key: Dict[tuple, list] = {}
                    for _, w in df_dd_windows.iterrows():
                        windows_by_key.setdefault((w['id_empresa'], w['id_norm']), []).append((w['inicio'], w['fin']))

                    dd_inicio_list = []
                    dd_fin_list = []
                    dd_vigente_list = []
                    for eid, sid, tx_ts in zip(df['_eid'].tolist(), df['_sid'].tolist(), df['_tx_fecha'].tolist()):
                        windows = windows_by_key.get((eid, sid), [])
                        if not windows and len(sid) > 5:
                            windows = windows_by_key.get((eid, sid[:-1]), [])
                        tx_ts = pd.Timestamp(tx_ts) if pd.notna(tx_ts) else pd.NaT
                        matched = None
                        for inicio, fin in windows:
                            if pd.notna(tx_ts) and inicio <= tx_ts <= fin:
                                matched = (inicio, fin, True)
                                break
                        if matched is None and windows:
                            matched = (windows[0][0], windows[0][1], False)
                        if matched is None:
                            dd_inicio_list.append(None)
                            dd_fin_list.append(None)
                            dd_vigente_list.append(False)
                        else:
                            inicio, fin, vigente = matched
                            dd_inicio_list.append(inicio.strftime('%Y-%m-%d') if pd.notna(inicio) else None)
                            dd_fin_list.append(fin.strftime('%Y-%m-%d') if pd.notna(fin) else None)
                            dd_vigente_list.append(vigente)
                    df['dd_inicio'] = dd_inicio_list
                    df['dd_fin'] = dd_fin_list
                    df['dd_vigente_en_fecha_transaccion'] = dd_vigente_list
                    return df

            def compute_dd_vigencia(eid: int, sid: str, tx_fecha: Any) -> Dict[str, Any]:
                """Fallback single-row DD vigencia check (kept for backward compatibility)."""
                if df_dd_windows.empty:
                    return {"dd_inicio": None, "dd_fin": None, "dd_vigente_en_fecha_transaccion": False}
                mask = (df_dd_windows['id_empresa'] == eid) & (df_dd_windows['id_norm'] == sid)
                candidates = df_dd_windows.loc[mask]
                if candidates.empty and len(sid) > 5:
                    mask = (df_dd_windows['id_empresa'] == eid) & (df_dd_windows['id_norm'] == sid[:-1])
                    candidates = df_dd_windows.loc[mask]
                if candidates.empty:
                    return {"dd_inicio": None, "dd_fin": None, "dd_vigente_en_fecha_transaccion": False}

                tx_ts = pd.to_datetime(tx_fecha, errors='coerce') if tx_fecha is not None else pd.NaT
                for _, w in candidates.iterrows():
                    if pd.notna(tx_ts) and w['inicio'] <= tx_ts <= w['fin']:
                        return {"dd_inicio": w['inicio'].strftime('%Y-%m-%d'), "dd_fin": w['fin'].strftime('%Y-%m-%d'), "dd_vigente_en_fecha_transaccion": True}

                first = candidates.iloc[0]
                return {"dd_inicio": first['inicio'].strftime('%Y-%m-%d'), "dd_fin": first['fin'].strftime('%Y-%m-%d'), "dd_vigente_en_fecha_transaccion": False}

            missing_tx = []
            total_missing_tx = 0
            limit_missing = int(os.getenv("LIMIT_MISSING_COLLECT", os.getenv("JSON_TXN_LIMIT", "5000")))

            # entidades_sin_dd_map se llena con TODAS las transacciones sin DD, no solo
            # las primeras `limit_missing`. Ese límite existe para acotar el tamaño del
            # detalle crudo (`transacciones_sin_dd`) exportado en el JSON, pero si también
            # recorta este resumen por entidad, contrapartes reales con huecos de DD
            # (p. ej. cuyas transacciones problemáticas no caen entre las primeras 5000
            # encontradas) desaparecen por completo del reporte en vez de aparecer con
            # su hueco puntual.
            entidades_sin_dd_map: Dict[tuple, Dict[str, Any]] = {}

            def _parse_riesgo_simple(val) -> int:
                """Misma escala usada en CrucesAnalytics.map_risk: ALTO/HIGH/5->5, MEDIO/MEDIUM/3->3, BAJO/LOW/1->1."""
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return 0
                s = str(val).strip().lower()
                if s in ('alto', 'high', '5'): return 5
                if s in ('medio', 'medium', '3'): return 3
                if s in ('bajo', 'low', '1'): return 1
                if s.isdigit(): return int(s)
                return 0

            def _agregar_entidad_sin_dd(row: Dict[str, Any]) -> None:
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
                        "empleado_sum": 0.0,
                        "riesgo_maximo": 0
                    }
                    ent = entidades_sin_dd_map[key]

                amt = pd.to_numeric(row.get("monto"), errors='coerce')
                val = float(amt) if not pd.isna(amt) else 0.0
                riesgo_val = _parse_riesgo_simple(row.get("riesgo"))
                if riesgo_val > ent["riesgo_maximo"]:
                    ent["riesgo_maximo"] = riesgo_val

                tx_item = {
                    "fecha": row.get("fecha"),
                    "monto": val,
                    "actividad": row.get("actividad"),
                    "medio_pago": row.get("medio"),
                    "dd_inicio": row.get("dd_inicio"),
                    "dd_fin": row.get("dd_fin"),
                    "dd_vigente_en_fecha_transaccion": row.get("dd_vigente_en_fecha_transaccion", False)
                }

                if row["tipo"] == "cliente":
                    ent["cliente_txs"].append(tx_item)
                    ent["cliente_sum"] += val
                elif row["tipo"] == "proveedor":
                    ent["proveedor_txs"].append(tx_item)
                    ent["proveedor_sum"] += val
                elif row["tipo"] == "empleado":
                    ent["empleado_txs"].append(tx_item)
                    ent["empleado_sum"] += val

            def collect(df, tipo, id_opts, risk_opts):
                nonlocal total_missing_tx, missing_tx
                if df is None or df.empty: return
                id_col = pick_col(df, id_opts)
                dcol = pick_date(df)
                acol = pick_amount(df)

                name_opts = ['nombre', 'razon_social', 'nombre_cliente', 'nombre_proveedor', 'empleado', 'nombre_empleado',
                             'empresa', 'nombre_completo']
                loc_opts = ['municipio', 'ciudad', 'departamento', 'ubicacion', 'localizacion']
                act_opts = ['actividad', 'ciiu_descripcion', 'concepto_pago', 'cargo', 'detalle_transaccion', 'descripcion']
                medio_opts = ['medio_pago', 'forma_pago', 'metodo_pago']

                def pick_col_first(df_src, opts):
                    for c in opts:
                        if c in df_src.columns:
                            return c
                    return None

                name_col = pick_col_first(df, name_opts)
                loc_col = pick_col_first(df, loc_opts)
                act_col = pick_col_first(df, act_opts)
                medio_col = pick_col_first(df, medio_opts)
                risk_col = pick_col_first(df, risk_opts)

                # Vectorized ID normalization
                df_work = df.copy()
                df_work['_eid'] = df_work['id_empresa'].apply(lambda x: int(x) if pd.notna(x) else 0)
                df_work['_sid'] = df_work[id_col].apply(self.normalize_id) if id_col else ''
                df_work['_has_id'] = (df_work['_eid'] > 0) & (df_work['_sid'] != '')

                # Vectorized DD vigencia batch
                if dcol:
                    df_work = compute_dd_vigencia_batch(df_work, id_col, dcol)
                else:
                    df_work['dd_inicio'] = None
                    df_work['dd_fin'] = None
                    df_work['dd_vigente_en_fecha_transaccion'] = False

                if 'dd_vigente_en_fecha_transaccion' in df_work.columns:
                    dd_vigente_col = df_work['dd_vigente_en_fecha_transaccion'].fillna(False).astype(bool)
                else:
                    dd_vigente_col = pd.Series(False, index=df_work.index)

                # La falta de DD se determina para la fecha de cada transaccion.
                # forms_set solo representa formularios actuales y no debe ocultar
                # transacciones anteriores o posteriores a su ventana de vigencia.
                df_work['_has_dd'] = dd_vigente_col
                df_work['_is_missing'] = df_work['_has_id'] & ~df_work['_has_dd']

                # Filter missing transactions
                df_missing = df_work[df_work['_is_missing']].copy()
                total_missing_tx += len(df_missing)

                if not df_missing.empty:
                    # Build result in one vectorized pass
                    tx_date_col = dcol if dcol else '_fecha_dummy'

                    def _build_result_df(src_df):
                        return pd.DataFrame({
                            'tipo': tipo,
                            'id_empresa': src_df['_eid'],
                            'id': src_df['_sid'],
                            'fecha': src_df[tx_date_col].values if tx_date_col in src_df.columns else None,
                            'monto': src_df[acol].values if acol and acol in src_df.columns else None,
                            'nombre': src_df[name_col].values if name_col else None,
                            'ubicacion': src_df[loc_col].values if loc_col else None,
                            'actividad': src_df[act_col].values if act_col else None,
                            'medio': src_df[medio_col].values if medio_col else None,
                            'riesgo': src_df[risk_col].values if risk_col else None,
                            'dd_inicio': src_df['dd_inicio'].values,
                            'dd_fin': src_df['dd_fin'].values,
                            'dd_vigente_en_fecha_transaccion': src_df['dd_vigente_en_fecha_transaccion'].values,
                        })

                    result_df = _build_result_df(df_missing)

                    # Resumen por entidad: una contraparte que tiene AL MENOS UNA
                    # transaccion sin DD entra en entidades_sin_dd, pero el detalle
                    # mostrado ahi debe cubrir TODO su historial (con DD y sin DD),
                    # no solo las lineas faltantes. Sin esto, el modulo no sirve para
                    # ver el panorama completo de una contraparte sin cruces (que solo
                    # aparece como proveedor/cliente/empleado, nunca en tabla_detalles).
                    qualifying_keys = set(zip(df_missing['_eid'], df_missing['_sid']))
                    df_work['_key'] = list(zip(df_work['_eid'], df_work['_sid']))
                    df_full_hist = df_work[df_work['_has_id'] & df_work['_key'].isin(qualifying_keys)]
                    result_df_full = _build_result_df(df_full_hist)
                    for rec in result_df_full.to_dict('records'):
                        _agregar_entidad_sin_dd(rec)

                    # transacciones_sin_dd (lista plana global) sigue siendo SOLO las
                    # que realmente faltan DD - no se toca con el cambio de arriba.
                    remaining = limit_missing - len(missing_tx)
                    if remaining > 0:
                        missing_tx.extend(result_df.head(remaining).to_dict('records'))

            risk_opts_cli_pro = ['orden_clasificacion_del_riesgo', 'riesgo', 'nivel_riesgo', 'categoria_jurisdicciones']
            risk_opts_emp = ['conteo_alto', 'riesgo', 'nivel_riesgo', 'categoria_jurisdicciones']
            collect(df_clientes, "cliente", id_opts_cli, risk_opts_cli_pro)
            collect(df_proveedores, "proveedor", id_opts_pro, risk_opts_cli_pro)
            collect(df_empleados, "empleado", id_opts_emp, risk_opts_emp)

            # entidades_sin_dd_map ya quedó completamente poblado dentro de collect().

            def _agrupar_txs_por_dd(txs: list) -> list:
                """Agrupa transacciones por rango DD unico, devuelve 1 entrada por rango
                (con fecha_min/fecha_max de las transacciones que caen en ese rango)."""
                grupos: dict = {}
                for tx in txs:
                    key = (tx.get("dd_inicio"), tx.get("dd_fin"), bool(tx.get("dd_vigente_en_fecha_transaccion", False)))
                    if key not in grupos:
                        grupos[key] = {
                            "dd_inicio": tx.get("dd_inicio"),
                            "dd_fin": tx.get("dd_fin"),
                            "dd_vigente_en_fecha_transaccion": bool(tx.get("dd_vigente_en_fecha_transaccion", False)),
                            "count": 0,
                            "monto_total": 0.0,
                            "fecha_min": None,
                            "fecha_max": None,
                        }
                    g = grupos[key]
                    g["count"] += 1
                    g["monto_total"] += float(tx.get("monto") or 0)
                    fecha_tx = tx.get("fecha")
                    if fecha_tx is not None and pd.notna(fecha_tx):
                        fecha_str = str(fecha_tx)
                        if g["fecha_min"] is None or fecha_str < g["fecha_min"]:
                            g["fecha_min"] = fecha_str
                        if g["fecha_max"] is None or fecha_str > g["fecha_max"]:
                            g["fecha_max"] = fecha_str
                return list(grupos.values())

            entidades_sin_dd = []
            for (_, _), ent in entidades_sin_dd_map.items():
                eid_ent = int(ent["id_empresa"]) if ent["id_empresa"] else 0
                sid_ent = ent["id_contraparte"] or ""
                # Estar en esta lista significa que ALGUNAS de sus transacciones
                # cayeron fuera de una ventana DD vigente, no que la contraparte
                # carezca por completo de formulario. Si tiene un formulario
                # registrado (forms_set), debe reflejarse aquí en vez de un False
                # fijo que oculta el hecho de que sí diligenció el formulario.
                tiene_formulario_ent = (
                    (eid_ent, sid_ent) in forms_set or
                    (len(sid_ent) > 5 and (eid_ent, sid_ent[:-1]) in forms_set)
                )
                entidades_sin_dd.append({
                    "id": ent["id_contraparte"],
                    "empresa": ent["nombre"],
                    "ubicacion": ent["ubicacion"],
                    "id_contraparte": ent["id_contraparte"],
                    "id_empresa": ent["id_empresa"],
                    "cruces_count": 0,
                    "conteo_categorias": int((len(ent["cliente_txs"]) > 0) + (len(ent["proveedor_txs"]) > 0) + (
                            len(ent["empleado_txs"]) > 0)),
                    # transacciones_detalles: el frontend (showMetricDetails en
                    # ia_analitica.blade.php) espera esta clave para poblar el modal
                    # de detalle; sin ella cae en "El recuento indica que existen
                    # transacciones, pero los detalles no fueron reportados por la
                    # base de datos" aunque count > 0. cliente_txs/proveedor_txs/
                    # empleado_txs ya traen el historial COMPLETO de la contraparte
                    # (no solo las transacciones sin DD) y se listan una por una; el
                    # rango DD (vigente/no vigente) se ve por transaccion en la
                    # columna "DD en la fecha de la transacción" del frontend, no
                    # agrupando filas. rangos_dd se deja como resumen aparte.
                    "cliente": {"count": len(ent["cliente_txs"]), "amount": ent["cliente_sum"],
                                "risk_class": "secondary", "risk_label": "N/A",
                                "rangos_dd": _agrupar_txs_por_dd(ent["cliente_txs"]),
                                "transacciones_detalles": ent["cliente_txs"]},
                    "proveedor": {"count": len(ent["proveedor_txs"]), "amount": ent["proveedor_sum"],
                                  "risk_class": "secondary", "risk_label": "N/A",
                                  "rangos_dd": _agrupar_txs_por_dd(ent["proveedor_txs"]),
                                  "transacciones_detalles": ent["proveedor_txs"]},
                    "empleado": {"count": len(ent["empleado_txs"]), "amount": ent["empleado_sum"],
                                 "risk_class": "secondary", "risk_label": "N/A",
                                 "rangos_dd": _agrupar_txs_por_dd(ent["empleado_txs"]),
                                 "transacciones_detalles": ent["empleado_txs"]},
                    "risk_factors": {},
                    "riesgo_maximo": ent["riesgo_maximo"],
                    "dd": tiene_formulario_ent,
                    "tiene_formulario": tiene_formulario_ent
                })

            # 4. Generar gráficos
            print("[CHART] Generando gráficos...")
            graph_gen = CrucesGraphGenerator(analytics)
            charts = graph_gen.generate_all_charts()

            # 4.1 Anotar DD en tabla_detalles y construir dd_ids para el frontend (vectorized)
            dd_ids = []
            try:
                def extract_sid(entry: Dict[str, Any]) -> str:
                    for key in ["id", "num_id", "no_documento_de_identidad", "id_empleado", "id_contraparte",
                                "identificacion", "nit", "numero_documento"]:
                        v = entry.get(key)
                        if v:
                            s = self.normalize_id(v)
                            if s: return s
                    return ""

                empresa_id_safe = int(empresa_id) if empresa_id else 0

                if isinstance(tabla_detalles, list):
                    # Pre-build a lookup dict for fast DD window matching
                    dd_windows_dict: Dict[tuple, list] = {}
                    if not df_dd_windows.empty:
                        emp_mask = df_dd_windows['id_empresa'] == empresa_id_safe if empresa_id_safe else pd.Series(True, index=df_dd_windows.index)
                        for _, w in df_dd_windows[emp_mask].iterrows():
                            key = (int(w['id_empresa']), w['id_norm'])
                            dd_windows_dict.setdefault(key, []).append((w['inicio'], w['fin']))

                    def fast_dd_check(eid, sid, fecha):
                        windows = dd_windows_dict.get((eid, sid), [])
                        if not windows and len(sid) > 5:
                            windows = dd_windows_dict.get((eid, sid[:-1]), [])
                        if not windows:
                            return None, None, False
                        tx_ts = pd.to_datetime(fecha, errors='coerce') if fecha else pd.NaT
                        closest = None
                        for inicio, fin in windows:
                            if pd.notna(tx_ts) and inicio <= tx_ts <= fin:
                                return inicio.strftime('%Y-%m-%d'), fin.strftime('%Y-%m-%d'), True
                            if pd.isna(tx_ts) or inicio <= tx_ts:
                                closest = (inicio, fin)
                        if closest is None:
                            closest = windows[0]
                        return closest[0].strftime('%Y-%m-%d'), closest[1].strftime('%Y-%m-%d'), False

                    def add_dd_periods(relation: Dict[str, Any]) -> None:
                        """Resume varias transacciones bajo una misma ventana DD."""
                        transaction_key = "transacciones_detalles" if isinstance(
                            relation.get("transacciones_detalles"), list
                        ) else "transacciones"
                        transactions = relation.get(transaction_key)
                        if not isinstance(transactions, list):
                            return
                        grouped: Dict[tuple, Dict[str, Any]] = {}
                        period_seen = set()
                        for tx in transactions:
                            if not isinstance(tx, dict) or not tx.get("dd_vigente_en_fecha_transaccion"):
                                continue
                            period = (tx.get("dd_inicio"), tx.get("dd_fin"))
                            # La vigencia se muestra una sola vez por rango; las
                            # transacciones siguen completas y marcadas como vigentes.
                            if period in period_seen:
                                tx["dd_inicio"] = None
                                tx["dd_fin"] = None
                            else:
                                period_seen.add(period)
                            if period not in grouped:
                                grouped[period] = {
                                    "inicio": period[0],
                                    "fin": period[1],
                                    "cantidad": 0,
                                    "monto_total": 0.0
                                }
                            grouped[period]["cantidad"] += 1
                            amount = pd.to_numeric(tx.get("monto"), errors="coerce")
                            if pd.notna(amount):
                                grouped[period]["monto_total"] += float(amount)
                        relation["dd_periodos"] = list(grouped.values())

                    for e in tabla_detalles:
                        sid = extract_sid(e)
                        if sid:
                            has_dd = (
                                (empresa_id_safe, sid) in forms_set or
                                (len(sid) > 5 and (empresa_id_safe, sid[:-1]) in forms_set)
                            )
                            e["dd"] = has_dd
                            if has_dd:
                                dd_ids.append(sid)
                        else:
                            e.setdefault("dd", False)

                        if sid:
                            for tipo_rel in ("cliente", "proveedor", "empleado"):
                                rel = e.get(tipo_rel)
                                if not isinstance(rel, dict):
                                    continue
                                detalles_rel = rel.get("transacciones_detalles")
                                if not isinstance(detalles_rel, list):
                                    continue
                                for tx in detalles_rel:
                                    if not isinstance(tx, dict):
                                        continue
                                    dd_inicio, dd_fin, dd_vigente = fast_dd_check(empresa_id_safe, sid, tx.get("fecha"))
                                    tx["dd_inicio"] = dd_inicio
                                    tx["dd_fin"] = dd_fin
                                    tx["dd_vigente_en_fecha_transaccion"] = dd_vigente
                                add_dd_periods(rel)
                # entidades_sin_dd NO pasa por add_dd_periods: esa función anula
                # (null) dd_inicio/dd_fin en transacciones repetidas dentro del mismo
                # rango para no repetir la etiqueta visualmente. Aquí cada fila de
                # transacciones_detalles es una transacción real e individual que el
                # frontend muestra tal cual (a pedido explícito: "todas las
                # transacciones, no agrupadas") - anular sus fechas haría que se vea
                # "Sin formulario DD" en transacciones que sí tienen DD vigente.
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
                "entidades_cruces": tabla_detalles,  # Alias for PHP Controller compatibility
                "estadisticas_formularios": estadisticas_formularios,
                "faltantes_dd": missing_dd_report,
                "transacciones_sin_dd_total": int(total_missing_tx),
                "transacciones_sin_dd": missing_tx,
                "dd_ids": dd_ids,
                "entidades_sin_dd": entidades_sin_dd,
                "charts": charts
            }

            # Compaction: generar JSON liviano para evitar desbordamiento de memoria en frontend
            try:
                compact_enabled = (not full_detail) and (os.getenv("COMPACT_JSON", "true").lower() in ("true", "1", "yes"))
                limit = int(os.getenv("JSON_LIMIT", "5000"))
                txn_limit = int(os.getenv("JSON_TXN_LIMIT", "20"))
                if compact_enabled:
                    def slice_with_meta(arr, key_name):
                        if not isinstance(arr, list):
                            return arr, {"total": 0, "limit": limit, "has_more": False}
                        total = len(arr)
                        sliced = arr[:limit]
                        meta = {"total": total, "limit": limit, "has_more": total > limit}
                        analytics_data[key_name + "_meta"] = meta
                        return sliced, meta

                    # Tablas principales. entidades_sin_dd queda fuera a propósito: es el
                    # resumen por contraparte de casos sin DD (no una lista de transacciones
                    # crudas), y recortarla a `limit` descarta contrapartes reales sin DD en
                    # vez de solo acotar el tamaño del payload — contradice el comentario en
                    # _agregar_entidad_sin_dd() que documenta que este resumen no debe recortarse.
                    for k in ["tabla_detalles", "entidades_cruces", "transacciones_sin_dd"]:
                        if k in analytics_data:
                            sliced, _ = slice_with_meta(analytics_data[k], k)
                            analytics_data[k] = sliced

                    # Limitar transacciones anidadas por contraparte en cruces y en entidades_sin_dd
                    containers = [
                        *(analytics_data.get("entidades_cruces") or []),
                        *(analytics_data.get("tabla_detalles") or []),
                        *(analytics_data.get("entidades_sin_dd") or [])
                    ]
                    for e in containers:
                        if not isinstance(e, dict):
                            continue
                        for tipo in ("cliente", "proveedor", "empleado"):
                            rel = e.get(tipo)
                            if isinstance(rel, dict):
                                for subk in ("transacciones", "transacciones_detalles"):
                                    if isinstance(rel.get(subk), list):
                                        rel[subk] = rel[subk][:txn_limit]
                    analytics_data["payload_compact"] = True
                    analytics_data["compact_limits"] = {"limit": limit, "txn_limit": txn_limit}
            except Exception as _ce:
                print(f"   [WARN] Error en compactación de JSON: {_ce}")

            analytics_data = self.clean_nans(analytics_data)

            # Pre-paginacion en Python para el caso sin compactar (--full/--universo):
            # con entidades que pueden traer cientos de transacciones cada una (p. ej.
            # CIALTA con 176), un solo blob de estas tablas puede pesar cientos de MB.
            # Intentar paginar DESPUES en PHP no sirve: requeriria decodificar ese
            # blob completo en cada request de pagina, lo cual agota la memoria
            # virtual del proceso (confirmado en pruebas: VirtualAlloc() failed).
            # En cambio, aqui - con las listas todavia como objetos Python nativos,
            # sin pasar por un json.dumps() gigante - se parten en paginas chicas
            # (acotadas por bytes, no solo por cantidad de entidades) y cada una se
            # sube como su propio archivo; PHP solo necesita transmitirlas tal cual,
            # sin decodificar nada.
            #
            # IMPORTANTE: esta extraccion se aplica a upload_data, una copia
            # superficial de analytics_data - NUNCA a analytics_data en si mismo.
            # report_orchestrator.generate_pdf() llama a esta funcion con
            # full_detail=True (que implica compact_enabled=False) y usa
            # directamente el dict devuelto en "data" para armar el PDF; si se
            # vaciaran tabla_detalles/entidades_sin_dd ahi, el PDF quedaria sin
            # las tablas de detalle. La paginacion es solo para lo que se sube/
            # persiste (lo que despues consume el dashboard via HTTP).
            upload_data = analytics_data
            if not compact_enabled:
                upload_data = dict(analytics_data)
                import gzip as _gzip, io as _io
                from src.services.s3_service import s3_service as _s3

                def _upload_paginated(key_name: str, items: list, max_page_bytes: int = 25 * 1024 * 1024):
                    if not isinstance(items, list) or not items:
                        return None
                    base_key = f"analytics/pages/{empresa_id}_{timestamp}"
                    pages = []
                    current_page: list = []
                    current_bytes = 2  # "[]"
                    for item in items:
                        item_bytes = len(json.dumps(item, ensure_ascii=False, allow_nan=False).encode("utf-8")) + 1
                        if current_page and current_bytes + item_bytes > max_page_bytes:
                            pages.append(current_page)
                            current_page = []
                            current_bytes = 2
                        current_page.append(item)
                        current_bytes += item_bytes
                    if current_page:
                        pages.append(current_page)

                    for idx, page_items in enumerate(pages):
                        page_str = json.dumps(page_items, ensure_ascii=False, allow_nan=False)
                        buf = _io.BytesIO()
                        with _gzip.GzipFile(fileobj=buf, mode="wb") as f:
                            f.write(page_str.encode("utf-8"))
                        page_key = f"{base_key}/{key_name}_page_{idx}.json.gz"
                        _s3.upload_file(buf.getvalue(), page_key, content_type="application/gzip")

                    return {"total": len(items), "pages": len(pages), "base_key": base_key}

                pagination_manifest = {}
                for _k in ["tabla_detalles", "entidades_sin_dd"]:
                    _items = upload_data.get(_k)
                    _manifest = _upload_paginated(_k, _items)
                    if _manifest:
                        pagination_manifest[_k] = _manifest
                        upload_data[_k] = []
                # entidades_cruces es el mismo contenido que tabla_detalles (alias para
                # el controlador PHP) - se referencia al mismo manifest en vez de
                # duplicar la subida.
                if "tabla_detalles" in pagination_manifest:
                    pagination_manifest["entidades_cruces"] = pagination_manifest["tabla_detalles"]
                    upload_data["entidades_cruces"] = []
                if pagination_manifest:
                    upload_data["_pagination"] = pagination_manifest

                # transacciones_sin_dd (lista plana global) no la usa el frontend del
                # dashboard cuando entidades_sin_dd viene poblado (solo es fallback) -
                # sin recortarla aqui seguiria pesando cientos de MB y arrastraria el
                # mismo problema que ya sacamos de tabla_detalles/entidades_sin_dd.
                if "entidades_sin_dd" in pagination_manifest and isinstance(upload_data.get("transacciones_sin_dd"), list):
                    upload_data["transacciones_sin_dd"] = []

            db_json_path = "STORED_IN_DB"
            src_db = SourceSessionLocal()
            try:
                import gzip, io
                from src.services.s3_service import s3_service
                json_str = json.dumps(upload_data, ensure_ascii=False, allow_nan=False)
                payload = json_str.encode("utf-8")
                if len(payload) > 8 * 1024 * 1024:
                    buf = io.BytesIO()
                    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
                        f.write(payload)
                    gz = buf.getvalue()
                    key = f"analytics/analytics_{empresa_id}_{timestamp}.json.gz"
                    url = s3_service.upload_file(gz, key, content_type="application/gzip")
                    if url:
                        self.repo.create(src_db, empresa_id, key, data_json=None)
                    else:
                        # Nunca registrar una key ficticia: el lector intentaria
                        # descargarla y el dashboard terminaria en 404.
                        self.repo.create(src_db, empresa_id, "STORED_IN_DB", data_json=json_str)
                else:
                    self.repo.create(src_db, empresa_id, db_json_path, data_json=json_str)
            finally:
                src_db.close()

            print("[OK] Analytics guardado en base de datos")

            return {
                "status": "success",
                "json_path": None,
                "data": analytics_data
            }

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"[ERROR] Error generando analytics de cruces: {e}")
            return {
                "status": "error",
                "message": str(e)
            }


# Singleton instance
cruces_analytics_service = CrucesAnalyticsService()