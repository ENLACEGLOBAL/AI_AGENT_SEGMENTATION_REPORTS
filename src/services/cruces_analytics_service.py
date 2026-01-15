# src/services/cruces_analytics_service.py
"""
Servicio orquestador para análisis de cruces de entidades
Lee datos desde BD (no CSV) y genera JSON + gráficos
"""
import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
import pandas as pd

from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
from src.analytics_modules.cruces_entidades.cruces_graph_generator import CrucesGraphGenerator
from src.db.models.cliente import Cliente
from src.db.models.proveedor import Proveedor
from src.db.models.empleado import Empleado
from src.db.models.formulario import Formulario
from src.db.models.reference_tables import HistoricoPaises
from src.db.repositories.cruces_entidades_analytics_repo import CrucesEntidadesAnalyticsRepository
from src.db.base import SourceSessionLocal


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
        empresa_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Carga formularios desde la vista forms_existentes.
        """
        try:
            query = db.query(Formulario)

            if empresa_id:
                query = query.filter(Formulario.id_empresa == empresa_id)

            df = pd.read_sql(query.statement, db.bind)
        except Exception as e:
            # Check for missing table error (MySQL 1146)
            if "1146" in str(e) or "doesn't exist" in str(e):
                print(f"   ⚠️ Vista/Tabla 'forms_existentes' no encontrada. Se omite análisis de formularios.")
                return pd.DataFrame(columns=[
                    'id_formulario',
                    'id_empresa',
                    'fecha_registro',
                    'nombre_completo',
                    'numero_id',
                    'contraparte'
                ])
            raise e

        if df.empty:
            return pd.DataFrame(columns=[
                'id_formulario',
                'id_empresa',
                'fecha_registro',
                'nombre_completo',
                'numero_id',
                'contraparte'
            ])

        # Limpiezas defensivas (aunque la vista ya limpia)
        df['numero_id'] = df['numero_id'].astype(str).str.strip()
        df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], errors='coerce')

        print(f"   ✅ Formularios cargados desde BD: {len(df)} registros")

        return df

    def get_active_companies(self, db: Session) -> list[int]:
        """Obtiene lista de IDs de empresas que tienen datos en la BD."""
        try:
            # Consultar IDs únicos de clientes
            # Nota: Podríamos unir con proveedores/empleados, pero asumimos que si hay clientes, hay actividad
            result = db.query(Cliente.id_empresa).distinct().all()
            return [row[0] for row in result if row[0] is not None]
        except Exception as e:
            print(f"Error obteniendo empresas activas: {e}")
            return []

    def _load_data_from_db(self, db: Session, empresa_id: Optional[int] = None) -> tuple:
        """
        Carga datos desde la base de datos.
        
        Args:
            db: Sesión de base de datos
            empresa_id: Opcional, filtra por empresa específica
            
        Returns:
            Tuple de (df_clientes, df_proveedores, df_empleados)
        """
        # NOTA: Ajusta estas tablas según tu esquema real de BD
        # Aquí usamos la tabla 'clientes' como ejemplo
        
        # 1. Cargar clientes
        query_clientes = db.query(Cliente)
        if empresa_id:
            query_clientes = query_clientes.filter(Cliente.id_empresa == empresa_id)
        
        df_clientes = pd.read_sql(query_clientes.statement, db.bind)
        
        # 2. Cargar proveedores (asume tabla similar a clientes)
        # AJUSTA según tu esquema real - ejemplo:
        # from src.db.models.proveedor import Proveedor
        query_proveedores = db.query(Proveedor)
        if empresa_id:
            query_proveedores = query_proveedores.filter(Proveedor.id_empresa == empresa_id)
        df_proveedores = pd.read_sql(query_proveedores.statement, db.bind)
        
        # Por ahora, creamos DF vacío - DEBES IMPLEMENTAR ESTO
        #df_proveedores = pd.DataFrame(columns=[
        #    'id_empresa', 'no_documento_de_identidad', 'valor_transaccion', 
        #    'orden_clasificacion_del_riesgo'
        #])
        
        # 3. Cargar empleados (asume tabla similar)
        # from src.db.models.empleado import Empleado
        query_empleados = db.query(Empleado)
        if empresa_id:
            query_empleados = query_empleados.filter(Empleado.id_empresa == empresa_id)
        df_empleados = pd.read_sql(query_empleados.statement, db.bind)
        
        # Por ahora, creamos DF vacío - DEBES IMPLEMENTAR ESTO
        #df_empleados = pd.DataFrame(columns=[
        #    'id_empresa', 'id_empleado', 'valor', 'conteo_alto'
        #])
        
        return df_clientes, df_proveedores, df_empleados
    
    @staticmethod
    def clean_nans(obj):
        if isinstance(obj, dict):
            return {k: CrucesAnalyticsService.clean_nans(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [CrucesAnalyticsService.clean_nans(v) for v in obj]
        elif isinstance(obj, float):
            return None if pd.isna(obj) else obj
        else:
            return obj

    def generate_cruces_analytics(
        self, 
        db: Session, 
        empresa_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Genera análisis completo de cruces de entidades.
        
        Args:
            db: Sesión de base de datos
            empresa_id: Opcional, filtra por empresa específica
            
        Returns:
            Dictionary con status y datos de analytics
        """
        try:
            print(f"🔍 Cargando datos desde BD para cruces...")
            
            # 1. Cargar datos desde BD
            df_clientes, df_proveedores, df_empleados = self._load_data_from_db(db, empresa_id)
            
            if df_clientes.empty:
                return {
                    "status": "error",
                    "message": f"No se encontraron datos de clientes{' para empresa ' + str(empresa_id) if empresa_id else ''}"
                }
            
            print(f"   ✅ Clientes: {len(df_clientes)} registros")
            print(f"   ✅ Proveedores: {len(df_proveedores)} registros")
            print(f"   ✅ Empleados: {len(df_empleados)} registros")

            print(f"📋 Cargando formularios desde la BD...")
            df_formularios = self._load_formularios_from_db(db, empresa_id)
            
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
            
            # 4. Generar gráficos
            print("📊 Generando gráficos...")
            graph_gen = CrucesGraphGenerator(analytics)
            charts = graph_gen.generate_all_charts()
            
            # 5. Ensamblar payload completo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            analytics_data = {
                "generated_at": timestamp,
                "empresa_id": empresa_id,
                "kpis": kpis,
                "distribucion_riesgo": distribucion_riesgo,
                "tipos_cruces": tipos_cruces,
                "distribucion_categorias": distribucion_categorias,
                "top_empresas": top_empresas,
                "tabla_detalles": tabla_detalles,
                "estadisticas_formularios": estadisticas_formularios,
                "charts": charts
            }
            
            analytics_data = self.clean_nans(analytics_data)

            db_json_path = "STORED_IN_DB"
            src_db = SourceSessionLocal()
            try:
                json_str = json.dumps(analytics_data, ensure_ascii=False)
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
