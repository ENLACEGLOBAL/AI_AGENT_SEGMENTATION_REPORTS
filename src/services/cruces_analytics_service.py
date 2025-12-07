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
from src.db.models.reference_tables import HistoricoPaises

# Directorio para almacenar JSON generados
DATA_PROVISIONAL_DIR = "data_provisional"
os.makedirs(DATA_PROVISIONAL_DIR, exist_ok=True)


class CrucesAnalyticsService:
    """
    Servicio principal para análisis de cruces de entidades.
    Lee datos desde BD, procesa y genera analytics JSON.
    """
    
    def __init__(self):
        pass
    
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
        # query_proveedores = db.query(Proveedor)
        # if empresa_id:
        #     query_proveedores = query_proveedores.filter(Proveedor.id_empresa == empresa_id)
        # df_proveedores = pd.read_sql(query_proveedores.statement, db.bind)
        
        # Por ahora, creamos DF vacío - DEBES IMPLEMENTAR ESTO
        df_proveedores = pd.DataFrame(columns=[
            'id_empresa', 'no_documento_de_identidad', 'valor_transaccion', 
            'orden_clasificacion_del_riesgo'
        ])
        
        # 3. Cargar empleados (asume tabla similar)
        # from src.db.models.empleado import Empleado
        # query_empleados = db.query(Empleado)
        # if empresa_id:
        #     query_empleados = query_empleados.filter(Empleado.id_empresa == empresa_id)
        # df_empleados = pd.read_sql(query_empleados.statement, db.bind)
        
        # Por ahora, creamos DF vacío - DEBES IMPLEMENTAR ESTO
        df_empleados = pd.DataFrame(columns=[
            'id_empresa', 'id_empleado', 'valor', 'conteo_alto'
        ])
        
        return df_clientes, df_proveedores, df_empleados
    
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
            
            # 2. Procesar con CrucesAnalytics
            print("🔄 Procesando cruces de entidades...")
            analytics = CrucesAnalytics(df_clientes, df_proveedores, df_empleados)
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
                "charts": charts
            }
            
            # 6. Guardar JSON
            json_filename = f"cruces_analytics_{empresa_id if empresa_id else 'all'}_{timestamp}.json"
            json_path = os.path.join(DATA_PROVISIONAL_DIR, json_filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(analytics_data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Analytics guardado: {json_path}")
            
            return {
                "status": "success",
                "json_path": json_path,
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