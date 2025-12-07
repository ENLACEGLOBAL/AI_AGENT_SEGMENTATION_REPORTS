# src/analytics_modules/cruces_entidades/cruces_analytics.py
"""
Módulo para detectar cruces de entidades (conflictos de interés)
cuando una misma persona aparece en múltiples roles
"""
import pandas as pd
from typing import Dict, Any, List


class CrucesAnalytics:
    """Analiza cruces entre clientes, proveedores y empleados"""
    
    def __init__(self, df_clientes: pd.DataFrame, df_proveedores: pd.DataFrame, df_empleados: pd.DataFrame):
        self.df_clientes = df_clientes
        self.df_proveedores = df_proveedores
        self.df_empleados = df_empleados
        self.df_cruces = None
        
    def procesar_datos(self) -> pd.DataFrame:
        """
        Procesa los datos según la lógica del notebook:
        - Agrupa por empresa e id_contraparte
        - Identifica entidades con >= 2 categorías
        - Filtra por alto riesgo
        """
        # 1. Agregar Clientes
        df_clientes_agg = (
            self.df_clientes.rename(columns={
                'num_id': 'id_contraparte', 
                'valor_transaccion': 'valor_suma', 
                'orden_clasificacion_del_riesgo': 'riesgo'
            }, errors='ignore')
            .assign(id_contraparte=lambda x: x['id_contraparte'].astype(str))
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(
                cantidad_clientes=('id_contraparte', 'size'),
                suma_clientes=('valor_suma', 'sum'),
                Mayor_riesgo_clientes=('riesgo', 'max')
            )
        )
        
        # 2. Agregar Proveedores
        df_proveedores_agg = (
            self.df_proveedores.rename(columns={
                'no_documento_de_identidad': 'id_contraparte', 
                'valor_transaccion': 'valor_suma', 
                'orden_clasificacion_del_riesgo': 'riesgo'
            }, errors='ignore')
            .assign(id_contraparte=lambda x: x['id_contraparte'].astype(str))
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(
                cantidad_proveedores=('id_contraparte', 'size'),
                suma_proveedores=('valor_suma', 'sum'),
                Mayor_riesgo_proveedores=('riesgo', 'max')
            )
        )
        
        # 3. Agregar Empleados
        df_empleados_agg = (
            self.df_empleados.rename(columns={
                'id_empleado': 'id_contraparte', 
                'valor': 'valor_suma', 
                'conteo_alto': 'riesgo'
            }, errors='ignore')
            .assign(id_contraparte=lambda x: x['id_contraparte'].astype(str))
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(
                cantidad_empleados=('id_contraparte', 'size'),
                suma_empleados=('valor_suma', 'sum'),
                Mayor_riesgo_empleados=('riesgo', 'max')
            )
        )
        
        # Combinar todos los dataframes
        df_resumen = df_clientes_agg.join(df_proveedores_agg, how='outer')
        df_resumen = df_resumen.join(df_empleados_agg, how='outer')
        
        # Llenar valores nulos
        columnas_cantidad = [col for col in df_resumen.columns if 'cantidad' in col]
        df_resumen[columnas_cantidad] = df_resumen[columnas_cantidad].fillna(0)
        
        # Calcular conteo de categorías
        df_resumen['conteo_categorias'] = (
            (df_resumen['cantidad_clientes'] > 0).astype(int) +
            (df_resumen['cantidad_proveedores'] > 0).astype(int) +
            (df_resumen['cantidad_empleados'] > 0).astype(int)
        )
        
        # Filtrar contrapartes con al menos 2 categorías
        df_filtrado = df_resumen[df_resumen['conteo_categorias'] >= 2].copy()
        
        columnas_suma = [col for col in df_filtrado.columns if 'suma' in col]
        df_filtrado[columnas_suma] = df_filtrado[columnas_suma].fillna(0)
        
        df_filtrado = df_filtrado.reset_index()
        
        # Filtrar por alto riesgo
        df_final = df_filtrado[
            (df_filtrado['Mayor_riesgo_clientes'] >= 3) | 
            (df_filtrado['Mayor_riesgo_proveedores'] >= 3) | 
            (df_filtrado['Mayor_riesgo_empleados'] == 'Alto')
        ]
        
        self.df_cruces = df_final
        return df_final
    
    def get_kpis(self) -> Dict[str, Any]:
        """Calcula KPIs principales"""
        if self.df_cruces is None or self.df_cruces.empty:
            return {
                "total_registros": 0,
                "entidades_cruces": 0,
                "porcentaje_cruces": 0.0,
                "riesgo_promedio": 0.0,
                "alto_riesgo_count": 0,
                "valor_total_riesgo": 0.0
            }
        
        df = self.df_cruces
        
        # Calcular riesgo promedio numérico
        riesgos = []
        for _, row in df.iterrows():
            r_cliente = row.get('Mayor_riesgo_clientes', 0)
            r_proveedor = row.get('Mayor_riesgo_proveedores', 0)
            r_empleado = 5 if str(row.get('Mayor_riesgo_empleados', '')).upper() == 'ALTO' else 0
            riesgos.append(max(r_cliente, r_proveedor, r_empleado))
        
        riesgo_promedio = sum(riesgos) / len(riesgos) if riesgos else 0.0
        alto_riesgo_count = sum(1 for r in riesgos if r >= 4)
        
        valor_total = (
            df['suma_clientes'].sum() + 
            df['suma_proveedores'].sum() + 
            df['suma_empleados'].sum()
        )
        
        return {
            "total_registros": len(df),
            "entidades_cruces": df['id_contraparte'].nunique(),
            "porcentaje_cruces": round((len(df) / max(len(self.df_clientes), 1)) * 100, 2),
            "riesgo_promedio": round(riesgo_promedio, 1),
            "alto_riesgo_count": alto_riesgo_count,
            "valor_total_riesgo": float(valor_total)
        }
    
    def get_distribucion_riesgo(self) -> Dict[str, int]:
        """Distribución por nivel de riesgo"""
        if self.df_cruces is None or self.df_cruces.empty:
            return {"bajo": 0, "medio": 0, "alto": 0}
        
        bajo = medio = alto = 0
        for _, row in self.df_cruces.iterrows():
            r_cliente = row.get('Mayor_riesgo_clientes', 0)
            r_proveedor = row.get('Mayor_riesgo_proveedores', 0)
            r_empleado = 5 if str(row.get('Mayor_riesgo_empleados', '')).upper() == 'ALTO' else 0
            max_r = max(r_cliente, r_proveedor, r_empleado)
            
            if max_r >= 4:
                alto += 1
            elif max_r == 3:
                medio += 1
            else:
                bajo += 1
        
        return {"bajo": bajo, "medio": medio, "alto": alto}
    
    def get_tipos_cruces(self) -> Dict[str, int]:
        """Tipos de cruces detectados"""
        if self.df_cruces is None or self.df_cruces.empty:
            return {}
        
        tipos = {
            "cliente_proveedor": 0,
            "proveedor_empleado": 0,
            "cliente_empleado": 0,
            "triple_cruce": 0
        }
        
        for _, row in self.df_cruces.iterrows():
            tiene_cliente = row['cantidad_clientes'] > 0
            tiene_proveedor = row['cantidad_proveedores'] > 0
            tiene_empleado = row['cantidad_empleados'] > 0
            
            if tiene_cliente and tiene_proveedor and tiene_empleado:
                tipos['triple_cruce'] += 1
            elif tiene_cliente and tiene_proveedor:
                tipos['cliente_proveedor'] += 1
            elif tiene_proveedor and tiene_empleado:
                tipos['proveedor_empleado'] += 1
            elif tiene_cliente and tiene_empleado:
                tipos['cliente_empleado'] += 1
        
        return tipos
    
    def get_distribucion_categorias(self) -> Dict[str, int]:
        """Distribución por número de categorías"""
        if self.df_cruces is None or self.df_cruces.empty:
            return {}
        
        return self.df_cruces['conteo_categorias'].value_counts().to_dict()
    
    def get_top_empresas(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """Top empresas por cantidad de cruces"""
        if self.df_cruces is None or self.df_cruces.empty:
            return []
        
        empresa_counts = self.df_cruces['id_empresa'].value_counts().head(top_n)
        return [
            {"empresa": str(emp), "cruces": int(count)}
            for emp, count in empresa_counts.items()
        ]
    
    def get_tabla_detalles(self, empresa_id: int = None) -> List[Dict[str, Any]]:
        """Tabla de detalles de entidades con cruces"""
        if self.df_cruces is None or self.df_cruces.empty:
            return []
        
        df = self.df_cruces
        if empresa_id is not None:
            df = df[df['id_empresa'] == empresa_id]
        
        tabla = []
        for _, row in df.iterrows():
            # Determinar riesgo máximo
            r_cliente = row.get('Mayor_riesgo_clientes', 0)
            r_proveedor = row.get('Mayor_riesgo_proveedores', 0)
            r_empleado_str = str(row.get('Mayor_riesgo_empleados', '')).upper()
            r_empleado = 5 if r_empleado_str == 'ALTO' else 0
            
            # Construir info de cliente
            cliente_info = None
            if row['cantidad_clientes'] > 0:
                cliente_info = {
                    "cantidad": int(row['cantidad_clientes']),
                    "suma": float(row['suma_clientes']),
                    "riesgo": int(r_cliente)
                }
            
            # Construir info de proveedor
            proveedor_info = None
            if row['cantidad_proveedores'] > 0:
                proveedor_info = {
                    "cantidad": int(row['cantidad_proveedores']),
                    "suma": float(row['suma_proveedores']),
                    "riesgo": int(r_proveedor)
                }
            
            # Construir info de empleado
            empleado_info = None
            if row['cantidad_empleados'] > 0:
                empleado_info = {
                    "cantidad": int(row['cantidad_empleados']),
                    "suma": float(row['suma_empleados']),
                    "riesgo": r_empleado_str
                }
            
            tabla.append({
                "id_contraparte": str(row['id_contraparte']),
                "id_empresa": int(row['id_empresa']),
                "conteo_categorias": int(row['conteo_categorias']),
                "cliente": cliente_info,
                "proveedor": proveedor_info,
                "empleado": empleado_info,
                "riesgo_maximo": max(r_cliente, r_proveedor, r_empleado)
            })
        
        return tabla