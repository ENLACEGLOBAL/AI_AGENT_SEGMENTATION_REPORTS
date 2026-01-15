# src/analytics_modules/cruces_entidades/cruces_analytics.py
"""
Módulo para detectar cruces de entidades (conflictos de interés)
cuando una misma persona aparece en múltiples roles
"""
import pandas as pd
from typing import Dict, Any, List


class CrucesAnalytics:
    """Analiza cruces entre clientes, proveedores y empleados"""
    
    def __init__(self, df_clientes: pd.DataFrame, df_proveedores: pd.DataFrame, df_empleados: pd.DataFrame, df_formularios: pd.DataFrame | None = None):
        self.df_clientes = df_clientes
        self.df_proveedores = df_proveedores
        self.df_empleados = df_empleados
        self.df_formularios = df_formularios
        self.df_cruces = None
        
    def _ensure_columns(self, df: pd.DataFrame, id_col: str, val_col_options: List[str], risk_col_options: List[str]) -> pd.DataFrame:
        """Normaliza columnas para el análisis"""
        df = df.copy()
        
        # 1. ID
        if id_col in df.columns:
            # Evitar duplicados si ya existe la columna destino
            if id_col != 'id_contraparte' and 'id_contraparte' in df.columns:
                df = df.drop(columns=['id_contraparte'])
            df = df.rename(columns={id_col: 'id_contraparte'})
            
        if 'id_contraparte' not in df.columns:
            df['id_contraparte'] = 'UNKNOWN'
            
        # 2. Valor (Suma)
        val_found = False
        for col in val_col_options:
            if col in df.columns:
                # Evitar duplicados
                if col != 'valor_suma' and 'valor_suma' in df.columns:
                    df = df.drop(columns=['valor_suma'])
                df = df.rename(columns={col: 'valor_suma'})
                val_found = True
                break
        
        # Si no encontramos una de las opciones, pero ya existía 'valor_suma', la mantenemos.
        # Si no existe, creamos default.
        if not val_found and 'valor_suma' not in df.columns:
            df['valor_suma'] = 0.0
            
        # 3. Riesgo
        risk_found = False
        for col in risk_col_options:
            if col in df.columns:
                # Evitar duplicados
                if col != 'riesgo' and 'riesgo' in df.columns:
                    df = df.drop(columns=['riesgo'])
                df = df.rename(columns={col: 'riesgo'})
                risk_found = True
                break
                
        if not risk_found and 'riesgo' not in df.columns:
            df['riesgo'] = 0 # Default bajo riesgo
            
        return df

    def procesar_datos(self) -> pd.DataFrame:
        """
        Procesa los datos según la lógica del notebook:
        - Agrupa por empresa e id_contraparte
        - Identifica entidades con >= 2 categorías
        - Filtra por alto riesgo
        """
        # 1. Agregar Clientes
        df_cli_norm = self._ensure_columns(
            self.df_clientes, 
            'num_id', 
            ['valor_transaccion', 'valor', 'monto'], 
            ['orden_clasificacion_del_riesgo', 'riesgo', 'nivel_riesgo']
        )
        
        # Asegurar tipo string explícitamente antes del groupby
        if 'id_contraparte' in df_cli_norm.columns:
            df_cli_norm['id_contraparte'] = df_cli_norm['id_contraparte'].astype(str)

        df_clientes_agg = (
            df_cli_norm
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(
                cantidad_clientes=('id_contraparte', 'size'),
                suma_clientes=('valor_suma', 'sum'),
                Mayor_riesgo_clientes=('riesgo', 'max')
            )
        )
        
        # 2. Agregar Proveedores
        df_pro_norm = self._ensure_columns(
            self.df_proveedores,
            'no_documento_de_identidad',
            ['valor_transaccion', 'valor', 'monto'],
            ['orden_clasificacion_del_riesgo', 'riesgo', 'nivel_riesgo']
        )
        
        # Asegurar tipo string explícitamente
        if 'id_contraparte' in df_pro_norm.columns:
            df_pro_norm['id_contraparte'] = df_pro_norm['id_contraparte'].astype(str)

        df_proveedores_agg = (
            df_pro_norm
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(
                cantidad_proveedores=('id_contraparte', 'size'),
                suma_proveedores=('valor_suma', 'sum'),
                Mayor_riesgo_proveedores=('riesgo', 'max')
            )
        )
        
        # 3. Agregar Empleados
        df_emp_norm = self._ensure_columns(
            self.df_empleados,
            'id_empleado',
            ['valor', 'valor_transaccion', 'monto', 'salario'],
            ['conteo_alto', 'riesgo', 'nivel_riesgo']
        )
        
        # Asegurar tipo string explícitamente
        if 'id_contraparte' in df_emp_norm.columns:
            df_emp_norm['id_contraparte'] = df_emp_norm['id_contraparte'].astype(str)

        df_empleados_agg = (
            df_emp_norm
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
        
        # Llenar valores nulos en columnas de riesgo (CRITICO para evitar NaN en gráficos)
        columnas_riesgo = [col for col in df_resumen.columns if 'Mayor_riesgo' in col]
        df_resumen[columnas_riesgo] = df_resumen[columnas_riesgo].fillna(0)
        
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
        df_final = df_filtrado
        if self.df_formularios is not None and not self.df_formularios.empty:
            df_final = self._verificar_formularios(df_final)
        else:
            df_final['tiene_formulario'] = False
            df_final['fecha_formulario'] = None
        self.df_cruces = df_final
        return df_final
    
    def _verificar_formularios(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Verifica si cada contraparte tiene formulario de debida diligencia.
        Cruza por id_empresa y numero_id (id_contraparte).
        """
        # Normalizar columna de número ID en formularios
        df_forms = self.df_formularios.copy()
        df_forms['numero_id'] = df_forms['numero_id'].astype(str)
        
        # Crear set de (empresa, numero_id) que tienen formulario
        formularios_set = set(
            zip(df_forms['id_empresa'], df_forms['numero_id'])
        )
        
        # Crear diccionario de fechas de registro
        fecha_dict = dict(
            zip(
                zip(df_forms['id_empresa'], df_forms['numero_id']),
                df_forms['fecha_registro']
            )
        )
        
        # Verificar cada fila
        df['tiene_formulario'] = df.apply(
            lambda row: (int(row['id_empresa']), str(row['id_contraparte'])) in formularios_set,
            axis=1
        )
        
        df['fecha_formulario'] = df.apply(
            lambda row: fecha_dict.get((int(row['id_empresa']), str(row['id_contraparte']))),
            axis=1
        )
        
        return df
    
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
        
        con_formulario = df['tiene_formulario'].sum()
        sin_formulario = len(df) - con_formulario
        porcentaje_con_formulario = (con_formulario / len(df) * 100) if len(df) > 0 else 0.0
        
        return {
            "total_registros": len(df),
            "entidades_cruces": df['id_contraparte'].nunique(),
            "porcentaje_cruces": round((len(df) / max(len(self.df_clientes), 1)) * 100, 2),
            "riesgo_promedio": round(riesgo_promedio, 1),
            "alto_riesgo_count": alto_riesgo_count,
            "valor_total_riesgo": float(valor_total),
            "con_formulario": int(con_formulario),
            "sin_formulario": int(sin_formulario),
            "porcentaje_con_formulario": round(porcentaje_con_formulario, 1)
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
            {"empresa": str(emp), "cruces": int(count or 0)}
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
            if pd.isna(r_cliente): r_cliente = 0
            
            r_proveedor = row.get('Mayor_riesgo_proveedores', 0)
            if pd.isna(r_proveedor): r_proveedor = 0
            
            r_empleado_str = str(row.get('Mayor_riesgo_empleados', '')).upper()
            r_empleado = 5 if r_empleado_str == 'ALTO' else 0
            
            # Construir info de cliente
            cliente_info = None
            cant_cli = row.get('cantidad_clientes', 0)
            if pd.notna(cant_cli) and cant_cli > 0:
                cliente_info = {
                    "cantidad": int(cant_cli),
                    "suma": float(row.get('suma_clientes', 0) or 0),
                    "riesgo": int(r_cliente)
                }
            
            # Construir info de proveedor
            proveedor_info = None
            cant_prov = row.get('cantidad_proveedores', 0)
            if pd.notna(cant_prov) and cant_prov > 0:
                proveedor_info = {
                    "cantidad": int(cant_prov),
                    "suma": float(row.get('suma_proveedores', 0) or 0),
                    "riesgo": int(r_proveedor)
                }
            
            # Construir info de empleado
            empleado_info = None
            cant_emp = row.get('cantidad_empleados', 0)
            if pd.notna(cant_emp) and cant_emp > 0:
                empleado_info = {
                    "cantidad": int(cant_emp),
                    "suma": float(row.get('suma_empleados', 0) or 0),
                    "riesgo": r_empleado_str
                }
            
            tiene_formulario = bool(row.get('tiene_formulario', False))
            fecha_formulario = row.get('fecha_formulario')
            
            tabla.append({
                "id_contraparte": str(row['id_contraparte']),
                "id_empresa": int(row.get('id_empresa', 0) or 0),
                "conteo_categorias": int(row.get('conteo_categorias', 0) or 0),
                "cliente": cliente_info,
                "proveedor": proveedor_info,
                "empleado": empleado_info,
                "riesgo_maximo": max(r_cliente, r_proveedor, r_empleado),
                "tiene_formulario": tiene_formulario,
                "fecha_formulario": str(fecha_formulario) if fecha_formulario else None
            })
        
        return tabla
    
    def get_estadisticas_formularios(self) -> Dict[str, Any]:
        """
        ⭐ NUEVO: Estadísticas específicas de formularios
        """
        if self.df_cruces is None or self.df_cruces.empty:
            return {
                "total": 0,
                "con_formulario": 0,
                "sin_formulario": 0,
                "porcentaje_completado": 0.0,
                "alto_riesgo_sin_formulario": 0
            }
        
        df = self.df_cruces
        total = len(df)
        con_formulario = df['tiene_formulario'].sum()
        sin_formulario = total - con_formulario
        
        # Contar alto riesgo sin formulario
        alto_riesgo_sin_form = 0
        for _, row in df[~df['tiene_formulario']].iterrows():
            r_cliente = row.get('Mayor_riesgo_clientes', 0)
            r_proveedor = row.get('Mayor_riesgo_proveedores', 0)
            r_empleado = 5 if str(row.get('Mayor_riesgo_empleados', '')).upper() == 'ALTO' else 0
            if max(r_cliente, r_proveedor, r_empleado) >= 4:
                alto_riesgo_sin_form += 1
        
        return {
            "total": total,
            "con_formulario": int(con_formulario),
            "sin_formulario": int(sin_formulario),
            "porcentaje_completado": round((con_formulario / total * 100) if total > 0 else 0.0, 1),
            "alto_riesgo_sin_formulario": alto_riesgo_sin_form
        }
