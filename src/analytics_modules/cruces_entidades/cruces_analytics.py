# src/analytics_modules/cruces_entidades/cruces_analytics.py
"""
Módulo para detectar cruces de entidades (conflictos de interés)
cuando una misma persona aparece en múltiples roles
"""
import pandas as pd
from typing import Dict, Any, List, Optional


class CrucesAnalytics:
    """Analiza cruces entre clientes, proveedores y empleados"""
    
    def __init__(self, df_clientes: pd.DataFrame, df_proveedores: pd.DataFrame, df_empleados: pd.DataFrame, df_formularios: pd.DataFrame | None = None):
        self.df_clientes = df_clientes
        self.df_proveedores = df_proveedores
        self.df_empleados = df_empleados
        self.df_formularios = df_formularios
        self.df_cruces = None
        
    def _ensure_columns(self, df: pd.DataFrame, id_col_options: List[str], val_col_options: List[str], risk_col_options: List[str], 
                       name_col_options: List[str] = [], pay_method_options: List[str] = [], risk_detail_options: List[str] = [],
                       trans_id_options: List[str] = [], date_options: List[str] = [], 
                       risk_columns_to_capture: List[str] = [], actividad_options: List[str] = []) -> pd.DataFrame:
        """Normaliza columnas para el análisis"""
        if df.empty:
            return pd.DataFrame(columns=['id_empresa', 'id_contraparte', 'valor_suma', 'riesgo', 'nombre', 'medio_pago', 'riesgo_detalle', 'id_transaccion', 'fecha_transaccion', 'actividad'])
        df = df.copy()
        
        # Helper to find first existing column (case insensitive)
        def get_col(options):
            df_cols_lower = {str(c).lower(): c for c in df.columns}
            for col in options:
                if str(col).lower() in df_cols_lower:
                    return df_cols_lower[str(col).lower()]
            return None

        # 1. Map risk factors to internal standardized 'risk_' columns
        risk_factors_map = {
            'categoria_riesgo_pais': ['categoria_riesgo_pais'],
            'categoria_riesgo_ciiu': ['categoria_riesgo_ciiu'],
            'categoria_riesgo_tipo_persona': ['categoria_riesgo_tipo_persona'],
            'categoria_riesgo_montos': ['categoria_riesgo_montos'],
            'categoria_riesgo_medio_pago': ['categoria_riesgo_medio_pago'],
            'categoria_riesgo_valor_mas_10pct': ['categoria_riesgo_valor_mas_10pct', 'val_tx_mas_10_porciento'],
            'categoria_riesgo_relacion': ['categoria_riesgo_relacion', 'tipo_de_relacion_contratista_proveedor'],
            'categoria_riesgo_localizacion': ['categoria_riesgo_localizacion', 'localizacion_nacional_internacional'],
            'categoria_riesgo_canal_distribucion': ['categoria_riesgo_canal_distribucion', 'canal_distribucion'],
            'categoria_riesgo_medio_venta': ['categoria_riesgo_medio_venta', 'medio_venta'],
            'criterio_sueldo_mas_20pct': ['criterio_sueldo_mas_20pct', 'tx_hist_mas_20pct'],
            'criterio_viaticos': ['criterio_viaticos'],
            'criterio_comisiones': ['criterio_comisiones'],
            'criterio_bonificaciones': ['criterio_bonificaciones'],
            'criterio_otros_pagos': ['criterio_otros_pagos'],
            'criterio_incentivos': ['criterio_incentivos'],
            'criterio_premios': ['criterio_premios'],
            'criterio_prestaciones_sociales': ['criterio_prestaciones_sociales'],
            'cargo': ['cargo', 'area_cargo'],
            'segmentacion_empleado': ['segmentacion_empleado_tx_his_prom_hist'],
            'conteo_alto': ['conteo_alto', 'conteo_alto_extremo', 'lista_conteo_alto_extremo'],
            'puntaje_riesgo_total': ['puntaje_riesgo_total', 'nivel_riesgo']
        }

        # 2. Add special mappings
        special_risk_map = {
            'pais': ['pais', 'pais_estandar'],
            'categoria_jurisdicciones': ['categoria_jurisdicciones', 'valor_jurisdicciones']
        }

        # 3. Apply mappings
        for risk_key, column_options in {**risk_factors_map, **special_risk_map}.items():
            real_col = get_col(column_options)
            if real_col:
                df[f'risk_{risk_key}'] = df[real_col]
            else:
                df[f'risk_{risk_key}'] = 'N/A'

        # 4. Standard Column Normalization
        # ID (Always rename or create copy)
        real_id_col = get_col(id_col_options)
        if real_id_col:
            df['id_contraparte'] = df[real_id_col]
        elif 'id_contraparte' not in df.columns:
            df['id_contraparte'] = 'UNKNOWN'
            
        # 2. Valor (Suma)
        col = get_col(val_col_options)
        if col:
            df['valor_suma'] = df[col]
        elif 'valor_suma' not in df.columns:
            df['valor_suma'] = 0.0
            
        # 3. Riesgo
        col = get_col(risk_col_options)
        if col:
            df['riesgo'] = df[col]
        elif 'riesgo' not in df.columns:
            df['riesgo'] = 0
            
        # 4. Nombre
        col = get_col(name_col_options)
        if col:
            df['nombre'] = df[col]
        elif 'nombre' not in df.columns:
            df['nombre'] = None # Allow NaNs so aggregation can find real names
            
        # 5. Medio de Pago
        col = get_col(pay_method_options)
        if col:
            df['medio_pago'] = df[col]
        elif 'medio_pago' not in df.columns:
            df['medio_pago'] = None # Allow NaNs
            
        # 6. Detalle Riesgo (Legacy single column)
        col = get_col(risk_detail_options)
        if col:
            df['riesgo_detalle'] = df[col]
        elif 'riesgo_detalle' not in df.columns:
            df['riesgo_detalle'] = 'N/A'

        # 7. ID Transaccion
        col = get_col(trans_id_options)
        if col:
            df['id_transaccion'] = df[col]
        elif 'id_transaccion' not in df.columns:
            df['id_transaccion'] = 'N/A'
            
        # 8. Fecha Transaccion
        col = get_col(date_options)
        if col:
            df['fecha_transaccion'] = df[col]
        elif 'fecha_transaccion' not in df.columns:
            df['fecha_transaccion'] = 'N/A'
            
        # 9. Actividad / CIIU
        col = get_col(actividad_options)
        if col:
            df['actividad'] = df[col]
        elif 'actividad' not in df.columns:
            df['actividad'] = 'N/A'
            
        return df

    def procesar_datos(self) -> pd.DataFrame:
        """
        Procesa los datos según la lógica del notebook:
        - Agrupa por empresa e id_contraparte
        - Identifica entidades con >= 2 categorías
        - Filtra por alto riesgo
        """
        # Helper for name aggregation
        get_first_name = lambda x: x.dropna().iloc[0] if not x.dropna().empty else None
        
        # Helper for risk mapping
        def map_risk(val):
            if pd.isna(val): return 0
            s = str(val).strip().lower()
            if s in ['alto', 'high', '5']: return 5
            if s in ['medio', 'medium', '3']: return 3
            if s in ['bajo', 'low', '1']: return 1
            if s.isdigit(): return int(s)
            return 0

        # Risk columns to capture (Dynamic list covering all counterparty types)
        # We must define the mapping again or move it to a class level/shared function
        # For now, let's use a robust way to get all risk_ keys
        risk_cols = [
            'categoria_riesgo_pais', 'categoria_riesgo_ciiu', 'categoria_riesgo_tipo_persona',
            'categoria_riesgo_montos', 'categoria_riesgo_medio_pago', 'categoria_riesgo_valor_mas_10pct',
            'categoria_riesgo_relacion', 'categoria_riesgo_localizacion', 'categoria_riesgo_canal_distribucion',
            'categoria_riesgo_medio_venta', 'criterio_sueldo_mas_20pct', 'criterio_viaticos',
            'criterio_comisiones', 'criterio_bonificaciones', 'criterio_otros_pagos',
            'criterio_incentivos', 'criterio_premios', 'criterio_prestaciones_sociales',
            'cargo', 'segmentacion_empleado', 'conteo_alto', 'puntaje_riesgo_total',
            'pais', 'categoria_jurisdicciones'
        ]

        actividad_opts = ['actividad', 'ciiu_descripcion', 'descripcion_ciiu', 'descripcion_actividad', 'oficio', 'profesion', 'cargo']

        # 1. Agregar Clientes
        df_cli_norm = self._ensure_columns(
            self.df_clientes, 
            ['num_id', 'nit', 'id', 'cedula', 'identificacion', 'numero_id', 'numero_documento', 'cliente_id', 'nit_cliente'], 
            ['valor_transaccion', 'valor', 'monto'], 
            ['orden_clasificacion_del_riesgo', 'riesgo', 'nivel_riesgo', 'categoria_jurisdicciones'],
            name_col_options=['nombre', 'razon_social', 'nombre_completo', 'nombres', 'apellidos', 'cliente', 'tercero', 'titular', 'beneficiario', 'nombre_razon_social', 'descripcion', 'destinatario', 'participante', 'sujeto', 'nombre_comercial'],
            pay_method_options=['medio_pago', 'metodo_pago', 'forma_pago', 'forma_de_pago', 'detalle_pago', 'producto'],
            risk_detail_options=['senal_alerta', 'descripcion_riesgo', 'causa_riesgo', 'tipo_riesgo', 'nivel_riesgo', 'orden_clasificacion_del_riesgo', 'puntaje_riesgo_total'],
            trans_id_options=['id', 'transaccion_id', 'id_transaccion', 'numero_transaccion', 'consecutivo', 'id_tx', 'id_movimiento', 'doc_referencia', 'num_doc', 'referencia', 'documento', 'comprobante', 'numero_documento', 'nro_doc', 'tx_id'],
            date_options=['fecha_transaccion', 'fecha', 'created_at', 'fecha_registro', 'fecha_movimiento', 'date', 'timestamp', 'fecha_doc', 'fec_mov', 'fecha_corte', 'fecha_operacion', 'fec_doc', 'fecha_mvto'],
            risk_columns_to_capture=risk_cols,
            actividad_options=actividad_opts
        )
        
        # Asegurar tipo string explícitamente antes del groupby
        if 'id_contraparte' in df_cli_norm.columns:
            df_cli_norm['id_contraparte'] = df_cli_norm['id_contraparte'].astype(str)
        
        # Map risk to numeric
        if 'riesgo' in df_cli_norm.columns:
            df_cli_norm['riesgo'] = df_cli_norm['riesgo'].apply(map_risk)

        # Build aggregation dict dynamically for risk columns
        agg_dict_cli = {
            'cantidad_clientes': ('id_contraparte', 'size'),
            'suma_clientes': ('valor_suma', 'sum'),
            'lista_clientes': ('valor_suma', list),
            'Mayor_riesgo_clientes': ('riesgo', 'max'),
            'nombre_cliente': ('nombre', get_first_name),
            'lista_medios_pago_clientes': ('medio_pago', list),
            'lista_riesgos_detalle_clientes': ('riesgo_detalle', list),
            'lista_ids_transaccion_clientes': ('id_transaccion', list),
            'lista_fechas_transaccion_clientes': ('fecha_transaccion', list),
            'lista_actividad_clientes': ('actividad', list)
        }
        for rc in risk_cols:
            agg_dict_cli[f'lista_{rc}_clientes'] = (f'risk_{rc}', list)

        df_clientes_agg = (
            df_cli_norm
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(**agg_dict_cli)
        )
        
        # 2. Agregar Proveedores
        df_pro_norm = self._ensure_columns(
            self.df_proveedores,
            ['no_documento_de_identidad', 'nit_proveedor', 'nit', 'cedula', 'identificacion', 'numero_id', 'numero_documento', 'proveedor_id', 'nit_beneficiario', 'id'],
            ['valor_transaccion', 'valor', 'monto'], 
            ['orden_clasificacion_del_riesgo', 'riesgo', 'nivel_riesgo', 'categoria_jurisdicciones'],
            name_col_options=['nombre', 'razon_social', 'nombre_completo', 'nombres', 'apellidos', 'proveedor', 'tercero', 'titular', 'beneficiario', 'nombre_razon_social', 'descripcion', 'destinatario', 'participante', 'sujeto', 'nombre_comercial'],
            pay_method_options=['medio_pago', 'metodo_pago', 'forma_pago', 'forma_de_pago', 'detalle_pago', 'producto'],
            risk_detail_options=['senal_alerta', 'descripcion_riesgo', 'causa_riesgo', 'tipo_riesgo', 'nivel_riesgo', 'orden_clasificacion_del_riesgo', 'puntaje_riesgo_total'],
            trans_id_options=['transaccion_id', 'id_transaccion', 'numero_transaccion', 'consecutivo', 'id_tx', 'id_movimiento', 'doc_referencia', 'num_doc', 'referencia', 'documento', 'comprobante', 'numero_documento', 'nro_doc', 'tx_id', 'id'],
            date_options=['fecha_transaccion', 'fecha', 'created_at', 'fecha_registro', 'fecha_movimiento', 'date', 'timestamp', 'fecha_doc', 'fec_mov', 'fecha_corte', 'fecha_operacion', 'fec_doc', 'fecha_mvto'],
            risk_columns_to_capture=risk_cols,
            actividad_options=actividad_opts
        )
        
        # Asegurar tipo string explícitamente
        if 'id_contraparte' in df_pro_norm.columns:
            df_pro_norm['id_contraparte'] = df_pro_norm['id_contraparte'].astype(str)

        # Map risk to numeric
        if 'riesgo' in df_pro_norm.columns:
            df_pro_norm['riesgo'] = df_pro_norm['riesgo'].apply(map_risk)

        agg_dict_pro = {
            'cantidad_proveedores': ('id_contraparte', 'size'),
            'suma_proveedores': ('valor_suma', 'sum'),
            'lista_proveedores': ('valor_suma', list),
            'Mayor_riesgo_proveedores': ('riesgo', 'max'),
            'nombre_proveedor': ('nombre', get_first_name),
            'lista_medios_pago_proveedores': ('medio_pago', list),
            'lista_riesgos_detalle_proveedores': ('riesgo_detalle', list),
            'lista_ids_transaccion_proveedores': ('id_transaccion', list),
            'lista_fechas_transaccion_proveedores': ('fecha_transaccion', list),
            'lista_actividad_proveedores': ('actividad', list)
        }
        for rc in risk_cols:
            col_name = f'risk_{rc}'
            if col_name in df_pro_norm.columns:
                agg_dict_pro[f'lista_{rc}_proveedores'] = (col_name, list)

        df_proveedores_agg = (
            df_pro_norm
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(**agg_dict_pro)
        )
        
        # 3. Agregar Empleados
        df_emp_norm = self._ensure_columns(
            self.df_empleados,
            ['id_empleado', 'cedula_empleado', 'cedula', 'identificacion', 'numero_id', 'numero_documento', 'empleado_id', 'nit', 'documento_identidad', 'id'],
            ['valor', 'valor_transaccion', 'monto', 'salario'],
            ['conteo_alto', 'riesgo', 'nivel_riesgo', 'categoria_jurisdicciones'],
            name_col_options=['empleado', 'nombre', 'nombres_apellidos', 'nombre_completo', 'nombres', 'apellidos', 'tercero', 'titular', 'beneficiario', 'nombre_razon_social', 'descripcion', 'destinatario', 'participante', 'sujeto', 'nombre_comercial'],
            pay_method_options=['cat_concep_pago', 'concepto_pago', 'tipo_pago', 'medio_pago', 'forma_pago', 'detalle_pago', 'metodo_pago', 'forma_de_pago', 'producto'],
            risk_detail_options=['senal_alerta', 'descripcion_riesgo', 'causa_riesgo', 'tipo_riesgo', 'conteo_alto', 'categoria_jurisdicciones', 'nivel_riesgo', 'puntaje_riesgo_total'],
            trans_id_options=['transaccion_id', 'id_transaccion', 'numero_transaccion', 'consecutivo', 'id_tx', 'id_movimiento', 'doc_referencia', 'num_doc', 'referencia', 'documento', 'comprobante', 'numero_documento', 'nro_doc', 'tx_id', 'id'],
            date_options=['fecha_transaccion', 'fecha', 'created_at', 'fecha_registro', 'fecha_movimiento', 'date', 'timestamp', 'fecha_doc', 'fec_mov', 'fecha_corte', 'fecha_operacion', 'fec_doc', 'fecha_mvto'],
            risk_columns_to_capture=risk_cols,
            actividad_options=actividad_opts
        )
        
        # Asegurar tipo string explícitamente
        if 'id_contraparte' in df_emp_norm.columns:
            df_emp_norm['id_contraparte'] = df_emp_norm['id_contraparte'].astype(str)

        # Map risk to numeric
        if 'riesgo' in df_emp_norm.columns:
            df_emp_norm['riesgo'] = df_emp_norm['riesgo'].apply(map_risk)

        agg_dict_emp = {
            'cantidad_empleados': ('id_contraparte', 'size'),
            'suma_empleados': ('valor_suma', 'sum'),
            'lista_empleados': ('valor_suma', list),
            'Mayor_riesgo_empleados': ('riesgo', 'max'),
            'nombre_empleado': ('nombre', get_first_name),
            'lista_medios_pago_empleados': ('medio_pago', list),
            'lista_riesgos_detalle_empleados': ('riesgo_detalle', list),
            'lista_ids_transaccion_empleados': ('id_transaccion', list),
            'lista_fechas_transaccion_empleados': ('fecha_transaccion', list),
            'lista_actividad_empleados': ('actividad', list)
        }
        for rc in risk_cols:
            col_name = f'risk_{rc}'
            if col_name in df_emp_norm.columns:
                agg_dict_emp[f'lista_{rc}_empleados'] = (col_name, list)

        df_empleados_agg = (
            df_emp_norm
            .groupby(['id_empresa', 'id_contraparte'])
            .agg(**agg_dict_emp)
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
        
        # --- NUEVA LÓGICA: Verificar formularios para TODOS antes de filtrar cruces ---
        df_resumen = df_resumen.reset_index()
        if self.df_formularios is not None and not self.df_formularios.empty:
            df_resumen = self._verificar_formularios(df_resumen)
        else:
            df_resumen['tiene_formulario'] = False
            df_resumen['fecha_formulario'] = None

        # Guardamos el resumen completo (con o sin cruces) para detectar falta de DD global
        self.df_universo_resumen = df_resumen.copy()

        # Filtrar contrapartes con al menos 2 categorías para la analítica de cruces
        df_filtrado = df_resumen[df_resumen['conteo_categorias'] >= 2].copy()
        columnas_suma = [col for col in df_filtrado.columns if 'suma' in col]
        df_filtrado[columnas_suma] = df_filtrado[columnas_suma].fillna(0)
        
        self.df_cruces = df_filtrado
        return df_filtrado
    
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
        def get_ids(df):
            if df is None or df.empty: return set()
            if 'id_contraparte' in df.columns:
                return set(df['id_contraparte'].astype(str))
            # Fallback a columnas comunes de ID
            id_opts = ['num_id', 'nit', 'id', 'cedula', 'identificacion', 'numero_id', 'numero_documento', 'cliente_id', 'nit_cliente', 'no_documento_de_identidad', 'nit_proveedor']
            for col in id_opts:
                if col in df.columns:
                    return set(df[col].astype(str))
            return set()

        # Total de registros únicos analizados (Unión de todas las categorías)
        ids_clientes = get_ids(self.df_clientes)
        ids_proveedores = get_ids(self.df_proveedores)
        ids_empleados = get_ids(self.df_empleados)
        
        total_universo = len(ids_clientes | ids_proveedores | ids_empleados)

        if self.df_cruces is None or self.df_cruces.empty:
            return {
                "total_registros": total_universo,
                "entidades_cruces": 0,
                "porcentaje_cruces": 0.0,
                "riesgo_promedio": 0.0,
                "alto_riesgo_count": 0,
                "valor_total_riesgo": 0.0
            }
        
        df = self.df_cruces
        
        # Calcular riesgo promedio numérico
        riesgos = []
        
        def parse_r(v):
            if pd.isna(v): return 0
            if isinstance(v, (int, float)): return int(v)
            s = str(v).strip().upper()
            if s in ['ALTO', 'HIGH']: return 5
            if s in ['MEDIO', 'MEDIUM']: return 3
            if s in ['BAJO', 'LOW']: return 1
            try: return int(float(s))
            except: return 0

        for _, row in df.iterrows():
            r_cliente = parse_r(row.get('Mayor_riesgo_clientes'))
            r_proveedor = parse_r(row.get('Mayor_riesgo_proveedores'))
            r_empleado = parse_r(row.get('Mayor_riesgo_empleados'))
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
        
        entidades_con_cruces = df['id_contraparte'].nunique()
        porcentaje_cruces = (entidades_con_cruces / total_universo * 100) if total_universo > 0 else 0.0

        return {
            "total_registros": total_universo,
            "entidades_cruces": entidades_con_cruces,
            "porcentaje_cruces": round(porcentaje_cruces, 2),
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
        
        def parse_r(v):
            if pd.isna(v): return 0
            if isinstance(v, (int, float)): return int(v)
            s = str(v).strip().upper()
            if s in ['ALTO', 'HIGH']: return 5
            if s in ['MEDIO', 'MEDIUM']: return 3
            if s in ['BAJO', 'LOW']: return 1
            try: return int(float(s))
            except: return 0

        for _, row in self.df_cruces.iterrows():
            r_cliente = parse_r(row.get('Mayor_riesgo_clientes'))
            r_proveedor = parse_r(row.get('Mayor_riesgo_proveedores'))
            r_empleado = parse_r(row.get('Mayor_riesgo_empleados'))
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
    
    def get_missing_dd_report(self) -> List[Dict[str, Any]]:
        """
        Obtiene el reporte global de contrapartes sin debida diligencia,
        incluso si no tienen cruces de categorías.
        """
        if not hasattr(self, 'df_universo_resumen') or self.df_universo_resumen is None:
            return []
            
        df = self.df_universo_resumen.copy()
        
        # Filtrar solo las que NO tienen formulario
        df_missing = df[~df['tiene_formulario']].copy()
        
        reporte = []
        for _, row in df_missing.iterrows():
            # Determinar Nombre para mostrar
            nombre_mostrar = f"ID: {row['id_contraparte']}"
            if pd.notna(row.get('nombre_empleado')) and str(row.get('nombre_empleado')) not in ['nan', 'None', '']:
                nombre_mostrar = str(row.get('nombre_empleado'))
            elif pd.notna(row.get('nombre_cliente')) and str(row.get('nombre_cliente')) not in ['nan', 'None', '']:
                nombre_mostrar = str(row.get('nombre_cliente'))
            elif pd.notna(row.get('nombre_proveedor')) and str(row.get('nombre_proveedor')) not in ['nan', 'None', '']:
                nombre_mostrar = str(row.get('nombre_proveedor'))
                
            reporte.append({
                "id_empresa": int(row['id_empresa']),
                "id_contraparte": str(row['id_contraparte']),
                "nombre": nombre_mostrar,
                "tiene_cruces": bool(row['conteo_categorias'] >= 2),
                "conteo_categorias": int(row['conteo_categorias']),
                "riesgo_max": max(
                    row.get('Mayor_riesgo_clientes', 0),
                    row.get('Mayor_riesgo_proveedores', 0),
                    # Para empleados ya vimos que puede ser texto o número
                    self._parse_risk_value(row.get('Mayor_riesgo_empleados', 0))
                )
            })
            
        return sorted(reporte, key=lambda x: (-x["riesgo_max"], -x["conteo_categorias"]))

    def _parse_risk_value(self, val) -> int:
        """Helper para convertir valores de riesgo a numérico"""
        if pd.isna(val) or val is None: return 0
        s = str(val).upper()
        if s in ['ALTO', 'HIGH']: return 5
        if s in ['MEDIO', 'MEDIUM']: return 3
        if s in ['BAJO', 'LOW', 'ACEPTABLE']: return 1
        try:
            return int(float(val))
        except:
            return 0

    def get_tabla_detalles(self, empresa_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Tabla de detalles de entidades con cruces"""
        if self.df_cruces is None or self.df_cruces.empty:
            return []
        
        df = self.df_cruces
        if empresa_id is not None:
            df = df[df['id_empresa'] == empresa_id]
        
        tabla = []
        for _, row in df.iterrows():
            # Helper para parsear riesgo a int
            def parse_risk_val(val):
                if pd.isna(val): return 0
                s = str(val).upper()
                if s in ['ALTO', 'HIGH', 'CRITICO']: return 5
                if s in ['MEDIO', 'MEDIUM']: return 3
                if s in ['BAJO', 'LOW']: return 1
                try:
                    return int(float(val))
                except:
                    return 0

            # Determinar riesgo máximo (Normalizado a Enteros)
            r_cliente_raw = row.get('Mayor_riesgo_clientes', 0)
            r_cliente = parse_risk_val(r_cliente_raw)
            
            r_proveedor_raw = row.get('Mayor_riesgo_proveedores', 0)
            r_proveedor = parse_risk_val(r_proveedor_raw)
            
            r_empleado_raw = row.get('Mayor_riesgo_empleados', 0)
            r_empleado = parse_risk_val(r_empleado_raw)
            
            # Helper para formateo de riesgo y moneda (Label UI)
            def get_risk_props(val):
                # Si ya es int procesado (0-5)
                if isinstance(val, (int, float)):
                    if val >= 4: return 'danger', 'Alto'
                    if val >= 3: return 'warning', 'Medio'
                    return 'success', 'Bajo'
                
                # Fallback para strings crudos (por si acaso se usa en otro lado)
                v_str = str(val).upper()
                if v_str in ['ALTO', 'HIGH']: return 'danger', 'Alto'
                if v_str in ['MEDIO', 'MEDIUM']: return 'warning', 'Medio'
                return 'success', 'Bajo'

            def fmt_money(val):
                try:
                    return "${:,.0f}".format(float(val))
                except:
                    return "$0"

            # Construir info de cliente
            cant_cli = row.get('cantidad_clientes', 0)
            lista_cli = row.get('lista_clientes')
            if not isinstance(lista_cli, list): lista_cli = []
            
            r_c_class, r_c_label = get_risk_props(r_cliente)
            suma_cli = float(row.get('suma_clientes', 0) or 0)
            
            # Listas de detalles
            l_mp_cli = row.get('lista_medios_pago_clientes', [])
            if not isinstance(l_mp_cli, list): l_mp_cli = []
            l_rd_cli = row.get('lista_riesgos_detalle_clientes', [])
            if not isinstance(l_rd_cli, list): l_rd_cli = []
            l_ids_cli = row.get('lista_ids_transaccion_clientes', [])
            if not isinstance(l_ids_cli, list): l_ids_cli = []
            l_dates_cli = row.get('lista_fechas_transaccion_clientes', [])
            if not isinstance(l_dates_cli, list): l_dates_cli = []
            l_act_cli = row.get('lista_actividad_clientes', [])
            if not isinstance(l_act_cli, list): l_act_cli = []

            # Prepare lists for JSON, handling potential NaNs
            def clean_list(lst):
                if not isinstance(lst, list):
                    return []
                return [str(x) if pd.notna(x) and str(x).strip().lower() not in ['nan', 'none', 'nat', 'n/a', 'unknown', ''] else "" for x in lst]

            def clean_payment_list(lst):
                if not isinstance(lst, list):
                    return []
                return [str(x) if pd.notna(x) and str(x).strip().lower() not in ['nan', 'none', 'nat', 'n/a', 'unknown', 'desconocido', ''] else "" for x in lst]

            # Risk factor mapping for all counterparty types
            risk_factors_map = {
                'pais': ['lista_categoria_riesgo_pais', 'lista_pais'],
                'ciiu': 'lista_categoria_riesgo_ciiu',
                'tipo_persona': 'lista_categoria_riesgo_tipo_persona',
                'montos': 'lista_categoria_riesgo_montos',
                'medio_pago': 'lista_categoria_riesgo_medio_pago',
                'valor_10pct': ['lista_categoria_riesgo_valor_mas_10pct', 'lista_val_tx_mas_10_porciento'],
                'relacion': ['lista_categoria_riesgo_relacion', 'lista_tipo_de_relacion_contratista_proveedor'],
                'localizacion': ['lista_categoria_riesgo_localizacion', 'lista_localizacion_nacional_internacional'],
                'jurisdicciones': 'lista_categoria_jurisdicciones',
                'canal': ['lista_categoria_riesgo_canal_distribucion', 'lista_canal_distribucion'],
                'medio_venta': 'lista_categoria_riesgo_medio_venta',
                'sueldo_20pct': ['lista_criterio_sueldo_mas_20pct', 'lista_tx_hist_mas_20pct'],
                'viaticos': 'lista_criterio_viaticos',
                'comisiones': 'lista_criterio_comisiones',
                'bonificaciones': 'lista_criterio_bonificaciones',
                'otros_pagos': 'lista_criterio_otros_pagos',
                'incentivos': 'lista_criterio_incentivos',
                'premios': 'lista_criterio_premios',
                'prestaciones': 'lista_criterio_prestaciones_sociales',
                'conteo_alto': ['lista_conteo_alto_extremo', 'lista_conteo_alto'],
                'puntaje_riesgo': ['lista_puntaje_riesgo_total', 'lista_puntaje_riesgo'],
                'nivel_riesgo': 'lista_nivel_riesgo'
            }

            def build_trans_detalles(count, lista_montos, lista_medios, lista_ids, lista_fechas, lista_actividades, rf_map, row, suffix):
                details = []
                for i in range(int(count)):
                    item = {
                        "monto": lista_montos[i] if i < len(lista_montos) else 0,
                        "medio": lista_medios[i] if i < len(lista_medios) else "N/A",
                        "id": lista_ids[i] if i < len(lista_ids) else "N/A",
                        "fecha": str(lista_fechas[i]) if i < len(lista_fechas) else "N/A",
                        "actividad": lista_actividades[i] if i < len(lista_actividades) else "N/A",
                    }
                    for key, list_keys in rf_map.items():
                        # Handle multiple potential list keys (e.g. for conteo_alto)
                        if isinstance(list_keys, str):
                            list_keys = [list_keys]
                        
                        val = "N/A"
                        for l_key in list_keys:
                            full_list_key = f"{l_key}_{suffix}"
                            lst = row.get(full_list_key, [])
                            if i < len(lst) and pd.notna(lst[i]):
                                val = lst[i]
                                if str(val).strip().lower() not in ['nan', 'none', 'n/a', '']:
                                    break
                        item[key] = str(val) if pd.notna(val) else "N/A"
                    details.append(item)
                return details

            # Helper to get first non-empty risk value from a list
            def get_first_risk(lst):
                if not isinstance(lst, list): return "N/A"
                for x in lst:
                    s = str(x).strip()
                    if pd.notna(x) and s.lower() not in ['nan', 'none', 'n/a', '']:
                        return s
                return "N/A"

            def get_first_risk_multi(row, list_keys, suffix):
                if isinstance(list_keys, str):
                    list_keys = [list_keys]
                for l_key in list_keys:
                    full_key = f"{l_key}_{suffix}"
                    lst = row.get(full_key, [])
                    val = get_first_risk(lst)
                    if val != "N/A":
                        return val
                return "N/A"

            cliente_risk_factors = {k: get_first_risk_multi(row, v, "clientes") for k, v in risk_factors_map.items()}
            proveedor_risk_factors = {k: get_first_risk_multi(row, v, "proveedores") for k, v in risk_factors_map.items()}
            empleado_risk_factors = {k: get_first_risk_multi(row, v, "empleados") for k, v in risk_factors_map.items()}

            # Consolidated risk factors (first non-null from any relationship)
            consolidated_rf = {}
            for k in risk_factors_map.keys():
                val = "N/A"
                for rf_dict in [cliente_risk_factors, proveedor_risk_factors, empleado_risk_factors]:
                    if rf_dict[k] != "N/A":
                        val = rf_dict[k]
                        break
                consolidated_rf[k] = val

            cliente_info = {
                "count": int(cant_cli) if pd.notna(cant_cli) else 0,
                "amount": fmt_money(suma_cli),
                "risk_class": r_c_class,
                "risk_label": r_c_label,
                "cantidad": int(cant_cli) if pd.notna(cant_cli) else 0,
                "suma": suma_cli,
                "riesgo": r_cliente,
                "transacciones": [float(x) for x in lista_cli if pd.notna(x)],
                "transacciones_detalles": build_trans_detalles(cant_cli, lista_cli, l_mp_cli, l_ids_cli, l_dates_cli, l_act_cli, risk_factors_map, row, "clientes"),
                "medios_pago": clean_payment_list(l_mp_cli),
                "riesgos_detalle": clean_list(l_rd_cli),
                "ids_transaccion": clean_list(l_ids_cli),
                "fechas_transaccion": clean_list(l_dates_cli),
                "actividades": clean_list(l_act_cli),
                "risk_factors": cliente_risk_factors
            }
            
            # Construir info de proveedor
            cant_prov = row.get('cantidad_proveedores', 0)
            r_p_class, r_p_label = get_risk_props(r_proveedor)
            suma_prov = float(row.get('suma_proveedores', 0) or 0)
            
            l_mp_pro = row.get('lista_medios_pago_proveedores', [])
            if not isinstance(l_mp_pro, list): l_mp_pro = []
            l_rd_pro = row.get('lista_riesgos_detalle_proveedores', [])
            if not isinstance(l_rd_pro, list): l_rd_pro = []
            l_ids_pro = row.get('lista_ids_transaccion_proveedores', [])
            if not isinstance(l_ids_pro, list): l_ids_pro = []
            l_dates_pro = row.get('lista_fechas_transaccion_proveedores', [])
            if not isinstance(l_dates_pro, list): l_dates_pro = []
            l_act_pro = row.get('lista_actividad_proveedores', [])
            if not isinstance(l_act_pro, list): l_act_pro = []

            proveedor_info = {
                "count": int(cant_prov) if pd.notna(cant_prov) else 0,
                "amount": fmt_money(suma_prov),
                "risk_class": r_p_class,
                "risk_label": r_p_label,
                "cantidad": int(cant_prov) if pd.notna(cant_prov) else 0,
                "suma": suma_prov,
                "riesgo": r_proveedor,
                "transacciones": [float(x) for x in (row.get('lista_proveedores') if isinstance(row.get('lista_proveedores'), list) else []) if pd.notna(x)],
                "transacciones_detalles": build_trans_detalles(cant_prov, row.get('lista_proveedores', []), l_mp_pro, l_ids_pro, l_dates_pro, l_act_pro, risk_factors_map, row, "proveedores"),
                "medios_pago": clean_payment_list(l_mp_pro),
                "riesgos_detalle": clean_list(l_rd_pro),
                "ids_transaccion": clean_list(l_ids_pro),
                "fechas_transaccion": clean_list(l_dates_pro),
                "actividades": clean_list(l_act_pro),
                "risk_factors": proveedor_risk_factors
            }

            # Construir info de empleado
            cant_emp = row.get('cantidad_empleados', 0)
            r_e_class, r_e_label = get_risk_props(r_empleado)
            suma_emp = float(row.get('suma_empleados', 0) or 0)
            
            l_mp_emp = row.get('lista_medios_pago_empleados', [])
            if not isinstance(l_mp_emp, list): l_mp_emp = []
            l_rd_emp = row.get('lista_riesgos_detalle_empleados', [])
            if not isinstance(l_rd_emp, list): l_rd_emp = []
            l_ids_emp = row.get('lista_ids_transaccion_empleados', [])
            if not isinstance(l_ids_emp, list): l_ids_emp = []
            l_dates_emp = row.get('lista_fechas_transaccion_empleados', [])
            if not isinstance(l_dates_emp, list): l_dates_emp = []
            l_act_emp = row.get('lista_actividad_empleados', [])
            if not isinstance(l_act_emp, list): l_act_emp = []
            if len(l_act_emp) == 0:
                l_act_emp = row.get('lista_actividad_clientes', []) # Fallback to client activity if missing
                if not isinstance(l_act_emp, list): l_act_emp = []

            # Obtener factores de riesgo específicos de empleados
            def get_emp_rf(key):
                # Intentar obtener de la lista específica de empleados
                lst = row.get(f'lista_{key}_empleados', [])
                if isinstance(lst, list) and len(lst) > 0:
                    val = get_first_risk(lst)
                    if val != "N/A": return val
                
                # Si no está en la lista, intentar del mapeo general (ya capturado en _ensure_columns)
                val = row.get(f'risk_{key}')
                if pd.notna(val) and str(val).strip().lower() not in ['nan', 'none', 'n/a', '']:
                    return str(val)
                
                return "N/A"

            empleado_risk_factors = {k: get_first_risk_multi(row, v, "empleados") for k, v in risk_factors_map.items()}
            
            # Forzar actualización de campos específicos de empleados si salieron N/A
            emp_specific_keys = ['sueldo_20pct', 'viaticos', 'comisiones', 'bonificaciones', 'otros_pagos', 'incentivos', 'premios', 'prestaciones']
            for k in emp_specific_keys:
                if empleado_risk_factors.get(k) == "N/A":
                    empleado_risk_factors[k] = get_emp_rf(risk_factors_map[k][0] if isinstance(risk_factors_map[k], list) else risk_factors_map[k])

            # Obtener lista de transacciones de empleados de forma segura
            lista_emp_raw = row.get('lista_empleados')
            lista_emp = lista_emp_raw if isinstance(lista_emp_raw, list) else []

            empleado_info = {
                "count": int(cant_emp) if pd.notna(cant_emp) else 0,
                "amount": fmt_money(suma_emp),
                "risk_class": r_e_class,
                "risk_label": r_e_label,
                "cantidad": int(cant_emp) if pd.notna(cant_emp) else 0,
                "suma": suma_emp,
                "riesgo": r_empleado,
                "transacciones": [float(x) for x in lista_emp if pd.notna(x)],
                "transacciones_detalles": build_trans_detalles(cant_emp, lista_emp, l_mp_emp, l_ids_emp, l_dates_emp, l_act_emp, risk_factors_map, row, "empleados"),
                "medios_pago": clean_payment_list(l_mp_emp),
                "riesgos_detalle": clean_list(l_rd_emp),
                "ids_transaccion": clean_list(l_ids_emp),
                "fechas_transaccion": clean_list(l_dates_emp),
                "actividades": clean_list(l_act_emp),
                "risk_factors": empleado_risk_factors
            }
            
            tiene_formulario = bool(row.get('tiene_formulario', False))
            fecha_formulario = row.get('fecha_formulario')
            
            # Determinar Nombre para mostrar
            nombre_mostrar = f"ID: {row['id_contraparte']}"
            # Prioridad: Empleado > Cliente > Proveedor
            if pd.notna(row.get('nombre_empleado')) and str(row.get('nombre_empleado')) not in ['nan', 'None', '']:
                nombre_mostrar = str(row.get('nombre_empleado'))
            elif pd.notna(row.get('nombre_cliente')) and str(row.get('nombre_cliente')) not in ['nan', 'None', '']:
                nombre_mostrar = str(row.get('nombre_cliente'))
            elif pd.notna(row.get('nombre_proveedor')) and str(row.get('nombre_proveedor')) not in ['nan', 'None', '']:
                nombre_mostrar = str(row.get('nombre_proveedor'))

            tabla.append({
                "id": str(row['id_contraparte']),
                "empresa": nombre_mostrar, # Nombre real en vez de ID
                "id_contraparte": str(row['id_contraparte']),
                "id_empresa": int(row.get('id_empresa', 0) or 0),
                "cruces_count": int(row.get('conteo_categorias', 0) or 0),
                "conteo_categorias": int(row.get('conteo_categorias', 0) or 0),
                "cliente": cliente_info,
                "proveedor": proveedor_info,
                "empleado": empleado_info,
                "risk_factors": consolidated_rf,
                "riesgo_maximo": max(r_cliente, r_proveedor, r_empleado),
                "dd": tiene_formulario,
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
            r_emp_raw = str(row.get('Mayor_riesgo_empleados', '')).upper()
            if r_emp_raw in ['ALTO', 'HIGH']: r_empleado = 5
            elif r_emp_raw in ['MEDIO', 'MEDIUM']: r_empleado = 3
            elif r_emp_raw.isdigit(): r_empleado = int(r_emp_raw)
            else: r_empleado = 0
            if max(r_cliente, r_proveedor, r_empleado) >= 4:
                alto_riesgo_sin_form += 1
        
        return {
            "total": total,
            "con_formulario": int(con_formulario),
            "sin_formulario": int(sin_formulario),
            "porcentaje_completado": round((con_formulario / total * 100) if total > 0 else 0.0, 1),
            "alto_riesgo_sin_formulario": alto_riesgo_sin_form
        }
