# src/analytics_modules/sector_ubicacion/sector_geo_analytics.py
import pandas as pd
from typing import Dict, Any, List

class SectorGeoAnalytics:

    def __init__(self, df_transacciones: pd.DataFrame, df_fatf: pd.DataFrame):
        self.df = df_transacciones
        self.df_fatf = df_fatf

    # ---------------------------------------------------------
    # 1. KPIs
    # ---------------------------------------------------------
    def get_kpis(self) -> Dict[str, Any]:
        """Calculate KPIs from transaction data."""
        # Process ALL transactions, not just high risk ones
        df_alto = self.df
        
        return {
            "total_transacciones": len(df_alto),
            "empresas_involucradas": df_alto.get('empresa', df_alto.get('id_empresa', pd.Series([]))).nunique() if not df_alto.empty else 0,
            "monto_total": float(df_alto.get('monto', df_alto.get('valor_transaccion', pd.Series([0]))).sum()) if not df_alto.empty else 0
        }

    # ---------------------------------------------------------
    # 2. MAPA COLOMBIA
    # ---------------------------------------------------------
    def get_mapa_colombia(self) -> List[Dict[str, Any]]:
        """Generate map data for Colombia (Risk Only: ALTO/MEDIO)."""
        df_all_transactions = self.df
        if df_all_transactions.empty:
            return []
            
        mapa_data = []
        
        # Filter for risks only
        # Normalizamos a mayúsculas para comparar
        riesgos_permitidos = ['ALTO', 'MEDIO', 'HIGH', 'MEDIUM']
        
        # Iteramos pero limitamos para evitar colapsar el navegador con 500k puntos
        count = 0
        limit = 1000
        
        # Ordenamos por riesgo (prioridad a ALTO) si es posible, o por monto descendente
        # Asumiendo que tenemos 'riesgo' y 'monto'
        try:
            # Crear copia para no afectar original
            df_sorted = df_all_transactions.copy()
            # Asegurar columna monto como float
            col_monto = 'monto' if 'monto' in df_sorted.columns else 'valor_transaccion'
            if col_monto in df_sorted.columns:
                df_sorted[col_monto] = pd.to_numeric(df_sorted[col_monto], errors='coerce').fillna(0)
                df_sorted = df_sorted.sort_values(by=col_monto, ascending=False)
        except:
            df_sorted = df_all_transactions

        for _, row in df_sorted.iterrows():
            if count >= limit:
                break
                
            riesgo = str(row.get('riesgo', '')).upper()
            
            # FILTRO: Solo agregar si es ALTO o MEDIO
            if riesgo not in riesgos_permitidos:
                continue

            mapa_data.append({
                "lat": row.get('lat', 4.5709),
                "lon": row.get('lon', -74.2973),
                "monto": float(row.get('monto', row.get('valor_transaccion', 0))),
                "contraparte": row.get('nombre', row.get('id_contraparte', 'Unknown')),
                "riesgo": riesgo
            })
            count += 1
            
        return mapa_data

    # ---------------------------------------------------------
    # 3. MAPA FATF / GAFI
    # ---------------------------------------------------------
    def get_fatf_status(self) -> Dict[str, str]:
        result = {}
        for _, row in self.df_fatf.iterrows():
            pais = str(row["pais"]).strip().upper()
            estatus = str(row["estatus"]).strip().upper()
            result[pais] = estatus
        return result
