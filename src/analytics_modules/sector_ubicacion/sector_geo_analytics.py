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
        """Generate map data for Colombia (only ALTO risk)."""
        df_all_transactions = self.df
        if df_all_transactions.empty:
            return []
        mapa_data = []
        for _, row in df_all_transactions.iterrows():
            riesgo = str(row.get('riesgo', '')).upper()
            if riesgo != 'ALTO':
                continue
            mapa_data.append({
                "lat": row.get('lat', 4.5709),
                "lon": row.get('lon', -74.2973),
                "monto": float(row.get('monto', row.get('valor_transaccion', 0))),
                "contraparte": row.get('nombre', row.get('id_contraparte', 'Unknown')),
                "riesgo": riesgo
            })
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
