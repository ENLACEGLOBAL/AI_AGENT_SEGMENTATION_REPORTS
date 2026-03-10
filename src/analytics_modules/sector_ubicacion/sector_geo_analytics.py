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
        
        # Calculate alto_riesgo count based on multiple factors (same logic as map/distribution)
        alto_riesgo_count = 0
        
        if not df_alto.empty:
            for _, row in df_alto.iterrows():
                # Check all risk factors
                riesgo = str(row.get('riesgo', '')).upper()
                pais_riesgo = str(row.get('pais_riesgo', '')).upper()
                cat_jur = str(row.get('categoria_jurisdicciones', '')).upper()
                act_riesgo = str(row.get('ciiu_categoria', '')).upper()
                
                is_alto = False
                all_risks = [riesgo, pais_riesgo, cat_jur, act_riesgo]
                
                for r_val in all_risks:
                    if 'ALTO' in r_val or 'HIGH' in r_val or 'NO COOPERANTE' in r_val:
                        is_alto = True
                        break
                    elif r_val.isdigit():
                         try:
                             val = int(r_val)
                             if val >= 5: is_alto = True
                         except: pass
                
                if is_alto:
                    alto_riesgo_count += 1

        return {
            "total_transacciones": len(df_alto),
            "total_registros": len(df_alto), # Alias for frontend compatibility (PHP Dashboard)
            "empresas_involucradas": df_alto.get('id_contraparte', df_alto.get('nit', df_alto.get('num_id', pd.Series([])))).nunique() if not df_alto.empty else 0,
            "monto_total": float(df_alto.get('monto', df_alto.get('valor_transaccion', pd.Series([0]))).sum()) if not df_alto.empty else 0,
            "alto_riesgo": alto_riesgo_count
        }

    # ---------------------------------------------------------
    # 2. MAPA COLOMBIA
    # ---------------------------------------------------------
    
    # Centros aproximados de departamentos para fallback
    COLOMBIA_DEPT_COORDS = {
        "AMAZONAS": (-1.4429, -71.5724), "ANTIOQUIA": (6.9996, -75.4057), "ARAUCA": (6.6539, -71.2185),
        "ATLANTICO": (10.6696, -74.9658), "BOLIVAR": (8.6795, -74.0309), "BOYACA": (5.6300, -73.0698),
        "CALDAS": (5.3117, -75.3340), "CAQUETA": (1.0020, -74.0087), "CASANARE": (5.3615, -71.6105),
        "CAUCA": (2.3653, -76.8123), "CESAR": (9.3283, -73.6558), "CHOCO": (5.3218, -76.8437),
        "CORDOBA": (8.4116, -75.7699), "CUNDINAMARCA": (4.8938, -74.0163), "GUAINIA": (2.5854, -68.5247),
        "GUAVIARE": (1.8532, -72.0298), "HUILA": (2.5359, -75.4485), "LA GUAJIRA": (11.3548, -72.5205),
        "MAGDALENA": (10.3707, -74.1956), "META": (3.2719, -73.0877), "NARINO": (1.5645, -77.5872),
        "NORTE DE SANTANDER": (7.9463, -72.8988), "PUTUMAYO": (0.4359, -76.1264), "QUINDIO": (4.4619, -75.6668),
        "RISARALDA": (4.9961, -75.9260), "SAN ANDRES Y PROVIDENCIA": (12.5376, -81.7169), "SANTANDER": (6.6437, -73.3444),
        "SUCRE": (9.0768, -75.0503), "TOLIMA": (4.0925, -75.1545), "VALLE DEL CAUCA": (3.8009, -76.3659),
        "VAUPES": (0.6416, -70.7303), "VICHADA": (4.4371, -69.4533), "BOGOTA": (4.6097, -74.0817), "BOGOTA D.C.": (4.6097, -74.0817),
        "SIN_INFORMACION": (4.5709, -74.2973) # Default center
    }

    def get_mapa_colombia(self) -> List[Dict[str, Any]]:
        """
        Generate map data for Colombia (Risk Only: ALTO/MEDIO).
        Aggregates multiple transactions in the same location (lat/lon) to prevent stacking.
        """
        df_all_transactions = self.df
        if df_all_transactions.empty:
            return []
            
        # Dictionary to aggregate data by location: (lat, lon) -> data
        aggregated_data = {}
        
        # Filter for risks only
        for _, row in df_all_transactions.iterrows():
            # Check all risk factors
            riesgo = str(row.get('riesgo', '')).upper()
            pais_riesgo = str(row.get('pais_riesgo', '')).upper()
            cat_jur = str(row.get('categoria_jurisdicciones', '')).upper()
            act_riesgo = str(row.get('ciiu_categoria', '')).upper()
            
            # Aggregate all risks to check validity
            all_risks = [riesgo, pais_riesgo, cat_jur, act_riesgo]
            
            is_valid_risk = False
            is_alto = False
            is_medio = False
            
            for r_val in all_risks:
                if 'ALTO' in r_val or 'HIGH' in r_val or 'NO COOPERANTE' in r_val:
                    is_valid_risk = True
                    is_alto = True
                    break # Optimization: If Alto found, we know it's valid and Alto
                elif 'MEDIO' in r_val or 'MEDIUM' in r_val:
                    is_valid_risk = True
                    is_medio = True
                elif r_val.isdigit():
                    try:
                        val = int(r_val)
                        if val >= 5: # 5=Alto
                             is_valid_risk = True
                             is_alto = True
                        elif val >= 3: # 3=Medio
                             is_valid_risk = True
                             is_medio = True
                    except:
                        pass
            
            if not is_valid_risk:
                continue

            # Normalizar etiqueta de riesgo
            riesgo_out = 'MEDIO' # Default
            if is_alto:
                 riesgo_out = 'ALTO'

            # Coordinate Logic with Fallback
            lat = row.get('lat')
            lon = row.get('lon')
            
            # If lat/lon missing or zero, try Dept lookup
            if not lat or not lon:
                dept_name = str(row.get('departamento', '')).upper().strip()
                # Try standardizing common variations
                if dept_name == 'BOGOTA': dept_name = 'BOGOTA D.C.'
                
                if dept_name in self.COLOMBIA_DEPT_COORDS:
                    lat, lon = self.COLOMBIA_DEPT_COORDS[dept_name]
                else:
                    # Default to center if unknown department
                    lat, lon = (4.5709, -74.2973)
            
            # Ensure floats
            try:
                lat = float(lat)
                lon = float(lon)
            except:
                continue

            # Key for aggregation (rounded to avoid micro-misalignments)
            key = (round(lat, 4), round(lon, 4))
            
            monto = float(row.get('monto', row.get('valor_transaccion', 0)))
            nombre = str(row.get('nombre', row.get('id_contraparte', 'Unknown')))
            loc_name = f"{row.get('ciudad', '')} {row.get('departamento', '')}".strip()

            if key not in aggregated_data:
                aggregated_data[key] = {
                    "lat": lat,
                    "lon": lon,
                    "monto": 0.0,
                    "count": 0,
                    "riesgos": set(),
                    "empresas": set(),
                    "locations": set()
                }
            
            aggregated_data[key]["monto"] += monto
            aggregated_data[key]["count"] += 1
            aggregated_data[key]["riesgos"].add(riesgo_out)
            aggregated_data[key]["empresas"].add(nombre)
            if loc_name:
                aggregated_data[key]["locations"].add(loc_name)

        # Convert aggregated data to list
        mapa_data = []
        for key, data in aggregated_data.items():
            # Determine highest risk
            final_riesgo = 'MEDIO'
            if 'ALTO' in data['riesgos']:
                final_riesgo = 'ALTO'
            
            # Construct display name (e.g. "Bogota (5 empresas)")
            loc_str = ", ".join(list(data['locations'])[:2])
            if not loc_str: loc_str = "Ubicación Desconocida"
            
            if data['count'] > 1:
                name = f"{loc_str} ({data['count']} regs)"
                empresa_label = f"{data['count']} Empresas"
            else:
                name = loc_str
                empresa_label = list(data['empresas'])[0] if data['empresas'] else "Unknown"

            mapa_data.append({
                "lat": data["lat"],
                "lon": data["lon"],
                "coords": [data["lat"], data["lon"]],
                "monto": data["monto"],
                "contraparte": empresa_label,
                "empresa": empresa_label,
                "name": name,
                "riesgo": final_riesgo,
                "color": '#FF9800' if final_riesgo == 'MEDIO' else '#e63946',
                "count": data["count"]
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
