import json
import os
import urllib.request
import unicodedata
from typing import Dict, List, Tuple

from ..common import write_json, load_csv_rows
from .sector_geo_analytics import (
    compute_sector_ubicacion_analytics,
)


# URLs GeoJSON
COLOMBIA_DEPARTAMENTOS_URL = (
    "https://raw.githubusercontent.com/johnguerra/colombia-geojson/master/colombia.json"
)
WORLD_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
)
# src/analytics_modules/sector_geo/graph_generator.py
import pandas as pd
from typing import Dict, Any, List


class GraphGenerator:
    def __init__(self, df_transacciones: pd.DataFrame):
        """
        df_transacciones debe tener:
        ['id','empresa','nit','ciiu','actividad','departamento','monto','riesgo']
        """
        self.df = df_transacciones
        self.alto = df_transacciones[df_transacciones["riesgo"].str.upper() == "ALTO"]

    # ---------------------------------------------------------
    # 1. Dataset para Doughnut CIIU
    # ---------------------------------------------------------
    def get_donut_ciiu(self) -> Dict[str, Any]:
        group = self.alto.groupby("actividad")["monto"].sum()

        return {
            "labels": group.index.tolist(),
            "values": group.values.tolist()
        }

    # ---------------------------------------------------------
    # 2. Tabla detallada (para DataTables)
    # ---------------------------------------------------------
    def get_tabla_detalle(self) -> List[Dict[str, Any]]:
        return self.alto.to_dict(orient="records")

    # ---------------------------------------------------------
    # 3. Resumen por CIIU
    # ---------------------------------------------------------
    def resumen_por_ciiu(self) -> List[Dict[str, Any]]:
        group = (
            self.alto.groupby(["ciiu", "actividad"])["monto"]
            .sum()
            .reset_index()
        )
        return group.to_dict(orient="records")
