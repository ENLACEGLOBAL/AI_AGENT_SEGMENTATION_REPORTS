import json
import os
import io
import base64
import urllib.request
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, Any

COLOMBIA_DEPARTAMENTOS_URL = (
    "https://raw.githubusercontent.com/johnguerra/colombia-geojson/master/colombia.json"
)

WORLD_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
)


class GraphGenerator:

    def __init__(self, df_transacciones: pd.DataFrame):
        self.df = df_transacciones
        # self.alto no se usará exclusivamente para el gráfico general de CIIU

    # -----------------------------------------
    # 1. DATASET para el JSON
    # -----------------------------------------
    def get_donut_dataset(self) -> Dict[str, Any]:
        """
        Genera distribución por CIIU/Actividad usando TODAS las transacciones
        para dar contexto real de la operación de la empresa.
        """
        if self.df.empty:
            return {"labels": [], "values": []}

        # Intentar usar columna 'ciiu' o 'actividad'
        col_group = 'ciiu' if 'ciiu' in self.df.columns else 'actividad'
        if col_group not in self.df.columns:
            # Fallback si no existe ninguna
            return {"labels": ["Desconocido"], "values": [1]}

        # Agrupar por la columna detectada y sumar montos (o contar si no hay montos)
        col_monto = 'monto' if 'monto' in self.df.columns else 'valor_transaccion'
        
        if col_monto in self.df.columns:
            # Asegurar numérico
            self.df[col_monto] = pd.to_numeric(self.df[col_monto], errors='coerce').fillna(0)
            group = self.df.groupby(col_group)[col_monto].sum()
        else:
            group = self.df[col_group].value_counts()

        # Tomar top 10 para no saturar el gráfico
        group = group.sort_values(ascending=False).head(10)
        
        return {
            "labels": [str(x) for x in group.index],
            "values": [float(v) for v in group.values]
        }

    # -----------------------------------------
    # 2. Gráfico en Base64
    # -----------------------------------------
    def get_donut_base64(self) -> str:
        dataset = self.get_donut_dataset()

        plt.figure(figsize=(4, 4))
        plt.pie(
            dataset["values"],
            labels=dataset["labels"],
            autopct='%1.1f%%'
        )

        buffer = io.BytesIO()
        plt.savefig(buffer, format="png", bbox_inches="tight")
        plt.close()
        buffer.seek(0)

        base64_data = base64.b64encode(buffer.read()).decode("utf-8")
        return f"data:image/png;base64,{base64_data}"

    # -----------------------------------------
    # 3. Save chart to file
    # -----------------------------------------
    def save_donut_chart(self, filepath: str) -> str:
        """Save donut chart to file and return the filepath."""
        dataset = self.get_donut_dataset()

        plt.figure(figsize=(6, 6))
        plt.pie(
            dataset["values"],
            labels=dataset["labels"],
            autopct='%1.1f%%',
            startangle=90
        )
        plt.title('Distribución por Actividad')
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()
        
        return filepath

    # -----------------------------------------
    # 4. Cargar GeoJSON desde las URLs
    # -----------------------------------------
    def load_geojson(self, url: str) -> Dict[str, Any]:
        with urllib.request.urlopen(url) as response:
            return json.loads(response.read().decode())

    # -----------------------------------------
    # 5. Paquete completo para el JSON final
    # -----------------------------------------
    def build_sector_geo_payload(self) -> Dict[str, Any]:
        return {
            "dataset": self.get_donut_dataset(),
            "grafico_base64": self.get_donut_base64(),
            "mapas": {
                "colombia_departamentos": self.load_geojson(COLOMBIA_DEPARTAMENTOS_URL),
                "world_countries": self.load_geojson(WORLD_COUNTRIES_URL)
            }
        }
