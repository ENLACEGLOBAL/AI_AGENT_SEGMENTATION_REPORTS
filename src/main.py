# src/analytics_modules/sector_ubicacion/main.py
import pandas as pd
from .graph_generator import GraphGenerator
from .sector_geo_analytics import SectorGeoAnalytics
from .builder import SectorUbicacionBuilder


def generar_analitica_sector_ubicacion(
    df_transacciones: pd.DataFrame,
    df_fatf: pd.DataFrame
):
    geo = SectorGeoAnalytics(df_transacciones, df_fatf)
    graphs = GraphGenerator(df_transacciones)

    resultado = {
        "kpis": geo.get_kpis(),
        "mapa_colombia": geo.get_mapa_colombia(),
        "mapa_fatf": geo.get_fatf_status(),
        "graficos": {
            "donut_ciiu": {
                "labels": graphs.df["actividad"].unique().tolist(),
                "image_base64": graphs.generate_donut_base64()
            }
        },
        "tabla": geo.df[geo.df["riesgo"]=="ALTO"].to_dict(orient="records")
    }

    builder = SectorUbicacionBuilder(
        "src/analytics_modules/sector_ubicacion/output/sector_ubicacion_report.json"
    )

    return builder.save_report(resultado)
