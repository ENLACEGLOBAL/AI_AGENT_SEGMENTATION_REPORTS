# src/analytics_modules/builder.py

import pandas as pd
import urllib.request
import json

from src.analytics_modules.analyzer import Analyzer
from src.analytics_modules.common import write_json
from src.analytics_modules.sector_ubicacion.graph_generator import GraphGenerator
from src.analytics_modules.sector_ubicacion.sector_geo_analytics import SectorGeoAnalytics


COLOMBIA_DEPARTAMENTOS_URL = (
    "https://raw.githubusercontent.com/johnguerra/colombia-geojson/master/colombia.json"
)
WORLD_COUNTRIES_URL = (
    "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
)


def generar_json_sector_ubicacion(
    id_empresa: int,
    fecha_inicio=None,
    fecha_fin=None,
    tipo_contraparte=None
):
    """
    Builder general.
    Produce JSON completo incluso si la base de datos no tiene registros.
    """

    # ------------------------------------------------------------------
    # 1. Ejecutar el analizador (puede devolver 0 registros)
    # ------------------------------------------------------------------
    analyzer = Analyzer()
    datos = analyzer.analizar(
        id_empresa=id_empresa,
        tipo_contraparte=tipo_contraparte,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin
    )

    registros = datos["registros"]["clientes"] + datos["registros"]["proveedores"]

    if registros:
        df = pd.DataFrame([r.to_dict() for r in registros])
    else:
        # DataFrame vacío con todas las columnas necesarias
        df = pd.DataFrame(columns=[
            "id",
            "empresa",
            "nit",
            "ciiu",
            "actividad",
            "departamento",
            "lat",
            "lon",
            "monto",
            "riesgo"
        ])

    # ------------------------------------------------------------------
    # 2. Cargar tablas FATF (si no existe, usar DataFrame vacío)
    # ------------------------------------------------------------------
    try:
        df_fatf = pd.read_csv("data/fatf/segmentacion_jurisdicciones.csv")
    except FileNotFoundError:
        df_fatf = pd.DataFrame(columns=["pais", "estatus"])

    # ------------------------------------------------------------------
    # 3. Graficos
    # ------------------------------------------------------------------
    graph = GraphGenerator(df)

    # ------------------------------------------------------------------
    # 4. Analítica geográfica
    # ------------------------------------------------------------------
    geo = SectorGeoAnalytics(df, df_fatf)

    # ------------------------------------------------------------------
    # 5. GeoJSON (mapas)
    # ------------------------------------------------------------------
    try:
        colombia_geojson = json.loads(urllib.request.urlopen(COLOMBIA_DEPARTAMENTOS_URL).read())
    except:
        colombia_geojson = {}

    try:
        world_geojson = json.loads(urllib.request.urlopen(WORLD_COUNTRIES_URL).read())
    except:
        world_geojson = {}

    # ------------------------------------------------------------------
    # 6. Construcción del JSON maestro
    # ------------------------------------------------------------------
    final_json = {
        "empresa_id": id_empresa,
        "filtros": datos["filtros"],

        "kpis": geo.get_kpis(),

        "graficos": {
            "donut_dataset": graph.get_donut_dataset(),
            "donut_img_base64": graph.get_donut_base64(),
        },

        "tablas": {
            "detalle": graph.get_tabla_detalle()
        },

        "mapas": {
            "colombia": geo.get_mapa_colombia(),
            "fatf_status": geo.get_fatf_status(),
            "geojson": {
                "colombia": colombia_geojson,
                "world": world_geojson
            }
        },

        "raw_data": df.to_dict(orient="records")
    }

    # ------------------------------------------------------------------
    # 7. Guardar archivo
    # ------------------------------------------------------------------
    path = f"data/sector_ubicacion/empresa_{id_empresa}.json"
    write_json(path, final_json)

    return final_json
