import json
import os
import base64
import urllib.request
from typing import Dict, Any
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon

WORLD_URL = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"

GENERATED_IMAGES_DIR = "generated_images"
os.makedirs(GENERATED_IMAGES_DIR, exist_ok=True)

def _fetch_geojson(url: str) -> Dict[str, Any]:
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read().decode())

def _save_base64(path: str) -> str:
    with open(path, "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()

class MapImageService:
    def world_fatf_map(self, fatf_status: Dict[str, str]) -> Dict[str, Any]:
        geo = _fetch_geojson(WORLD_URL)
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.set_xlim(-180, 180)
        ax.set_ylim(-90, 90)
        ax.axis('off')

        def norm(s: str) -> str:
            return (s or '').upper().strip()
        alias = {
            "UNITED STATES OF AMERICA": "ESTADOS UNIDOS",
            "UNITED KINGDOM": "REINO UNIDO",
            "GERMANY": "ALEMANIA",
            "SPAIN": "ESPAÑA",
            "KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF": "COREA DEL NORTE",
            "IRAN (ISLAMIC REPUBLIC OF)": "IRAN",
        }

        for feat in geo.get('features', []):
            name = norm(feat.get('properties', {}).get('name', ''))
            status = norm(fatf_status.get(name, ''))
            if not status and name in alias:
                status = norm(fatf_status.get(alias[name], ''))
            color = '#cccccc'
            if status == 'NO COOPERANTE':
                color = '#de2d26'
            elif status == 'COOPERANTE':
                color = '#2ca25f'

            geom = feat.get('geometry', {})
            gtype = geom.get('type')
            coords = geom.get('coordinates', [])

            def draw_polygon(coord_list):
                for ring in coord_list:
                    poly = Polygon(ring, closed=True, facecolor=color, edgecolor='#555', linewidth=0.4)
                    ax.add_patch(poly)

            if gtype == 'Polygon':
                draw_polygon(coords)
            elif gtype == 'MultiPolygon':
                for poly in coords:
                    draw_polygon(poly)

        out_path = os.path.join(GENERATED_IMAGES_DIR, 'world_fatf_map.png')
        plt.tight_layout()
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        return {"path": out_path, "base64": _save_base64(out_path)}

    def colombia_empresa_map(self, points: Any, empresa_id: int) -> Dict[str, Any]:
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.set_xlim(-79, -66)
        ax.set_ylim(-4, 12)
        ax.axis('off')

        for p in points:
            r = (p.get('riesgo', '') or '').upper()
            if r and r != 'ALTO':
                continue
            lat = float(p.get('lat', 4.5709))
            lon = float(p.get('lon', -74.2973))
            monto = float(p.get('monto', 0))
            size = max(30, min(300, monto/1e6))
            ax.scatter(lon, lat, s=size, c='#e63946', edgecolors='#660000', linewidths=0.5, alpha=0.8)

        out_path = os.path.join(GENERATED_IMAGES_DIR, f'colombia_empresa_{empresa_id}.png')
        plt.tight_layout()
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        return {"path": out_path, "base64": _save_base64(out_path)}

map_image_service = MapImageService()
