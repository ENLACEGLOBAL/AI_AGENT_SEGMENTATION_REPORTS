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

# URL para el GeoJSON de Colombia (Departamentos)
COLOMBIA_GEOJSON_URL = "https://gist.githubusercontent.com/john-guerra/43c7656821069d00dcbc/raw/be6a6e239cd5b5b803c6e7c2ec405b793a9064dd/Colombia.geo.json"

# Centros aproximados de departamentos para fallback o plotting
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
}

def _fetch_geojson(url: str) -> Dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception:
        return {}

def _save_base64(path: str) -> str:
    with open(path, "rb") as f:
        return "data:image/png;base64," + base64.b64encode(f.read()).decode()

class MapImageService:
    def world_fatf_map(self, fatf_status: Dict[str, str]) -> Dict[str, Any]:
        geo = _fetch_geojson(WORLD_URL)
        # Usamos figsize cuadrado para estandarizar tamaño con mapa de Colombia
        fig, ax = plt.subplots(figsize=(10, 10))
        # Mantener proporción correcta (evitar distorsión) y centrar
        ax.set_aspect('equal')
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
        
        # Si falla la carga del GeoJSON, al menos devolvemos una imagen vacía o básica
        if not geo:
             plt.text(0, 0, "No se pudo cargar el mapa mundial.", ha='center')
        else:
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

    def colombia_risk_map(self, dept_counts: Dict[str, int], empresa_id: int) -> Dict[str, Any]:
        """
        Genera un mapa de Colombia con burbujas rojas en los departamentos con riesgo.
        Intenta cargar GeoJSON de departamentos para el fondo.
        """
        geo = _fetch_geojson(COLOMBIA_GEOJSON_URL)
        
        # Usamos figsize cuadrado para estandarizar tamaño
        fig, ax = plt.subplots(figsize=(10, 10))
        # Mantener proporción correcta
        ax.set_aspect('equal')
        ax.set_xlim(-80, -66)
        ax.set_ylim(-4.5, 13)
        ax.axis('off')
        ax.set_title("Mapa de Transacciones Alto Riesgo - Colombia", fontsize=14, color='#003366', fontweight='bold')

        # Dibujar mapa base
        if geo:
            for feat in geo.get('features', []):
                color = '#e9ecef' # Gris claro fondo
                geom = feat.get('geometry', {})
                gtype = geom.get('type')
                coords = geom.get('coordinates', [])

                def draw_polygon(coord_list):
                    for ring in coord_list:
                        poly = Polygon(ring, closed=True, facecolor=color, edgecolor='#adb5bd', linewidth=0.5)
                        ax.add_patch(poly)

                if gtype == 'Polygon':
                    draw_polygon(coords)
                elif gtype == 'MultiPolygon':
                    for poly in coords:
                        draw_polygon(poly)
        else:
             # Fallback: Texto o cuadro simple si no hay internet
             ax.text(-74, 4, "Mapa Base no disponible (Offline)", ha='center')

        # Dibujar burbujas
        max_val = max(dept_counts.values()) if dept_counts else 1
        
        for dept, count in dept_counts.items():
            norm_dept = dept.upper().strip()
            # Mapeo manual de nombres comunes si es necesario
            if norm_dept == "BOGOTA": norm_dept = "BOGOTA D.C."
            
            if norm_dept in COLOMBIA_DEPT_COORDS:
                lat, lon = COLOMBIA_DEPT_COORDS[norm_dept]
                # Tamaño de burbuja proporcional
                size = 100 + (count / max_val) * 800
                ax.scatter(lon, lat, s=size, c='#e63946', edgecolors='#b71c1c', alpha=0.6, zorder=10)
                # Texto con conteo
                ax.text(lon, lat, str(count), ha='center', va='center', color='white', fontweight='bold', fontsize=8, zorder=11)
            else:
                # Si no encuentra coordenada, ignora o loguea
                pass

        out_path = os.path.join(GENERATED_IMAGES_DIR, f'colombia_risk_{empresa_id}.png')
        plt.tight_layout()
        plt.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        return {"path": out_path, "base64": _save_base64(out_path)}

    # Mantener compatibilidad con llamadas anteriores si existen
    def colombia_empresa_map(self, points: Any, empresa_id: int) -> Dict[str, Any]:
        # Convertir lista de puntos a conteo por departamento si es posible, o usar logica anterior
        # Pero para este caso, implementamos la logica de puntos si tienen lat/lon
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.set_xlim(-79, -66)
        ax.set_ylim(-4, 12)
        ax.axis('off')
        
        # Intentar cargar mapa base tambien aqui
        geo = _fetch_geojson(COLOMBIA_GEOJSON_URL)
        if geo:
            for feat in geo.get('features', []):
                color = '#f8f9fa'
                geom = feat.get('geometry', {})
                gtype = geom.get('type')
                coords = geom.get('coordinates', [])
                def draw_polygon(coord_list):
                    for ring in coord_list:
                        poly = Polygon(ring, closed=True, facecolor=color, edgecolor='#dee2e6', linewidth=0.5)
                        ax.add_patch(poly)
                if gtype == 'Polygon': draw_polygon(coords)
                elif gtype == 'MultiPolygon': 
                    for poly in coords: draw_polygon(poly)

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
