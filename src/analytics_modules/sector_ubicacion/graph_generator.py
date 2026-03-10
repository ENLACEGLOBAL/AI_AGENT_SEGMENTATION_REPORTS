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

        # Determinar columna de agrupación (Prioridad: Descripción > Código > Actividad)
        col_group = None
        
        # 1. Intentar usar descripción del CIIU si existe
        if 'ciiu_descripcion' in self.df.columns:
            # Rellenar descripciones faltantes con el código
            if 'ciiu' in self.df.columns:
                self.df['label_final'] = self.df['ciiu_descripcion'].fillna(self.df['ciiu'].astype(str))
            else:
                self.df['label_final'] = self.df['ciiu_descripcion'].fillna("Desconocido")
            col_group = 'label_final'
            
        # 2. Si no hay descripción, usar código CIIU
        elif 'ciiu' in self.df.columns:
            col_group = 'ciiu'
            
        # 3. Fallback a 'actividad'
        elif 'actividad' in self.df.columns:
            col_group = 'actividad'
            
        if not col_group:
            # Fallback total
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
        
        # Helper interno para limpiar texto
        def _clean_text(t):
            t = str(t).replace('nan', 'Sin Clasificar')
            # Intentar arreglar mojibake (encoding incorrecto)
            try:
                # Caso común: UTF-8 interpretado como CP1252 (e.g. Ã³ -> ó)
                t = t.encode('cp1252').decode('utf-8')
            except:
                pass
            # Intentar segunda pasada por si es doble encoding
            try:
                t = t.encode('cp1252').decode('utf-8')
            except:
                pass
            
            # Truncar si es muy largo para la leyenda
            if len(t) > 45:
                t = t[:42] + "..."
            
            # Si sigue siendo solo dígitos (no se encontró descripción), hacerlo más explícito
            if t.isdigit():
                t = f"CIIU {t} (Sin Descripción)"
                
            return t

        # Limpiar etiquetas
        labels = [_clean_text(x) for x in group.index]
        
        return {
            "labels": labels,
            "values": [float(v) for v in group.values]
        }

    # -----------------------------------------
    # 2. DATASET para Ubicación (Tabla)
    # -----------------------------------------
    def get_location_dataset(self) -> Dict[str, Any]:
        """
        Genera distribución por Jurisdicción/País.
        """
        if self.df.empty:
            return {"labels": [], "values": []}

        # Prioridad: departamento > ciudad > pais
        col_loc = None
        if 'departamento' in self.df.columns:
            col_loc = 'departamento'
        elif 'ciudad' in self.df.columns:
            col_loc = 'ciudad'
        elif 'pais' in self.df.columns:
            col_loc = 'pais'
            
        if not col_loc:
            return {"labels": ["Desconocido"], "values": [1]}
            
        # Agrupar
        group = self.df[col_loc].value_counts().head(5)
        
        return {
            "labels": [str(x).title() for x in group.index],
            "values": [int(x) for x in group.values]
        }

    # -----------------------------------------
    # 3. Gráfico Combinado (Stacked Bar Chart: Location x Sector)
    # -----------------------------------------
    def get_combined_chart_base64(self) -> str:
        """
        Generates a Stacked Horizontal Bar Chart (Location + Sector) in Base64.
        Replacing the 2-chart + table layout.
        Uses exact values (billions/millions) and Green/Yellow/Blue palette.
        Panoramic size for 3020px width PDF.
        """
        # DEBUG: Force generation even if empty to show debug image
        if self.df.empty:
             print("⚠️ DataFrame is empty in get_combined_chart_base64")
             fig, ax = plt.subplots(figsize=(15, 4))
             ax.text(0.5, 0.5, "Error: DataFrame vacío (No hay datos cargados)", 
                    ha='center', va='center', fontsize=20, color='red')
             ax.axis('off')
             buffer = io.BytesIO()
             plt.savefig(buffer, format='png', bbox_inches='tight', dpi=300)
             plt.close(fig)
             buffer.seek(0)
             return f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('utf-8')}"

        # 1. Definir columnas dinámicamente (Case Insensitive)
        cols_map = {c.lower(): c for c in self.df.columns}
        
        col_loc = None
        for c in ['ciudad', 'municipio', 'ubicacion', 'departamento', 'jurisdiccion', 'jurisdiccion_geografica', 'pais']:
             if c in cols_map:
                 col_loc = cols_map[c]
                 break
        
        col_sector = None
        for c in ['ciiu_descripcion', 'label_final', 'ciiu', 'actividad', 'sector', 'industria']:
             if c in cols_map:
                 col_sector = cols_map[c]
                 break
        
        col_val = None
        for c in ['valor_transaccion', 'monto', 'total', 'cantidad']:
            if c in cols_map:
                col_val = cols_map[c]
                break

        # DEBUG: Log missing columns
        if not col_loc or not col_sector:
            print(f"⚠️ Missing columns in get_combined_chart_base64: col_loc={col_loc}, col_sector={col_sector}, cols={self.df.columns.tolist()}")
            # Generate a placeholder "No Data" chart instead of empty string
            fig, ax = plt.subplots(figsize=(15, 4))
            ax.text(0.5, 0.5, f"No hay datos suficientes para generar el gráfico\n(Faltan columnas: Ubicación={col_loc or 'Falta'}, Sector={col_sector or 'Falta'})\nCols: {list(self.df.columns)}", 
                   ha='center', va='center', fontsize=20, color='#555555')
            ax.axis('off')
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=300)
            plt.close(fig)
            buffer.seek(0)
            return f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('utf-8')}"
            
        if not col_val:
            col_val = 'conteo'
            self.df['conteo'] = 1

        loc_priority_cols = []
        # Priority: Ciudad > Municipio > Departamento > Jurisdiccion > Pais
        # Added 'jurisdiccion' and 'jurisdiccion_geografica' to ensure they are detected
        for key in ['ciudad', 'municipio', 'departamento', 'jurisdiccion', 'jurisdiccion_geografica', 'pais']:
            if key in cols_map:
                loc_priority_cols.append(cols_map[key])

        if loc_priority_cols:
            def choose_loc(row):
                for c in loc_priority_cols:
                    v = row.get(c)
                    # Check for valid value AND not a placeholder like 'SIN DEPTO' or 'NO DEFINIDO'
                    if pd.notna(v):
                        s = str(v).strip()
                        # Ignore generic placeholders to allow fallback to broader categories (like Country)
                        if s and s.upper() not in ['SIN DEPTO', 'SIN DEPTO.', 'NO DEFINIDO', 'DESCONOCIDO', 'NAN', 'NONE', '0']:
                            return s
                return 'SIN INFORMACION'
            self.df['ubicacion_principal'] = self.df.apply(choose_loc, axis=1)
            col_loc = 'ubicacion_principal'

        # 2. Limpieza de datos
        self.df[col_loc] = self.df[col_loc].fillna('Desconocido').astype(str)
        self.df[col_sector] = self.df[col_sector].fillna('Otros Sectores').astype(str)
        
        # Replace 'nan' string if it occurred
        self.df[col_loc] = self.df[col_loc].replace({'nan': 'Desconocido', 'Nan': 'Desconocido', 'NaN': 'Desconocido'})
        self.df[col_sector] = self.df[col_sector].replace({'nan': 'Otros Sectores', 'Nan': 'Otros Sectores', 'NaN': 'Otros Sectores'})

        self.df[col_loc] = self.df[col_loc].str.replace('_', ' ').str.strip()
        
        # Clean encoding for display
        # The screenshot shows "ConstrucciÃ³n", which is UTF-8 decoded as Latin-1.
        # We need to reverse this: text.encode('latin1').decode('utf-8')
        def fix_encoding(text):
            if not isinstance(text, str):
                return text
            s = text
            # Try up to 2 passes to fix double-encoded mojibake (e.g., ÃƒÂ³ -> ó)
            for _ in range(2):
                try:
                    new_s = s.encode('latin1').decode('utf-8')
                    if new_s == s:
                        break
                    s = new_s
                except Exception:
                    break
            return s
        
        self.df[col_loc] = self.df[col_loc].apply(fix_encoding)
        self.df[col_sector] = self.df[col_sector].apply(fix_encoding)
        
        # Extra check for common Mojibake if the above didn't catch it
        # Sometimes 'Ã³' is literally in the string if it was already saved that way
        replacements = {
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú', 'Ã±': 'ñ',
            'ÃÁ': 'Á', 'ÃÉ': 'É', 'ÃÍ': 'Í', 'ÃÓ': 'Ó', 'ÃÚ': 'Ú', 'ÃÑ': 'Ñ',
            'ÃƒÂ¡': 'á', 'ÃƒÂ©': 'é', 'ÃƒÂ­': 'í', 'ÃƒÂ³': 'ó', 'ÃƒÂº': 'ú', 'ÃƒÂ±': 'ñ',
            'Ãƒâ€œ': 'Ó', 'Ãƒâ€˜': 'Ñ'
        }
        for bad, good in replacements.items():
            self.df[col_loc] = self.df[col_loc].str.replace(bad, good, regex=False)
            self.df[col_sector] = self.df[col_sector].str.replace(bad, good, regex=False)
        
        # Final case normalization after fixing encoding
        self.df[col_loc] = self.df[col_loc].str.upper()
        self.df[col_sector] = self.df[col_sector].str.upper()

        if self.df.empty:
             # Placeholder for empty data
            fig, ax = plt.subplots(figsize=(15, 4))
            ax.text(0.5, 0.5, "No hay datos disponibles para mostrar", 
                   ha='center', va='center', fontsize=20, color='#555555')
            ax.axis('off')
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100)
            plt.close(fig)
            buffer.seek(0)
            return f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('utf-8')}"

        # ---------------------------------------------------------
        # LOGICA "TOP 10" (Revertida por solicitud del usuario)
        # ---------------------------------------------------------
        
        # 1. Agrupar por Sector y obtener Top 10 Sectores
        sector_totals = self.df.groupby(col_sector)[col_val].sum().sort_values(ascending=False).head(10)
        top_sectors = sector_totals.index.tolist()
        
        # 2. Filtrar DataFrame para incluir solo esos sectores
        df_filtered = self.df[self.df[col_sector].isin(top_sectors)]
        
        # 3. Agrupar por Ubicación y Sector
        df_grouped = df_filtered.groupby([col_loc, col_sector])[col_val].sum().reset_index()
        
        # 4. Pivot para gráfico apilado
        df_pivot = df_grouped.pivot(index=col_loc, columns=col_sector, values=col_val).fillna(0)
        
        # 5. Ordenar ubicaciones por total y tomar Top 10 Ubicaciones (para evitar saturación horizontal)
        df_pivot['total'] = df_pivot.sum(axis=1)
        df_pivot = df_pivot.sort_values('total', ascending=False).head(10)
        df_pivot = df_pivot.drop(columns='total')

        # ---------------------------------------------------------
        # PLOTTING
        # ---------------------------------------------------------
        # Configurar fuente global para consistencia con PDF (Helvetica/Arial)
        plt.rcParams['font.family'] = 'sans-serif'
        plt.rcParams['font.sans-serif'] = ['Helvetica', 'Arial', 'DejaVu Sans']

        # Tamaño panorámico para PDF 3020px
        fig, ax = plt.subplots(figsize=(22, 10))
        
        # Paleta de colores verdes y azules (Green & Blue tones)
        colors = ['#1565C0', '#009688', '#00BCD4', '#4CAF50', '#2196F3', '#00897B', '#03A9F4', '#8BC34A', '#0288D1', '#00796B']
        
        # Plotting
        df_pivot.plot(kind='bar', stacked=True, ax=ax, color=colors[:len(df_pivot.columns)], width=0.6)
        
        # Títulos y Etiquetas
        # ax.set_title("Transacciones por Sector Económico y Ubicación Geográfica (Top 10)", fontsize=36, fontweight='bold', color='#264653', pad=20)
        ax.set_xlabel("") # REMOVED: "Ubicación Geográfica" text per user request
        ax.set_ylabel("Cantidad de Transacciones", fontsize=28, fontweight='bold', color='#333333', labelpad=20)
        
        # Ejes
        # FIX: Explicitly set x-tick labels to ensure names appear instead of numbers
        ax.set_xticks(range(len(df_pivot)))
        ax.set_xticklabels(df_pivot.index, rotation=30, ha='right')
        ax.tick_params(axis='x', labelsize=24, labelcolor='#333333')
        ax.tick_params(axis='y', labelsize=22, labelcolor='#555555')
        
        # Formateo de Eje Y (Billions/Millions)
        from matplotlib.ticker import FuncFormatter
        def human_format(num, pos):
            magnitude = 0
            while abs(num) >= 1000:
                magnitude += 1
                num /= 1000.0
            return '%.1f%s' % (num, ['', 'K', 'M', 'B', 'T'][magnitude])
            
        # ax.xaxis.set_major_formatter(FuncFormatter(human_format)) # REMOVED: This was causing X-axis labels to be 0.0, 1.0, etc.
        ax.yaxis.set_major_formatter(FuncFormatter(human_format))
        
        # Leyenda (Outside to prevent overlap)
        # Moved further down to avoid overlap with x-axis labels
        ax.legend(
            title="Sector Económico", 
            title_fontsize=24, 
            fontsize=20, 
            loc='upper center', 
            bbox_to_anchor=(0.5, -0.35), # Moved further down
            ncol=3, 
            frameon=False
        )
        
        # Grid
        ax.grid(axis='y', linestyle='--', alpha=0.3)
        ax.set_axisbelow(True)
        
        # Ajuste de Layout
        # plt.tight_layout() # REMOVED: Causing UserWarning and conflict with subplots_adjust
        plt.subplots_adjust(bottom=0.4) # Increased bottom margin significantly

        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100)
        plt.close(fig)
        buffer.seek(0)
        
        return f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('utf-8')}"

    # -----------------------------------------
    # 4. Gráfico en Base64 (Legacy Wrapper)
    # -----------------------------------------
    def get_donut_base64(self) -> str:
        """
        Legacy name, now returns the combined chart as requested.
        """
        return self.get_combined_chart_base64()

    # -----------------------------------------
    # 3. Save chart to file
    # -----------------------------------------
    def save_donut_chart(self, filepath: str) -> str:
        """Save donut chart to file and return the filepath."""
        dataset = self.get_donut_dataset()
        values = dataset["values"]

        def make_autopct(vals):
            def my_autopct(pct):
                total = sum(vals)
                val = int(round(pct*total/100.0))
                return f'{val}' if val > 0 else ''
            return my_autopct

        plt.figure(figsize=(6, 6))
        plt.pie(
            values,
            labels=dataset["labels"],
            autopct=make_autopct(values),
            startangle=90
        )
        plt.title('Distribución por Actividad')
        plt.tight_layout()
        plt.savefig(filepath, dpi=200, bbox_inches='tight')
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
