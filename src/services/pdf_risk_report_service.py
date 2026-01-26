# PDF Risk Report Service (Refactor Profesional)
# Diseño ejecutivo en 3 páginas: Visión / Decisión / Análisis
# Autor: Refactor técnico

import json
import os
import base64
import io
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional

from cryptography.fernet import Fernet
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    PageBreak, Image, KeepInFrame
)
from reportlab.graphics.shapes import Drawing, Circle, String, Rect
from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
from src.analytics_modules.cruces_entidades.cruces_graph_generator import CrucesGraphGenerator
from src.analytics_modules.sector_ubicacion.graph_generator import GraphGenerator

from src.services.local_ai_report_service import local_ai_report_service
from src.services.map_image_service import map_image_service
from src.db.base import TargetSessionLocal
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.services.s3_service import s3_service
from src.db.base import SourceSessionLocal
from src.db.models.cliente import Cliente
from src.db.models.proveedor import Proveedor
from src.db.models.empleado import Empleado
from src.db.models.reference_tables import AuxiliarCiiu

# --------------------
# Paleta corporativa (User Requested: Teal, Sand, Blue - No Red)
# --------------------
COLORS = {
    'primary': colors.HexColor('#2a9d8f'),   # Green (Teal)
    'secondary': colors.HexColor('#2AB4EB'), # Light Blue (User Request)
    'success': colors.HexColor('#2a9d8f'),   # Green
    'warning': colors.HexColor('#e9c46a'),   # Sand/Yellow
    'danger': colors.HexColor('#264653'),    # Dark Blue/Cyan (Alternative to Red)
    'info': colors.HexColor('#2AB4EB'),      # Light Blue
    'light': colors.HexColor('#f8f9fa'),
    'gray': colors.HexColor('#6c757d'),
    'dark': colors.HexColor('#333333'),
    'purple': colors.HexColor('#1a7bb9'),    # Darker Blue variation
}


class PDFRiskReportService:
    """Generador profesional de reportes PDF de riesgo (nivel corporativo)."""

    # --------------------
    # API pública
    # --------------------
    def generate_pdf_report(
        self,
        analytics_json_path: Optional[str] = None,
        analytics_data: Optional[Dict[str, Any]] = None,
        tipo_contraparte: str = "cliente",
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:

        analytics = {}
        if analytics_data:
            analytics = analytics_data
        elif analytics_json_path:
            with open(analytics_json_path, 'r', encoding='utf-8') as f:
                analytics = json.load(f)
        else:
            raise ValueError("Must provide either analytics_json_path or analytics_data")

        empresa_id = analytics.get('empresa_id')
        entidades_cruces = analytics.get('entidades_cruces', None)
        
        if not empresa_id and analytics_json_path:
            # Fallback: Intentar extraer ID del nombre del archivo
            # Formato esperado: analytics_ID_TIMESTAMP.json o similar
            try:
                basename = os.path.basename(analytics_json_path)
                parts = basename.split('_')
                for part in parts:
                    if part.isdigit():
                        empresa_id = int(part)
                        break
            except Exception:
                pass

        if not empresa_id:
            raise ValueError("empresa_id no encontrado en JSON ni en nombre de archivo")

        df_all, df_clientes, df_proveedores, df_empleados = self._load_dataframes(empresa_id)
        df_empresa = df_all[df_all['id_empresa'].astype(str) == str(empresa_id)].copy()

        df_alto = self._filter_high_risk(df_empresa)
        avg_score = self._avg_score(df_alto)
        risk_level = self._get_risk_level(avg_score)

        # ---------------------------------------------------------
        # MODIFICADO: Generar en Memoria (BytesIO) - No guardar en disco
        # ---------------------------------------------------------
        buffer = io.BytesIO()
        
        # Generar PDF en el buffer
        self._build_pdf(
            buffer,  # Pasamos el buffer en lugar de un path
            empresa_id,
            df_all,
            df_alto,
            avg_score,
            risk_level,
            df_clientes,
            df_proveedores,
            df_empleados,
            entidades_cruces=entidades_cruces
        )
        
        # Obtener bytes del buffer
        pdf_bytes = buffer.getvalue()
        buffer.close()

        # Opcional: guardar copia local si se solicita (modo demo/validación)
        local_file = None
        if output_path:
            try:
                dirpath = os.path.dirname(output_path)
                if dirpath:
                    os.makedirs(dirpath, exist_ok=True)
                with open(output_path, "wb") as f:
                    f.write(pdf_bytes)
                local_file = output_path
                print(f"✅ Copia local del PDF guardada en: {local_file}")
            except Exception as e:
                print(f"⚠️ No se pudo guardar copia local: {e}")

        # Intentar subir a S3
        filename = f"Reporte_{empresa_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        s3_key = f"reports/{filename}" # Store in reports folder
        s3_url = s3_service.upload_file(pdf_bytes, s3_key)

        pdf_content_to_store = pdf_bytes
        if s3_url:
            # GUARDAR SOLO EL NOMBRE DEL ARCHIVO (KEY)
            # Esto permite que el controlador PHP genere URLs firmadas (temporaryUrl)
            # y valide correctamente el acceso al bucket privado.
            virtual_path = s3_key
            pdf_content_to_store = None # Optimization: Don't store BLOB if S3 worked
        else:
            # Fallback a almacenamiento solo en BD (BLOB)
            virtual_path = f"DB_STORED:{filename}"

        # Guardar en Base de Datos (BLOB + Path)
        self._save_to_db(empresa_id, virtual_path, pdf_content_to_store)

        return {
            'status': 'success',
            'file': virtual_path,
            'empresa_id': empresa_id,
            'risk_level': risk_level,
            'score': round(avg_score, 2),
            'local_file': local_file
        }

    def _save_to_db(self, empresa_id: int, file_path: str, pdf_bytes: Optional[bytes]):
        """Guarda el contenido del PDF en la base de datos."""
        try:
            # 1. Preparar Key (Prioridad: ENCRYPTION_KEY > JWT_SECRET)
            enc_key = os.getenv('ENCRYPTION_KEY')
            if enc_key:
                cipher = Fernet(enc_key)
            else:
                jwt_secret = os.getenv('JWT_SECRET', 'super-secret')
                raw_key = hashlib.sha256(jwt_secret.encode()).digest()
                fernet_key = base64.urlsafe_b64encode(raw_key)
                cipher = Fernet(fernet_key)

            # 2. Encriptar path (aunque sea virtual, lo mantenemos por consistencia)
            encrypted_path = cipher.encrypt(file_path.encode()).decode()

            # 3. Guardar en DB
            db = TargetSessionLocal()
            try:
                repo = GeneratedReportRepository()
                repo.create_report(
                    db, 
                    file_path=encrypted_path, 
                    company_id=empresa_id,
                    pdf_content=pdf_bytes
                )
                
                storage_type = "S3 (Path Only)" if pdf_bytes is None else "BLOB (Fallback)"
                print(f"✅ Reporte registrado en DB para empresa {empresa_id}. Storage: {storage_type}")
            finally:
                db.close()
        except Exception as e:
            print(f"⚠️ Error guardando reporte en DB: {e}")

    # --------------------
    # Construcción PDF
    # --------------------
    def _build_pdf(self, output, empresa_id, df_all, df_alto, avg_score, risk_level,
                   df_clientes, df_proveedores, df_empleados, entidades_cruces=None):

        # User Request: 3020 pixels width (assuming points for PDF)
        # Height: Increased to 4000 to accommodate 5x scaled content on a "single page"
        PAGE_WIDTH = 3020
        PAGE_HEIGHT = 3200
        
        doc = SimpleDocTemplate(
            output,
            pagesize=(PAGE_WIDTH, PAGE_HEIGHT),
            leftMargin=0.5 * inch,
            rightMargin=0.5 * inch,
            topMargin=0.2 * inch,
            bottomMargin=0.2 * inch
        )

        styles = self._styles()
        story = []

        # ================= PAGE 1: EXECUTIVE SUMMARY & CRUCES =================
        # 1. Header Compacto (Logo + Título)
        self._cover_compact(story, styles, empresa_id, page_width=PAGE_WIDTH)
        story.append(Spacer(1, 1.0 * inch))
        
        # 2. KPIs Globales
        self._kpi_panel(story, styles, df_all, empresa_id, df_clientes, df_proveedores, df_empleados, entidades_cruces, page_width=PAGE_WIDTH)
        story.append(Spacer(1, 1.0 * inch))

        # 3. Sector y Ubicación (Compacto) - MOVIDO ANTES DE CRUCES
        self._sector_location_compact(story, styles, df_all, page_width=PAGE_WIDTH)
        story.append(Spacer(1, 0.8 * inch))
        
        # 4. Cruces de Entidades (Full Dashboard)
        self._entity_crosses_page_full(story, styles, empresa_id, df_clientes, df_proveedores, df_empleados, entidades_cruces, page_width=PAGE_WIDTH)

        # Footer Simple
        # Eliminado por solicitud del usuario
        
        doc.build(story)

    def _entity_crosses_page_full(self, story, styles, empresa_id, df_clientes, df_proveedores, df_empleados, entidades_cruces=None, page_width=None):
        story.append(Paragraph("Análisis de Cruces de Entidades (Colusión)", styles['CustomTitle']))
        # Subtitle removed to reduce redundancy
        story.append(Spacer(1, 0.5 * inch)) # SCALED Spacer

        try:
            # Check if we have valid precomputed data (must have 'distribucion' and 'chart_heatmap_base64')
            use_precomputed = False
            if entidades_cruces and 'distribucion' in entidades_cruces and entidades_cruces.get('chart_heatmap_base64'):
                 use_precomputed = True
            
            if use_precomputed:
                # Use precomputed data
                dist = entidades_cruces.get('distribucion', {})
                tabla = entidades_cruces.get('tabla', [])
                total_cruces_calc = sum(int(v) for v in dist.values())
                b64_types = entidades_cruces.get('chart_types_base64')
                b64_heat = entidades_cruces.get('chart_heatmap_base64')
                
            else:
                # Fallback to CSV processing (legacy/backup)
                analytics = CrucesAnalytics(df_clientes, df_proveedores, df_empleados)
                df_cruces = analytics.procesar_datos()
                dist = analytics.get_distribucion_riesgo()
                tabla = analytics.get_tabla_detalles(empresa_id)
                total_cruces_calc = len(df_cruces)
                
                # Graphs generation on the fly
                gen = CrucesGraphGenerator(analytics)
                b64_types = gen.generate_cross_types_chart()
                b64_heat = gen.generate_cruces_heatmap_chart()

            metrics = [
                (str(total_cruces_calc), 'Entidades', 'con Cruces', COLORS['info']),
                (str(dist.get('alto', 0)), 'Riesgo Alto', 'Alerta Extrema', COLORS['danger']),
                (str(dist.get('medio', 0)), 'Riesgo Medio', 'Seguimiento', COLORS['warning']),
                (str(dist.get('bajo', 0)), 'Riesgo Bajo', 'Normal', COLORS['success']),
            ]

            # Reverting to Circular Metric Row as requested by user
            # This ensures the specific "Circle + Text" design is used
            self._metric_row(story, styles, metrics, page_width=page_width)
            story.append(Spacer(1, 0.3 * inch))
            
            if total_cruces_calc == 0:
                story.append(Paragraph("<b>Sin Hallazgos de Colusión:</b> No se detectaron entidades que actúen simultáneamente como clientes, proveedores y/o empleados. Esto sugiere una adecuada segregación de funciones.", styles['CustomBody']))
                story.append(Spacer(1, 1.0 * inch)) # SCALED Spacer
                return

            # 2. Gráfico Principal (Side-by-Side: Pie Chart + Texto)
            # story.append(Paragraph("Distribución y Tipología de Riesgos", styles['CustomH1']))
            
            # ELIMINADO POR SOLICITUD DEL USUARIO
            # img_types = None
            # if isinstance(b64_types, str) and b64_types.startswith("data:image/png;base64,"):
            #    img_bytes = base64.b64decode(b64_types.split(",", 1)[1])
            #    bio = io.BytesIO(img_bytes)
            #    img_types = Image(bio, width=3.4 * inch, height=2.4 * inch)
            
            # if img_types:
            #    img_types.hAlign = 'CENTER'
            #    story.append(img_types)
            # else:
            #    story.append(Paragraph("Sin datos de tipología.", styles['CompactBody']))
            
            # story.append(Spacer(1, 0.1 * inch))
            
            # 3. Dashboard Moderno (Gauges + Progress Bars)
            if isinstance(b64_heat, str) and b64_heat.startswith("data:image/png;base64,"):
                # Removed redundant title to save space
                story.append(Paragraph(
                    "Guía de Visualización: El gráfico de la izquierda (Gauges) muestra el porcentaje de entidades clasificadas en cada nivel de riesgo (Bajo, Medio, Alto). "
                    "El gráfico de la derecha detalla la Tipología del Cruce, indicando qué roles simultáneos están desempeñando las entidades detectadas (ej. Cliente que también es Proveedor).",
                    styles['CompactBody']
                ))
                story.append(Spacer(1, 0.5 * inch)) # SCALED Spacer

                img_bytes = base64.b64decode(b64_heat.split(",", 1)[1])
                bio = io.BytesIO(img_bytes)
                
                available_width = (page_width - 1.0 * inch) if page_width else (7.5 * inch)
                target_height = available_width * (1.4 / 7.5)
                
                img_heat = Image(bio, width=available_width, height=target_height)
                img_heat.hAlign = 'CENTER'
                story.append(img_heat)

            story.append(Spacer(1, 0.5 * inch)) # SCALED Spacer

        except Exception as e:
            story.append(Paragraph(f"Error generando sección de cruces: {str(e)}", styles['CompactBody']))


    def _sector_location_compact(self, story, styles, df_all, page_width=None):
        story.append(Paragraph("Transacciones por Sector Económico y Ubicación Geográfica", styles['CustomH1Compact']))
        story.append(Spacer(1, 0.5 * inch)) # Increased spacer to fix overlap with subtitle
        story.append(Paragraph("Análisis de concentración por actividad económica y ubicación.", styles['CustomMeta']))
        story.append(Spacer(1, 0.8 * inch)) # Increased spacer to prevent overlap

        try:
            # 1. Donut Chart (CIIU) + Location Table (Combined Chart)
            # Use GraphGenerator to get the base64 string
            gen = GraphGenerator(df_all)
            b64_combined = gen.get_combined_chart_base64()
            
            img_combined = None
            if isinstance(b64_combined, str) and b64_combined.startswith("data:image/png;base64,"):
                img_bytes = base64.b64decode(b64_combined.split(",", 1)[1])
                bio = io.BytesIO(img_bytes)
                
                # Calculate available width
                available_width = (page_width - 1.0 * inch) if page_width else (7.5 * inch)
                # Height proportional to current generator figure (22x8)
                target_height = available_width * (8.0 / 22.0)
                
                img_combined = Image(bio, width=available_width, height=target_height)
                img_combined.hAlign = 'CENTER'

            if img_combined:
                story.append(img_combined)
            else:
                story.append(Paragraph("No hay datos suficientes para generar el gráfico.", styles['CompactBody']))

        except Exception as e:
             story.append(Paragraph(f"No disponible: {str(e)}", styles['CompactBody']))
        
        story.append(Spacer(1, 0.5 * inch)) # SCALED Spacer


    # --------------------
    # Secciones
    # --------------------
    def _cover_compact(self, story, styles, empresa_id, page_width=None):
        # Header en Tabla: Logo | Título + Meta
        logo_img = None
        # ELIMINADO LOGO POR SOLICITUD DEL USUARIO
        # try:
        #    logo_path = os.path.join(os.getcwd(), "Logo.png")
        #    if os.path.exists(logo_path):
        #        logo_img = Image(logo_path, width=2.0 * inch, height=0.75 * inch)
        #        logo_img.hAlign = 'LEFT'
        # except Exception:
        #    pass
            
        # User requested using image colors (#2a9d8f Green, #e9c46a Sand, #2AB4EB Blue)
        # Adding color to the header text
        title_text = [
            Paragraph(f"<font color='#2a9d8f'><b>Empresa ID: {empresa_id}</b></font> <font color='#e9c46a'>·</font> Generado: {datetime.now():%d/%m/%Y %H:%M}", styles['CustomMeta'])
        ]
        
        # Calculate available width
        available_width = (page_width - 1.0 * inch) if page_width else (7.5 * inch)
        
        if logo_img:
            col_widths = [2.2 * inch, available_width - 2.2 * inch]
            data = [[logo_img, title_text]]
        else:
            col_widths = [available_width]
            data = [[title_text]]
            
        t = Table(data, colWidths=col_widths)
        t.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 0),
            ('RIGHTPADDING', (0,0), (-1,-1), 0),
            ('TOPPADDING', (0,0), (-1,-1), 40),
            ('BOTTOMPADDING', (0,0), (-1,-1), 20),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.6 * inch))

    def _cover(self, story, styles, empresa_id):
        # Deprecated for single page, kept for reference if needed
        self._cover_compact(story, styles, empresa_id)

    def _metric_row(self, story, styles, metrics, page_width=None):
        # Create a single table row with multiple columns (one per metric)
        cells = []
        for val, label, sublabel, color in metrics:
            # Inner table for the cell content: Circle | Text Stack
            # SCALED FOR 3020px (approx 5x)
            d = Drawing(180, 180)
            d.add(Circle(90, 90, 85, fillColor=color, strokeColor=color))
            # Center the value inside the circle
            # Adjust font size based on value length to fit in circle
            fsize = 48 if len(str(val)) <= 3 else 42
            d.add(String(90, 90 - (fsize/3), str(val), fillColor=colors.white, textAnchor='middle', fontSize=fsize, fontName='Helvetica-Bold'))
            
            # Text stack: Label \n Sublabel
            # Use smaller font for sublabel
            text_col = []
            text_col.append(Paragraph(f"<b>{label}</b>", styles['CompactBody']))
            if sublabel:
                text_col.append(Paragraph(f"<font color='#666666' size=35>{sublabel}</font>", styles['CompactBody']))
            
            # Combine Drawing and Text in a nested table
            # Adjusted Column widths to prevent overlap: 
            # Circle Column (3.0 inch) + Text Column (6.0 inch)
            cell_content = Table([[d, text_col]], colWidths=[3.0*inch, 6.0*inch])
            cell_content.setStyle(TableStyle([
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                ('LEFTPADDING', (0,0), (-1,-1), 0),   # Padding for Circle cell
                ('RIGHTPADDING', (0,0), (-1,-1), 0),
                ('LEFTPADDING', (1,0), (1,0), 20),    # Extra padding for Text cell to separate from circle
                ('TOPPADDING', (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 0),
            ]))
            cells.append(cell_content)

        # Main wrapper table
        # Calculate width per cell based on page width
        # Letter width = 8.5 inch. Margins = 0.5 left + 0.5 right = 1.0. Content = 7.5 inch.
        # 4 metrics -> 1.8 inch per cell approx
        available_width = (page_width - 1.0 * inch) if page_width else (7.5 * inch)
        col_w = available_width / len(cells)
        t = Table([cells], colWidths=[col_w] * len(cells))
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), colors.white),
            ('BOX', (0,0), (-1,-1), 1.0, colors.HexColor('#e5e7eb')),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 16),
            ('RIGHTPADDING', (0,0), (-1,-1), 16),
            ('TOPPADDING', (0,0), (-1,-1), 25), # Increased padding to prevent circle clipping
            ('BOTTOMPADDING', (0,0), (-1,-1), 25), # Increased padding
        ]))
        story.append(t)

    def _kpi_panel(self, story, styles, df_all, empresa_id, df_clientes, df_proveedores, df_empleados, entidades_cruces=None, page_width=None):
        df = df_all[df_all.get('id_empresa', pd.Series()).astype(str) == str(empresa_id)]
        total = len(df)
        s = df.get('riesgo', pd.Series()).astype(str).str.upper()
        bajo, medio, alto = [(s == x).sum() for x in ['BAJO', 'MEDIO', 'ALTO']]
        
        cruces_count = 0
        try:
            if entidades_cruces:
                 # Use precomputed data
                 dist = entidades_cruces.get('distribucion', {})
                 # Sum values to get total count
                 cruces_count = sum(int(v) for v in dist.values())
            else:
                analytics = CrucesAnalytics(df_clientes, df_proveedores, df_empleados)
                df_cruces = analytics.procesar_datos()
                cruces_count = len(df_cruces)
        except Exception:
            cruces_count = 0
        
        score5 = 0.0
        if total > 0:
            score_map = {'BAJO': 2.0, 'MEDIO': 3.0, 'ALTO': 4.5}
            score5 = round(((bajo*score_map['BAJO'] + medio*score_map['MEDIO'] + alto*score_map['ALTO'])/total), 1)
        
        pct_alto = round((alto / max(total, 1)) * 100, 1)
        
        story.append(Paragraph("Panel Ejecutivo de Distribución de Riesgo", styles['CustomH1Compact']))
        story.append(Spacer(1, 0.4 * inch)) # Added spacer to prevent overlap with KPI box
        
        # Helper for formatting numbers
        def fmt(n):
            return f"{n:,.0f}".replace(",", ".")
            
        # Helper for percentages
        def fmt_pct(n):
            return f"{n}".replace(".", ".") # Keep dot as decimal separator as per user request (4.2%)

        # Metrics format: (Value, Main Label, Sub Label, Color)
        self._metric_row(story, styles, [
            (fmt(total), 'Total Registros', 'Analizados', COLORS['secondary']), # Green
            (fmt(cruces_count), 'Contrapartes', 'con Cruces', COLORS['warning']), # Yellow
            (f"{score5}", 'Riesgo Promedio', 'Escala 1-5', COLORS['secondary']), # Green
            (fmt(alto), 'Alto riesgo', f"{fmt_pct(pct_alto)}% del total", '#e9c46a'), # Sand/Yellow (Replacing Danger/Red)
        ], page_width=page_width)
        story.append(Spacer(1, 0.5 * inch)) # SCALED Spacer

    def _risk_region_country(self, story, styles, df_all, empresa_id):
        # Contenedor para KeepInFrame
        frame_content = []
        
        # Título
        frame_content.append(Paragraph("Geografía del Riesgo (Jurisdicciones)", styles['CustomH1Compact']))
        
        df = df_all[df_all.get('id_empresa', pd.Series()).astype(str) == str(empresa_id)]
        
        # Filtrar Alto Riesgo
        s = df.get('riesgo', pd.Series()).astype(str).str.upper()
        df = df.assign(_riesgo=s)
        altos = df[df['_riesgo'] == 'ALTO']
        
        # --- Mapa Colombia ---
        dept_counts = {}
        if not altos.empty and 'departamento' in altos.columns:
            counts = altos['departamento'].fillna('SIN INFORMACION').value_counts()
            dept_counts = {str(k): int(v) for k, v in counts.items() if k != 'SIN INFORMACION'}
        
        img_colombia = None
        try:
            res = map_image_service.colombia_risk_map(dept_counts, empresa_id)
            if res and res.get('base64'):
                img_bytes = base64.b64decode(res['base64'].split(",", 1)[1])
                bio = io.BytesIO(img_bytes)
                # Tamaño ajustado para encajar y ser igual al mapa mundial
                img_colombia = Image(bio, width=3.4 * inch, height=3.4 * inch)
        except Exception:
            pass
            
        # --- Mapa Mundial ---
        fatf_status = {}
        clas_col = 'pais_clasificacion' if 'pais_clasificacion' in df.columns else None
        for _, r in df.iterrows():
            p = r.get('pais')
            if pd.notna(p):
                 status = str(r.get(clas_col, 'COOPERANTE')).upper() if clas_col else 'COOPERANTE'
                 fatf_status[str(p)] = status
        
        img_world = None
        if fatf_status:
            try:
                res = map_image_service.world_fatf_map(fatf_status)
                if res and res.get('base64'):
                    img_bytes = base64.b64decode(res['base64'].split(",", 1)[1])
                    bio = io.BytesIO(img_bytes)
                    # Tamaño ajustado igual al de Colombia
                    img_world = Image(bio, width=3.4 * inch, height=3.4 * inch)
            except Exception:
                pass

        # --- Tabla Layout ---
        # Columna 1: Nacional
        c1 = [Paragraph("<b>Nacional (Deptos)</b>", styles['CompactBody']), Spacer(1, 0.05 * inch)]
        if img_colombia:
            c1.append(img_colombia)
        else:
            c1.append(Paragraph("Sin datos.", styles['CompactBody']))
            
        # Columna 2: Internacional
        c2 = [Paragraph("<b>Internacional</b>", styles['CompactBody']), Spacer(1, 0.05 * inch)]
        if img_world:
            c2.append(img_world)
        else:
            c2.append(Paragraph("Sin datos.", styles['CompactBody']))
        
        # Top texto
        txt_dept = "Sin concentraciones."
        if dept_counts:
            sorted_depts = sorted(dept_counts.items(), key=lambda item: item[1], reverse=True)[:3]
            txt_dept = ", ".join([f"{k} ({v})" for k, v in sorted_depts])
            
        txt_pais = "Sin concentraciones."
        if not altos.empty and 'pais' in altos.columns:
            grp_pais = altos['pais'].fillna('SIN INFORMACION').value_counts().head(3)
            txt_pais = ", ".join([f"{k} ({v})" for k, v in grp_pais.items()])
            
        # Agregar resumen bajo mapa mundial
        c2.append(Spacer(1, 0.1*inch))
        c2.append(Paragraph(f"<b>Top Colombia:</b> {txt_dept}", styles['CompactBodySmall']))
        c2.append(Paragraph(f"<b>Top Mundo:</b> {txt_pais}", styles['CompactBodySmall']))

        tbl_data = [[c1, c2]]
        t = Table(tbl_data, colWidths=[3.5*inch, 3.5*inch])
        t.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'TOP'),
            ('LEFTPADDING', (0,0), (-1,-1), 0),
            ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ]))
        frame_content.append(t)
        
        # Wrapper
        protected = KeepInFrame(
            maxWidth=7.5 * inch,
            maxHeight=4.5 * inch,
            content=frame_content,
            mode="shrink"
        )
        story.append(protected)
        story.append(Spacer(1, 0.1 * inch))

    def _entity_crosses_section_compact(self, story, styles, empresa_id, df_clientes, df_proveedores, df_empleados):
        story.append(Paragraph("Cruces de Entidades (Colusión)", styles['CustomH1Compact']))
        try:
            analytics = CrucesAnalytics(df_clientes, df_proveedores, df_empleados)
            df_cruces = analytics.procesar_datos()
            dist = analytics.get_distribucion_riesgo()
            total_cruces = len(df_cruces)
            
            # 1. Metricas en una fila
            self._metric_row(story, styles, [
                (str(total_cruces), 'Entidades', 'con Cruces', COLORS['info']),
                (str(dist.get('alto', 0)), 'Riesgo Alto', 'Alerta', COLORS['danger']),
                (str(dist.get('medio', 0)), 'Riesgo Medio', 'Seguimiento', COLORS['warning']),
            ])
            story.append(Spacer(1, 0.1 * inch))
            
            # 2. Gráfico (opcional, si cabe) o Tabla de Top Cruces
            # Intentamos poner el gráfico pero muy bajito (wide)
            gen = CrucesGraphGenerator(analytics)
            b64 = gen.generate_composite_dashboard_chart()
            if isinstance(b64, str) and b64.startswith("data:image/png;base64,"):
                img_bytes = base64.b64decode(b64.split(",", 1)[1])
                bio = io.BytesIO(img_bytes)
                img = Image(bio, width=7.0 * inch, height=1.8 * inch) # Altura reducida
                story.append(img)
                
        except Exception:
            story.append(Paragraph("No hay información de cruces disponible.", styles['CompactBody']))
        story.append(Spacer(1, 0.1 * inch))



    def _geographic_maps(self, story, styles, df_all, empresa_id):
        # 1. Colombia Map
        story.append(Paragraph("Mapa de Riesgo: Colombia", styles['CustomH1']))
        df = df_all[df_all['id_empresa'] == empresa_id]
        
        # Prepare points for Colombia map
        points = []
        for _, row in df.iterrows():
            # Check if lat/lon exists (or similar fields)
            lat = row.get('lat') or row.get('latitud')
            lon = row.get('lon') or row.get('longitud')
            if pd.notna(lat) and pd.notna(lon):
                 points.append({
                    'lat': lat,
                    'lon': lon,
                    'monto': row.get('valor_transaccion', 0),
                    'riesgo': row.get('riesgo', 'BAJO')
                 })
        
        if points:
            try:
                res = map_image_service.colombia_empresa_map(points, empresa_id)
                if res and res.get('base64'):
                    img_bytes = base64.b64decode(res['base64'].split(",", 1)[1])
                    bio = io.BytesIO(img_bytes)
                    # SCALED: 30x30 inch
                    img = Image(bio, width=30.0 * inch, height=30.0 * inch)
                    story.append(img)
            except Exception as e:
                story.append(Paragraph(f"No se pudo generar el mapa de Colombia: {str(e)}", styles['CompactBody']))
        else:
             story.append(Paragraph("No hay datos geográficos suficientes para el mapa de Colombia.", styles['CompactBody']))

        story.append(Spacer(1, 1.0 * inch)) # SCALED Spacer

        # 2. World Map
        story.append(Paragraph("Mapa Mundial: Jurisdicciones y Cooperación", styles['CustomH1']))
        fatf_status = {}
        # Columns might be 'pais', 'pais_clasificacion'
        clas_col = 'pais_clasificacion' if 'pais_clasificacion' in df.columns else None
        
        for _, r in df.iterrows():
            p = r.get('pais')
            if pd.notna(p):
                 # Default to COOPERANTE if column missing
                 status = str(r.get(clas_col, 'COOPERANTE')).upper() if clas_col else 'COOPERANTE'
                 fatf_status[str(p)] = status
        
        if fatf_status:
            try:
                res = map_image_service.world_fatf_map(fatf_status)
                if res and res.get('base64'):
                    img_bytes = base64.b64decode(res['base64'].split(",", 1)[1])
                    bio = io.BytesIO(img_bytes)
                    # SCALED: 35x17.5 inch
                    img = Image(bio, width=35.0 * inch, height=17.5 * inch)
                    story.append(img)
            except Exception as e:
                story.append(Paragraph(f"No se pudo generar el mapa mundial: {str(e)}", styles['CompactBody']))
        else:
             story.append(Paragraph("No hay información de países para el mapa mundial.", styles['CompactBody']))
        
        story.append(Spacer(1, 1.0 * inch)) # SCALED Spacer

    def _ai_recommendations_compact(
        self, story, styles, empresa_id, df_alto, df_clientes, df_proveedores, df_empleados
    ):
        story.append(Paragraph("Recomendaciones (IA)", styles['CompactH1']))
        try:
            cruces_total = 0
            cruces_dist = {"alto": 0, "medio": 0, "bajo": 0}
            try:
                cruz_analytics = CrucesAnalytics(df_clientes, df_proveedores, df_empleados)
                df_cruces = cruz_analytics.procesar_datos()
                cruces_total = int(len(df_cruces))
                cruces_dist = cruz_analytics.get_distribucion_riesgo()
            except Exception:
                cruces_total = 0
                cruces_dist = {"alto": 0, "medio": 0, "bajo": 0}
            kpis = {
                "total_transacciones": int(len(df_alto)) if df_alto is not None else 0,
                "monto_total": float(df_alto.get('valor_transaccion', pd.Series([0]*len(df_alto))).sum()) if df_alto is not None else 0.0,
                "cruces_total": cruces_total,
                "cruces_riesgo_alto": int(cruces_dist.get("alto", 0) or 0),
                "cruces_riesgo_medio": int(cruces_dist.get("medio", 0) or 0),
                "cruces_riesgo_bajo": int(cruces_dist.get("bajo", 0) or 0),
            }
            fatf_status = {}
            if df_alto is not None and 'pais' in df_alto.columns:
                clas_col = 'pais_clasificacion' if 'pais_clasificacion' in df_alto.columns else None
                for _, r in df_alto.iterrows():
                    p = r.get('pais')
                    if pd.notna(p):
                        fatf_status[str(p)] = str(r.get(clas_col, 'COOPERANTE')).upper() if clas_col else 'COOPERANTE'
            ai_report = local_ai_report_service.generate_report({
                "empresa_id": empresa_id,
                "kpis": kpis,
                "fatf_status": fatf_status,
                "chart_data": {}
            })
            text = ai_report.get("report", {}).get("sections", {}).get("recommendations", "")
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            bullets = [ln for ln in lines if ln[:2].isdigit() or ln[:1].isdigit()]
            if not bullets:
                bullets = lines
            truncated = bullets[:3]
            story.append(Paragraph("• " + "<br/>• ".join(truncated), styles['CompactBody']))
        except Exception:
            story.append(Paragraph("No fue posible generar recomendaciones automáticas.", styles['CompactBody']))
        story.append(Spacer(1, 0.15 * inch))

    def _alerts_table_compact(self, story, styles, df_alto, empresa_id):
        story.append(Paragraph("Alertas Detectadas", styles['CompactH1']))
        try:
            if df_alto is None or df_alto.empty:
                story.append(Paragraph("<b>Excelente:</b> No se han detectado transacciones que superen los umbrales de riesgo configurados. Esto indica un comportamiento transaccional saludable según los criterios actuales.", styles['CompactBody']))
                return
            cols_map = {
                'ID': ['id_transaccion', 'id_tx', 'tx_id'],
                'Empresa': ['empresa', 'id_empresa'],
                'NIT': ['nit', 'num_id', 'no_documento_de_identidad'],
                'CIIU': ['ciiu'],
                'Actividad': ['actividad', 'ciiu_descripcion'],
                'Departamento': ['departamento'],
                'Monto': ['valor_transaccion', 'monto'],
                'Tipo': ['tipo_contraparte']
            }
            def pick(row, keys):
                for k in keys:
                    v = row.get(k)
                    if pd.notna(v):
                        return v
                return ""
            data = [["ID", "Empresa", "NIT", "CIIU", "Actividad", "Departamento", "Monto", "Tipo"]]
            for _, r in df_alto.head(5).iterrows():
                data.append([
                    str(pick(r, cols_map['ID'])),
                    str(pick(r, cols_map['Empresa'])),
                    str(pick(r, cols_map['NIT'])),
                    str(pick(r, cols_map['CIIU'])),
                    str(pick(r, cols_map['Actividad'])),
                    str(pick(r, cols_map['Departamento'])),
                    f"${float(pick(r, cols_map['Monto']) or 0):,.2f}",
                    str(pick(r, cols_map['Tipo']))
                ])
            
            # SCALED WIDTHS: Total ~38-40 inches
            t = Table(data, repeatRows=1, colWidths=[
                2.6*inch, 5.2*inch, 3.9*inch, 2.6*inch, 10.4*inch, 5.2*inch, 5.2*inch, 5.2*inch
            ])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), COLORS['light']),
                ('TEXTCOLOR', (0,0), (-1,0), COLORS['secondary']),
                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE', (0,0), (-1,0), 40), # SCALED
                ('FONTSIZE', (0,1), (-1,-1), 40), # SCALED
                ('GRID', (0,0), (-1,-1), 2.0, colors.HexColor('#dddddd')), # SCALED
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fbfbfb')]),
                ('TOPPADDING', (0,0), (-1,-1), 10), # SCALED
                ('BOTTOMPADDING', (0,0), (-1,-1), 10), # SCALED
                ('LEFTPADDING', (0,0), (-1,-1), 10),
                ('RIGHTPADDING', (0,0), (-1,-1), 10),
            ]))
            story.append(t)
        except Exception:
            story.append(Paragraph("No fue posible construir la tabla de alertas.", styles['CompactBody']))
        story.append(Spacer(1, 0.1 * inch))

    def _dd_missing_table_compact(self, story, styles, df_all, empresa_id):
        story.append(Paragraph("Transacciones sin Debida Diligencia", styles['CompactH1']))
        try:
            df = df_all[df_all.get('id_empresa', pd.Series()).astype(str) == str(empresa_id)].copy()
            if df.empty:
                story.append(Paragraph("No hay transacciones para la empresa.", styles['CompactBody']))
                return
            def motivos(row):
                m = []
                if str(row.get('tipo_de_relacion_contratista_proveedor', '')).upper() == 'SIN INFO':
                    m.append('Relación proveedor SIN INFO')
                if str(row.get('localizacion_nacional_internacional', '')).upper() == 'SIN INFO':
                    m.append('Localización SIN INFO')
                if pd.isna(row.get('pais_clasificacion')):
                    m.append('País sin clasificación FATF')
                if pd.isna(row.get('ciiu')):
                    m.append('CIIU sin registro')
                id_prov = row.get('no_documento_de_identidad')
                id_cli = row.get('num_id')
                if pd.isna(id_prov) and pd.isna(id_cli):
                    m.append('Identificación faltante')
                if pd.isna(row.get('medio_pago')):
                    m.append('Medio de pago faltante')
                tipo_pers = row.get('tipo_persona')
                tipo_pers_alt = row.get('tipo_persona_natural_juridica_estatal_mixta')
                if pd.isna(tipo_pers) and pd.isna(tipo_pers_alt):
                    m.append('Tipo de persona faltante')
                return m
            records = []
            for _, r in df.iterrows():
                ms = motivos(r)
                if ms:
                    records.append((r, ms))
            if not records:
                story.append(Paragraph("No se detectaron transacciones con debida diligencia incompleta.", styles['CompactBody']))
                return
            records = sorted(records, key=lambda x: float(x[0].get('valor_transaccion', x[0].get('monto', 0)) or 0), reverse=True)[:5]
            def pick(row, keys):
                for k in keys:
                    v = row.get(k)
                    if pd.notna(v):
                        return v
                return ""
            cols_map = {
                'ID': ['id_transaccion', 'id_tx', 'tx_id'],
                'Empresa': ['empresa', 'id_empresa'],
                'NIT': ['nit', 'num_id', 'no_documento_de_identidad'],
                'CIIU': ['ciiu'],
                'Monto': ['valor_transaccion', 'monto'],
                'Tipo': ['tipo_contraparte']
            }
            data = [["ID", "Empresa", "NIT", "CIIU", "Monto", "Tipo", "Motivo"]]
            for row, ms in records:
                data.append([
                    str(pick(row, cols_map['ID'])),
                    str(pick(row, cols_map['Empresa'])),
                    str(pick(row, cols_map['NIT'])),
                    str(pick(row, cols_map['CIIU'])),
                    f"${float(pick(row, cols_map['Monto']) or 0):,.2f}",
                    str(pick(row, cols_map['Tipo'])),
                    "; ".join(ms)
                ])
            
            # SCALED WIDTHS: Total ~39.5 inches
            t = Table(data, repeatRows=1, colWidths=[
                2.5*inch, 5.5*inch, 4.0*inch, 2.5*inch, 5.5*inch, 5.5*inch, 14.0*inch
            ])
            t.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), COLORS['light']),
                ('TEXTCOLOR', (0,0), (-1,0), COLORS['secondary']),
                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE', (0,0), (-1,0), 40), # SCALED
                ('FONTSIZE', (0,1), (-1,-1), 40), # SCALED
                ('GRID', (0,0), (-1,-1), 2.0, colors.HexColor('#dddddd')), # SCALED
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fbfbfb')]),
                ('TOPPADDING', (0,0), (-1,-1), 10), # SCALED
                ('BOTTOMPADDING', (0,0), (-1,-1), 10), # SCALED
                ('LEFTPADDING', (0,0), (-1,-1), 10),
                ('RIGHTPADDING', (0,0), (-1,-1), 10),
            ]))
            story.append(t)
        except Exception:
            story.append(Paragraph("No fue posible construir la sección de debida diligencia.", styles['CompactBody']))
        story.append(Spacer(1, 0.1 * inch))

    def _entity_crosses_table(self, story, styles, detalles, empresa_id):
        story.append(Paragraph("Detalle de Cruces Detectados (Top 10)", styles['CompactH1']))
        
        if not detalles:
            story.append(Paragraph("No hay detalles disponibles.", styles['CompactBody']))
            return

        # Headers - SCALED for 3020px
        headers = ["ID Entidad", "Roles", "Trans. Cliente", "Trans. Proveedor", "Pagos Empleado"]
        data = [headers]
        
        # Ordenar por riesgo y luego por monto total
        detalles.sort(key=lambda x: (x.get('riesgo_maximo', 0), 
                                     (x.get('cliente', {}) or {}).get('suma', 0) + 
                                     (x.get('proveedor', {}) or {}).get('suma', 0) + 
                                     (x.get('empleado', {}) or {}).get('suma', 0)), 
                      reverse=True)

        for d in detalles[:10]: # Limitar a 10
            id_entidad = d['id_contraparte']
            cats = d['conteo_categorias']
            
            # Helper para formatear celdas con detalle (Cant, Monto, Riesgo)
            def fmt_details(val_dict):
                # Ahora val_dict siempre existe, verificamos la cantidad
                cant = val_dict.get('cantidad', 0)
                
                if cant == 0:
                    # Estilo N/A: Cant: 0 y badge N/A
                    # SCALED FONTS
                    line1 = f"<font size=35 color='#555555'>Cant: 0</font>"
                    line2 = f"<font size=35 color='#663399' backcolor='#f3e5f5'><b> N/A </b></font>" # Purple style for N/A (Purple is OK, user only banned Red)
                    return Paragraph(f"{line1}<br/>{line2}", styles['CompactBody'])
                
                suma = val_dict.get('suma', 0)
                riesgo = val_dict.get('riesgo', 0)
                
                # Obtener lista de transacciones
                tx_list = val_dict.get('transacciones', [])
                tx_str = ""
                if tx_list:
                    # Formatear valores
                    formatted_tx = [f"${float(v):,.0f}" for v in tx_list[:3]]
                    tx_str = "<br/>".join(formatted_tx)
                    if len(tx_list) > 3:
                        tx_str += f"<br/><i>(+{len(tx_list)-3} más)</i>"
                    # SCALED FONTS
                    tx_str = f"<font size=30 color='#666666'>{tx_str}</font><br/>"

                # Color y texto de riesgo
                # User Palette: Green (#2a9d8f), Sand (#e9c46a), Blue (#2AB4EB)
                risk_color = "#2a9d8f" # Verde (Bajo)
                risk_text = f"Riesgo {riesgo}"
                
                # Normalizar riesgo para lógica de color
                r_val = 0
                try:
                    r_val = int(riesgo)
                except:
                    if str(riesgo).upper() in ['ALTO', 'CRITICO', '5', '4']: r_val = 5
                    elif str(riesgo).upper() in ['MEDIO', '3']: r_val = 3
                
                if r_val >= 4:
                    risk_color = "#e9c46a" # Sand/Yellow (Replacing Red)
                    risk_text = f"ALTO ({riesgo})"
                elif r_val == 3:
                    risk_color = "#2AB4EB" # Blue (Replacing Orange)

                # Construir contenido
                # Checkmark + Cantidad
                # SCALED FONTS
                line1 = f"<font color='#2a9d8f' size=40>✓</font> <font size=35 color='#555555'>Cant: {cant}</font>"
                # Monto
                line2 = f"<font size=40><b>${suma:,.0f}</b></font>"
                # Badge de Riesgo (Texto coloreado)
                line3 = f"<font size=35 color='{risk_color}'><b>{risk_text}</b></font>"
                
                return Paragraph(f"{line1}<br/>{tx_str}{line2}<br/>{line3}", styles['CompactBody'])

            cli_cell = fmt_details(d.get('cliente'))
            prov_cell = fmt_details(d.get('proveedor'))
            emp_cell = fmt_details(d.get('empleado'))
            
            # Estilo para ID (Negrita si es riesgo alto)
            id_style = styles['CompactBodySmall']
            
            data.append([
                Paragraph(str(id_entidad), id_style),
                str(cats),
                cli_cell,
                prov_cell,
                emp_cell
            ])

        # Estilo de tabla
        # Ancho total disponible aprox 28-30 inch for 3020px width
        # Previous: [1.8*inch, 0.7*inch, 1.6*inch, 1.6*inch, 1.6*inch] (Total ~7.3)
        # Scaled x4 approx: [7.2, 2.8, 6.4, 6.4, 6.4]
        # Let's adjust to fit 3020px (approx 41 inches total width, minus margins ~2 inches = 39 inches)
        # Let's use: 9, 3.5, 8, 8, 8 = 36.5 inches
        t = Table(data, colWidths=[9.0*inch, 3.5*inch, 8.0*inch, 8.0*inch, 8.0*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), COLORS['secondary']),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('ALIGN', (0,0), (1,0), 'LEFT'), 
            ('ALIGN', (2,0), (-1,-1), 'LEFT'), # Alinear detalles a la izquierda
            ('VALIGN', (0,0), (-1,-1), 'TOP'), # Alinear arriba
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 45), # SCALED Header Font
            ('BOTTOMPADDING', (0,0), (-1,0), 30), # SCALED Padding
            ('TOPPADDING', (0,0), (-1,0), 30),
            ('GRID', (0,0), (-1,-1), 2.5, colors.HexColor('#e0e0e0')), # SCALED Grid
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f9f9f9')]),
            ('LEFTPADDING', (0,0), (-1,-1), 20), # SCALED Padding
            ('RIGHTPADDING', (0,0), (-1,-1), 20),
            ('TOPPADDING', (0,1), (-1,-1), 20), # SCALED Padding
            ('BOTTOMPADDING', (0,1), (-1,-1), 20),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.1 * inch))

    def _styles(self):
        s = getSampleStyleSheet()
        # Ensure Helvetica (Standard Sans-Serif) is used to match the clean look
        # User requested Green (#2a9d8f) for titles instead of Primary (Pink)
        title_color = COLORS['secondary'] # #2a9d8f
        
        # SCALED FOR 3020px WIDTH (approx 5x)
        s.add(ParagraphStyle('CustomTitle', fontName='Helvetica-Bold', fontSize=80, alignment=TA_CENTER, textColor=title_color, spaceAfter=80, leading=96))
        s.add(ParagraphStyle('CustomTitleCompact', fontName='Helvetica-Bold', fontSize=72, alignment=TA_LEFT, textColor=title_color, leading=90))
        s.add(ParagraphStyle('CustomH1', fontName='Helvetica-Bold', fontSize=64, textColor=title_color, spaceBefore=80, spaceAfter=40))
        s.add(ParagraphStyle('CustomH1Compact', fontName='Helvetica-Bold', fontSize=54, textColor=title_color, spaceBefore=20, spaceAfter=20))
        s.add(ParagraphStyle('CustomBody', fontName='Helvetica', fontSize=44, alignment=TA_JUSTIFY, leading=62))
        s.add(ParagraphStyle('CustomMeta', fontName='Helvetica', fontSize=40, alignment=TA_LEFT, textColor=COLORS['gray']))
        s.add(ParagraphStyle('CompactH1', fontName='Helvetica-Bold', fontSize=54, textColor=title_color, spaceBefore=40, spaceAfter=24))
        s.add(ParagraphStyle('CompactBody', fontName='Helvetica', fontSize=40, alignment=TA_JUSTIFY, leading=58))
        s.add(ParagraphStyle('CompactBodySmall', fontName='Helvetica', fontSize=36, alignment=TA_LEFT, leading=48, textColor=COLORS['dark']))
        s.add(ParagraphStyle('FooterText', fontName='Helvetica', fontSize=40, alignment=TA_CENTER, textColor=COLORS['gray'], leading=50))
        return s

    # --------------------
    # Stubs (mantén tus implementaciones reales)
    # --------------------
    def _load_dataframes(self, empresa_id: Optional[int] = None):
        try:
            db = SourceSessionLocal()
            
            # 1. Cargar clientes
            query_cli = db.query(Cliente)
            if empresa_id:
                query_cli = query_cli.filter(Cliente.id_empresa == empresa_id)
            df_cli = pd.read_sql(query_cli.statement, db.bind)
            
            # 2. Cargar proveedores
            query_pro = db.query(Proveedor)
            if empresa_id:
                query_pro = query_pro.filter(Proveedor.id_empresa == empresa_id)
            df_pro = pd.read_sql(query_pro.statement, db.bind)
            
            # 3. Cargar empleados
            query_emp = db.query(Empleado)
            if empresa_id:
                query_emp = query_emp.filter(Empleado.id_empresa == empresa_id)
            df_emp = pd.read_sql(query_emp.statement, db.bind)
            
            # 4. Cargar CIIU Referencia
            df_ciiu_ref = pd.read_sql(db.query(AuxiliarCiiu).statement, db.bind)
            
            db.close()
            
            # Merge CIIU Description
            if not df_ciiu_ref.empty:
                # Prepare ref df: ciiu (code) -> descripcion
                # Ensure code is string and stripped
                if 'ciiu' in df_ciiu_ref.columns:
                    df_ciiu_ref['ciiu'] = df_ciiu_ref['ciiu'].astype(str).str.strip()
                    
                    # Merge for Cliente
                    if not df_cli.empty and 'ciiu' in df_cli.columns:
                         df_cli['ciiu'] = df_cli['ciiu'].astype(str).str.strip()
                         df_cli = pd.merge(df_cli, df_ciiu_ref[['ciiu', 'descripcion']], on='ciiu', how='left')
                         df_cli.rename(columns={'descripcion': 'ciiu_descripcion'}, inplace=True)

                    # Merge for Proveedor (if column exists)
                    if not df_pro.empty and 'ciiu' in df_pro.columns:
                         df_pro['ciiu'] = df_pro['ciiu'].astype(str).str.strip()
                         df_pro = pd.merge(df_pro, df_ciiu_ref[['ciiu', 'descripcion']], on='ciiu', how='left')
                         df_pro.rename(columns={'descripcion': 'ciiu_descripcion'}, inplace=True)
            
            # Normalizar columnas
            if not df_cli.empty and 'tipo_contraparte' not in df_cli.columns:
                df_cli['tipo_contraparte'] = 'cliente'
            if not df_pro.empty and 'tipo_contraparte' not in df_pro.columns:
                df_pro['tipo_contraparte'] = 'proveedor'
            if not df_emp.empty and 'tipo_contraparte' not in df_emp.columns:
                df_emp['tipo_contraparte'] = 'empleado'
            
            if not df_emp.empty:
                if 'valor' in df_emp.columns and 'valor_transaccion' not in df_emp.columns:
                    df_emp = df_emp.rename(columns={'valor': 'valor_transaccion'})
                # Riesgo desde conteo_alto si existe
                if 'conteo_alto' in df_emp.columns and 'riesgo' not in df_emp.columns:
                    df_emp['riesgo'] = df_emp['conteo_alto'].astype(str).str.upper().map({
                        'ALTO': 'ALTO',
                        'MEDIO': 'MEDIO',
                        'BAJO': 'BAJO'
                    }).fillna('BAJO')
            
            # Unificar clientes y proveedores para analisis general
            df_all = pd.concat([df_cli, df_pro], ignore_index=True, sort=False)
            
            # Asegurar columna riesgo
            if not df_all.empty and 'riesgo' not in df_all.columns:
                if 'nivel_riesgo' in df_all.columns:
                    df_all['riesgo'] = df_all['nivel_riesgo'].astype(str).str.upper()
                elif 'orden_clasificacion_del_riesgo' in df_all.columns:
                    def map_ord(x):
                        try:
                            v = int(x)
                        except Exception:
                            return 'BAJO'
                        if v >= 3:
                            return 'ALTO'
                        elif v == 2:
                            return 'MEDIO'
                        else:
                            return 'BAJO'
                    df_all['riesgo'] = df_all['orden_clasificacion_del_riesgo'].apply(map_ord)
                else:
                    df_all['riesgo'] = 'BAJO'
            
            # Asegurar columnas clave
            for d in (df_all, df_cli, df_pro, df_emp):
                if 'id_empresa' not in d.columns:
                    d['id_empresa'] = empresa_id if empresa_id else ''
            
            return df_all, df_cli, df_pro, df_emp
            
        except Exception as e:
            print(f"⚠️ Error loading dataframes from DB: {e}")
            # Fallback seguro
            cols = ['id_empresa', 'riesgo', 'valor_transaccion', 'tipo_contraparte']
            return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols), pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    def _filter_high_risk(self, df):
        if df.empty or 'riesgo' not in df.columns:
            return pd.DataFrame(columns=df.columns)
        return df[df['riesgo'].astype(str).str.upper() == 'ALTO']

    def _avg_score(self, df):
        return float(df.get('score', pd.Series()).mean() or 0)

    def _get_risk_level(self, score):
        return 'RIESGO ALTO' if score >= 2 else 'RIESGO BAJO'

    def _top_risk_causes(self, story, styles, df_all, empresa_id):
        story.append(Paragraph("Causas Principales de Riesgo", styles['CustomH1']))

    def _entity_crosses(self, story, styles, empresa_id, df_clientes, df_proveedores, df_empleados):
        story.append(Paragraph("Cruces de Entidades", styles['CustomH1']))
        try:
            analytics = CrucesAnalytics(df_clientes, df_proveedores, df_empleados)
            analytics.procesar_datos()
            gen = CrucesGraphGenerator(analytics)
            b64 = gen.generate_composite_dashboard_chart()
            if isinstance(b64, str) and b64.startswith("data:image/png;base64,"):
                img_bytes = base64.b64decode(b64.split(",", 1)[1])
                bio = io.BytesIO(img_bytes)
                img = Image(bio, width=6.5 * inch, height=2.0 * inch)
                story.append(img)
        except Exception:
            pass
        story.append(Spacer(1, 0.25 * inch))

    def _ai_recommendations(self, story, styles, empresa_id, df_alto):
        story.append(Paragraph("Recomendaciones (IA)", styles['CustomH1']))
        try:
            kpis = {
                "total_transacciones": int(len(df_alto)) if df_alto is not None else 0,
                "monto_total": float(df_alto.get('valor_transaccion', pd.Series([0]*len(df_alto))).sum()) if df_alto is not None else 0.0,
            }
            fatf_status = {}
            if df_alto is not None and 'pais' in df_alto.columns:
                clas_col = 'pais_clasificacion' if 'pais_clasificacion' in df_alto.columns else None
                for _, r in df_alto.iterrows():
                    p = r.get('pais')
                    if pd.notna(p):
                        fatf_status[str(p)] = str(r.get(clas_col, 'COOPERANTE')).upper() if clas_col else 'COOPERANTE'
            ai_report = local_ai_report_service.generate_report({
                "empresa_id": empresa_id,
                "kpis": kpis,
                "fatf_status": fatf_status,
                "chart_data": {}
            })
            text = ai_report.get("report", {}).get("sections", {}).get("recommendations", "")
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            bullets = [ln for ln in lines if ln[:2].isdigit() or ln[:1].isdigit()]
            if not bullets:
                bullets = lines
            truncated = bullets[:3]
            story.append(Paragraph("• " + "<br/>• ".join(truncated), styles['CustomBody']))
        except Exception:
            story.append(Paragraph("No fue posible generar recomendaciones automáticas.", styles['CustomBody']))
        story.append(Spacer(1, 0.25 * inch))

    def _alerts_table(self, story, styles, df_alto, empresa_id):
        story.append(Paragraph("Alertas Detectadas", styles['CustomH1']))
        try:
            if df_alto is None or df_alto.empty:
                story.append(Paragraph("<b>Excelente:</b> No se han detectado transacciones que superen los umbrales de riesgo configurados. Esto indica un comportamiento transaccional saludable según los criterios actuales.", styles['CustomBody']))
                return
            cols_map = {
                'ID': ['id_transaccion', 'id_tx', 'tx_id'],
                'Empresa': ['empresa', 'id_empresa'],
                'NIT': ['nit', 'num_id', 'no_documento_de_identidad'],
                'CIIU': ['ciiu'],
                'Actividad': ['actividad', 'ciiu_descripcion'],
                'Departamento': ['departamento'],
                'Monto': ['valor_transaccion', 'monto'],
                'Tipo': ['tipo_contraparte']
            }
            def pick(row, keys):
                for k in keys:
                    v = row.get(k)
                    if pd.notna(v):
                        return v
                return ""
            data = [["ID", "Empresa", "NIT", "CIIU", "Actividad", "Departamento", "Monto", "Tipo"]]
            for _, r in df_alto.head(5).iterrows():
                data.append([
                    str(pick(r, cols_map['ID'])),
                    str(pick(r, cols_map['Empresa'])),
                    str(pick(r, cols_map['NIT'])),
                    str(pick(r, cols_map['CIIU'])),
                    str(pick(r, cols_map['Actividad'])),
                    str(pick(r, cols_map['Departamento'])),
                    f"${float(pick(r, cols_map['Monto']) or 0):,.2f}",
                    str(pick(r, cols_map['Tipo']))
                ])
            t = Table(data, repeatRows=1)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), COLORS['light']),
                ('TEXTCOLOR', (0,0), (-1,0), COLORS['primary']),
                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE', (0,0), (-1,0), 10),
                ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#dddddd')),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fafafa')]),
            ]))
            story.append(t)
        except Exception:
            story.append(Paragraph("No fue posible construir la tabla de alertas.", styles['CustomBody']))

    def _dd_missing_table(self, story, styles, df_all, empresa_id):
        story.append(Paragraph("Transacciones sin Debida Diligencia", styles['CustomH1']))
        try:
            if df_all is None or df_all.empty:
                story.append(Paragraph("No hay transacciones disponibles.", styles['CustomBody']))
                return
            df = df_all[df_all.get('id_empresa', pd.Series()).astype(str) == str(empresa_id)].copy()
            if df.empty:
                story.append(Paragraph("No hay transacciones para la empresa.", styles['CustomBody']))
                return
            def motivos(row):
                m = []
                if str(row.get('tipo_de_relacion_contratista_proveedor', '')).upper() == 'SIN INFO':
                    m.append('Relación proveedor SIN INFO')
                if str(row.get('localizacion_nacional_internacional', '')).upper() == 'SIN INFO':
                    m.append('Localización SIN INFO')
                if pd.isna(row.get('pais_clasificacion')):
                    m.append('País sin clasificación FATF')
                if pd.isna(row.get('ciiu')):
                    m.append('CIIU sin registro')
                id_prov = row.get('no_documento_de_identidad')
                id_cli = row.get('num_id')
                if pd.isna(id_prov) and pd.isna(id_cli):
                    m.append('Identificación faltante')
                if pd.isna(row.get('medio_pago')):
                    m.append('Medio de pago faltante')
                tipo_pers = row.get('tipo_persona')
                tipo_pers_alt = row.get('tipo_persona_natural_juridica_estatal_mixta')
                if pd.isna(tipo_pers) and pd.isna(tipo_pers_alt):
                    m.append('Tipo de persona faltante')
                return m
            records = []
            for _, r in df.iterrows():
                ms = motivos(r)
                if ms:
                    records.append((r, ms))
            if not records:
                story.append(Paragraph("No se detectaron transacciones con debida diligencia incompleta.", styles['CustomBody']))
                return
            records = sorted(records, key=lambda x: float(x[0].get('valor_transaccion', x[0].get('monto', 0)) or 0), reverse=True)[:5]
            def pick(row, keys):
                for k in keys:
                    v = row.get(k)
                    if pd.notna(v):
                        return v
                return ""
            cols_map = {
                'ID': ['id_transaccion', 'id_tx', 'tx_id'],
                'Empresa': ['empresa', 'id_empresa'],
                'NIT': ['nit', 'num_id', 'no_documento_de_identidad'],
                'CIIU': ['ciiu'],
                'Monto': ['valor_transaccion', 'monto'],
                'Tipo': ['tipo_contraparte']
            }
            data = [["ID", "Empresa", "NIT", "CIIU", "Monto", "Tipo", "Motivo"]]
            for row, ms in records:
                data.append([
                    str(pick(row, cols_map['ID'])),
                    str(pick(row, cols_map['Empresa'])),
                    str(pick(row, cols_map['NIT'])),
                    str(pick(row, cols_map['CIIU'])),
                    f"${float(pick(row, cols_map['Monto']) or 0):,.2f}",
                    str(pick(row, cols_map['Tipo'])),
                    "; ".join(ms)
                ])
            t = Table(data, repeatRows=1)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), COLORS['light']),
                ('TEXTCOLOR', (0,0), (-1,0), COLORS['primary']),
                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE', (0,0), (-1,0), 10),
                ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#dddddd')),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fafafa')]),
            ]))
            story.append(t)
        except Exception:
            story.append(Paragraph("No fue posible construir la sección de debida diligencia.", styles['CustomBody']))
        story.append(Spacer(1, 0.2 * inch))

pdf_risk_report_service = PDFRiskReportService()
