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
from src.services.local_ai_report_service import local_ai_report_service
from src.services.map_image_service import map_image_service
from src.db.base import TargetSessionLocal
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.services.s3_service import s3_service

# --------------------
# Paleta corporativa (Basada en Logo)
# --------------------
COLORS = {
    'primary': colors.HexColor('#00a19c'),   # Turquesa Corporativo
    'secondary': colors.HexColor('#005fa3'), # Azul complementario
    'success': colors.HexColor('#2a9d8f'),
    'warning': colors.HexColor('#f0b323'),   # Amarillo Corporativo
    'danger': colors.HexColor('#e40046'),    # Magenta Corporativo
    'info': colors.HexColor('#457b9d'),
    'light': colors.HexColor('#f8f9fa'),
    'gray': colors.HexColor('#6c757d'),
    'dark': colors.HexColor('#343a40'),
    'purple': colors.HexColor('#7c3aed'),
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

        df_all, df_clientes, df_proveedores, df_empleados = self._load_dataframes()
        df_empresa = df_all[df_all['id_empresa'] == empresa_id].copy()

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
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, "wb") as f:
                    f.write(pdf_bytes)
                local_file = output_path
                print(f"✅ Copia local del PDF guardada en: {local_file}")
            except Exception as e:
                print(f"⚠️ No se pudo guardar copia local: {e}")

        # Intentar subir a S3
        filename = f"Reporte_{empresa_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        s3_url = s3_service.upload_file(pdf_bytes, filename)

        pdf_content_to_store = pdf_bytes
        if s3_url:
            virtual_path = s3_url
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

        doc = SimpleDocTemplate(
            output,
            pagesize=letter,
            leftMargin=0.5 * inch,
            rightMargin=0.5 * inch,
            topMargin=0.4 * inch,
            bottomMargin=0.4 * inch
        )

        styles = self._styles()
        story = []

        # ================= PAGE 1: EXECUTIVE SUMMARY & GEOGRAPHY =================
        # 1. Header Compacto (Logo + Título)
        self._cover_compact(story, styles, empresa_id)
        
        # 2. KPIs Globales
        self._kpi_panel(story, styles, df_all, empresa_id, df_clientes, df_proveedores, df_empleados)
        
        # 3. Mapas (Geografía) - Side by Side
        self._risk_region_country(story, styles, df_all, empresa_id)

        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph("<b>Interpretación de Gráficas:</b> El <b>Mapa de Colombia</b> destaca regiones con mayor concentración de operaciones riesgosas. El <b>Mapa Mundial</b> indica la clasificación FATF de las contrapartes internacionales (ej. países no cooperantes).", styles['FooterText']))
        story.append(PageBreak())
        
        # ================= PAGE 2: ENTITY CROSSES ANALYSIS =================
        # 4. Cruces de Entidades (Full Page)
        self._entity_crosses_page_full(story, styles, empresa_id, df_clientes, df_proveedores, df_empleados, entidades_cruces)

        # Footer Simple
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph("<b>Interpretación de Gráficas:</b> <b>Gráfica Circular:</b> Muestra la proporción de los diferentes tipos de conflictos (ej. Cliente-Proveedor). <b>Dashboard:</b> Panel detallado con indicadores de severidad (Gauges) y barras de progreso por tipología.", styles['FooterText']))
        story.append(Paragraph("<b>Nota:</b> Reporte elaborado con <b>uso de analítica especializada</b>; consolida la exposición geográfica y el riesgo de colusión.",
                               styles['FooterText']))
        
        doc.build(story)

    def _entity_crosses_page_full(self, story, styles, empresa_id, df_clientes, df_proveedores, df_empleados, entidades_cruces=None):
        story.append(Paragraph("Análisis de Cruces de Entidades (Colusión)", styles['CustomTitle']))
        story.append(Paragraph("Detección de cruces entre contrapartes (solo se detectan cruces).", styles['CustomMeta']))
        story.append(Spacer(1, 0.2 * inch))

        try:
            if entidades_cruces:
                # Use precomputed data
                dist = entidades_cruces.get('distribucion', {})
                tabla = entidades_cruces.get('tabla', [])
                total_cruces_calc = sum(int(v) for v in dist.values())
                
                # Check if we have graphs
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

            # 1. Metricas Principales (Row)
            self._metric_row(story, styles, [
                (str(total_cruces_calc), 'Entidades', 'con Cruces', COLORS['info']),
                (str(dist.get('alto', 0)), 'Riesgo Alto', 'Alerta Extrema', COLORS['danger']),
                (str(dist.get('medio', 0)), 'Riesgo Medio', 'Seguimiento', COLORS['warning']),
                (str(dist.get('bajo', 0)), 'Riesgo Bajo', 'Normal', COLORS['success']),
            ])
            story.append(Spacer(1, 0.2 * inch))
            
            if total_cruces_calc == 0:
                story.append(Paragraph("<b>Sin Hallazgos de Colusión:</b> No se detectaron entidades que actúen simultáneamente como clientes, proveedores y/o empleados. Esto sugiere una adecuada segregación de funciones.", styles['CustomBody']))
                story.append(Spacer(1, 0.2 * inch))
                return

            # 2. Gráfico Principal (Side-by-Side: Pie Chart + Texto)
            story.append(Paragraph("Distribución y Tipología de Riesgos", styles['CustomH1']))
            
            img_types = None
            if isinstance(b64_types, str) and b64_types.startswith("data:image/png;base64,"):
                img_bytes = base64.b64decode(b64_types.split(",", 1)[1])
                bio = io.BytesIO(img_bytes)
                img_types = Image(bio, width=3.4 * inch, height=2.4 * inch)
            
            if img_types:
                img_types.hAlign = 'CENTER'
                story.append(img_types)
            else:
                story.append(Paragraph("Sin datos de tipología.", styles['CompactBody']))
            
            story.append(Spacer(1, 0.1 * inch))
            
            # 3. Dashboard Moderno (Gauges + Progress Bars)
            if isinstance(b64_heat, str) and b64_heat.startswith("data:image/png;base64,"):
                story.append(Paragraph("Dashboard de Riesgos Detectados", styles['CustomH1']))
                
                story.append(Paragraph(
                    "<b>Guía de Visualización:</b> El gráfico de la izquierda (<i>Gauges</i>) muestra el porcentaje de entidades clasificadas en cada nivel de riesgo (Bajo, Medio, Alto). "
                    "El gráfico de la derecha detalla la <i>Tipología del Cruce</i>, indicando qué roles simultáneos están desempeñando las entidades detectadas (ej. Cliente que también es Proveedor).",
                    styles['CompactBody']
                ))
                story.append(Spacer(1, 0.05 * inch))

                img_bytes = base64.b64decode(b64_heat.split(",", 1)[1])
                bio = io.BytesIO(img_bytes)
                img_heat = Image(bio, width=7.5 * inch, height=2.6 * inch)
                img_heat.hAlign = 'CENTER'
                story.append(img_heat)

            story.append(Spacer(1, 0.2 * inch))

        except Exception as e:
            story.append(Paragraph(f"Error generando sección de cruces: {str(e)}", styles['CompactBody']))


    # --------------------
    # Secciones
    # --------------------
    def _cover_compact(self, story, styles, empresa_id):
        # Header en Tabla: Logo | Título + Meta
        logo_img = None
        try:
            logo_path = os.path.join(os.getcwd(), "Logo.png")
            if os.path.exists(logo_path):
                logo_img = Image(logo_path, width=2.0 * inch, height=0.75 * inch)
                logo_img.hAlign = 'LEFT'
        except Exception:
            pass
            
        title_text = [
            Paragraph("REPORTE EJECUTIVO DE RIESGO", styles['CustomTitleCompact']),
            Paragraph(f"Empresa ID: {empresa_id} · Generado: {datetime.now():%d/%m/%Y %H:%M}", styles['CustomMeta']),
            Paragraph("Detección de relaciones ocultas entre Clientes, Proveedores y Empleados.", styles['CustomMeta'])
        ]
        
        if logo_img:
            data = [[logo_img, title_text]]
            col_widths = [2.2 * inch, 5.0 * inch]
        else:
            data = [[title_text]]
            col_widths = [7.2 * inch]
            
        t = Table(data, colWidths=col_widths)
        t.setStyle(TableStyle([
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 0),
            ('RIGHTPADDING', (0,0), (-1,-1), 0),
            ('TOPPADDING', (0,0), (-1,-1), 0),
            ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.2 * inch))

    def _cover(self, story, styles, empresa_id):
        # Deprecated for single page, kept for reference if needed
        self._cover_compact(story, styles, empresa_id)

    def _metric_row(self, story, styles, metrics):
        # Create a single table row with multiple columns (one per metric)
        cells = []
        for val, label, sublabel, color in metrics:
            # Inner table for the cell content: Circle | Text Stack
            d = Drawing(40, 40)
            d.add(Circle(20, 20, 18, fillColor=color, strokeColor=color))
            # Center the value inside the circle
            # Adjust font size based on value length to fit in circle
            fsize = 11 if len(str(val)) <= 3 else 9
            d.add(String(20, 20 - (fsize/3), str(val), fillColor=colors.white, textAnchor='middle', fontSize=fsize, fontName='Helvetica-Bold'))
            
            # Text stack: Label \n Sublabel
            # Use smaller font for sublabel
            text_col = []
            text_col.append(Paragraph(f"<b>{label}</b>", styles['CompactBody']))
            if sublabel:
                text_col.append(Paragraph(f"<font color='#666666' size=7>{sublabel}</font>", styles['CompactBody']))
            
            # Combine Drawing and Text in a nested table
            # Column widths: Drawing (fixed), Text (flexible)
            cell_content = Table([[d, text_col]], colWidths=[0.6*inch, 1.2*inch])
            cell_content.setStyle(TableStyle([
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                ('LEFTPADDING', (0,0), (-1,-1), 0),
                ('RIGHTPADDING', (0,0), (-1,-1), 0),
                ('TOPPADDING', (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 0),
            ]))
            cells.append(cell_content)

        # Main wrapper table
        # Calculate width per cell based on page width
        # Letter width = 8.5 inch. Margins = 0.5 left + 0.5 right = 1.0. Content = 7.5 inch.
        # 4 metrics -> 1.8 inch per cell approx
        col_w = 1.8 * inch
        t = Table([cells], colWidths=[col_w] * len(cells))
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), colors.white),
            ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), # Outer border
            ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), # Vertical separators
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 5),
            ('RIGHTPADDING', (0,0), (-1,-1), 5),
            ('TOPPADDING', (0,0), (-1,-1), 8),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ]))
        story.append(t)

    def _kpi_panel(self, story, styles, df_all, empresa_id, df_clientes, df_proveedores, df_empleados, entidades_cruces=None):
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
        
        # Metrics format: (Value, Main Label, Sub Label, Color)
        self._metric_row(story, styles, [
            (str(total), 'Total Registros', 'Analizados', COLORS['secondary']), # Blue
            (str(cruces_count), 'Contrapartes', 'con Cruces', COLORS['warning']), # Yellow
            (f"{score5}", 'Riesgo Promedio', 'Escala 1-5', COLORS['purple']), # Purple
            (f"{alto}", 'Alto riesgo', f"{pct_alto}% del total", COLORS['danger']), # Red
        ])
        story.append(Spacer(1, 0.15 * inch))

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
                    img = Image(bio, width=6.0 * inch, height=6.0 * inch)
                    story.append(img)
            except Exception as e:
                story.append(Paragraph(f"No se pudo generar el mapa de Colombia: {str(e)}", styles['CompactBody']))
        else:
             story.append(Paragraph("No hay datos geográficos suficientes para el mapa de Colombia.", styles['CompactBody']))

        story.append(Spacer(1, 0.2 * inch))

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
                    # World map is wider
                    img = Image(bio, width=7.0 * inch, height=3.5 * inch)
                    story.append(img)
            except Exception as e:
                story.append(Paragraph(f"No se pudo generar el mapa mundial: {str(e)}", styles['CompactBody']))
        else:
             story.append(Paragraph("No hay información de países para el mapa mundial.", styles['CompactBody']))
        
        story.append(Spacer(1, 0.2 * inch))

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
            t = Table(data, repeatRows=1)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), COLORS['light']),
                ('TEXTCOLOR', (0,0), (-1,0), COLORS['primary']),
                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE', (0,0), (-1,0), 8),
                ('FONTSIZE', (0,1), (-1,-1), 8),
                ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#dddddd')),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fbfbfb')]),
                ('TOPPADDING', (0,0), (-1,-1), 2),
                ('BOTTOMPADDING', (0,0), (-1,-1), 2),
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
            t = Table(data, repeatRows=1)
            t.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), COLORS['light']),
                ('TEXTCOLOR', (0,0), (-1,0), COLORS['primary']),
                ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                ('FONTSIZE', (0,0), (-1,0), 8),
                ('FONTSIZE', (0,1), (-1,-1), 8),
                ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#dddddd')),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fbfbfb')]),
                ('TOPPADDING', (0,0), (-1,-1), 2),
                ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ]))
            story.append(t)
        except Exception:
            story.append(Paragraph("No fue posible construir la sección de debida diligencia.", styles['CompactBody']))
        story.append(Spacer(1, 0.1 * inch))

    def _entity_crosses_table(self, story, styles, detalles, empresa_id):
        story.append(Paragraph("Detalle de Cruces Detectados (Top 20)", styles['CompactH1']))
        
        if not detalles:
            story.append(Paragraph("No hay detalles disponibles.", styles['CompactBody']))
            return

        # Headers
        headers = ["ID Entidad", "Roles", "Trans. Cliente", "Trans. Proveedor", "Pagos Empleado"]
        data = [headers]
        
        # Ordenar por riesgo y luego por monto total
        detalles.sort(key=lambda x: (x.get('riesgo_maximo', 0), 
                                     (x.get('cliente', {}) or {}).get('suma', 0) + 
                                     (x.get('proveedor', {}) or {}).get('suma', 0) + 
                                     (x.get('empleado', {}) or {}).get('suma', 0)), 
                      reverse=True)

        for d in detalles[:20]: # Limitar a 20 para que quepa
            id_entidad = d['id_contraparte']
            cats = d['conteo_categorias']
            
            # Helper para formatear montos y manejar NaN
            def fmt_monto(val_dict):
                if not val_dict:
                    return "—"
                m = val_dict.get('suma', 0)
                if pd.isna(m):
                    m = 0
                return f"${m:,.0f}"

            cli_str = fmt_monto(d.get('cliente'))
            prov_str = fmt_monto(d.get('proveedor'))
            emp_str = fmt_monto(d.get('empleado'))
            
            # Estilo para ID (Negrita si es riesgo alto)
            id_style = styles['CompactBodySmall']
            
            data.append([
                Paragraph(str(id_entidad), id_style),
                str(cats),
                cli_str,
                prov_str,
                emp_str
            ])

        # Estilo de tabla
        # Ancho total disponible aprox 7.5 inch
        t = Table(data, colWidths=[2.0*inch, 0.8*inch, 1.5*inch, 1.5*inch, 1.5*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), COLORS['primary']),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('ALIGN', (0,0), (1,0), 'LEFT'), # Headers left (except ID/Roles maybe?)
            ('ALIGN', (2,1), (-1,-1), 'RIGHT'), # Montos a la derecha
            ('ALIGN', (0,1), (1,-1), 'LEFT'), # ID y Roles a la izquierda
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 9),
            ('BOTTOMPADDING', (0,0), (-1,0), 6),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e0e0e0')),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f9f9f9')]),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(t)
        story.append(Spacer(1, 0.2 * inch))

    def _styles(self):
        s = getSampleStyleSheet()
        # Ensure Helvetica (Standard Sans-Serif) is used to match the clean look
        s.add(ParagraphStyle('CustomTitle', fontName='Helvetica-Bold', fontSize=18, alignment=TA_CENTER, textColor=COLORS['primary'], spaceAfter=20, leading=22))
        s.add(ParagraphStyle('CustomTitleCompact', fontName='Helvetica-Bold', fontSize=16, alignment=TA_LEFT, textColor=COLORS['primary'], leading=20))
        s.add(ParagraphStyle('CustomH1', fontName='Helvetica-Bold', fontSize=14, textColor=COLORS['primary'], spaceBefore=20, spaceAfter=10))
        s.add(ParagraphStyle('CustomH1Compact', fontName='Helvetica-Bold', fontSize=12, textColor=COLORS['primary'], spaceBefore=5, spaceAfter=5))
        s.add(ParagraphStyle('CustomBody', fontName='Helvetica', fontSize=10, alignment=TA_JUSTIFY, leading=14))
        s.add(ParagraphStyle('CustomMeta', fontName='Helvetica', fontSize=9, alignment=TA_LEFT, textColor=COLORS['gray']))
        s.add(ParagraphStyle('CompactH1', fontName='Helvetica-Bold', fontSize=12, textColor=COLORS['primary'], spaceBefore=10, spaceAfter=6))
        s.add(ParagraphStyle('CompactBody', fontName='Helvetica', fontSize=9, alignment=TA_JUSTIFY, leading=12))
        s.add(ParagraphStyle('CompactBodySmall', fontName='Helvetica', fontSize=8, alignment=TA_LEFT, leading=10, textColor=COLORS['dark']))
        s.add(ParagraphStyle('FooterText', fontName='Helvetica', fontSize=8, alignment=TA_CENTER, textColor=COLORS['gray'], leading=10))
        return s

    # --------------------
    # Stubs (mantén tus implementaciones reales)
    # --------------------
    def _load_dataframes(self):
        try:
            base = "data_provisional"
            cli_path = os.path.join(base, "datos prueba clientes.csv")
            pro_path = os.path.join(base, "datos prueba proveedores.csv")
            emp_path = os.path.join(base, "datos prueba.csv")
            df_cli = pd.read_csv(cli_path) if os.path.exists(cli_path) else pd.DataFrame()
            df_pro = pd.read_csv(pro_path) if os.path.exists(pro_path) else pd.DataFrame()
            df_emp = pd.read_csv(emp_path) if os.path.exists(emp_path) else pd.DataFrame()
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
            # Unificar clientes y proveedores
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
                    d['id_empresa'] = ''
            return df_all, df_cli, df_pro, df_emp
        except Exception:
            # Fallback seguro
            cols = ['id_empresa', 'riesgo', 'valor_transaccion', 'tipo_contraparte']
            return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols), pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    def _filter_high_risk(self, df):
        return df[df.get('riesgo', '').astype(str).str.upper() == 'ALTO']

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
