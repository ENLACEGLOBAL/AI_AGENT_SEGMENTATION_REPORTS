# PDF Risk Report Service (Refactor Profesional)
# Diseño ejecutivo en 3 páginas: Visión / Decisión / Análisis
# Autor: Refactor técnico

import json
import os
import base64
import io
import hashlib
import time
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import exc, text

from cryptography.fernet import Fernet
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    PageBreak, Image, KeepInFrame
)
from reportlab.lib.utils import ImageReader
from reportlab.graphics.shapes import Drawing, Circle, String, Rect
from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
from src.analytics_modules.cruces_entidades.cruces_graph_generator import CrucesGraphGenerator

from src.services.local_ai_report_service import local_ai_report_service
from src.db.base import TargetSessionLocal
from src.db.repositories.generated_report_repo import GeneratedReportRepository
from src.services.s3_service import s3_service
from src.db.base import SourceSessionLocal
from src.db.models.cliente import Cliente
from src.db.models.proveedor import Proveedor
from src.db.models.empleado import Empleado
from src.db.models.reference_tables import AuxiliarCiiu

# --------------------
# Paleta corporativa (User Requested: Teal, Orange, Blue)
# --------------------
COLORS = {
    'primary': colors.HexColor('#009688'),   # Teal
    'secondary': colors.HexColor('#1565C0'), # Blue
    'success': colors.HexColor('#009688'),   # Green/Teal
    'warning': colors.HexColor('#FF9800'),   # Orange
    'danger': colors.HexColor('#e40046'),    # Red (used for high risk dots)
    'info': colors.HexColor('#00BCD4'),      # Cyan
    'light': colors.HexColor('#f8f9fa'),
    'gray': colors.HexColor('#6c757d'),
    'dark': colors.HexColor('#333333'),
}


class PDFRiskReportService:
    """Generador profesional de reportes PDF de riesgo (nivel corporativo)."""

    def generate_pdf_report(
        self,
        analytics_json_path: Optional[str] = None,
        analytics_data: Optional[Dict[str, Any]] = None,
        tipo_contraparte: str = "cliente",
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Punto de entrada principal para generar el reporte.
        """
        analytics = {}
        if analytics_data:
            analytics = analytics_data
        elif analytics_json_path:
            with open(analytics_json_path, 'r', encoding='utf-8') as f:
                analytics = json.load(f)
        else:
            raise ValueError("Must provide either analytics_json_path or analytics_data")

        empresa_id = analytics.get('empresa_id')
        if not empresa_id:
            raise ValueError("empresa_id no encontrado en la analítica")

        # 1. Generar en memoria
        buffer = io.BytesIO()
        self._build_executive_pdf(buffer, empresa_id, analytics)
        pdf_bytes = buffer.getvalue()
        buffer.close()

        # 2. Guardar localmente si se solicita (DESHABILITADO PARA PRODUCCIÓN/DOCKER)
        local_file = None
        # if output_path:
        #     try:
        #         dirpath = os.path.dirname(output_path)
        #         if dirpath: os.makedirs(dirpath, exist_ok=True)
        #         with open(output_path, "wb") as f: f.write(pdf_bytes)
        #         local_file = output_path
        #         print(f"✅ Reporte guardado localmente: {local_file}")
        #     except Exception as e:
        #         print(f"⚠️ Error al guardar localmente: {e}")

        # 3. Subir a S3 y guardar en DB
        filename = f"Reporte_Riesgo_{empresa_id}_{datetime.now():%Y%m%d_%H%M%S}.pdf"
        s3_key = f"reports/{filename}"
        s3_url = s3_service.upload_file(pdf_bytes, s3_key)
        
        virtual_path = s3_key if s3_url else f"DB_STORED:{filename}"
        self._save_to_db(empresa_id, virtual_path, None if s3_url else pdf_bytes)

        return {
            'status': 'success',
            'file': virtual_path,
            'empresa_id': empresa_id,
            'local_file': local_file
        }

    def _get_company_name_from_db(self, empresa_id):
        """Intenta obtener el nombre de la empresa desde la base de datos (fallback)."""
        try:
            db = TargetSessionLocal()
            # No existe tabla 'empresas' o 'users' con company_id en este esquema.
            # Intentamos deducirlo de las tablas de datos (clientes/proveedores) 
            # asumiendo que el campo 'nombre' NO es el de la empresa, sino de la contraparte.
            # Sin embargo, en sistemas multi-tenant a veces se guarda el nombre de la empresa padre en otra tabla.
            
            # Si no hay tabla de empresas, devolvemos un string genérico o intentamos buscar en una tabla de configuración si existiera.
            # Dado el esquema actual, NO hay una tabla central de empresas visible.
            # Retornaremos "Empresa ID: {id}" si no se encuentra nada mejor, 
            # o intentamos buscar en una tabla 'usuarios_empresas' si existiera en el futuro.
            
            # Hack temporal: A veces se guarda en una tabla de metadatos o se pasa por contexto.
            # Si no está, devolvemos None para que el caller use el ID.
            
            db.close()
        except Exception as e:
            print(f"⚠️ Error buscando nombre empresa: {e}")
        return None

    def _build_executive_pdf(self, output, empresa_id, data):
        doc = SimpleDocTemplate(output, pagesize=A4, 
                                leftMargin=50, rightMargin=50, topMargin=50, bottomMargin=50)
        styles = self._styles()
        story = []

        # --- DATOS PARA EL REPORTE ---
        kpis = data.get('kpis', {})
        total_reg = int(data.get('total_transacciones') or kpis.get('total_registros') or 0)
        cruces_count = int(kpis.get('entidades_cruces') or 0)
        pct_cruces = float(kpis.get('porcentaje_cruces') or 0.0)
        riesgo_prom = float(kpis.get('riesgo_promedio') or 0.0)
        
        periodo = data.get('filtros', {}).get('fecha') or datetime.now().strftime("%d/%m/%Y")
        
        # Nombre de la empresa: Prioridad Data > DB > Fallback
        # IMPORTANTE: El PHP debe enviar 'empresa_nombre' o 'company_name' en el JSON.
        # Si no lo envía, se intentará buscar en BD, pero si falla, mostrará ID.
        empresa_nombre = data.get('empresa_nombre') or data.get('company_name')
        if not empresa_nombre or str(empresa_nombre).strip() == "" or str(empresa_nombre).startswith("Empresa ID:"):
            # Intento de fallback si el nombre no viene en el JSON
            db_name = self._get_company_name_from_db(empresa_id)
            if db_name:
                empresa_nombre = db_name
            elif not empresa_nombre: # Solo poner ID si realmente no tenemos nada
                empresa_nombre = f"Empresa ID: {empresa_id}"
        
        # Tipos de cruces para la gráfica horizontal
        tipos = data.get('tipos_cruces', {})
        cli_pro = int(tipos.get('cliente_proveedor') or 0)
        pro_emp = int(tipos.get('proveedor_empleado') or 0)
        cli_emp = int(tipos.get('cliente_empleado') or 0)
        triple = int(tipos.get('triple_relacion') or 0)

        # Determinar el tipo de cruce con mayor concentración para el análisis dinámico
        counts = {
            'Cliente – Proveedor': cli_pro,
            'Proveedor – Empleado': pro_emp,
            'Cliente – Empleado': cli_emp,
            'Triple Relación': triple
        }
        max_type = max(counts, key=counts.get) if any(counts.values()) else "Ninguna"

        # Sin DD
        sin_dd_total = int(data.get('transacciones_sin_dd_total') or 0)
        sin_dd_list = data.get('transacciones_sin_dd') or []
        sin_dd_top50 = sorted(sin_dd_list, key=lambda x: float(x.get('monto') or 0), reverse=True)[:50]

        # --- HEADER ---
        try:
            logo_path = None
            possible_paths = [
                os.path.join(os.getcwd(), "Logo.png"), # Local/Dev
                "/app/Logo.png", # Docker
                "Logo.png" # Relative
            ]
            
            for p in possible_paths:
                if os.path.exists(p):
                    logo_path = p
                    break
            
            if logo_path:
                # Calcular aspect ratio para que se vea bien proporcionado
                img_reader = ImageReader(logo_path)
                iw, ih = img_reader.getSize()
                aspect = ih / float(iw)
                
                # Definir ancho objetivo y calcular alto
                target_width = 1.8 * inch
                target_height = target_width * aspect
                
                # Si el alto es demasiado grande, limitar por alto
                if target_height > 0.8 * inch:
                    target_height = 0.8 * inch
                    target_width = target_height / aspect
                
                logo = Image(logo_path, width=target_width, height=target_height)
                logo.hAlign = 'LEFT'
                story.append(logo)
                story.append(Spacer(1, 10))
            else:
                print("⚠️ Logo no encontrado en ninguna ruta probable")
        except Exception as e:
            print(f"⚠️ Error cargando logo: {e}")

        story.append(Paragraph("INFORME", styles['ReportTitle']))
        story.append(Paragraph("INFORME EJECUTIVO", styles['ReportSubtitle']))
        story.append(Paragraph("Análisis de Relaciones Cruzadas – Plataforma Riesgos 365", styles['ReportDesc']))
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"<b>Empresa:</b> {empresa_nombre}", styles['NormalText']))
        story.append(Paragraph(f"<b>Periodo evaluado:</b> {periodo}", styles['NormalText']))
        story.append(Spacer(1, 20))

        # --- 1. PANORAMA GENERAL ---
        story.append(Paragraph("1. Panorama General", styles['Heading1']))
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"Durante el periodo evaluado se analizaron <b>{total_reg:,}</b> registros correspondientes a clientes, proveedores y empleados.".replace(",", "."), styles['NormalText']))
        story.append(Spacer(1, 8))
        story.append(Paragraph(f"Se identificaron <b>{cruces_count:,}</b> contrapartes con relaciones cruzadas, equivalentes al <b>{pct_cruces:.2f}%</b> del total analizado.".replace(".", ","), styles['NormalText']))
        story.append(Spacer(1, 8))
        
        riesgo_label = "bajo-medio" if riesgo_prom < 2.5 else "medio-alto"
        story.append(Paragraph(f"El nivel de riesgo promedio identificado fue de <b>{riesgo_prom:.1f}</b> sobre 5, ubicándose en un rango {riesgo_label} dentro de la escala institucional. Este promedio se calcula con base en la columna de alertas de la base de datos, consolidando la totalidad de las alertas de riesgo detectadas para cada contraparte analizada.".replace(".", ","), styles['NormalText']))
        story.append(Spacer(1, 20))

        # --- 2. DISTRIBUCIÓN ESTRATÉGICA (BARRA HORIZONTAL LATERAL) ---
        story.append(Paragraph("2. Distribución Estratégica de Relaciones Cruzadas", styles['Heading1']))
        story.append(Spacer(1, 10))

        # Crear gráfica horizontal lateral
        chart_buf = self._generate_horizontal_bars(cli_pro, pro_emp, cli_emp, triple)
        img_chart = Image(chart_buf, width=4.5*inch, height=2.2*inch)
        
        # Texto de análisis dinámico
        analysis_text = [
            Paragraph("<b>Análisis Ejecutivo:</b>", styles['NormalText']),
            Paragraph(f"• La mayor concentración se presenta en relaciones {max_type}.", styles['BulletText']),
            Paragraph("• Los casos de Triple Relación, aunque menos frecuentes, representan el mayor nivel de exposición por potencial conflicto estructural.", styles['BulletText']),
            Paragraph("• Las relaciones múltiples incrementan el riesgo de favorecimiento indebido o pérdida de independencia en la toma de decisiones.", styles['BulletText'])
        ]
        
        # Tabla para poner gráfica a la izquierda y texto a la derecha
        table_data = [[img_chart, analysis_text]]
        t = Table(table_data, colWidths=[4.6*inch, 2.4*inch])
        t.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'TOP'), ('LEFTPADDING', (1,0), (1,0), 10)]))
        story.append(t)
        story.append(Spacer(1, 20))

        # --- 3. EVALUACIÓN ESTRATÉGICA DEL RIESGO ---
        story.append(Paragraph("3. Evaluación Estratégica del Riesgo", styles['Heading1']))
        story.append(Spacer(1, 10))
        story.append(Paragraph("Se identifican tres niveles de análisis:", styles['NormalText']))
        story.append(Spacer(1, 8))
        story.append(Paragraph("🟢 <b>Riesgo Estadístico: Bajo</b>", styles['NormalText']))
        story.append(Paragraph("Porcentaje reducido frente al total de la población analizada.", styles['IndentText']))
        story.append(Spacer(1, 5))
        story.append(Paragraph("🟡 <b>Riesgo Operativo: Medio</b>", styles['NormalText']))
        story.append(Paragraph("Existencia de relaciones cruzadas con impacto potencial en procesos internos.", styles['IndentText']))
        story.append(Spacer(1, 5))
        story.append(Paragraph("🔴 <b>Riesgo Estratégico: Alto (casos específicos)</b>", styles['NormalText']))
        story.append(Paragraph("• Triple vinculación.", styles['IndentBullet']),)
        story.append(Paragraph("• Ausencia de debida diligencia.", styles['IndentBullet']))
        story.append(Paragraph("• Concentración económica relevante.", styles['IndentBullet']))
        story.append(Spacer(1, 20))

        # --- 4. SIN DEBIDA DILIGENCIA ---
        story.append(Paragraph("4. Contrapartes sin Debida Diligencia", styles['Heading1']))
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"Se identificaron <b>{sin_dd_total:,}</b> contrapartes que cumplen los criterios establecidos por la compañía para la aplicación de debida diligencia y que actualmente no cuentan con actualización vigente.".replace(",", "."), styles['NormalText']))
        story.append(Spacer(1, 10))
        story.append(Paragraph("🔵 <b>Descargar adjunto:</b> Tabla con contrapartes sin debida diligencia que cumplen con los criterios establecidos por la compañía para realizar debida diligencia.", styles['NormalText']))
        story.append(Spacer(1, 15))
        
        # Tabla Top 50
        story.append(Paragraph(f"Top 50 Transacciones sin DD por Monto:", styles['SmallHeading']))
        dd_data = [["NIT / ID", "Nombre / Entidad", "Monto", "Fecha", "Tipo"]]
        for r in sin_dd_top50:
            monto = f"${float(r.get('monto') or 0):,.0f}".replace(",", ".")
            dd_data.append([
                str(r.get('id') or ""),
                str(r.get('nombre') or "N/D").upper()[:30],
                monto,
                str(r.get('fecha') or "")[:10],
                str(r.get('tipo') or "").capitalize()
            ])
        
        t_dd = Table(dd_data, colWidths=[1.2*inch, 2.8*inch, 1.2*inch, 1.0*inch, 0.8*inch], repeatRows=1)
        t_dd.setStyle(TableStyle([
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
            ('BACKGROUND', (0,0), (-1,0), COLORS['light']),
            ('TEXTCOLOR', (0,0), (-1,0), COLORS['dark']),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,-1), 8),
            ('ALIGN', (2,1), (2,-1), 'RIGHT')
        ]))
        story.append(t_dd)
        story.append(PageBreak())

        # --- SECCIÓN 5 ELIMINADA (IMPACTO POTENCIAL) ---

        # --- 5. RECOMENDACIONES (Renumerado) ---
        story.append(Paragraph("5. Recomendaciones", styles['Heading1']))
        story.append(Spacer(1, 10))
        recs = [
            "1. Solicitar actualización inmediata de debida diligencia en los casos identificados.",
            "2. Exigir declaración formal de conflicto de interés en casos de doble o triple vinculación.",
            "3. Implementar monitoreo reforzado."
        ]
        for r in recs:
            story.append(Paragraph(r, styles['NormalText']))
            story.append(Spacer(1, 5))
        story.append(Spacer(1, 20))

        # --- 6. CONCLUSIÓN EJECUTIVA (Renumerado) ---
        story.append(Paragraph("6. Conclusión Ejecutiva", styles['Heading1']))
        story.append(Spacer(1, 10))
        story.append(Paragraph("El nivel general de exposición se mantiene controlado desde una perspectiva estadística; no obstante, existen casos puntuales que requieren atención prioritaria debido a su potencial impacto estratégico.", styles['NormalText']))
        story.append(Spacer(1, 8))
        story.append(Paragraph("Se recomienda mantener monitoreo continuo y fortalecer los mecanismos de prevención asociados a conflicto de interés.", styles['NormalText']))

        doc.build(story)

    def _generate_horizontal_bars(self, cli_pro, pro_emp, cli_emp, triple):
        labels = ['Cliente – Proveedor', 'Proveedor – Empleado', 'Cliente – Empleado', 'Triple Relación']
        values = [cli_pro, pro_emp, cli_emp, triple]
        
        # Ordenar de mayor a menor
        sorted_data = sorted(zip(labels, values), key=lambda x: x[1], reverse=False)
        labels, values = zip(*sorted_data)

        fig, ax = plt.subplots(figsize=(6, 3))
        bars = ax.barh(labels, values, color='#009688', height=0.6)
        
        ax.set_facecolor('white')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.set_xticks([]) # Sin eje X
        
        # Etiquetas de valores
        for bar in bars:
            width = bar.get_width()
            ax.text(width + 0.1, bar.get_y() + bar.get_height()/2, f'{int(width)}', 
                    va='center', fontweight='bold', color='#333333')

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf

    def _generate_comparison_chart(self, total, cruces):
        labels = ['Total Población', 'Casos con Cruces']
        values = [total, cruces]
        colors_comp = ['#1565C0', '#FF9800']

        fig, ax = plt.subplots(figsize=(6, 1.5))
        bars = ax.barh(labels, values, color=colors_comp, height=0.5)
        
        ax.set_xscale('log') # Escala logarítmica para que se vean ambos
        ax.set_facecolor('white')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.set_xticks([])
        
        for bar, val in zip(bars, values):
            ax.text(bar.get_width(), bar.get_y() + bar.get_height()/2, f' {val:,}'.replace(",", "."), 
                    va='center', fontweight='bold')

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        return buf

    def _styles(self):
        s = getSampleStyleSheet()
        # Eliminar estilos existentes si ya están en el stylesheet para evitar duplicados
        for style_name in ['ReportTitle', 'ReportSubtitle', 'ReportDesc', 'Heading1', 'NormalText', 'BulletText', 'IndentText', 'IndentBullet', 'SmallHeading']:
            if style_name in s:
                del s.byName[style_name]
        
        s.add(ParagraphStyle('ReportTitle', fontName='Helvetica-Bold', fontSize=20, textColor=COLORS['dark']))
        s.add(ParagraphStyle('ReportSubtitle', fontName='Helvetica-Bold', fontSize=16, textColor=COLORS['dark'], spaceBefore=10))
        s.add(ParagraphStyle('ReportDesc', fontName='Helvetica', fontSize=12, textColor=COLORS['gray']))
        s.add(ParagraphStyle('Heading1', fontName='Helvetica-Bold', fontSize=14, textColor=COLORS['primary'], spaceBefore=15, spaceAfter=10))
        s.add(ParagraphStyle('NormalText', fontName='Helvetica', fontSize=10, leading=14, alignment=TA_JUSTIFY))
        s.add(ParagraphStyle('BulletText', fontName='Helvetica', fontSize=10, leading=14, leftIndent=15, bulletIndent=5))
        s.add(ParagraphStyle('IndentText', fontName='Helvetica', fontSize=10, leading=12, leftIndent=25))
        s.add(ParagraphStyle('IndentBullet', fontName='Helvetica', fontSize=10, leading=12, leftIndent=35))
        s.add(ParagraphStyle('SmallHeading', fontName='Helvetica-Bold', fontSize=10, spaceBefore=10, spaceAfter=5))
        return s

    def _save_to_db(self, empresa_id: int, file_path: str, pdf_bytes: Optional[bytes]):
        try:
            enc_key = os.getenv('ENCRYPTION_KEY')
            if enc_key:
                cipher = Fernet(enc_key)
            else:
                jwt_secret = os.getenv('JWT_SECRET', 'super-secret')
                raw_key = hashlib.sha256(jwt_secret.encode()).digest()
                fernet_key = base64.urlsafe_b64encode(raw_key)
                cipher = Fernet(fernet_key)

            encrypted_path = cipher.encrypt(file_path.encode()).decode()
            db = SourceSessionLocal()
            try:
                repo = GeneratedReportRepository()
                repo.create_report(db, file_path=encrypted_path, company_id=empresa_id, pdf_content=pdf_bytes)
                print(f"✅ Reporte registrado en DB para empresa {empresa_id}")
            finally:
                db.close()
        except Exception as e:
            print(f"⚠️ Error guardando reporte en DB: {e}")

    def _load_dataframes(self, empresa_id):
        # Stub para compatibilidad si se llama externamente, 
        # pero ahora usamos analytics_data como fuente primaria
        return None, None, None, None

    def _filter_high_risk(self, df): return df
    def _avg_score(self, df): return 0.0
    def _get_risk_level(self, score): return "MEDIO"

# Singleton instance
pdf_risk_report_service = PDFRiskReportService()
