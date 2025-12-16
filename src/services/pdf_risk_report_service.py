"""
PDF Risk Report Service
Generates detailed PDF risk classification reports with weighted scoring
"""
import json
import pandas as pd
from typing import Dict, Any
from datetime import datetime
import os
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image, KeepInFrame
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.graphics.shapes import Drawing, Circle, String, Rect, Line


class PDFRiskReportService:
    """Service for generating PDF risk classification reports."""
    
    # Risk criteria weights (must sum to ~100%)
    CRITERIA_WEIGHTS = {
        'pais': 0.11,           # 11%
        'jurisdiccion': 0.06,   # 6%
        'ciiu': 0.10,           # 10%
        'tipo_persona': 0.05,   # 5%
        'medio_pago': 0.08,     # 8%
        'monto_efectivo': 0.10, # 10%
        'valor_10pct': 0.10,    # 10%
        'tipo_relacion': 0.08,  # 8%
        'localizacion': 0.09    # 9%
    }
    
    def generate_pdf_report(
        self,
        analytics_json_path: str,
        tipo_contraparte: str = "cliente",
        output_path: str = None
    ) -> Dict[str, Any]:
        """
        Generate PDF risk classification report.
        
        Args:
            analytics_json_path: Path to analytics JSON
            tipo_contraparte: "cliente" or "proveedor"
            output_path: Optional output path
            
        Returns:
            Dict with status and file path
        """
        try:
            # Load analytics
            with open(analytics_json_path, 'r', encoding='utf-8') as f:
                analytics = json.load(f)
            
            empresa_id = analytics.get('empresa_id')

            clientes_path = os.path.join("data_provisional", "datos prueba clientes.csv")
            proveedores_path = os.path.join("data_provisional", "datos prueba proveedores.csv")
            empleados_path = os.path.join("data_provisional", "datos prueba.csv")
            dfs = []
            df_clientes = pd.DataFrame()
            df_proveedores = pd.DataFrame()
            df_empleados = pd.DataFrame()
            if os.path.exists(clientes_path):
                df_clientes = pd.read_csv(clientes_path)
                df_clientes["tipo_contraparte"] = "cliente"
                dfs.append(df_clientes)
            if os.path.exists(proveedores_path):
                df_proveedores = pd.read_csv(proveedores_path)
                df_proveedores["tipo_contraparte"] = "proveedor"
                dfs.append(df_proveedores)
            if os.path.exists(empleados_path):
                df_empleados = pd.read_csv(empleados_path)
                df_empleados["tipo_contraparte"] = df_empleados.get("tipo_contraparte", "empleado")
            df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            
            # Filter for company and high risk
            df_empresa = df[df['id_empresa'] == empresa_id]

            # Enrich with reference tables for country classification
            try:
                from src.db.base import target_engine as engine
                from src.db.models.reference_tables import HistoricoPaises
                paises = pd.read_sql_table(HistoricoPaises.__tablename__, con=engine)
                paises = paises[['pais', 'clasificacion']].drop_duplicates()
                df_empresa = (
                    df_empresa.merge(paises, on='pais', how='left')
                    .rename(columns={'clasificacion': 'pais_clasificacion'})
                )
            except Exception:
                pass
            
            risk_col = 'nivel_riesgo' if 'nivel_riesgo' in df.columns else 'riesgo'
            
            # Normalize risk values and include NO COOPERANTE / PARAISO FISCAL as alto
            if not df_empresa.empty:
                riesgo_series = df_empresa[risk_col]
                if isinstance(riesgo_series, pd.DataFrame):
                    riesgo_series = riesgo_series.iloc[:, 0]
                riesgo_series = riesgo_series.astype(str).str.upper()
                df_empresa[risk_col] = riesgo_series
                pais_is_high = False
                if 'pais_clasificacion' in df_empresa.columns:
                    tmp = df_empresa['pais_clasificacion']
                    if isinstance(tmp, pd.DataFrame):
                        tmp = tmp.iloc[:, 0]
                    pais_col = tmp.astype(str).str.upper()
                    pais_is_high = pais_col.isin(['NO COOPERANTE', 'PARAISO FISCAL'])
                riesgo_is_high = riesgo_series.isin(['ALTO', 'EXTREMO', 'CRITICO'])
                mask = riesgo_is_high | (pais_is_high if isinstance(pais_is_high, pd.Series) else False)
                df_alto = df_empresa[mask]
            else:
                df_alto = pd.DataFrame()
            
            # If no high risk, we still generate a report but with "No Alerts" status
            # if df_alto.empty:
            #     return {
            #         'status': 'error',
            #         'message': f'No high-risk transactions for company {empresa_id}'
            #     }
            
            # Calculate risk scores for each transaction
            risk_scores = []
            for _, row in df_alto.iterrows():
                score = self._calculate_risk_score(row)
                risk_scores.append(score)
            
            # Get average risk score
            avg_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0.0
            
            # Determine risk level
            if df_alto.empty:
                risk_level = "RIESGO BAJO (SIN ALERTAS)"
            else:
                risk_level = self._get_risk_level(avg_score)
            
            # Generate PDF
            if output_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                from os import makedirs
                from os.path import join
                makedirs("data_provisional/reports", exist_ok=True)
                output_path = join("data_provisional", "reports", f"pdf_risk_report_{tipo_contraparte}_{empresa_id}_{timestamp}.pdf")
            
            self._create_pdf(
                output_path=output_path,
                empresa_id=empresa_id,
                tipo_contraparte=tipo_contraparte,
                df_alto=df_alto,
                avg_score=avg_score,
                risk_level=risk_level,
                high_risk_count=len(df_alto),
                analytics=analytics,
                df_all=df,
                df_clientes=df_clientes,
                df_proveedores=df_proveedores,
                df_empleados=df_empleados
            )
            
            return {
                'status': 'success',
                'file_path': output_path,
                'empresa_id': empresa_id,
                'risk_level': risk_level,
                'avg_score': round(avg_score, 2),
                'high_risk_count': len(df_alto)
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def _calculate_risk_score(self, row: pd.Series) -> float:
        """Calculate weighted risk score for a transaction."""
        score = 0.0
        
        # País (11%)
        if row.get('pais_clasificacion') == 'NO COOPERANTE':
            score += self.CRITERIA_WEIGHTS['pais'] * 4  # Extremo
        elif row.get('pais_clasificacion') == 'PARAISO FISCAL':
            score += self.CRITERIA_WEIGHTS['pais'] * 4  # Extremo
        else:
            score += self.CRITERIA_WEIGHTS['pais'] * float(row.get('valor_riesgo_pais', 0))
        
        # Jurisdicción (6%)
        score += self.CRITERIA_WEIGHTS['jurisdiccion'] * float(row.get('valor_jurisdicciones', 0))
        
        # CIIU (10%)
        score += self.CRITERIA_WEIGHTS['ciiu'] * float(row.get('valor_riesgo_ciiu', 0))
        
        # Tipo persona (5%)
        score += self.CRITERIA_WEIGHTS['tipo_persona'] * float(row.get('valor_riesgo_tipo_persona', 0))
        
        # Medio de pago (8%)
        score += self.CRITERIA_WEIGHTS['medio_pago'] * float(row.get('valor_riesgo_medio_pago', 0))
        
        # Monto + Efectivo (10%)
        score += self.CRITERIA_WEIGHTS['monto_efectivo'] * float(row.get('valor_riesgo_montos', 0))
        
        # Valor > 10% promedio (10%)
        score += self.CRITERIA_WEIGHTS['valor_10pct'] * float(row.get('valor_riesgo_valor_mas_10pct', 0))
        
        # Tipo relación (8%) - For providers
        if 'valor_riesgo_relacion' in row:
            score += self.CRITERIA_WEIGHTS['tipo_relacion'] * float(row.get('valor_riesgo_relacion', 0))
        
        # Localización (9%) - For providers
        if 'valor_riesgo_localizacion' in row:
            score += self.CRITERIA_WEIGHTS['localizacion'] * float(row.get('valor_riesgo_localizacion', 0))
        
        return score
    
    def _get_risk_level(self, score: float) -> str:
        """Determine risk level from score."""
        if score >= 3.0:
            return "RIESGO EXTREMO"
        elif score >= 2.0:
            return "RIESGO ALTO"
        elif score >= 1.0:
            return "RIESGO MEDIO"
        else:
            return "RIESGO BAJO"
    
    def _create_pdf(
        self,
        output_path: str,
        empresa_id: int,
        tipo_contraparte: str,
        df_alto: pd.DataFrame,
        avg_score: float,
        risk_level: str,
        high_risk_count: int,
        analytics: Dict[str, Any],
        df_all: pd.DataFrame,
        df_clientes: pd.DataFrame | None = None,
        df_proveedores: pd.DataFrame | None = None,
        df_empleados: pd.DataFrame | None = None
    ):
        """Create the PDF document with AI narratives."""
        from src.services.local_ai_report_service import local_ai_report_service
        import matplotlib.pyplot as plt
        import os
        import io
        import base64
        from src.analytics_modules.cruces_entidades.cruces_analytics import CrucesAnalytics
        from src.analytics_modules.cruces_entidades.cruces_graph_generator import CrucesGraphGenerator
        
        # Generate AI Content
        # Create synthetic analytics for the AI service
        ai_input = {
            "empresa_id": empresa_id,
            "kpis": {
                "total_transacciones": len(df_alto),
                "monto_total": float(df_alto['valor_transaccion'].sum()),
                "empresas_involucradas": df_alto['num_id'].nunique() if 'num_id' in df_alto else 0
            },
            "fatf_status": {row['pais']: row.get('pais_clasificacion', 'UNKNOWN') for _, row in df_alto.iterrows() if pd.notna(row.get('pais'))}
        }
        ai_report = local_ai_report_service.generate_report(ai_input)
        ai_sections = ai_report['report']['sections']

        doc = SimpleDocTemplate(output_path, pagesize=letter)
        story = []
        styles = getSampleStyleSheet()
        
        # Custom styles
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#1b263b'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        
        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#415a77'),
            spaceAfter=12,
            spaceBefore=12
        )
        
        normal_style = styles['Normal']
        
        # Company logo
        try:
            logo_path = r"C:\Users\Usuario\AI_AGENT_SEGMENTATION_REPORTS\Logo.png"
            if os.path.exists(logo_path):
                story.append(Image(logo_path, width=2.8*inch, height=0.8*inch))
                story.append(Spacer(1, 0.15*inch))
        except Exception:
            pass
        
        story.append(Paragraph(
            "REPORTE DE CLASIFICACIÓN DE RIESGO Y ALERTA EXTREMA (CLIENTES Y PROVEEDORES)",
            title_style
        ))
        story.append(Spacer(1, 0.2*inch))
        
        # Página 1: Header + Panel + Distribución + Departamentos + Sector-Ubicación

        try:
            story.append(Paragraph("Panel Ejecutivo de Distribución de Riesgo", heading_style))
            data_all = df_all[df_all['id_empresa'] == empresa_id].copy() if isinstance(df_all, pd.DataFrame) else pd.DataFrame()
            total_registros = int(len(data_all))
            alto_count = int(len(df_alto))
            pct_alto = round((alto_count / max(total_registros, 1)) * 100, 2)
            avg5 = round((avg_score / 3.0) * 5.0, 1)
            nivel_txt = "crítico" if avg_score >= 2.5 else ("alto" if avg_score >= 2.0 else ("medio" if avg_score >= 1.0 else "bajo"))
            nivel_color = '#e63946' if avg_score >= 2.5 else ('#fb8500' if avg_score >= 2.0 else ('#ffbe0b' if avg_score >= 1.0 else '#2a9d8f'))
            def _badge(txt: str, bg: colors.Color) -> Drawing:
                d = Drawing(56, 56)
                d.add(Circle(29, 27, 22, fillColor=colors.HexColor('#e6ebf1'), strokeColor=colors.HexColor('#e6ebf1')))
                d.add(Circle(28, 28, 22, fillColor=bg, strokeColor=colors.whitesmoke, strokeWidth=2))
                d.add(String(28, 28, txt, textAnchor='middle', fillColor=colors.whitesmoke, fontSize=13, fontName='Helvetica-Bold'))
                return d
            b1 = _badge(str(total_registros), colors.HexColor('#3a86ff'))
            b2 = _badge(str(alto_count), colors.HexColor('#ffbe0b'))
            b3 = _badge(f"{avg5}", colors.HexColor('#8338ec'))
            b4 = _badge(f"{pct_alto:.0f}%", colors.HexColor('#e63946'))
            c1_text = Paragraph(f"<font size=9 color='#6c757d'><b>Total Registros</b></font><br/><font size=15 color='#1b263b'><b>{total_registros:,}</b></font><br/><font size=8 color='#6c757d'>Analizados en el período</font>", styles['BodyText'])
            c2_text = Paragraph(f"<font size=9 color='#6c757d'><b>Contrapartes con Cruces</b></font><br/><font size=15 color='#1b263b'><b>{alto_count}</b></font><br/><font size=8 color='#6c757d'>{pct_alto:.2f}% del total</font>", styles['BodyText'])
            c3_text = Paragraph(f"<font size=9 color='#6c757d'><b>Riesgo Promedio</b></font><br/><font size=15 color='#1b263b'><b>{avg5}/5.0</b></font><br/><font size=8 color='{nivel_color}'>Nivel {nivel_txt}</font>", styles['BodyText'])
            c4_text = Paragraph(f"<font size=9 color='#6c757d'><b>Alto riesgo</b></font><br/><font size=15 color='#1b263b'><b>{alto_count}</b></font><br/><font size=8 color='#6c757d'>{pct_alto:.0f}% requiere acción inmediata</font>", styles['BodyText'])
            card_w = (7.5*inch) / 4
            badge_w = 0.7*inch
            text_w = card_w - badge_w - 0.1*inch
            c1 = Table([[b1, KeepInFrame(text_w, 0.8*inch, content=[c1_text], hAlign='LEFT')]], colWidths=[badge_w, text_w])
            c2 = Table([[b2, KeepInFrame(text_w, 0.8*inch, content=[c2_text], hAlign='LEFT')]], colWidths=[badge_w, text_w])
            c3 = Table([[b3, KeepInFrame(text_w, 0.8*inch, content=[c3_text], hAlign='LEFT')]], colWidths=[badge_w, text_w])
            c4 = Table([[b4, KeepInFrame(text_w, 0.8*inch, content=[c4_text], hAlign='LEFT')]], colWidths=[badge_w, text_w])
            for card in (c1, c2, c3, c4):
                card.setStyle(TableStyle([
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('LEFTPADDING', (0, 0), (-1, -1), 8),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                    ('TOPPADDING', (0, 0), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ]))
            panel = Table([[c1, c2, c3, c4]], colWidths=[(7.5*inch)/4]*4)
            panel.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
                ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#d9dee7')),
                ('INNERGRID', (0, 0), (-1, -1), 0.8, colors.HexColor('#e3e7ee')),
                ('LEFTPADDING', (0, 0), (-1, -1), 10),
                ('RIGHTPADDING', (0, 0), (-1, -1), 10),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ]))
            story.append(panel)
            story.append(Spacer(1, 0.2*inch))
        except Exception as _:
            story.append(Paragraph("No fue posible renderizar el panel ejecutivo de distribución.", normal_style))
            story.append(Spacer(1, 0.2*inch))

        try:
            story.append(Paragraph("Distribución por Nivel de Riesgo", heading_style))
            risk_col_name = 'nivel_riesgo' if 'nivel_riesgo' in df_all.columns else 'riesgo'
            df_emp = df_all[df_all['id_empresa'] == empresa_id].copy() if isinstance(df_all, pd.DataFrame) else pd.DataFrame()
            dist = {'BAJO': 0, 'MEDIO': 0, 'ALTO': 0}
            if not df_emp.empty and risk_col_name in df_emp.columns:
                s = df_emp[risk_col_name]
                if isinstance(s, pd.DataFrame):
                    s = s.iloc[:, 0]
                s = s.astype(str).str.upper()
                for k in dist.keys():
                    dist[k] = int((s == k).sum())
            total = max(sum(dist.values()), 1)
            def rowDots(n: int, fill: colors.Color) -> Drawing:
                d = Drawing(300, 24)
                count = max(1, min(12, n if n > 0 else int((n/total)*12)))
                for i in range(count):
                    d.add(Circle(12 + i*24, 12, 6, fillColor=fill, strokeColor=fill))
                return d
            d_bajo = rowDots(dist['BAJO'], colors.HexColor('#2a9d8f'))
            d_medio = rowDots(dist['MEDIO'], colors.HexColor('#ffbe0b'))
            d_alto = rowDots(dist['ALTO'], colors.HexColor('#e63946'))
            t_dist = Table([
                [Paragraph("<b>Bajo</b>", styles['BodyText']), d_bajo],
                [Paragraph("<b>Medio</b>", styles['BodyText']), d_medio],
                [Paragraph("<b>Alto</b>", styles['BodyText']), d_alto],
            ], colWidths=[1.2*inch, 5.3*inch])
            t_dist.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
                ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#d9dee7')),
                ('INNERGRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#e3e7ee')),
                ('LEFTPADDING', (0, 0), (-1, -1), 10),
                ('RIGHTPADDING', (0, 0), (-1, -1), 10),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ]))
            story.append(t_dist)
            story.append(Spacer(1, 0.2*inch))
        except Exception as _:
            story.append(Paragraph("No fue posible renderizar la distribución por nivel de riesgo.", normal_style))
            story.append(Spacer(1, 0.2*inch))

        try:
            story.append(Paragraph("Departamentos con Mayor Riesgo", heading_style))
            deps_counts = {}
            for _, r in df_alto.iterrows():
                dep = str(r.get('departamento', '') or '')
                if dep:
                    deps_counts[dep] = deps_counts.get(dep, 0) + 1
            top5 = sorted(deps_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            bars = []
            maxv = max([v for _, v in top5] + [1])
            for i, (dep, v) in enumerate(top5, start=1):
                w = int((v / maxv) * 280)
                d = Drawing(320, 22)
                d.add(Rect(30, 6, 280, 10, fillColor=colors.HexColor('#e6ebf1'), strokeColor=colors.HexColor('#e6ebf1')))
                d.add(Rect(30, 6, w, 10, fillColor=colors.HexColor('#00b4d8'), strokeColor=colors.HexColor('#00b4d8')))
                d.add(String(10, 6, f"{i}", fontSize=9, fillColor=colors.HexColor('#1b263b')))
                d.add(String(320, 6, str(v), textAnchor='end', fontSize=9, fillColor=colors.HexColor('#6c757d')))
                bars.append([KeepInFrame(2.6*inch, 0.3*inch, content=[Paragraph(dep, styles['BodyText'])], hAlign='LEFT'), d])
            if not bars:
                bars = [[Paragraph("Sin datos", styles['BodyText']), Paragraph("", styles['BodyText'])]]
            t_bars = Table(bars, colWidths=[2.6*inch, 4.9*inch])
            t_bars.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
                ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#d9dee7')),
                ('LEFTPADDING', (0, 0), (-1, -1), 10),
                ('RIGHTPADDING', (0, 0), (-1, -1), 10),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ]))
            story.append(t_bars)
            story.append(Spacer(1, 0.2*inch))
        except Exception as _:
            story.append(Paragraph("No fue posible renderizar los departamentos con mayor riesgo.", normal_style))
            story.append(Spacer(1, 0.2*inch))

        story.append(PageBreak())
        
        # Página 2: Cruces + Recomendaciones + Alertas
        story.append(Paragraph("Cruces de Entidades (Clientes, Proveedores y Empleados)", heading_style))
        try:
            df_cli_emp = (df_clientes[df_clientes['id_empresa'] == empresa_id].copy() if isinstance(df_clientes, pd.DataFrame) else pd.DataFrame())
            df_pro_emp = (df_proveedores[df_proveedores['id_empresa'] == empresa_id].copy() if isinstance(df_proveedores, pd.DataFrame) else pd.DataFrame())
            df_emp_emp = (df_empleados[df_empleados['id_empresa'] == empresa_id].copy() if isinstance(df_empleados, pd.DataFrame) else pd.DataFrame())
            cruces = CrucesAnalytics(df_cli_emp, df_pro_emp, df_emp_emp)
            df_cru = cruces.procesar_datos()
            gg = CrucesGraphGenerator(cruces)
            chart_b64 = gg.generate_composite_dashboard_chart() if not df_cru.empty else gg.generate_cruces_heatmap_chart()
            if chart_b64:
                b64 = chart_b64.split(",", 1)[1] if chart_b64.startswith("data:image") else chart_b64
                data = base64.b64decode(b64)
                img = Image(io.BytesIO(data), width=7.5*inch, height=5.5*inch)
                story.append(img)
                story.append(Spacer(1, 0.2*inch))
        except Exception:
            story.append(Paragraph("No fue posible renderizar los cruces de entidades.", normal_style))
            story.append(Spacer(1, 0.2*inch))
        expl = Table([
            [Paragraph("<b>Cómo leer estos gráficos</b>", styles['BodyText'])],
            [Paragraph("• Bajo (1–2): exposición limitada y controles suficientes. • Medio (3): requiere seguimiento. • Alto (4–5): requiere acción inmediata.", styles['BodyText'])],
            [Paragraph("• Cruce: misma contraparte aparece como Cliente, Proveedor y/o Empleado; incrementa riesgo por conflicto de interés.", styles['BodyText'])]
        ], colWidths=[7.5*inch])
        expl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
            ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#d9dee7')),
            ('LEFTPADDING', (0, 0), (-1, -1), 10),
            ('RIGHTPADDING', (0, 0), (-1, -1), 10),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ]))
        story.append(expl)
        story.append(Spacer(1, 0.15*inch))
        story.append(Paragraph("RECOMENDACIONES (IA)", heading_style))
        story.append(Paragraph(ai_sections['recommendations'], normal_style))

        # Alertas Detalladas
        story.append(Spacer(1, 0.2*inch))
        story.append(Paragraph("ALERTAS DETECTADAS", heading_style))
        if high_risk_count == 0:
            story.append(Paragraph("No se registran transacciones de alto riesgo.", normal_style))
        else:
            data_rows = [["ID Transacción", "Empresa", "Departamento", "Monto", "Acción"]]
            limit = min(high_risk_count, 6)
            idx = 0
            for _, row in df_alto.iterrows():
                if idx >= limit:
                    break
                idx += 1
                tx_id = row.get('id_transaccion') or row.get('id_tx') or row.get('tx_id') or f"TX-{empresa_id}-{idx}"
                raw_date = row.get('fecha_transaccion', row.get('fecha', row.get('fecha_tx', row.get('transaction_date', ''))))
                try:
                    fecha = pd.to_datetime(raw_date).strftime("%Y-%m-%d") if pd.notna(raw_date) else ""
                except Exception:
                    fecha = str(raw_date) if pd.notna(raw_date) else ""
                monto_val = row.get('valor_transaccion', row.get('monto', 0))
                try:
                    monto_num = float(monto_val)
                except Exception:
                    monto_num = 0.0
                monto = f"${monto_num:,.2f}"
                empresa_txt = str(row.get('empresa', row.get('id_empresa', '')))
                dep_txt = str(row.get('departamento', ''))
                data_rows.append([str(tx_id), empresa_txt, dep_txt, monto, "Ver detalles"])
            try:
                from collections import Counter
                def most_dep_for_id(idc: str) -> str:
                    dep_col = 'departamento'
                    deps = []
                    if isinstance(df_cli_emp, pd.DataFrame) and not df_cli_emp.empty and 'num_id' in df_cli_emp.columns:
                        deps += list(df_cli_emp[df_cli_emp['num_id'].astype(str) == str(idc)][dep_col].dropna().astype(str).values)
                    if isinstance(df_pro_emp, pd.DataFrame) and not df_pro_emp.empty and 'no_documento_de_identidad' in df_pro_emp.columns:
                        deps += list(df_pro_emp[df_pro_emp['no_documento_de_identidad'].astype(str) == str(idc)][dep_col].dropna().astype(str).values)
                    if isinstance(df_emp_emp, pd.DataFrame) and not df_emp_emp.empty and 'id_empleado' in df_emp_emp.columns:
                        deps += list(df_emp_emp[df_emp_emp['id_empleado'].astype(str) == str(idc)][dep_col].dropna().astype(str).values)
                    return Counter(deps).most_common(1)[0][0] if deps else ""
                cruces = CrucesAnalytics(df_cli_emp, df_pro_emp, df_emp_emp)
                df_cruces = cruces.procesar_datos()
                c_idx = 0
                for _, r in df_cruces.iterrows():
                    if c_idx >= 4:
                        break
                    c_idx += 1
                    tx_id = f"TX-{empresa_id}-C{c_idx}"
                    empresa_txt = str(r.get('id_empresa', empresa_id))
                    dep_txt = most_dep_for_id(str(r.get('id_contraparte', '')))
                    monto_num = float(r.get('suma_clientes', 0) or 0) + float(r.get('suma_proveedores', 0) or 0) + float(r.get('suma_empleados', 0) or 0)
                    monto = f"${monto_num:,.2f}"
                    data_rows.append([tx_id, empresa_txt, dep_txt, monto, "Ver detalles"])
            except Exception:
                pass
            t_alerts = Table(data_rows, colWidths=[1.8*inch, 1.6*inch, 1.4*inch, 1.5*inch, 1.2*inch])
            t_alerts.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#00b4d8')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(t_alerts)
            if high_risk_count > limit:
                story.append(Spacer(1, 0.1*inch))
                story.append(Paragraph(f"Se muestran {limit} registros. Total alertas: {high_risk_count}.", normal_style))

        story.append(Spacer(1, 0.2*inch))
        story.append(Paragraph("Cuadro de Riesgos de Contraparte", heading_style))
        try:
            df_emp_all = df_all[df_all['id_empresa'] == empresa_id].copy() if isinstance(df_all, pd.DataFrame) else pd.DataFrame()
            def count_alto(col: str) -> int:
                if df_emp_all.empty or col not in df_emp_all.columns:
                    return 0
                s = df_emp_all[col]
                if isinstance(s, pd.DataFrame):
                    s = s.iloc[:, 0]
                s = s.astype(str).str.upper()
                return int((s == 'ALTO').sum())
            weights = self.CRITERIA_WEIGHTS
            rows = [
                ("País", "Exposición a países no cooperantes/paraisos fiscales", count_alto('categoria_riesgo_pais'), int(round(weights.get('pais', 0)*100))),
                ("Jurisdicción", "Riesgo por jurisdicciones y cumplimiento FATF", count_alto('categoria_jurisdicciones'), int(round(weights.get('jurisdiccion', 0)*100))),
                ("Actividad (CIIU)", "Riesgo por actividad económica de la contraparte", count_alto('categoria_riesgo_ciiu'), int(round(weights.get('ciiu', 0)*100))),
                ("Tipo de Persona", "NATURAL/JURÍDICA/ESTATAL con mayor exposición", count_alto('categoria_riesgo_tipo_persona'), int(round(weights.get('tipo_persona', 0)*100))),
                ("Medio de Pago", "Medios que incrementan riesgo operativo/LA/FT", count_alto('categoria_riesgo_medio_pago'), int(round(weights.get('medio_pago', 0)*100))),
                ("Montos", "Operaciones con montos elevados/efectivo", count_alto('categoria_riesgo_montos'), int(round(weights.get('monto_efectivo', 0)*100))),
                (">10% Histórico", "Picos de valor sobre el promedio histórico", count_alto('categoria_riesgo_valor_mas_10pct'), int(round(weights.get('valor_10pct', 0)*100))),
                ("Relación", "Tipo de relación contractual con el proveedor", count_alto('categoria_riesgo_relacion'), int(round(weights.get('tipo_relacion', 0)*100))),
                ("Localización", "Riesgo por ubicación geográfica", count_alto('categoria_riesgo_localizacion'), int(round(weights.get('localizacion', 0)*100))),
            ]
            t_riesgos = Table(
                [["Factor", "Qué significa en este reporte", "Registros en alto", "Peso (%)"]] +
                [[f, d, str(c), f"{w}"] for (f, d, c, w) in rows],
                colWidths=[1.5*inch, 3.6*inch, 1.3*inch, 1.1*inch]
            )
            t_riesgos.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#00b4d8')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8fafc')),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))
            story.append(t_riesgos)
        except Exception:
            story.append(Paragraph("No fue posible construir el cuadro de riesgos de contraparte.", normal_style))

        # Build PDF
        doc.build(story)
    
    def _get_avg_classification(self, df: pd.DataFrame, column: str) -> str:
        """Get average classification for a criterion."""
        if column not in df.columns:
            return "N/A"
        
        values = df[column].value_counts()
        if values.empty:
            return "N/A"
        
        return values.index[0]  # Most common
    
    def _requires_due_diligence(self, df: pd.DataFrame, avg_score: float) -> bool:
        """Determine if due diligence is required."""
        # DDI required if:
        # 1. Average score >= 2.0 (High or Extreme risk)
        # 2. More than 5 high-risk transactions
        # 3. Any transaction with score >= 3.0
        
        if avg_score >= 2.0:
            return True
        
        if len(df) > 5:
            return True
        
        return False


# Singleton instance
pdf_risk_report_service = PDFRiskReportService()
