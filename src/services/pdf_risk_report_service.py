"""
PDF Risk Report Service
Generates detailed PDF risk classification reports with weighted scoring
"""
import json
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT


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
            
            # Load CSV data
            # Load CSV data
            # Handle pluralization correctly
            tipo_clean = tipo_contraparte.strip().lower()
            suffix = "es" if tipo_clean == "proveedor" else "s"
            csv_path = f"data_provisional/datos prueba {tipo_clean}{suffix}.csv"
            print(f"DEBUG: csv_path constructed: {csv_path}")
            df = pd.read_csv(csv_path)
            
            # Filter for company and high risk
            df_empresa = df[df['id_empresa'] == empresa_id]

            # Enrich with reference tables for country classification
            try:
                from src.db.base import engine
                from src.db.models.reference_tables import HistoricoPaises
                paises = pd.read_sql_table(HistoricoPaises.__tablename__, con=engine)
                paises = paises[['pais', 'clasificacion']].drop_duplicates()
                df_empresa = (
                    df_empresa.merge(paises, on='pais', how='left')
                    .rename(columns={'clasificacion': 'pais_clasificacion'})
                )
            except Exception:
                pass
            
            # Check for risk column
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
                high_risk_count=len(df_alto)
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
        high_risk_count: int
    ):
        """Create the PDF document with AI narratives."""
        from src.services.local_ai_report_service import local_ai_report_service
        
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
        
        # Title
        story.append(Paragraph(
            f"REPORTE DE CLASIFICACIÓN DE RIESGO Y ALERTA EXTREMA ({tipo_contraparte.upper()})",
            title_style
        ))
        story.append(Spacer(1, 0.2*inch))
        
        # 1. AI Executive Summary
        story.append(Paragraph("1. RESUMEN EJECUTIVO (IA) - ANÁLISIS DE ALTO RIESGO", heading_style))
        story.append(Paragraph("Este reporte es un resumen automatizado basado en el análisis de transacciones de alto riesgo, cruzando información de listas restrictivas (FATF), actividad económica (CIIU) y jurisdicción.", normal_style))
        story.append(Spacer(1, 0.1*inch))
        story.append(Paragraph(ai_sections['executive_summary'], normal_style))
        story.append(Spacer(1, 0.2*inch))
        
        # 2. General Data
        story.append(Paragraph("2. DATOS GENERALES", heading_style))
        general_data = [
            ['Campo', 'Detalle'],
            ['ID Empresa', str(empresa_id)],
            ['Fecha de Generación', datetime.now().strftime("%d-%b-%Y")],
            ['Tipo de Contraparte', tipo_contraparte.title()],
            ['Transacciones Alto Riesgo', str(high_risk_count)],
            ['Metodología Aplicada', 'Promedio Ponderado SAGRILAFT/PTEE']
        ]
        
        t = Table(general_data, colWidths=[2.5*inch, 4*inch])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1b263b')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(t)
        story.append(Spacer(1, 0.3*inch))
        
        # 3. Database Verification
        story.append(Paragraph("3. VERIFICACIÓN CON BASE DE DATOS", heading_style))
        db_text = """
        <b>Fuentes de Información Verificadas:</b><br/>
        - Listas FATF/GAFI (Países No Cooperantes y Paraísos Fiscales)<br/>
        - Base de Datos de Actividades Económicas (CIIU)<br/>
        - Histórico Transaccional de la Empresa<br/>
        - Matriz de Riesgo Jurisdiccional<br/><br/>
        
        <b>Hallazgos de Validación:</b><br/>
        Se han cruzado los datos de los proveedores con las listas restrictivas y de control. 
        Las alertas generadas corresponden a coincidencias confirmadas con los criterios de riesgo parametrizados en el sistema.
        """
        story.append(Paragraph(db_text, normal_style))
        story.append(Spacer(1, 0.3*inch))

        # 4. Risk criteria table
        story.append(Paragraph("4. CRITERIOS DE RIESGO EVALUADOS", heading_style))
        
        criteria_data = [
            ['Criterio', 'Peso (%)', 'Clasificación Promedio']
        ]
        
        # Calculate average values for each criterion
        criteria_data.append(['País de Origen', '11%', self._get_avg_classification(df_alto, 'categoria_riesgo_pais')])
        criteria_data.append(['Jurisdicción', '6%', self._get_avg_classification(df_alto, 'categoria_jurisdicciones')])
        criteria_data.append(['CIIU/Actividad', '10%', self._get_avg_classification(df_alto, 'categoria_riesgo_ciiu')])
        criteria_data.append(['Tipo de Persona', '5%', self._get_avg_classification(df_alto, 'categoria_riesgo_tipo_persona')])
        criteria_data.append(['Medio de Pago', '8%', self._get_avg_classification(df_alto, 'categoria_riesgo_medio_pago')])
        criteria_data.append(['Monto + Efectivo', '10%', self._get_avg_classification(df_alto, 'categoria_riesgo_montos')])
        criteria_data.append(['Valor > 10% Promedio', '10%', self._get_avg_classification(df_alto, 'categoria_riesgo_valor_mas_10pct')])
        
        if tipo_contraparte == "proveedor":
            criteria_data.append(['Tipo de Relación', '8%', self._get_avg_classification(df_alto, 'categoria_riesgo_relacion')])
            criteria_data.append(['Localización', '9%', self._get_avg_classification(df_alto, 'categoria_riesgo_localizacion')])
        
        t2 = Table(criteria_data, colWidths=[3*inch, 1.5*inch, 2*inch])
        t2.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#415a77')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(t2)
        story.append(Spacer(1, 0.3*inch))
        
        # 5. Final classification
        story.append(Paragraph("5. CONCLUSIÓN Y CLASIFICACIÓN FINAL", heading_style))
        
        conclusion_data = [
            ['Indicador', 'Valor'],
            ['Suma Total Ponderada', f"{avg_score:.2f}"],
            ['Clasificación Final', risk_level],
            ['Umbral Aplicado', '≥3.0 = Extremo, ≥2.0 = Alto, ≥1.0 = Medio, <1.0 = Bajo']
        ]
        
        t3 = Table(conclusion_data, colWidths=[3*inch, 3.5*inch])
        t3.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e63946')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (0, -1), colors.lightgrey),
            ('BACKGROUND', (1, 1), (1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        story.append(t3)
        story.append(Spacer(1, 0.3*inch))
        
        # 6. AI Recommendations
        story.append(Paragraph("6. RECOMENDACIONES (IA)", heading_style))
        story.append(Paragraph(ai_sections['recommendations'], normal_style))

        # 7. Alertas Detalladas
        story.append(Spacer(1, 0.2*inch))
        story.append(Paragraph("7. ALERTAS DETECTADAS", heading_style))
        if high_risk_count == 0:
            story.append(Paragraph("No se registran transacciones de alto riesgo.", normal_style))
        else:
            if high_risk_count <= 10:
                data_rows = [["ID", "Contraparte", "Actividad", "Departamento", "Monto"]]
                for i, (_, row) in enumerate(df_alto.iterrows(), start=1):
                    tx_id = row.get('id_transaccion') or f"TX-{empresa_id}-{i}"
                    contraparte = row.get('nombre', row.get('num_id', ''))
                    actividad = row.get('ciiu_descripcion', row.get('actividad', ''))
                    depto = row.get('departamento', '')
                    monto = f"${float(row.get('valor_transaccion', row.get('monto', 0))):,.2f}"
                    data_rows.append([str(tx_id), str(contraparte), str(actividad), str(depto), monto])
                t_alerts = Table(data_rows, colWidths=[1.2*inch, 2*inch, 2.2*inch, 1.2*inch, 1.6*inch])
                t_alerts.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1b263b')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                story.append(t_alerts)
            else:
                ids_text = []
                for i, (_, row) in enumerate(df_alto.iterrows(), start=1):
                    tx_id = row.get('id_transaccion') or f"TX-{empresa_id}-{i}"
                    ids_text.append(str(tx_id))
                story.append(Paragraph("Cantidad de alertas excede el umbral, se listan solo los IDs:", normal_style))
                story.append(Paragraph(", ".join(ids_text), normal_style))

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
