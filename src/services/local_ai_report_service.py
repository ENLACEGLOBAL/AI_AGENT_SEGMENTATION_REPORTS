# src/services/local_ai_report_service.py
"""
Local AI Report Service using template-based generation and NLP.
No external API required - runs completely locally.
"""
import json
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
import re


class LocalAIReportService:
    """
    Service for generating AI-powered reports using local NLP and templates.
    Uses rule-based generation with intelligent text assembly.
    """
    
    def __init__(self):
        self.risk_levels = {
            'ALTO': 'alto',
            'MEDIO': 'medio',
            'BAJO': 'bajo',
            'ACEPTABLE': 'aceptable'
        }
    
    def _load_analytics(self, json_path: str) -> Optional[Dict[str, Any]]:
        """Load analytics JSON file."""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading analytics: {e}")
            return None
    
    def _count_fatf_categories(self, fatf_status: Dict) -> Dict[str, int]:
        """Count countries by FATF category."""
        counts = {
            'cooperantes': 0,
            'no_cooperantes': 0,
            'paraisos': 0,
            'total': len(fatf_status)
        }
        
        for status in fatf_status.values():
            status_upper = status.upper()
            if 'COOPERANTE' in status_upper and 'NO' not in status_upper:
                counts['cooperantes'] += 1
            elif 'NO COOPERANTE' in status_upper:
                counts['no_cooperantes'] += 1
            elif 'PARAISO' in status_upper:
                counts['paraisos'] += 1
        
        return counts
    
    def _calculate_risk_score(self, fatf_counts: Dict, total_transacciones: int) -> str:
        """Calculate overall risk level based on FATF data."""
        if total_transacciones == 0:
            return "INDETERMINADO"
        
        # Calculate risk percentage
        high_risk = fatf_counts['no_cooperantes'] + fatf_counts['paraisos']
        total = fatf_counts['total']
        
        if total == 0:
            return "BAJO"
        
        risk_percentage = (high_risk / total) * 100
        
        if risk_percentage > 30:
            return "ALTO"
        elif risk_percentage > 15:
            return "MEDIO"
        elif risk_percentage > 5:
            return "ACEPTABLE"
        else:
            return "BAJO"
    
    def _generate_executive_summary(self, empresa_id: int, kpis: Dict, 
                                    fatf_counts: Dict, risk_level: str,
                                    chart_data: Dict) -> str:
        """Generate executive summary section."""
        total_tx = kpis.get('total_transacciones', 0)
        monto_total = kpis.get('monto_total', 0)
        
        # Analyze activity distribution
        activities = []
        if chart_data.get('labels') and chart_data.get('values'):
            for label, value in zip(chart_data['labels'][:3], chart_data['values'][:3]):
                activities.append(f"{label} ({value})")
        
        activities_text = ", ".join(activities) if activities else "diversas actividades económicas"
        
        summary = f"""La empresa {empresa_id} presenta un perfil de riesgo clasificado como {risk_level.upper()}. """
        
        if total_tx > 0:
            summary += f"""Durante el período analizado, se registraron {total_tx} transacciones por un monto total de ${monto_total:,.2f}. """
        
        summary += f"""Las operaciones se concentran principalmente en {activities_text}. """
        
        # FATF analysis
        if fatf_counts['no_cooperantes'] > 0 or fatf_counts['paraisos'] > 0:
            summary += f"""Se identificaron operaciones con {fatf_counts['no_cooperantes']} países no cooperantes y {fatf_counts['paraisos']} jurisdicciones consideradas paraísos fiscales, lo que representa un factor de riesgo significativo que requiere monitoreo continuo. """
        else:
            summary += f"""Las transacciones se realizan principalmente con países cooperantes ({fatf_counts['cooperantes']} jurisdicciones), lo que indica un perfil de riesgo geográfico favorable. """
        
        # Risk assessment
        if risk_level == "ALTO":
            summary += """Se recomienda implementar controles adicionales de debida diligencia y monitoreo reforzado."""
        elif risk_level == "MEDIO":
            summary += """Se sugiere mantener los controles actuales y realizar revisiones periódicas."""
        else:
            summary += """El perfil actual se encuentra dentro de parámetros aceptables de riesgo."""
        
        return summary
    
    def _generate_risk_analysis(self, empresa_id: int, kpis: Dict, 
                                fatf_counts: Dict, risk_level: str,
                                chart_data: Dict) -> str:
        """Generate detailed risk analysis section."""
        total_tx = kpis.get('total_transacciones', 0)
        monto_total = kpis.get('monto_total', 0)
        
        analysis = f"""**Evaluación General del Riesgo: {risk_level.upper()}**\n\n"""
        
        # Transaction volume analysis
        analysis += f"""**Volumen Transaccional:**\nLa empresa ha realizado {total_tx} transacciones por un valor acumulado de ${monto_total:,.2f}. """
        
        if total_tx > 100:
            analysis += """El alto volumen de operaciones requiere sistemas robustos de monitoreo automatizado. """
        elif total_tx > 50:
            analysis += """El volumen moderado de transacciones permite un seguimiento detallado de cada operación. """
        else:
            analysis += """El volumen limitado de transacciones facilita la revisión manual de cada caso. """
        
        # Activity analysis
        analysis += f"""\n\n**Distribución por Actividad Económica:**\n"""
        if chart_data.get('labels') and chart_data.get('values'):
            for label, value in zip(chart_data['labels'][:5], chart_data['values'][:5]):
                analysis += f"- {label}: {value} operaciones\n"
            
            # Risk assessment by activity
            analysis += """\nLa diversificación de actividades económicas puede indicar un modelo de negocio complejo que requiere análisis sectorial específico. """
        else:
            analysis += """No se dispone de información detallada sobre la distribución de actividades. """
        
        # Geographic risk analysis
        analysis += f"""\n\n**Riesgo Geográfico:**\n"""
        analysis += f"""- Países cooperantes: {fatf_counts['cooperantes']}\n"""
        analysis += f"""- Países no cooperantes: {fatf_counts['no_cooperantes']}\n"""
        analysis += f"""- Paraísos fiscales: {fatf_counts['paraisos']}\n\n"""
        
        high_risk_pct = ((fatf_counts['no_cooperantes'] + fatf_counts['paraisos']) / max(fatf_counts['total'], 1)) * 100
        
        if high_risk_pct > 30:
            analysis += f"""La exposición a jurisdicciones de alto riesgo ({high_risk_pct:.1f}%) es significativa y constituye el principal factor de riesgo identificado. """
        elif high_risk_pct > 15:
            analysis += f"""La exposición a jurisdicciones de alto riesgo ({high_risk_pct:.1f}%) es moderada y requiere controles específicos. """
        else:
            analysis += f"""La exposición a jurisdicciones de alto riesgo ({high_risk_pct:.1f}%) es limitada, lo que reduce el perfil de riesgo general. """
        
        # Comparative analysis
        analysis += f"""\n\n**Comparación con Estándares:**\n"""
        if risk_level == "ALTO":
            analysis += """El perfil de riesgo supera los umbrales estándar de la industria. Se requieren medidas de mitigación inmediatas."""
        elif risk_level == "MEDIO":
            analysis += """El perfil de riesgo se encuentra en línea con los estándares de la industria, pero con áreas de mejora identificadas."""
        else:
            analysis += """El perfil de riesgo está por debajo de los umbrales de alerta estándar de la industria."""
        
        return analysis
    
    def _generate_geographic_analysis(self, fatf_counts: Dict, fatf_status: Dict) -> str:
        """Generate geographic distribution analysis."""
        analysis = """**Distribución Geográfica de Operaciones**\n\n"""
        
        total_countries = fatf_counts['total']
        analysis += f"""La empresa opera con un total de {total_countries} jurisdicciones diferentes. """
        
        # Concentration analysis
        if total_countries > 50:
            analysis += """La amplia diversificación geográfica indica operaciones internacionales extensas. """
        elif total_countries > 20:
            analysis += """La empresa mantiene una presencia geográfica moderadamente diversificada. """
        else:
            analysis += """Las operaciones se concentran en un número limitado de jurisdicciones. """
        
        # Risk jurisdiction analysis
        analysis += f"""\n\n**Exposición a Jurisdicciones de Alto Riesgo:**\n"""
        
        if fatf_counts['no_cooperantes'] > 0:
            analysis += f"""Se identificaron operaciones con {fatf_counts['no_cooperantes']} países clasificados como no cooperantes por el FATF. """
            analysis += """Estas jurisdicciones presentan deficiencias en sus marcos de prevención de lavado de activos y financiamiento del terrorismo. """
        
        if fatf_counts['paraisos'] > 0:
            analysis += f"""\n\nAdicional mente, se detectaron transacciones con {fatf_counts['paraisos']} paraísos fiscales. """
            analysis += """Estas jurisdicciones se caracterizan por baja transparencia fiscal y regulación limitada, lo que incrementa el riesgo de uso indebido. """
        
        # Compliance implications
        analysis += f"""\n\n**Implicaciones de Cumplimiento:**\n"""
        
        cooperante_pct = (fatf_counts['cooperantes'] / max(total_countries, 1)) * 100
        
        if cooperante_pct > 80:
            analysis += f"""Con {cooperante_pct:.1f}% de operaciones en países cooperantes, la empresa mantiene un perfil de cumplimiento favorable. """
        else:
            analysis += f"""Con solo {cooperante_pct:.1f}% de operaciones en países cooperantes, se requiere reforzar los controles de debida diligencia. """
        
        analysis += """Se recomienda implementar procedimientos de KYC (Know Your Customer) reforzados para operaciones en jurisdicciones de alto riesgo."""
        
        return analysis
    
    def _generate_recommendations(self, risk_level: str, fatf_counts: Dict, 
                                  total_transacciones: int) -> str:
        """Generate specific recommendations based on risk profile."""
        recommendations = """**Recomendaciones de Cumplimiento y Mitigación de Riesgo**\n\n"""
        
        # Immediate actions
        recommendations += """**Medidas Inmediatas:**\n"""
        
        if risk_level == "ALTO":
            recommendations += """1. Implementar revisión manual del 100% de transacciones con jurisdicciones de alto riesgo\n"""
            recommendations += """2. Establecer límites transaccionales más estrictos para países no cooperantes\n"""
            recommendations += """3. Realizar debida diligencia reforzada (EDD) para todas las contrapartes en paraísos fiscales\n"""
        elif risk_level == "MEDIO":
            recommendations += """1. Implementar muestreo aleatorio del 30% de transacciones de alto riesgo\n"""
            recommendations += """2. Establecer alertas automáticas para operaciones con países no cooperantes\n"""
            recommendations += """3. Revisar y actualizar las políticas de aceptación de clientes\n"""
        else:
            recommendations += """1. Mantener el monitoreo continuo de transacciones\n"""
            recommendations += """2. Realizar revisiones trimestrales del perfil de riesgo\n"""
            recommendations += """3. Actualizar la matriz de riesgo geográfico semestralmente\n"""
        
        # Additional controls
        recommendations += f"""\n**Controles Adicionales Recomendados:**\n"""
        
        if fatf_counts['no_cooperantes'] > 0 or fatf_counts['paraisos'] > 0:
            recommendations += """1. Sistema de monitoreo de transacciones en tiempo real\n"""
            recommendations += """2. Verificación de beneficiarios finales para todas las contrapartes\n"""
            recommendations += """3. Documentación reforzada del origen y destino de fondos\n"""
            recommendations += """4. Capacitación especializada del equipo de cumplimiento\n"""
        
        recommendations += """5. Implementación de herramientas de screening contra listas de sanciones\n"""
        recommendations += """6. Establecimiento de un comité de revisión de operaciones inusuales\n"""
        
        # Continuous monitoring
        recommendations += f"""\n**Áreas de Monitoreo Continuo:**\n"""
        recommendations += """1. Cambios en la clasificación FATF de países operativos\n"""
        recommendations += """2. Patrones inusuales en volumen o frecuencia de transacciones\n"""
        recommendations += """3. Concentración geográfica de operaciones\n"""
        recommendations += """4. Cambios en el perfil de contrapartes\n"""
        
        # Best practices
        recommendations += f"""\n**Mejores Prácticas de Cumplimiento:**\n"""
        recommendations += """1. Mantener políticas de KYC actualizadas y alineadas con estándares internacionales\n"""
        recommendations += """2. Realizar auditorías internas trimestrales del sistema de prevención de LA/FT\n"""
        recommendations += """3. Establecer canales de reporte de operaciones sospechosas\n"""
        recommendations += """4. Mantener registros detallados de todas las transacciones por al menos 5 años\n"""
        recommendations += """5. Participar en programas de capacitación continua en prevención de lavado de activos\n"""
        
        return recommendations
    
    def generate_report(self, analytics_input) -> Dict[str, Any]:
        """
        Generate a comprehensive AI report from analytics JSON or dictionary.
        
        Args:
            analytics_input: Either a path to analytics JSON file (str) or analytics dictionary
            
        Returns:
            Dictionary with report sections
        """
        try:
            # Load analytics data - handle both file path and dictionary
            if isinstance(analytics_input, str):
                analytics = self._load_analytics(analytics_input)
                source = analytics_input
            elif isinstance(analytics_input, dict):
                analytics = analytics_input
                source = "Direct dictionary input"
            else:
                return {
                    "status": "error",
                    "message": f"Invalid input type: {type(analytics_input)}"
                }
            
            if not analytics:
                return {
                    "status": "error",
                    "message": "Failed to load analytics data"
                }
            
            empresa_id = analytics.get('empresa_id', 'Unknown')
            kpis = analytics.get('kpis', {})
            chart_data = analytics.get('chart_data', {})
            fatf_status = analytics.get('fatf_status', {})
            
            # Prepare data for analysis
            total_transacciones = kpis.get('total_transacciones', 0)
            monto_total = kpis.get('monto_total', 0)
            
            fatf_counts = self._count_fatf_categories(fatf_status)
            risk_level = self._calculate_risk_score(fatf_counts, total_transacciones)
            
            # Generate report sections
            print(f"🤖 Generating LOCAL AI report for company {empresa_id}...")
            
            print("   📝 Generating executive summary...")
            executive_summary = self._generate_executive_summary(
                empresa_id, kpis, fatf_counts, risk_level, chart_data
            )
            
            print("   🔍 Generating risk analysis...")
            risk_analysis = self._generate_risk_analysis(
                empresa_id, kpis, fatf_counts, risk_level, chart_data
            )
            
            print("   🌍 Generating geographic analysis...")
            geographic_analysis = self._generate_geographic_analysis(
                fatf_counts, fatf_status
            )
            
            print("   💡 Generating recommendations...")
            recommendations = self._generate_recommendations(
                risk_level, fatf_counts, total_transacciones
            )
            
            # Assemble complete report
            report = {
                "empresa_id": empresa_id,
                "generated_at": datetime.now().isoformat(),
                "analytics_source": source,
                "generation_method": "Local AI (Rule-based NLP)",
                "sections": {
                    "executive_summary": executive_summary,
                    "risk_analysis": risk_analysis,
                    "geographic_analysis": geographic_analysis,
                    "recommendations": recommendations
                },
                "data_summary": {
                    "total_transacciones": total_transacciones,
                    "monto_total": monto_total,
                    "risk_level": risk_level,
                    "fatf_cooperantes": fatf_counts['cooperantes'],
                    "fatf_no_cooperantes": fatf_counts['no_cooperantes'],
                    "fatf_paraisos": fatf_counts['paraisos']
                }
            }
            
            print(f"✅ Local AI report generated successfully!")
            
            return {
                "status": "success",
                "report": report
            }
            
        except Exception as e:
            print(f"❌ Error generating report: {e}")
            import traceback
            traceback.print_exc()
            return {
                "status": "error",
                "message": str(e)
            }


# Singleton instance
local_ai_report_service = LocalAIReportService()
