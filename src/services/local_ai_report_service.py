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

def _html_escape(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def _render_html(report: Dict[str, Any], analytics: Dict[str, Any]) -> str:
    empresa_id = report.get("empresa_id", "")
    generated_at = report.get("generated_at", "")
    data_summary = report.get("data_summary", {})
    sections = report.get("sections", {})
    chart_data = analytics.get("chart_data", {})
    fatf_status = analytics.get("fatf_status", {})
    mapa_colombia = analytics.get("mapa_colombia", [])
    tabla = analytics.get("tabla", [])
    total_tx_alto = len(tabla)
    empresas_involucradas = len({str(r.get("empresa", "")) for r in tabla}) if tabla else 0
    monto_total_alto = sum(float(r.get("monto", 0) or 0) for r in tabla) if tabla else 0.0
    es = _html_escape
    executive = es(sections.get("executive_summary", ""))
    risk_analysis = es(sections.get("risk_analysis", ""))
    geographic = es(sections.get("geographic_analysis", ""))
    recommendations = es(sections.get("recommendations", ""))
    labels = chart_data.get("labels", [])
    values = chart_data.get("values", [])
    fatf_js = {str(k): str(v) for k, v in fatf_status.items()}
    rows_js = []
    for r in tabla:
        rows_js.append({
            "id_transaccion": r.get("id_transaccion"),
            "empresa": r.get("empresa"),
            "nit": r.get("nit"),
            "ciiu": r.get("ciiu"),
            "actividad": r.get("actividad"),
            "departamento": r.get("departamento"),
            "monto": r.get("monto"),
            "tipo_contraparte": r.get("tipo_contraparte", "")
        })
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Análisis Geográfico de Riesgo Sectorial - Empresa {es(empresa_id)}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
<style>
:root{{--primary:#1b263b;--secondary:#415a77;--accent:#00b4d8;--danger:#e63946}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;background:linear-gradient(135deg,#f5f7fa 0%,#c3cfe2 100%);color:#333;line-height:1.6}}
.container{{max-width:1500px;margin:0 auto;padding:20px}}
header{{background:linear-gradient(135deg,var(--primary),var(--secondary));color:#fff;padding:35px 20px;text-align:center;border-radius:15px;margin-bottom:30px;box-shadow:0 10px 30px rgba(27,38,59,0.4)}}
.logo-image{{height:48px;display:block;margin:0 auto 10px auto;filter:drop-shadow(0 2px 4px rgba(0,0,0,0.3))}}
header h1{{font-size:2.2em;margin-bottom:8px}}
header p{{font-size:1.05em;opacity:.9}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:20px;margin-bottom:30px}}
.kpi-card{{background:#fff;padding:22px;border-radius:12px;box-shadow:0 5px 15px rgba(0,0,0,0.1);text-align:center}}
.kpi-value{{font-size:2.0em;font-weight:700;color:var(--danger)}}
.kpi-label{{color:#666;font-size:.95em}}
.section{{background:#fff;border-radius:12px;padding:25px;margin-bottom:30px;box-shadow:0 5px 20px rgba(0,0,0,0.08)}}
.section h2{{color:var(--primary);margin-bottom:20px;font-size:1.4em;border-bottom:3px solid var(--accent);padding-bottom:10px;display:inline-block}}
.dual-section{{display:grid;grid-template-columns:380px 1fr;gap:25px;margin-bottom:30px;align-items:start}}
.chart-box{{background:#fff;padding:20px;border-radius:12px;box-shadow:0 5px 20px rgba(0,0,0,0.08);height:420px;display:flex;flex-direction:column;justify-content:center}}
.map-world-box{{background:#fff;padding:15px;border-radius:12px;box-shadow:0 5px 20px rgba(0,0,0,0.08);height:420px}}
#mapWorld{{height:100%;border-radius:8px}}
#mapColombia{{height:500px;border-radius:10px;margin-bottom:15px}}
.legend{{background:#fff;padding:12px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,0.1);font-size:.9em;margin-top:10px}}
.legend i{{width:18px;height:18px;float:left;margin-right:8px;border-radius:50%;opacity:.8}}
.badge-high{{background:var(--danger);color:#fff;padding:6px 14px;border-radius:20px;font-weight:bold;font-size:.8em}}
@media (max-width:1100px){{.dual-section{{grid-template-columns:1fr}}.chart-box,.map-world-box{{height:400px}}}}
@media (max-width:768px){{header h1{{font-size:2em}}#mapColombia{{height:350px}}}}
</style>
</head>
<body>
<div class="container">
<header>
<img src="file:///C:/Users/Usuario/AI_AGENT_SEGMENTATION_REPORTS/Logo.png" alt="Logo" class="logo-image" />
<h1>Análisis de Riesgo Sectorial</h1>
<p>Transacciones de ALTO RIESGO detectadas por CIIU y zona geográfica</p>
</header>
<div class="kpi-grid">
<div class="kpi-card"><div class="kpi-value">{total_tx_alto}</div><div class="kpi-label">Transacciones Alto Riesgo</div></div>
<div class="kpi-card"><div class="kpi-value">{empresas_involucradas}</div><div class="kpi-label">Empresas Involucradas</div></div>
<div class="kpi-card"><div class="kpi-value">${monto_total_alto:,.2f}</div><div class="kpi-label">Monto Total Alto Riesgo</div></div>
</div>
<div class="section"><h2>Mapa de Transacciones Alto Riesgo - Colombia</h2><div id="mapColombia"></div></div>
<div class="dual-section">
<div class="chart-box"><h2 style="text-align:center;margin-bottom:15px;color:var(--primary)">Distribución por CIIU</h2><canvas id="riskChart"></canvas></div>
<div class="map-world-box"><h2 style="text-align:center;margin-bottom:10px;color:var(--primary)">Mapa Global FATF/GAFI</h2><div id="mapWorld"></div></div>
</div>
<div class="section">
<h2>Detalle de Transacciones Alto Riesgo</h2>
<table id="transaccionesTable" class="display" style="width:100%">
<thead><tr><th>ID Transacción</th><th>Empresa</th><th>NIT</th><th>CIIU</th><th>Actividad</th><th>Departamento</th><th>Monto</th><th>Tipo</th></tr></thead>
<tbody></tbody>
</table>
</div>
<div class="section"><h2>Recomendaciones Automatizadas - Alto Riesgo</h2><div style="background:#ffeaea;padding:20px;border-radius:10px;border-left:6px solid var(--danger)"><ul style="margin:15px 0;font-size:1.05em"><li>Monitoreo diario de operaciones de alto riesgo</li><li>KYC reforzado y verificación de beneficiarios finales</li><li>Validación documental completa</li><li>Auditoría interna trimestral y reporte inmediato</li><li>Revisión por Comité de Riesgo en 48 horas</li><li>Bloqueo preventivo hasta aprobación</li></ul></div></div>
<div class="section"><h2>Resumen Ejecutivo</h2><div>{executive}</div></div>
<div class="section"><h2>Análisis de Riesgo</h2><div>{risk_analysis}</div></div>
<div class="section"><h2>Análisis Geográfico</h2><div>{geographic}</div></div>
<div class="section"><h2>Recomendaciones</h2><div>{recommendations}</div></div>
</div>
<script>
const labels = {{json_labels}};
const values = {{json_values}};
const fatf = {{json_fatf}};
const rows = {{json_rows}};
const mapPoints = {{json_points}};
new Chart(document.getElementById('riskChart'),{{type:'doughnut',data:{{labels:labels,datasets:[{{data:values,backgroundColor:['#e63946','#c1121f','#9b2226','#8a1c1c','#5b0f0f'],borderWidth:3,borderColor:'#fff'}}]}} ,options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{padding:15}}}}}}}}}});
const mapCol = L.map('mapColombia').setView([4.5709,-74.2973],6);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png').addTo(mapCol);
if(Array.isArray(mapPoints)){{mapPoints.forEach(function(z){{if(z&&z.lat&&z.lon){{L.circleMarker([z.lat,z.lon],{{radius:12,color:'#e63946',weight:3,fillColor:'#e63946',fillOpacity:.8}}).addTo(mapCol).bindPopup('<strong>'+(z.departamento||'')+'</strong><br>Empresa: '+(z.empresa||'')+'<br>CIIU: '+(z.ciiu||'')+'<br>Monto: '+(z.monto||'')+'<br><span style="color:#e63946">ALTO RIESGO</span>');}}}});}}
const mapWorld = L.map('mapWorld').setView([20,0],2);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png').addTo(mapWorld);
const GEOJSON_URL="https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json";
function normalize(s){{return (s||'').normalize('NFD').replace(/[\\u0300-\\u036f]/g,'').trim().toUpperCase();}}
function statusOf(name){{const k=normalize(name);return fatf[k]||null;}}
fetch(GEOJSON_URL).then(function(r){{return r.json()}}).then(function(geo){{L.geoJson(geo,{{style:function(f){{return {{fillColor:statusOf(f.properties.name)==='COOPERANTE'?'#2ca25f':statusOf(f.properties.name)==='NO COOPERANTE'?'#de2d26':'#cccccc',weight:.5,color:'#555',fillOpacity:.7}};}},onEachFeature:function(f,l){{l.bindPopup('<b>'+f.properties.name+'</b><br>FATF: <strong>'+(statusOf(f.properties.name)||'Sin datos')+'</strong>');}}}}).addTo(mapWorld);}});
const legend=L.control({{position:'bottomright'}});legend.onAdd=()=>{{const d=L.DomUtil.create('div','legend');d.innerHTML=`<i style="background:#2ca25f"></i> Cooperante<br><i style="background:#de2d26"></i> No Cooperante<br><i style="background:#cccccc"></i> Sin datos`;return d;}};legend.addTo(mapWorld);
$(document).ready(()=>{{$('#transaccionesTable').DataTable({{data:rows,columns:[{{data:'id_transaccion'}},{{data:'empresa'}},{{data:'nit'}},{{data:'ciiu'}},{{data:'actividad'}},{{data:'departamento'}},{{data:'monto'}},{{data:'tipo_contraparte'}}],language:{{url:'https://cdn.datatables.net/plug-ins/1.13.7/i18n/es-ES.json'}},pageLength:10}});}});
</script>
</body>
</html>"""
    html = html.replace("{json_labels}", json.dumps(labels, ensure_ascii=False))
    html = html.replace("{json_values}", json.dumps(values, ensure_ascii=False))
    html = html.replace("{json_fatf}", json.dumps(fatf_js, ensure_ascii=False))
    html = html.replace("{json_rows}", json.dumps(rows_js, ensure_ascii=False))
    html = html.replace("{json_points}", json.dumps(mapa_colombia, ensure_ascii=False))
    return html

def generate_html_report(analytics_input, out_dir: str = os.path.join("data_provisional", "reports")) -> Dict[str, Any]:
    try:
        result = local_ai_report_service.generate_report(analytics_input)
        if result.get("status") != "success":
            return result
        report = result["report"]
        os.makedirs(out_dir, exist_ok=True)
        empresa_id = report.get("empresa_id", "unknown")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ai_report_{empresa_id}_{ts}.html"
        path = os.path.join(out_dir, filename)
        analytics_dict = analytics_input if isinstance(analytics_input, dict) else {}
        html = _render_html(report, analytics_dict)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        return {"status": "success", "path": path, "empresa_id": empresa_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}
