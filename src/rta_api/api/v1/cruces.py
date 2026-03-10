# src/rta_api/api/v1/cruces.py
"""
Endpoints API para análisis de cruces de entidades
"""
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from typing import Optional

from src.db.base import TargetSessionLocal
from src.services.cruces_analytics_service import cruces_analytics_service
from src.services.pdf_risk_report_service import PDFRiskReportService
from src.core.security import require_jwt

router = APIRouter(prefix="/api/v1/cruces", tags=["cruces"])


def get_db():
    db = TargetSessionLocal()
    try:
        yield db
    finally:
        db.close()

def generate_pdf_background(analytics_data: dict, empresa_id: int):
    """Tarea en segundo plano para generar el PDF"""
    try:
        service = PDFRiskReportService()
        result = service.generate_pdf_report(
            analytics_data=analytics_data,
            tipo_contraparte="cliente",
            output_path=None
        )
        print(f"✅ PDF generado automáticamente: {result.get('file')}")
    except Exception as e:
        print(f"❌ Error generando PDF en background: {e}")

@router.post("/process-batch")
def process_batch_analytics(
    empresa_id: int,
    fecha: str | None = Query(None, description="Fecha específica YYYY-MM-DD"),
    monto_min: float | None = Query(None, description="Monto mínimo de transacción"),
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    # claims: dict = Depends(require_jwt) # Opcional: si la otra app no tiene token, comentar esto o usar API Key
):
    """
    Endpoint Orquestador:
    1. Genera Analytics (JSON)
    2. Dispara generación de PDF en background
    3. Retorna estado inmediato
    """
    analytics_result = cruces_analytics_service.generate_cruces_analytics(db, empresa_id, fecha=fecha, monto_min=monto_min)
    
    if analytics_result.get("status") == "error":
        return analytics_result
        
    analytics_data = analytics_result.get("data")
    
    if analytics_data:
        background_tasks.add_task(generate_pdf_background, analytics_data, empresa_id)
        
    return {
        "status": "success",
        "message": "Analytics generado y reporte PDF en proceso",
        "analytics_path": None,
        "empresa_id": empresa_id,
        "filters": {
            "fecha": fecha,
            "monto_min": monto_min
        }
    }

@router.get("/analytics")
def get_cruces_analytics(
    empresa_id: int | None = Query(None),
    fecha: str | None = Query(None, description="Fecha específica YYYY-MM-DD"),
    monto_min: float | None = Query(None, description="Monto mínimo de transacción"),
    db: Session = Depends(get_db),
    claims: dict = Depends(require_jwt)
):
    """
    Genera analytics de cruces de entidades (conflictos de interés).
    
    Args:
        empresa_id: Opcional - ID de empresa para filtrar
        fecha: Opcional - fecha específica (YYYY-MM-DD)
        monto_min: Opcional - monto mínimo
        
    Returns:
        JSON con KPIs, distribuciones, gráficos y tabla de detalles
    """
    return cruces_analytics_service.generate_cruces_analytics(db, empresa_id, fecha=fecha, monto_min=monto_min)


@router.get("/dashboard", response_class=HTMLResponse)
def get_cruces_dashboard(
    empresa_id: int | None = Query(None),
    fecha: str | None = Query(None, description="Fecha específica YYYY-MM-DD"),
    monto_min: float | None = Query(None, description="Monto mínimo de transacción"),
    db: Session = Depends(get_db),
    claims: dict = Depends(require_jwt)
):
    """
    Retorna dashboard HTML con visualización de cruces.
    Usa la plantilla del archivo ANÁLISIS DE ENTIDADES.html
    """
    # Generar analytics
    result = cruces_analytics_service.generate_cruces_analytics(db, empresa_id, fecha=fecha, monto_min=monto_min)
    
    if result.get("status") != "success":
        return f"""
        <!DOCTYPE html>
        <html>
        <head><title>Error</title></head>
        <body>
            <h1>Error</h1>
            <p>{result.get('message', 'Error desconocido')}</p>
        </body>
        </html>
        """
    
    data = result.get("data", {})
    kpis = data.get("kpis", {})
    dist_riesgo = data.get("distribucion_riesgo", {})
    tipos_cruces = data.get("tipos_cruces", {})
    top_empresas = data.get("top_empresas", [])
    tabla = data.get("tabla_detalles", [])
    charts = data.get("charts", {})
    
    # Construir HTML (simplificado - puedes usar Jinja2 templates)
    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Análisis de Cruces - Empresa {empresa_id if empresa_id else 'Todas'}</title>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 20px; background: #f5f5f5; }}
            .kpi-card {{ display: inline-block; background: white; padding: 20px; margin: 10px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
            .kpi-value {{ font-size: 2em; font-weight: bold; color: #e63946; }}
            .kpi-label {{ color: #666; margin-top: 5px; }}
            .chart {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
            .chart img {{ max-width: 100%; height: auto; }}
            table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 20px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background: #1b263b; color: white; }}
            .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 0.85em; font-weight: bold; }}
            .badge-alto {{ background: #f8d7da; color: #721c24; }}
            .badge-medio {{ background: #fff3cd; color: #856404; }}
            .badge-bajo {{ background: #d4edda; color: #155724; }}
        </style>
    </head>
    <body>
        <h1>📊 ANÁLISIS DE CRUCES DE ENTIDADES</h1>
        <p>Sistema de Detección de Conflictos de Interés</p>
        
        <div class="kpis">
            <div class="kpi-card">
                <div class="kpi-value">{kpis.get('total_cruces', 0)}</div>
                <div class="kpi-label">Total Registros</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{kpis.get('entidades_cruces', 0)}</div>
                <div class="kpi-label">Entidades con Cruces</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{kpis.get('riesgo_promedio', 0)} / 5.0</div>
                <div class="kpi-label">Riesgo Promedio</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">{kpis.get('alto_riesgo_count', 0)}</div>
                <div class="kpi-label">Alto Riesgo</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-value">${kpis.get('valor_total_riesgo', 0):,.0f}</div>
                <div class="kpi-label">Valor Total en Riesgo</div>
            </div>
        </div>
        
        <div class="chart">
            <h2>Distribución por Nivel de Riesgo</h2>
            <img src="{charts.get('risk_distribution', '')}" alt="Distribución de Riesgo">
        </div>
        
        <div class="chart">
            <h2>Tipos de Cruces Detectados</h2>
            <img src="{charts.get('cross_types', '')}" alt="Tipos de Cruces">
        </div>
        
        <div class="chart">
            <h2>Top 10 Empresas por Cantidad de Cruces</h2>
            <img src="{charts.get('top_empresas', '')}" alt="Top Empresas">
        </div>
        
        <h2>📋 Detalle de Entidades con Cruces</h2>
        <table>
            <thead>
                <tr>
                    <th>ID Entidad</th>
                    <th>Empresa</th>
                    <th>Categorías</th>
                    <th>Cliente</th>
                    <th>Proveedor</th>
                    <th>Empleado</th>
                    <th>Riesgo Máximo</th>
                </tr>
            </thead>
            <tbody>
    """
    
    # Agregar filas de la tabla
    for item in tabla[:20]:  # Mostrar solo primeros 20
        id_contra = item.get('id_contraparte', '')
        empresa = item.get('id_empresa', '')
        cats = item.get('conteo_categorias', 0)
        riesgo_max = item.get('riesgo_maximo', 0)
        
        # Badge de riesgo

        riesgo_max = riesgo_max or 0
        
        if riesgo_max >= 4:
            badge_class = "badge-alto"
            badge_text = "ALTO"
        elif riesgo_max == 3:
            badge_class = "badge-medio"
            badge_text = "MEDIO"
        else:
            badge_class = "badge-bajo"
            badge_text = "BAJO"
        
        cliente = item.get('cliente')
        proveedor = item.get('proveedor')
        empleado = item.get('empleado')

        tiene_form = item.get('tiene_formulario', False)
        fecha_form = item.get('fecha_formulario')
        if tiene_form:
            form_badge = '<span class="badge badge-si">SÍ</span>'
            if fecha_form:
                form_badge += f'<br><small style="color:#666;">{fecha_form}</small>'
        else:
            form_badge = '<span class="badge badge-no">NO</span>'
        
        html_content += f"""
                <tr>
                    <td><strong>{id_contra}</strong></td>
                    <td>{empresa}</td>
                    <td>{cats} categorías</td>
                    <td>{f"${cliente['suma']:,.0f}" if cliente else "—"}</td>
                    <td>{f"${proveedor['suma']:,.0f}" if proveedor else "—"}</td>
                    <td>{f"${empleado['suma']:,.0f}" if empleado else "—"}</td>
                    <td><span class="badge {badge_class}">{badge_text}</span></td>
                    <td>{form_badge}</td>
                </tr>
        """
        
        html_content += f"""
                <tr>
                    <td><strong>{id_contra}</strong></td>
                    <td>{empresa}</td>
                    <td>{cats} categorías</td>
                    <td>{f"${cliente['suma']:,.0f}" if cliente else "—"}</td>
                    <td>{f"${proveedor['suma']:,.0f}" if proveedor else "—"}</td>
                    <td>{f"${empleado['suma']:,.0f}" if empleado else "—"}</td>
                    <td><span class="badge {badge_class}">{badge_text}</span></td>
                </tr>
        """
    
    html_content += """
            </tbody>
        </table>
        
        <div style="margin-top: 40px; padding: 20px; background: #fff3cd; border-left: 4px solid #f77f00; border-radius: 4px;">
            <h3>⚠️ Recomendaciones</h3>
            <ul>
                <li>Realizar auditoría inmediata a entidades con 3 cruces (riesgo máximo)</li>
                <li>Revisar transacciones superiores a $50M donde existe relación empleado-proveedor</li>
                <li>Implementar controles preventivos en regiones identificadas</li>
                <li>Validar autorización de conflictos de interés declarados</li>
                <li>KYC reforzado con verificación de beneficiarios finales</li>
            </ul>
        </div>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)


@router.get("/export-json")
def export_cruces_json(
    empresa_id: int | None = Query(None),
    fecha: str | None = Query(None, description="Fecha específica YYYY-MM-DD"),
    monto_min: float | None = Query(None, description="Monto mínimo de transacción"),
    db: Session = Depends(get_db),
    claims: dict = Depends(require_jwt)
):
    """
    Exporta los datos de cruces en formato JSON puro.
    Útil para consumo desde PHP u otros sistemas.
    """
    result = cruces_analytics_service.generate_cruces_analytics(db, empresa_id, fecha=fecha, monto_min=monto_min)
    
    if result.get("status") == "success":
        return result.get("data", {})
    else:
        return result
