from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from typing import Optional
import io

app = FastAPI(title="Análisis de Contrapartes")

# Configurar templates
templates = Jinja2Templates(directory="templates")

# Variables globales para almacenar datos
df_clientes = None
df_proveedores = None
df_empleados = None
df_final = None

def procesar_datos():
    """Procesa los datos según la lógica del notebook"""
    global df_clientes, df_proveedores, df_empleados, df_final
    
    # 1. Agregar Clientes
    df_clientes_agg = (
        df_clientes.rename(columns={
            'num_id': 'id_contraparte', 
            'valor_transaccion': 'valor_suma', 
            'orden_clasificacion_del_riesgo': 'riesgo'
        }, errors='ignore')
        .assign(id_contraparte=lambda x: x['id_contraparte'].astype(str))
        .groupby(['id_empresa', 'id_contraparte'])
        .agg(
            cantidad_clientes=('id_contraparte', 'size'),
            suma_clientes=('valor_suma', 'sum'),
            Mayor_riesgo_clientes=('riesgo', 'max')
        )
    )
    
    # 2. Agregar Proveedores
    df_proveedores_agg = (
        df_proveedores.rename(columns={
            'no_documento_de_identidad': 'id_contraparte', 
            'valor_transaccion': 'valor_suma', 
            'orden_clasificacion_del_riesgo': 'riesgo'
        }, errors='ignore')
        .assign(id_contraparte=lambda x: x['id_contraparte'].astype(str))
        .groupby(['id_empresa', 'id_contraparte'])
        .agg(
            cantidad_proveedores=('id_contraparte', 'size'),
            suma_proveedores=('valor_suma', 'sum'),
            Mayor_riesgo_proveedores=('riesgo', 'max')
        )
    )
    
    # 3. Agregar Empleados
    df_empleados_agg = (
        df_empleados.rename(columns={
            'id_empleado': 'id_contraparte', 
            'valor': 'valor_suma', 
            'conteo_alto': 'riesgo'
        }, errors='ignore')
        .assign(id_contraparte=lambda x: x['id_contraparte'].astype(str))
        .groupby(['id_empresa', 'id_contraparte'])
        .agg(
            cantidad_empleados=('id_contraparte', 'size'),
            suma_empleados=('valor_suma', 'sum'),
            Mayor_riesgo_empleados=('riesgo', 'max')
        )
    )
    
    # Combinar todos los dataframes
    df_resumen = df_clientes_agg.join(df_proveedores_agg, how='outer')
    df_resumen = df_resumen.join(df_empleados_agg, how='outer')
    
    # Llenar valores nulos y calcular conteo de categorías
    columnas_cantidad = [col for col in df_resumen.columns if 'cantidad' in col]
    df_resumen[columnas_cantidad] = df_resumen[columnas_cantidad].fillna(0)
    
    df_resumen['conteo_categorias'] = (
        (df_resumen['cantidad_clientes'] > 0).astype(int) +
        (df_resumen['cantidad_proveedores'] > 0).astype(int) +
        (df_resumen['cantidad_empleados'] > 0).astype(int)
    )
    
    # Filtrar contrapartes con al menos 2 categorías
    df_final = df_resumen[df_resumen['conteo_categorias'] >= 2].copy()
    
    columnas_suma = [col for col in df_final.columns if 'suma' in col]
    df_final[columnas_suma] = df_final[columnas_suma].fillna(0)
    
    df_final = df_final.reset_index()
    
    return df_final

def generar_graficos(df_data, empresa_id=None):
    """Genera todos los gráficos interactivos con Plotly"""
    graficos = {}
    
    # Filtrar por empresa si se especifica
    if empresa_id:
        df_empresa = df_data[df_data['id_empresa'] == empresa_id].copy()
    else:
        df_empresa = df_data.copy()
    
    # 1. Distribución por número de categorías
    fig1 = px.bar(
        df_empresa['conteo_categorias'].value_counts().reset_index(),
        x='conteo_categorias',
        y='count',
        title='Distribución de Contrapartes por Número de Categorías',
        labels={'conteo_categorias': 'Número de Categorías', 'count': 'Cantidad'}
    )
    graficos['distribucion_categorias'] = fig1.to_html(full_html=False)
    
    # 2. Correlación entre métricas
    numeric_cols = ['cantidad_clientes', 'suma_clientes', 'cantidad_proveedores', 
                    'suma_proveedores', 'cantidad_empleados', 'suma_empleados']
    corr_matrix = df_data[numeric_cols].corr()
    
    fig2 = px.imshow(
        corr_matrix,
        text_auto=True,
        title='Correlación entre Métricas',
        color_continuous_scale='RdBu_r'
    )
    graficos['correlacion'] = fig2.to_html(full_html=False)
    
    # 3. Scatter plots de cantidad vs suma
    fig3 = make_subplots(
        rows=1, cols=3,
        subplot_titles=('Clientes', 'Proveedores', 'Empleados')
    )
    
    fig3.add_trace(
        go.Scatter(x=df_data['cantidad_clientes'], y=df_data['suma_clientes'], 
                   mode='markers', name='Clientes'),
        row=1, col=1
    )
    fig3.add_trace(
        go.Scatter(x=df_data['cantidad_proveedores'], y=df_data['suma_proveedores'],
                   mode='markers', name='Proveedores'),
        row=1, col=2
    )
    fig3.add_trace(
        go.Scatter(x=df_data['cantidad_empleados'], y=df_data['suma_empleados'],
                   mode='markers', name='Empleados'),
        row=1, col=3
    )
    
    fig3.update_layout(title_text="Cantidad vs Valor Total por Categoría", showlegend=False)
    graficos['scatter_cantidad_valor'] = fig3.to_html(full_html=False)
    
    # 4. Top 10 contrapartes por valor
    top_contrapartes = df_data.nlargest(10, 'suma_clientes')
    fig4 = px.bar(
        top_contrapartes,
        x='suma_clientes',
        y='id_contraparte',
        orientation='h',
        title='Top 10 Contrapartes por Valor de Clientes'
    )
    graficos['top_contrapartes'] = fig4.to_html(full_html=False)
    
    # 5. Treemap por empresa
    if not empresa_id:
        agg_data = df_data.groupby('id_empresa').agg({
            'conteo_categorias': 'sum',
            'cantidad_clientes': 'sum',
            'cantidad_proveedores': 'sum',
            'cantidad_empleados': 'sum'
        }).reset_index()
        
        fig5 = px.treemap(
            agg_data, 
            path=['id_empresa'], 
            values='conteo_categorias',
            title='Distribución de Contrapartes por Empresa'
        )
        graficos['treemap_empresas'] = fig5.to_html(full_html=False)
    
    return graficos

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Página principal"""
    empresas = []
    if df_final is not None:
        empresas = sorted(df_final['id_empresa'].unique().tolist())
    
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "empresas": empresas, "datos_cargados": df_final is not None}
    )

@app.post("/cargar-datos")
async def cargar_datos(
    clientes: UploadFile = File(...),
    proveedores: UploadFile = File(...),
    empleados: UploadFile = File(...)
):
    """Endpoint para cargar los archivos CSV"""
    global df_clientes, df_proveedores, df_empleados, df_final
    
    try:
        # Leer los archivos CSV
        df_clientes = pd.read_csv(io.BytesIO(await clientes.read()))
        df_proveedores = pd.read_csv(io.BytesIO(await proveedores.read()))
        df_empleados = pd.read_csv(io.BytesIO(await empleados.read()))
        
        # Procesar los datos
        df_final = procesar_datos()
        
        return JSONResponse({
            "status": "success",
            "message": "Datos cargados exitosamente",
            "registros": len(df_final),
            "empresas": sorted(df_final['id_empresa'].unique().tolist())
        })
    except Exception as e:
        return JSONResponse({
            "status": "error",
            "message": f"Error al cargar datos: {str(e)}"
        }, status_code=400)

@app.get("/analisis", response_class=HTMLResponse)
async def analisis(request: Request, empresa_id: Optional[int] = None):
    """Página de análisis con gráficos"""
    if df_final is None:
        return templates.TemplateResponse(
            "error.html",
            {"request": request, "mensaje": "No hay datos cargados"}
        )
    
    # Generar gráficos
    graficos = generar_graficos(df_final, empresa_id)
    
    # Estadísticas generales
    if empresa_id:
        df_filtrado = df_final[df_final['id_empresa'] == empresa_id]
    else:
        df_filtrado = df_final
    
    estadisticas = {
        "total_contrapartes": len(df_filtrado),
        "empresas_analizadas": df_filtrado['id_empresa'].nunique(),
        "suma_total_clientes": f"${df_filtrado['suma_clientes'].sum():,.2f}",
        "suma_total_proveedores": f"${df_filtrado['suma_proveedores'].sum():,.2f}",
        "suma_total_empleados": f"${df_filtrado['suma_empleados'].sum():,.2f}"
    }
    
    empresas = sorted(df_final['id_empresa'].unique().tolist())
    
    return templates.TemplateResponse(
        "analisis.html",
        {
            "request": request,
            "graficos": graficos,
            "estadisticas": estadisticas,
            "empresas": empresas,
            "empresa_seleccionada": empresa_id
        }
    )

@app.get("/tabla-datos", response_class=HTMLResponse)
async def tabla_datos(request: Request, empresa_id: Optional[int] = None):
    """Página con tabla de datos"""
    if df_final is None:
        return templates.TemplateResponse(
            "error.html",
            {"request": request, "mensaje": "No hay datos cargados"}
        )
    
    if empresa_id:
        df_mostrar = df_final[df_final['id_empresa'] == empresa_id]
    else:
        df_mostrar = df_final
    
    # Convertir a formato para la tabla
    datos = df_mostrar.to_dict('records')
    columnas = df_mostrar.columns.tolist()
    
    empresas = sorted(df_final['id_empresa'].unique().tolist())
    
    return templates.TemplateResponse(
        "tabla.html",
        {
            "request": request,
            "datos": datos,
            "columnas": columnas,
            "empresas": empresas,
            "empresa_seleccionada": empresa_id
        }
    )

@app.get("/api/datos")
async def api_datos(empresa_id: Optional[int] = None):
    """API endpoint para obtener datos en JSON"""
    if df_final is None:
        return JSONResponse({"error": "No hay datos cargados"}, status_code=400)
    
    if empresa_id:
        df_filtrado = df_final[df_final['id_empresa'] == empresa_id]
    else:
        df_filtrado = df_final
    
    return JSONResponse(df_filtrado.to_dict('records'))

if __name__ == "__main__":
    import uvicorn
    print("🚀 Iniciando aplicación...")
    print("📍 Accede a: http://localhost:8000")
    print("📊 Documentación API: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)