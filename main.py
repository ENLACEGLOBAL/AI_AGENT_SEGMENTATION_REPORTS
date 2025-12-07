from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Riesgo Transaccional API")
from src.rta_api.api.v1.sector_ubicacion import router as analytics_router
app.include_router(analytics_router)
from src.rta_api.api.v1.geo import router as geo_router
app.include_router(geo_router)
from src.rta_api.api.v1.reports import router as reports_router
app.include_router(reports_router)
from src.rta_api.api.v1.ml import router as ml_router
app.include_router(ml_router)
from src.rta_api.api.v1.auth import router as auth_router
app.include_router(auth_router)
from src.rta_api.api.v1.maintenance import router as maintenance_router
app.include_router(maintenance_router)
from src.rta_api.api.v1.cruces import router as cruces_router
app.include_router(cruces_router)

# CORS para consumo desde PHP
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API sin vistas: todo se consume vía JSON

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

@app.get("/")
async def index():
    return {"status": "ok", "service": "riesgo-transaccional-api"}

@app.post("/cargar-datos")
async def cargar_datos():
    return {"status": "deprecated"}

@app.get("/analisis")
async def analisis():
    return {"status": "deprecated"}

@app.get("/tabla-datos")
async def tabla_datos():
    return {"status": "deprecated"}

@app.get("/api/datos")
async def api_datos():
    return {"status": "deprecated"}

if __name__ == "__main__":
    import uvicorn
    print("🚀 API iniciada...")
    print("📍 http://localhost:8000")
    print("📊 http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)
