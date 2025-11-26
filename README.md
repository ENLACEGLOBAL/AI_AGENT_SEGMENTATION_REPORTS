# RiesgoTransaccional-Analytics-Microservice

## Analítica: sector_ubicacion

Este módulo genera un resumen analítico a partir del archivo `data_provisional/Segmentacion_jurisdicciones (1).csv` y guarda los resultados en un JSON dentro de la misma carpeta.

- Entrada: `c:\Users\Usuario\AI_AGENT_SEGMENTATION_REPORTS\data_provisional\Segmentacion_jurisdicciones (1).csv`
- Salida: `c:\Users\Usuario\AI_AGENT_SEGMENTATION_REPORTS\data_provisional\sector_ubicacion_analytics.json`

### Cómo ejecutar

Opción A: Ejecutar directamente el módulo desde la línea de comandos.

```
python -m src.analytics_modules.sector_ubicacion.analytics
```

Opción B: Usar la función programáticamente.

```python
from analytics_modules.sector_ubicacion.analytics import save_sector_ubicacion_analytics

data_dir = r"c:\\Users\\Usuario\\AI_AGENT_SEGMENTATION_REPORTS\\data_provisional"
output_path = save_sector_ubicacion_analytics(data_dir)
print("JSON generado en:", output_path)
```

### Qué calcula

- Resumen global: registros totales, promedios y extremos de `VALOR-RIESGO`.
- Por categoría: conteo y porcentaje por `Categoría`.
- Por departamento: registros, promedio de riesgo, municipio con mayor riesgo y categoría más frecuente.
- Top 10 municipios por promedio de riesgo.
- Top 10 departamentos por promedio de riesgo.

### Notas

- El módulo no requiere dependencias externas (usa librerías estándar de Python).
- El CSV es leído con encabezados: `id`, `Departamento`, `Municipio`, `Divipola`, `Categoría`, `VALOR-RIESGO`.
- Los valores de riesgo admiten `,` o `.` como separador decimal.

## Generación de Mapas (HTML + JSON)

Se incluyen generadores de mapas que no requieren dependencias externas (Leaflet vía CDN):

### Colombia: Choropleth por riesgo promedio

- Genera JSON `colombia_departamentos_riesgo.json` en `data_provisional` y HTML `colombia_choropleth.html` en la raíz del proyecto.

Comando:

```
python -m src.analytics_modules.graph_generator
```

Qué hace:
- Calcula el promedio de `VALOR-RIESGO` por `Departamento` usando el CSV `Segmentacion_jurisdicciones (1).csv`.
- Descarga GeoJSON público de departamentos de Colombia.
- Colorea por terciles (bajo/medio/alto) y muestra popup por departamento.

### Mundo: Cooperantes vs No Cooperantes

- Genera JSON `world_cooperation_status.json` en `data_provisional` y HTML `world_cooperation_map.html` en la raíz del proyecto.

Fuente y datos:
- CSV: `Historico_paises_coop_nocop_par.csv` usando la columna `CLASIFICACION` (se reduce a binario; `PARAISO FISCAL` se trata como `NO COOPERANTE`).
- GeoJSON público de países del mundo.

Notas de matching:
- Los nombres del CSV están en español y el GeoJSON en inglés; se incluyen alias básicos (p.ej., `ALEMANIA -> GERMANY`). Países sin correspondencia aparecerán en gris.
- Requiere conexión a internet para descargar los GeoJSON.


Microservicio de Analytics para la Gestión de Riesgo Transaccional.

## Estructura del Proyecto
RiesgoTransaccional-Analytics-Microservice/
├── ci/ # Configuración de CI/CD
├── infrastructure/ # Infraestructura como código
├── src/
│ ├── analytics_modules/ # Lógica pura para generación de artefactos
│ │ ├── graph_generator.py # Genera bytes/base64 de gráficos (PNG/SVG)
│ │ ├── image_utils.py # Utilidades: raster/vector, thumbnail, metadata
│ │ └── ia_engine.py # Lógica IA para generar informes
│ ├── core/ # Configuraciones centrales
│ │ ├── config.py # Pydantic settings (env validation)
│ │ ├── logging_config.py # Configuración de logging
│ │ ├── security.py # JWT, scopes, roles
│ │ └── exceptions.py # Excepciones específicas y handlers
│ ├── db/ # Capa de base de datos
│ │ ├── base.py # Engine, sessionmaker
│ │ ├── models/ # SQLAlchemy models
│ │ ├── repositories/ # Patrones repository (DB access)
│ │ └── migrations/ # Alembic migrations
│ ├── domain/ # Lógica de negocio
│ │ ├── schemas/ # Pydantic schemas (requests/responses)
│ │ ├── services/ # Lógica de negocio (sin I/O directo)
│ │ │ ├── riesgo_service.py
│ │ │ └── analytics_service.py # Lógica central de tablas/analítica
│ │ └── adapters/ # Adaptadores para 3rd-party (storage, mail)
│ ├── ml_pipelines/ # Scripts de entrenamiento ML
│ │ ├── data_processor.py
│ │ ├── enrichment_logic.py
│ │ └── train_model.py
│ └── rta_api/ # Paquete principal FastAPI
│ ├── api/
│ │ ├── v1/
│ │ │ ├── init.py
│ │ │ ├── routes.py # Registra routers: dashboard, alerts, analytics, graphs, ia
│ │ │ ├── dashboard.py # Endpoints: /api/v1/dashboard
│ │ │ ├── alerts.py # Endpoints: /api/v1/alerts
│ │ │ ├── graphs.py # Endpoints: /api/v1/graphs (devuelve bytes/base64)
│ │ │ └── ia_reports.py # Endpoints: /api/v1/ia/report
│ │ └── deps.py # Dependencias para los routers (db, auth, cache)
│ ├── tasks/ # Workers asíncronos (celery / rq)
│ │ ├── graph_worker.py
│ │ └── ia_report_worker.py
│ ├── storage/ # Abstracción de almacenamiento (S3/local)
│ │ └── object_store.py
│ ├── utils/ # Utilidades
│ │ ├── serializers.py # Helpers para base64, json serializable
│ │ └── metrics.py # Hooks para Prometheus
│ └── main.py # Punto de arranque FastAPI
├── tests/ # Tests automatizados
│ ├── unit/ # Tests unitarios
│ ├── integration/ # Tests de integración
│ └── conftest.py # Configuración de pytest
├── env.example # Variables de entorno de ejemplo
├── pyproject.toml # Configuración de packaging y herramientas
└── README.md # Este archivo

text

## Descripción de Módulos Principales

### 🚀 **rta_api/**
- **API FastAPI** con versionamiento (v1)
- **Endpoints principales**: Dashboard, Alertas, Gráficos, Reportes IA
- **Dependencias centralizadas**: BD, autenticación, caché

### 🧠 **analytics_modules/**
- Generación de gráficos en bytes/base64 (PNG/SVG)
- Motor de IA para generación de reportes
- Utilidades de procesamiento de imágenes

### ⚙️ **core/**
- Configuración central con validación de environment variables
- Seguridad JWT y manejo de roles
- Configuración de logging y manejo de excepciones

### 🗄️ **db/**
- Modelos SQLAlchemy
- Patrón Repository para acceso a datos
- Migraciones con Alembic

### 📊 **domain/**
- Esquemas Pydantic para requests/responses
- Lógica de negocio pura (services)
- Adaptadores para servicios externos

### 🤖 **ml_pipelines/**
- Scripts de entrenamiento de modelos ML
- Procesamiento y enriquecimiento de datos
- Pipeline completo de machine learning

## Configuración Rápida

1. **Clonar y configurar environment**
   ```bash
   cp env.example .env
   # Editar .env con tus valores
Instalar dependencias

bash
pip install -e .
Ejecutar aplicación

bash
cd src/rta_api
python main.py
Desarrollo
Tests: pytest tests/

Formato: black src/ tests/

Linting: flake8 src/ tests/

Licencia
MIT License

text