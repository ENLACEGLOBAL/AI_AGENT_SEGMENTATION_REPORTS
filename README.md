# Riesgo Transaccional Analytics API

Servicio headless para analítica de riesgo sector-ubicación, generación de gráficos y PDF con IA. Toda la salida se consume vía API en JSON/Base64 para integraciones (PHP/u otros).

## Requisitos
- Python 3.10+
- MariaDB/MySQL accesible en solo lectura
- Variables de entorno en `.env`:
  - `DB_ENGINE`, `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
  - `JWT_SECRET`, `JWT_ALGORITHM`

## Arranque
- `python main.py`
- CORS habilitado; consumo externo requiere JWT.

## Autenticación
- `POST /api/v1/auth/token`
  - Respuesta: `{ "jwt": "..." }`
  - Usar en header: `Authorization: Bearer <jwt>`

## Endpoints
- `GET /api/v1/analytics/sector-ubicacion?empresa_id=38`
  - Devuelve analítica firmada y snapshot JSON
  - Claves: `kpis`, `mapa_colombia` (solo ALTO), `chart_data`, `fatf_status`, `tabla`, `images`, `json_path`, `jwt`
- `GET /api/v1/analytics/chart-image?empresa_id=38`
- `GET /api/v1/analytics/world-map?empresa_id=38`
- `GET /api/v1/analytics/colombia-map?empresa_id=38`
- `GET /api/v1/analytics/latest?empresa_id=38`
- `POST /api/v1/reports/pdf?empresa_id=38&tipo_contraparte=cliente|proveedor`
- `POST /api/v1/ml/train`


## Estructura de Proyecto (simplificada)
```
src/
  core/ (config, seguridad)
  db/ (base, models, repositories)
  rta_api/api/v1/ (auth, analytics, geo, reports, ml)
  services/ (sector_analytics_service, pdf_risk_report_service, report_orchestrator, map_image_service)
  ml_pipelines/ (data_loader, feature_engineering, model_trainer, train)
data_provisional/ (analytics, reports, processed)
generated_images/
main.py
```

text

## Extensión de Analítica
- Cálculo principal: `src/services/sector_analytics_service.py`
- Mapa Colombia (solo ALTO): `src/analytics_modules/sector_ubicacion/sector_geo_analytics.py`
- Render imágenes: `src/services/map_image_service.py`
- PDF IA: `src/services/pdf_risk_report_service.py`
- ML Pipeline: `src/ml_pipelines/`

## Ejemplos de Consumo
1) Obtener token
`curl -s -X POST http://localhost:8000/api/v1/auth/token`

2) Usar token en header
`curl -H "Authorization: Bearer <jwt>" "http://localhost:8000/api/v1/analytics/sector-ubicacion?empresa_id=38"`

3) Descargar imagen donut
`curl -H "Authorization: Bearer <jwt>" "http://localhost:8000/api/v1/analytics/chart-image?empresa_id=38"`

4) Mapa mundial FATF
`curl -H "Authorization: Bearer <jwt>" "http://localhost:8000/api/v1/analytics/world-map?empresa_id=38"`

5) Mapa Colombia ALTO
`curl -H "Authorization: Bearer <jwt>" "http://localhost:8000/api/v1/analytics/colombia-map?empresa_id=38"`

6) Generar PDF IA
`curl -s -X POST -H "Authorization: Bearer <jwt>" "http://localhost:8000/api/v1/reports/pdf?empresa_id=38&tipo_contraparte=cliente"`

## Buenas Prácticas
- Solo lectura sobre BD; enriquecimiento con tablas de referencia
- Firmas JWT para el payload de analítica
- Salidas en JSON/Base64 para integración sin estado
- Directorios de datos versionados por timestamp
