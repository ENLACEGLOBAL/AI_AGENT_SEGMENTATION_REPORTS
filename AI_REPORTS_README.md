# 🤖 Sistema de Generación de Informes con IA

## Descripción

Sistema completo para generar analíticas de riesgo y reportes narrativos usando IA (Google Gemini).

## 🚀 Endpoints Disponibles

### 1. Generar Analítica Individual
```bash
GET /api/analytics/sector-ubicacion?empresa_id=38
```
Genera analítica para una empresa específica.

### 2. Generar Analíticas en Lote
```bash
POST /api/analytics/batch-generate
```
Genera analíticas para TODAS las empresas en el CSV.

### 3. Generar Informe con IA
```bash
POST /api/reports/generate/{empresa_id}
```
Genera un informe narrativo usando IA basado en la analítica existente.

### 4. Exportar Informe a HTML
```bash
GET /api/reports/{empresa_id}/html
```
Exporta el informe a formato HTML con estilos.

## 📋 Flujo de Uso

### Paso 1: Generar Todas las Analíticas
```bash
curl -X POST http://localhost:8003/api/analytics/batch-generate
```

Esto generará archivos JSON en `data_provisional/` para cada empresa.

### Paso 2: Generar Informe con IA (Requiere API Key)

**Configurar API Key de Gemini:**
```bash
# Windows PowerShell
$env:GEMINI_API_KEY="tu-api-key-aqui"

# Windows CMD
set GEMINI_API_KEY=tu-api-key-aqui

# Linux/Mac
export GEMINI_API_KEY="tu-api-key-aqui"
```

**Generar informe:**
```bash
curl -X POST http://localhost:8003/api/reports/generate/38
```

### Paso 3: Exportar a HTML
```bash
curl http://localhost:8003/api/reports/38/html
```

El archivo HTML se guardará en `data_provisional/report_38_TIMESTAMP.html`

## 🔑 Obtener API Key de Google Gemini

1. Ve a https://makersuite.google.com/app/apikey
2. Crea un nuevo proyecto o selecciona uno existente
3. Genera una API key
4. Configúrala como variable de entorno

## 📁 Archivos Generados

- `data_provisional/analytics_{empresa_id}_{timestamp}.json` - Analítica de datos
- `data_provisional/ai_report_{empresa_id}_{timestamp}.json` - Informe IA (JSON)
- `data_provisional/report_{empresa_id}_{timestamp}.html` - Informe IA (HTML)
- `generated_images/chart_{empresa_id}_{timestamp}.png` - Gráficos

## 🎯 Secciones del Informe IA

El informe generado incluye:

1. **Resumen Ejecutivo** - Visión general del perfil de riesgo
2. **Análisis de Riesgo** - Evaluación detallada de factores de riesgo
3. **Análisis Geográfico** - Distribución y exposición por país
4. **Recomendaciones** - Medidas de mitigación y mejores prácticas

## ⚠️ Notas Importantes

- **Sin API Key**: El sistema funcionará pero los informes dirán "[AI service not configured]"
- **Límites de API**: Google Gemini tiene límites de uso gratuitos
- **Procesamiento en Lote**: Puede tardar varios minutos dependiendo del número de empresas

## 🧪 Ejemplo Completo

```bash
# 1. Generar todas las analíticas
curl -X POST http://localhost:8003/api/analytics/batch-generate

# 2. Configurar API key (PowerShell)
$env:GEMINI_API_KEY="AIza..."

# 3. Reiniciar el servidor
# Ctrl+C y luego: python main.py

# 4. Generar informe con IA para empresa 38
curl -X POST http://localhost:8003/api/reports/generate/38

# 5. Exportar a HTML
curl http://localhost:8003/api/reports/38/html

# 6. Abrir el archivo HTML generado en el navegador
```

## 📊 Documentación API Completa

Visita: http://localhost:8003/docs
