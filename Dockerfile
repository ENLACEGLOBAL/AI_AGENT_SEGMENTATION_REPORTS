# 1. Imagen base de Python liviana
FROM python:3.10-slim

# 2. Directorio de trabajo
WORKDIR /app

# 3. Dependencias del sistema (ESENCIAL para ReportLab, Matplotlib y fuentes)
RUN apt-get update && apt-get install -y \
    build-essential \
    libjpeg-dev \
    zlib1g-dev \
    libfreetype6-dev \
    liblcms2-dev \
    libopenjp2-7-dev \
    libtiff-dev \
    tk-dev \
    tcl-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Copiar e instalar requerimientos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar todo el código fuente
COPY . .

# 6. Crear carpetas de datos temporales para que Docker tenga permisos
RUN mkdir -p data_provisional/reports generated_images

# 7. Exponer el puerto de la API
EXPOSE 8585

# 8. Comando de ejecución
# Usamos 'python main.py api' para que pase por tu lógica de rutas de sistema
CMD ["python", "main.py", "api", "--host", "0.0.0.0", "--port", "8585"]