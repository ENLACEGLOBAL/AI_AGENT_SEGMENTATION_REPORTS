FROM python:3.11-slim

WORKDIR /app

# System deps for some wheels (optional)
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install via pyproject (PEP 517)
RUN pip install --no-cache-dir .

# Run FastAPI app
CMD ["python", "main.py", "api", "--host", "0.0.0.0", "--port", "8000"]

