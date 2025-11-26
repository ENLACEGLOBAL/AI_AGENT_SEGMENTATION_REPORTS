FROM python:3.11-slim

WORKDIR /app

# No external dependencies required; code uses Python stdlib
# Copy project files (will be overridden by bind mount in compose)
COPY . /app

# Default command generates the IA report
CMD ["python", "-m", "src.analytics_modules.ia_report_generator"]