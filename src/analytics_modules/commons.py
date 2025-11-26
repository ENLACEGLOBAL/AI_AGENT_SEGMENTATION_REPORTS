# src/analytics_modules/commons.py

from datetime import datetime
from decimal import Decimal


def safe_decimal(value) -> Decimal:
    """Convierte valores numéricos a Decimal de forma segura."""
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(0)


def parse_date(date_str: str, fmt: str = "%Y-%m-%d") -> datetime | None:
    """Convierte una cadena en fecha."""
    try:
        return datetime.strptime(date_str, fmt)
    except Exception:
        return None


def normalize_text(text: str) -> str:
    """Limpia y normaliza texto básico."""
    if not text:
        return ""
    return " ".join(text.strip().lower().split())


def merge_dicts(a: dict, b: dict) -> dict:
    """Mezcla dos diccionarios sin sobrescribir claves en conflicto."""
    result = a.copy()
    for k, v in b.items():
        if k not in result:
            result[k] = v
    return result
