import argparse
import json
import os
from datetime import datetime

from src.db.base import SourceSessionLocal
from src.services.cruces_analytics_service import cruces_analytics_service


def main():
    parser = argparse.ArgumentParser(description="Generar analítica completa para una empresa")
    parser.add_argument("--empresa", type=int, required=True, help="ID de empresa")
    parser.add_argument("--fecha", type=str, default=None, help="Fecha específica (YYYY-MM-DD)")
    parser.add_argument("--monto-min", type=float, default=None, help="Monto mínimo para filtrar transacciones")
    parser.add_argument("--out", type=str, default=None, help="Ruta de salida del JSON")
    parser.add_argument("--full", action="store_true", help="Desactivar compactación del JSON")
    parser.add_argument("--universo", action="store_true", help="Usar universo completo (sin filtro de cruces)")
    args = parser.parse_args()

    if args.full:
        os.environ["COMPACT_JSON"] = "false"
        os.environ["JSON_LIMIT"] = "9999999"
        os.environ["JSON_TXN_LIMIT"] = "9999999"
    if args.universo:
        os.environ["ANALYTICS_UNIVERSO"] = "true"

    out_path = args.out
    if not out_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join("data_provisional", "analytics")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"analytics_empresa_{args.empresa}_{ts}.json")

    db = SourceSessionLocal()
    try:
        res = cruces_analytics_service.generate_cruces_analytics(
            db, empresa_id=args.empresa, fecha=args.fecha, monto_min=args.monto_min
        )
    finally:
        db.close()

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    print(out_path)


if __name__ == "__main__":
    main()
