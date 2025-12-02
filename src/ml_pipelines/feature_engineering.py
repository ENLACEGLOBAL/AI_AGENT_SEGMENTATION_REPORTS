from typing import List, Dict, Any

def build_feature_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for it in items:
        kpis = it.get("kpis", {})
        chart = it.get("chart_data", {})
        labels = chart.get("labels", [])
        values = chart.get("values", [])
        dist = {f"ciiu_{str(l).lower()}": float(v) for l, v in zip(labels, values)}
        row = {
            "empresa_id": it.get("empresa_id"),
            "total_transacciones": float(kpis.get("total_transacciones", 0)),
            "monto_total": float(kpis.get("monto_total", 0)),
        }
        row.update(dist)
        rows.append(row)
    return rows
