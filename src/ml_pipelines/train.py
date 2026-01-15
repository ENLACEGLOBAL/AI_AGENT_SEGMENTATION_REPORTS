import os

from src.ml_pipelines.data_loader import find_analytics_json, load_analytics
from src.ml_pipelines.feature_engineering import build_feature_rows
from src.ml_pipelines.model_trainer import (
    load_docx_criteria,
    load_model,
    save_dataset,
    score_and_recommend,
    train_baseline,
)


def run(
    data_dir: str = "data_provisional",
    out_dir: str = os.path.join("data_provisional", "processed"),
    docx_path: str | None = None,
):
    paths = find_analytics_json(data_dir)
    if not paths:
        os.makedirs(out_dir, exist_ok=True)
        return {
            "dataset": None,
            "model": None,
            "recommendations": [],
            "message": "No se encontraron analytics_*.json para entrenar.",
        }
    analytics = load_analytics(paths)
    rows = build_feature_rows([a.get("data", a) for a in analytics])
    ds_path = save_dataset(rows, out_dir)
    model_path = train_baseline(rows, out_dir)
    model = load_model(model_path)
    if docx_path is None:
        default_docx = os.path.join(
            os.getcwd(), "CRITERIOS SEÑALES DE ALERTA- RECOMENDACIONES.docx"
        )
        if os.path.exists(default_docx):
            docx_path = default_docx
    docx_criteria = (
        load_docx_criteria(docx_path) if docx_path and os.path.exists(docx_path) else None
    )
    recs = score_and_recommend(rows, model, docx_criteria=docx_criteria)
    recs_path = os.path.join(out_dir, "recommendations.json")
    with open(recs_path, "w", encoding="utf-8") as f:
        import json

        json.dump({"recommendations": recs}, f, ensure_ascii=False, indent=2)
    return {"dataset": ds_path, "model": model_path, "recommendations": recs_path, "top": recs[:10]}
