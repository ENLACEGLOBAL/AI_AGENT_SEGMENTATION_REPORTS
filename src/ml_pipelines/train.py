import os
from src.ml_pipelines.data_loader import find_analytics_json, load_analytics
from src.ml_pipelines.feature_engineering import build_feature_rows
from src.ml_pipelines.model_trainer import save_dataset, train_baseline

def run(data_dir: str = "data_provisional", out_dir: str = os.path.join("data_provisional", "processed")):
    paths = find_analytics_json(data_dir)
    analytics = load_analytics(paths)
    rows = build_feature_rows([a.get("data", a) for a in analytics])
    ds_path = save_dataset(rows, out_dir)
    model_meta = train_baseline(rows, out_dir)
    return {"dataset": ds_path, "model": model_meta}
