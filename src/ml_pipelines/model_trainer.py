import json
import os
from typing import List, Dict, Any

def save_dataset(rows: List[Dict[str, Any]], out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "training_dataset.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"rows": rows}, f, ensure_ascii=False, indent=2)
    return path

def train_baseline(rows: List[Dict[str, Any]], out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "model_metadata.json")
    meta = {
        "samples": len(rows),
        "features": sorted({k for r in rows for k in r.keys() if k not in {"empresa_id"}}),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return path
