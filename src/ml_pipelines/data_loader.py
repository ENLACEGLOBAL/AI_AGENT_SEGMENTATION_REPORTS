import json
import os
from typing import List, Dict, Any

def find_analytics_json(base_dir: str) -> List[str]:
    files = []
    for name in os.listdir(base_dir):
        if name.startswith("analytics_") and name.endswith(".json"):
            files.append(os.path.join(base_dir, name))
    return sorted(files)

def load_analytics(paths: List[str]) -> List[Dict[str, Any]]:
    data = []
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            data.append(json.load(f))
    return data
