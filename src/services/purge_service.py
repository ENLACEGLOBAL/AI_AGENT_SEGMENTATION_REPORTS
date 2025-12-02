import os
from typing import List, Optional

DATA_DIR = "data_provisional"

def _list_files(pattern_prefix: str) -> List[str]:
    files = []
    for name in os.listdir(DATA_DIR):
        if name.startswith(pattern_prefix) and name.endswith(".json"):
            files.append(os.path.join(DATA_DIR, name))
    return sorted(files)

def purge_analytics(empresa_id: Optional[int] = None, retain: int = 3) -> List[str]:
    prefix = "analytics_" + (str(empresa_id) + "_" if empresa_id is not None else "")
    files = _list_files(prefix)
    if len(files) <= retain:
        return []
    to_delete = files[:-retain]
    for path in to_delete:
        try:
            os.remove(path)
        except Exception:
            pass
    return to_delete

