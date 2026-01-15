import os
import glob
from typing import List, Optional

DATA_DIR = "data_provisional"
REPORTS_DIR = os.path.join(DATA_DIR, "reports")
IMAGES_DIR = "generated_images"

def _cleanup_directory(directory: str, pattern: str, retain: int) -> List[str]:
    """Helper to clean files matching pattern in directory, keeping latest 'retain'."""
    if not os.path.exists(directory):
        return []
    
    # Get all files matching pattern
    files = glob.glob(os.path.join(directory, pattern))
    
    # Sort by modification time (newest last)
    files.sort(key=os.path.getmtime)
    
    if len(files) <= retain:
        return []
        
    to_delete = files[:-retain]
    deleted = []
    
    for path in to_delete:
        try:
            os.remove(path)
            deleted.append(path)
        except Exception as e:
            print(f"Error deleting {path}: {e}")
            
    return deleted

def purge_analytics(empresa_id: Optional[int] = None, retain: int = 3) -> List[str]:
    deleted_files = []
    
    # 1. Clean JSON analytics
    # analytics_{id}_*.json
    pattern_id = f"*{empresa_id}*" if empresa_id else "*"
    
    deleted_files.extend(_cleanup_directory(DATA_DIR, f"analytics_{pattern_id}.json", retain))
    deleted_files.extend(_cleanup_directory(DATA_DIR, f"cruces_analytics_{pattern_id}.json", retain))
    
    # 2. Clean PDF Reports
    # Reporte_Riesgo_{id}_*.pdf
    deleted_files.extend(_cleanup_directory(REPORTS_DIR, f"*{pattern_id}*.pdf", retain))
    
    # 3. Clean Generated Images
    # chart_{id}_*.png
    deleted_files.extend(_cleanup_directory(IMAGES_DIR, f"*{pattern_id}*.png", retain))
    
    return deleted_files

