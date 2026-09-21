import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

def extract_sources(source_dir: str, raw_dir: str, run_id: str = None) -> str:
    """
    Copies source files to a run-specific raw directory without altering content.
    Returns the path to the newly created raw run directory.
    """
    if not run_id:
        # Enforce UTC timestamps as required by Section 5 rules
        run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    
    source_path = Path(source_dir)
    run_raw_path = Path(raw_dir) / f"run_id={run_id}"
    
    # Create the run-specific directory
    run_raw_path.mkdir(parents=True, exist_ok=True)
    
    # Copy all files from source to the new raw directory
    for file_path in source_path.iterdir():
        if file_path.is_file():
            shutil.copy2(file_path, run_raw_path / file_path.name)
            
    print(f"Successfully extracted sources to {run_raw_path}")
    return str(run_raw_path)