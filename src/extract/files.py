from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    1. Create data/raw/run_id=<run_id>/.
    2. Copy customers.csv, products.json, and orders.csv from data/source/.
    3. Return the run-specific raw path.
    4. Never modifies source files in place (copy2 preserves metadata,
       source stays untouched -> General Rule 1).
    """
    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    raw_dir.mkdir(parents=True, exist_ok=True)

    required_files = ['customers.csv', 'products.json', 'orders.csv']
    for filename in required_files:
        src_path = source_dir / filename
        if not src_path.exists():
            raise FileNotFoundError(
                f'Expected source file missing: {src_path}. '
                'Check config/settings.yml -> pipeline.source_dir.'
            )
        dest_path = raw_dir / filename
        # copy2 opens/copies/closes the file itself; no need to hold
        # our own file handle open across the copy, which is what causes
        # the classic Windows "file still in use" / handle-sync problems.
        shutil.copy2(src_path, dest_path)

    return raw_dir