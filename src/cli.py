import argparse
import pandas as pd
from pathlib import Path
from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import upsert_curated

def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    run_id = new_run_id()

    if args.command == 'run-all':
        print(f"Starting pipeline run: {run_id}")
        try:
            # 1. Extract
            run_raw_path = extract_sources(run_id)
            
            # 2. Transform (Staging)
            print("Running staging transformations...")
            staging_result = build_staging(Path(run_raw_path), run_id)
            for name, df in staging_result['staging'].items():
                print(f" -> staged {name}: {len(df)} rows")
                
            # 3. Transform (Curated)
            print("Running curated transformations...")
            curated_count, curated_q_count = build_curated(run_id)
            print(f" -> curated sales_order_lines: {curated_count} rows")
            
            # 4. Load to PostgreSQL
            print("Loading into PostgreSQL...")
            curated_dir = path_for('curated_dir') / f'run_id={run_id}'
            final_df = pd.read_parquet(curated_dir / 'sales_order_lines.parquet')
            loaded_count = upsert_curated(final_df, run_id)
            print(f" -> Successfully upserted {loaded_count} records into curated.sales_order_lines")
            
            print("Pipeline run complete.")
            
        except Exception as e:
            print(f"\n[PIPELINE FAILURE] A system exception occurred.")
            print(f"Error Context: {str(e)}")
            raise

    else:
        raise NotImplementedError(f'Command not yet fully implemented: {args.command}')

if __name__ == '__main__':
    main()