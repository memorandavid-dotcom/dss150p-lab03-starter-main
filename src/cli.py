import argparse
import pandas as pd
from pathlib import Path
from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.load.postgres import upsert_curated, write_partitioned_parquet, load_partition
from src.benchmark.runner import run_benchmark

def get_latest_run_id():
    """Helper to find the most recent successful pipeline run."""
    curated_base = path_for('curated_dir')
    if not curated_base.exists():
        return None
    runs = sorted([d.name.split('=')[1] for d in curated_base.glob('run_id=*') if d.is_dir()])
    return runs[-1] if runs else None

def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    
    b = sub.add_parser('benchmark')
    b.add_argument('--repeats', type=int, default=5)
    
    p = sub.add_parser('load-partition')
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--month', type=int, required=True)
    
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    if args.command == 'benchmark':
        run_benchmark(args.repeats)
        return

    run_id = new_run_id()

    if args.command == 'load-partition':
        print(f"Loading specific partition: Year={args.year}, Month={args.month} (Run ID: {run_id})")
        load_partition(args.year, args.month, run_id)
        return

    if args.command == 'run-all':
        print(f"Starting pipeline run: {run_id}")
        try:
            run_raw_path = extract_sources(run_id)
            
            print("Running staging transformations...")
            staging_result = build_staging(Path(run_raw_path), run_id)
            for name, df in staging_result['staging'].items():
                print(f" -> staged {name}: {len(df)} rows")
                
            print("Running curated transformations...")
            curated_count, curated_q_count = build_curated(run_id)
            print(f" -> curated sales_order_lines: {curated_count} rows")
            
            print("Loading into PostgreSQL...")
            curated_dir = path_for('curated_dir') / f'run_id={run_id}'
            final_df = pd.read_parquet(curated_dir / 'sales_order_lines.parquet')
            loaded_count = upsert_curated(final_df, run_id)
            print(f" -> Successfully upserted {loaded_count} records into curated.sales_order_lines")
            
            print("Materializing partitioned Parquet dataset...")
            write_partitioned_parquet(run_id)
            
            print("Pipeline run complete.")
            
        except Exception as e:
            print(f"\n[PIPELINE FAILURE] A system exception occurred.")
            print(f"Error Context: {str(e)}")
            raise

    else:
        raise NotImplementedError(f'Command not yet fully implemented: {args.command}')

if __name__ == '__main__':
    main()