import argparse
from pathlib import Path
from src.config import PROJECT_ROOT, DB, SETTINGS
from src.common.audit import new_run_id
from src.extract.files import extract_sources
from src.transform.staging import build_staging

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

    # Generate a unique run ID for this execution
    run_id = new_run_id()
    source_dir = Path(PROJECT_ROOT) / SETTINGS['pipeline']['source_dir']
    raw_dir = Path(PROJECT_ROOT) / 'data' / 'raw'
    staging_dir = Path(PROJECT_ROOT) / 'data' / 'staging'
    quarantine_dir = Path(PROJECT_ROOT) / 'data' / 'quarantine'

    if args.command == 'run-all':
        print(f"Starting pipeline run: {run_id}")
        
        try:
            # 1. Extract
            run_raw_path = extract_sources(str(source_dir), str(raw_dir), run_id)
            
            # 2. Transform (Staging)
            print("Running staging transformations...")
            staged_dfs, quarantine_df = build_staging(run_raw_path, run_id)
            
            # Save Staging to Parquet
            staging_dir.mkdir(parents=True, exist_ok=True)
            for name, df in staged_dfs.items():
                df.to_parquet(staging_dir / f"{name}.parquet", index=False)
                print(f" -> [Debug] Saved {name}.parquet")
                
            # Save Quarantine to CSV
            if not quarantine_df.empty:
                quarantine_dir.mkdir(parents=True, exist_ok=True)
                quarantine_df.to_csv(quarantine_dir / f"quarantined_{run_id}.csv", index=False)
                print(f"Quarantined {len(quarantine_df)} invalid records.")
                
            print("Extract and Staging Transform complete.")
            
        except Exception as e:
            print(f"\n[PIPELINE FAILURE] A system exception occurred in the '{args.command}' process.")
            print(f"Error Context: {str(e)}")
            raise

    else:
        raise NotImplementedError(f'Command not yet fully implemented: {args.command}')

if __name__ == '__main__':
    main()