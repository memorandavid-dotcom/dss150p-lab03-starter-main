import time
import statistics
import os
import pandas as pd
from pathlib import Path
from src.config import PROJECT_ROOT, path_for
from src.load.postgres import _get_engine

def get_latest_curated():
    """Finds the most recent curated parquet file to run tests against."""
    curated_base = path_for('curated_dir')
    runs = sorted([d for d in curated_base.iterdir() if d.is_dir()])
    if not runs:
        raise FileNotFoundError("No curated data found. Run pipeline first.")
    return runs[-1] / 'sales_order_lines.parquet'

def run_benchmark(repeats: int = 5):
    print(f"Running benchmarks with {repeats} repetitions. Please wait...")
    source_file = get_latest_curated()
    df = pd.read_parquet(source_file)
    
    bench_dir = Path(PROJECT_ROOT) / 'data' / 'benchmarks'
    bench_dir.mkdir(parents=True, exist_ok=True)
    
    csv_path = bench_dir / 'data.csv'
    jsonl_path = bench_dir / 'data.jsonl'
    parquet_path = bench_dir / 'data.parquet'
    
    # --- 1. Measure File Sizes and Write Times ---
    t0 = time.perf_counter(); df.to_csv(csv_path, index=False)
    csv_write = time.perf_counter() - t0; csv_size = os.path.getsize(csv_path)
    
    t0 = time.perf_counter(); df.to_json(jsonl_path, orient='records', lines=True)
    jsonl_write = time.perf_counter() - t0; jsonl_size = os.path.getsize(jsonl_path)
    
    t0 = time.perf_counter(); df.to_parquet(parquet_path, index=False, compression='snappy')
    parq_write = time.perf_counter() - t0; parq_size = os.path.getsize(parquet_path)

    # --- 2. Measure Full Reads ---
    def measure_read(func, path, reps):
        times = []
        for _ in range(reps):
            t0 = time.perf_counter(); func(path); times.append(time.perf_counter() - t0)
        return statistics.median(times)

    csv_read = measure_read(pd.read_csv, csv_path, repeats)
    jsonl_read = measure_read(lambda p: pd.read_json(p, orient='records', lines=True), jsonl_path, repeats)
    parq_read = measure_read(pd.read_parquet, parquet_path, repeats)

    engine = _get_engine()
    pg_times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        with engine.connect() as conn:
            _ = pd.read_sql("SELECT * FROM curated.sales_order_lines", conn)
        pg_times.append(time.perf_counter() - t0)
    pg_read = statistics.median(pg_times)

    # --- 3. Measure Filtered Reads (status='DELIVERED') ---
    def measure_filt(func, path, reps):
        times = []
        for _ in range(reps):
            t0 = time.perf_counter(); tmp = func(path); _ = tmp[tmp['status'] == 'DELIVERED']
            times.append(time.perf_counter() - t0)
        return statistics.median(times)

    csv_filt = measure_filt(pd.read_csv, csv_path, repeats)
    jsonl_filt = measure_filt(lambda p: pd.read_json(p, orient='records', lines=True), jsonl_path, repeats)
    
    # Parquet allows native filter pushdown
    parq_filt_times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        _ = pd.read_parquet(parquet_path, filters=[('status', '=', 'DELIVERED')])
        parq_filt_times.append(time.perf_counter() - t0)
    parq_filt = statistics.median(parq_filt_times)

    # Postgres native WHERE clause
    pg_filt_times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        with engine.connect() as conn:
            _ = pd.read_sql("SELECT * FROM curated.sales_order_lines WHERE status='DELIVERED'", conn)
        pg_filt_times.append(time.perf_counter() - t0)
    pg_filt = statistics.median(pg_filt_times)

    # --- 4. Compile and Save ---
    res_df = pd.DataFrame([
        {'Format': 'CSV', 'Size_Bytes': csv_size, 'Write_Sec': round(csv_write, 4), 'Full_Read_Sec': round(csv_read, 4), 'Filtered_Read_Sec': round(csv_filt, 4)},
        {'Format': 'JSON Lines', 'Size_Bytes': jsonl_size, 'Write_Sec': round(jsonl_write, 4), 'Full_Read_Sec': round(jsonl_read, 4), 'Filtered_Read_Sec': round(jsonl_filt, 4)},
        {'Format': 'Parquet', 'Size_Bytes': parq_size, 'Write_Sec': round(parq_write, 4), 'Full_Read_Sec': round(parq_read, 4), 'Filtered_Read_Sec': round(parq_filt, 4)},
        {'Format': 'PostgreSQL', 'Size_Bytes': 'Server', 'Write_Sec': 'N/A', 'Full_Read_Sec': round(pg_read, 4), 'Filtered_Read_Sec': round(pg_filt, 4)}
    ])
    
    print("\n--- BENCHMARK RESULTS ---")
    print(res_df.to_string(index=False))
    res_df.to_csv(bench_dir / 'benchmark_results.csv', index=False)
    print(f"\nSaved to data/benchmarks/benchmark_results.csv")