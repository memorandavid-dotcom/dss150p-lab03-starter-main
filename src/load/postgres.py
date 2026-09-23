import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from src.config import DB, PROJECT_ROOT, path_for

def _get_engine():
    """Create a SQLAlchemy connection engine, forcing credentials from .env."""
    user = DB.get('user', 'postgres')
    host = DB.get('host', 'postgres')
    port = DB.get('port', 5432)
    db = DB.get('dbname', 'postgres')
    
    pwd = DB.get('password')
    if not pwd:
        env_file = Path(PROJECT_ROOT) / '.env'
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith('POSTGRES_PASSWORD='):
                    pwd = line.split('=', 1)[1].strip().strip('\'"')
                    break
                    
    if not pwd:
        pwd = 'postgres'
        
    return create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

def write_partitioned_parquet(run_id: str) -> Path:
    """Task C: Materialize curated dataset partitioned by order_year and order_month."""
    curated_dir = path_for('curated_dir') / f'run_id={run_id}'
    parquet_file = curated_dir / 'sales_order_lines.parquet'
    
    if not parquet_file.exists():
        raise FileNotFoundError(f"Curated file not found at {parquet_file}")
        
    df = pd.read_parquet(parquet_file)
    
    # Derive order_year and order_month from order_timestamp
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], errors='coerce')
    df['order_year'] = df['order_timestamp'].dt.year
    df['order_month'] = df['order_timestamp'].dt.month
    
    partitioned_dir = Path(PROJECT_ROOT) / 'data' / 'partitioned'
    
    # Write partitioned Parquet dataset
    df.to_parquet(
        partitioned_dir,
        index=False,
        partition_cols=['order_year', 'order_month'],
        compression='snappy'
    )
    print(f" -> Successfully wrote partitioned Parquet dataset to {partitioned_dir}")
    return partitioned_dir

def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics."""
    if df.empty:
        return 0
        
    engine = _get_engine()
    temp_table = f"stg_sales_order_lines_{run_id.replace('-', '_')}"
    
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS curated;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit;"))
        
        # Ensure audit table for partition loads exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit.partition_loads (
                id SERIAL PRIMARY KEY,
                run_id TEXT,
                order_year INT,
                order_month INT,
                rows_loaded INT,
                loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
        # 1. Load dataframe to temp table
        df.to_sql(name=temp_table, con=conn, schema='curated', if_exists='replace', index=False)
        
        # 2. Build dynamic SELECT list with explicit casts
        select_cols = []
        for c in df.columns:
            if c in ['order_timestamp', 'source_updated_at', 'processed_at_utc']:
                select_cols.append(f'CAST("{c}" AS TIMESTAMP WITH TIME ZONE)')
            elif c in ['quantity', 'unit_price', 'discount_pct', 'gross_amount', 'discount_amount', 'net_amount']:
                select_cols.append(f'CAST("{c}" AS NUMERIC)')
            else:
                select_cols.append(f'"{c}"')
                
        select_str = ", ".join(select_cols)
        cols_str = ", ".join([f'"{c}"' for c in df.columns])
        
        set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != 'order_id'])
        
        upsert_sql = f"""
            INSERT INTO curated.sales_order_lines ({cols_str})
            SELECT {select_str} FROM curated."{temp_table}"
            ON CONFLICT (order_id)
            DO UPDATE SET {set_clause};
        """
        
        conn.execute(text(upsert_sql))
        conn.execute(text(f'DROP TABLE curated."{temp_table}";'))
        
    return len(df)


def load_partition(year: int, month: int, run_id: str) -> int:
    """Task D: Load only a selected year/month partition and record in audit.partition_loads."""
    partitioned_dir = Path(PROJECT_ROOT) / 'data' / 'partitioned'
    target_partition = partitioned_dir / f"order_year={year}" / f"order_month={month}"
    
    if not target_partition.exists():
        print(f" [WARNING] Partition order_year={year}/order_month={month} does not exist.")
        return 0
        
    # Read only the selected partition directory
    df = pd.read_parquet(target_partition)
    
    if df.empty:
        return 0
        
    # Drop partition columns before database upsert if they aren't part of the main table schema
    db_df = df.drop(columns=['order_year', 'order_month'], errors='ignore')
    
    # Perform rerun-safe UPSERT
    loaded_count = upsert_curated(db_df, run_id)
    
    # Record execution in audit table
    engine = _get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO audit.partition_loads (run_id, order_year, order_month, rows_loaded)
                VALUES (:run_id, :year, :month, :count);
            """),
            {'run_id': run_id, 'year': year, 'month': month, 'count': loaded_count}
        )
        
    print(f" -> Successfully loaded partition (Year: {year}, Month: {month}) with {loaded_count} rows.")
    return loaded_count