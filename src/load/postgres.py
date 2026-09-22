import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from src.config import DB, PROJECT_ROOT

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

def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics."""
    if df.empty:
        return 0
        
    engine = _get_engine()
    temp_table = f"stg_sales_order_lines_{run_id.replace('-', '_')}"
    
    with engine.begin() as conn:
        # Create schema if it doesn't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS curated;"))
        
        # 1. Load current dataframe to a temporary table
        df.to_sql(name=temp_table, con=conn, schema='curated', if_exists='replace', index=False)
        
        # 2. Build dynamic SELECT list with explicit PostgreSQL CASTS
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
        
        # 3. Execute the UPSERT and drop the temporary table
        conn.execute(text(upsert_sql))
        conn.execute(text(f'DROP TABLE curated."{temp_table}";'))
        
    return len(df)


def load_partition(df: pd.DataFrame, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    raise NotImplementedError('Implement Goal 3 selected-partition load')