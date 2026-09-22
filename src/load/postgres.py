import pandas as pd
from sqlalchemy import create_engine, text

from src.config import DB


def _get_engine():
    """Build the SQLAlchemy engine from the single source of truth: config.DB.

    config.py already calls load_dotenv() and resolves POSTGRES_* from the
    environment (which docker-compose's `env_file: .env` on the pipeline
    service already injects). No need to re-read .env here, and no
    hard-coded fallback password (General Rule 2).
    """
    return create_engine(
        f"postgresql+psycopg2://{DB['user']}:{DB['password']}"
        f"@{DB['host']}:{DB['port']}/{DB['dbname']}"
    )


# Columns owned by curated.sales_order_lines (sql/init/01_warehouse_schema.sql).
# Kept here, not re-derived from the DataFrame, so an accidental extra/missing
# column in curated.py fails loudly instead of silently drifting the schema.
_TARGET_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct', 'gross_amount',
    'discount_amount', 'net_amount', 'status', 'source_updated_at',
    'pipeline_run_id', 'processed_at_utc', 'record_hash',
]


def upsert_curated(df: pd.DataFrame, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    order_id is the conflict key. A rerun with unchanged records must not
    create duplicate business keys.
    """
    if df.empty:
        return 0

    missing = [c for c in _TARGET_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f'curated DataFrame is missing expected columns: {missing}. '
            'Check src/transform/curated.py against sql/init/01_warehouse_schema.sql.'
        )
    df = df[_TARGET_COLUMNS]

    engine = _get_engine()
    staging_table = f'stg_sales_order_lines_{run_id}'.replace('-', '_')

    with engine.begin() as conn:
        # Load this run's rows into a run-scoped temp staging table rather
        # than recreating curated.sales_order_lines (that table and its
        # types already exist via sql/init/01_warehouse_schema.sql).
        df.to_sql(staging_table, con=conn, schema='curated',
                  if_exists='replace', index=False)

        cols = ', '.join(f'"{c}"' for c in _TARGET_COLUMNS)
        set_clause = ', '.join(
            f'"{c}" = EXCLUDED."{c}"' for c in _TARGET_COLUMNS if c != 'order_id'
        )
        conn.execute(text(f"""
            INSERT INTO curated.sales_order_lines ({cols})
            SELECT {cols} FROM curated."{staging_table}"
            ON CONFLICT (order_id) DO UPDATE SET {set_clause};
        """))
        conn.execute(text(f'DROP TABLE curated."{staging_table}";'))

    return len(df)


def load_partition(df: pd.DataFrame, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    raise NotImplementedError('Implement Goal 3 selected-partition load')