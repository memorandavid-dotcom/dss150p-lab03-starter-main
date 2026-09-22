import hashlib
import json
from pathlib import Path
import pandas as pd

from src.config import path_for
from src.common.audit import utc_now_iso

def _quarantine_row(source_table, business_key, reason, run_id, record):
    return {
        'source_table': source_table,
        'business_key': business_key,
        'reason': reason,
        'pipeline_run_id': run_id,
        'staged_at_utc': utc_now_iso(),
        'raw_record': json.dumps(record, default=str),
    }

def build_curated(run_id: str):
    """
    Joins staging datasets, quarantines orphans, calculates amounts, 
    and generates a deterministic record_hash with accurate schema mapping.
    """
    staging_dir = path_for('staging_dir') / f'run_id={run_id}'
    
    # 1. Load Staging Data
    customers = pd.read_parquet(staging_dir / 'customers.parquet')
    products = pd.read_parquet(staging_dir / 'products.parquet')
    orders = pd.read_parquet(staging_dir / 'orders.parquet')

    # 2. Join and Identify Orphans
    merged = orders.merge(customers, on='customer_id', how='left', indicator='_merge_cust', suffixes=('', '_cust'))
    merged = merged.merge(products, on='product_id', how='left', indicator='_merge_prod', suffixes=('', '_prod'))

    orphan_mask = (merged['_merge_cust'] == 'left_only') | (merged['_merge_prod'] == 'left_only')
    orphans = merged[orphan_mask].copy()
    
    quarantine_rows = []
    for _, row in orphans.iterrows():
        reasons = []
        if row['_merge_cust'] == 'left_only':
            reasons.append(f"Missing customer_id: {row['customer_id']}")
        if row['_merge_prod'] == 'left_only':
            reasons.append(f"Missing product_id: {row['product_id']}")
            
        quarantine_rows.append(_quarantine_row(
            'orders_curated_join', 
            row['order_id'], 
            " | ".join(reasons), 
            run_id, 
            {'order_id': row['order_id'], 'customer_id': row['customer_id'], 'product_id': row['product_id']}
        ))

    valid = merged[~orphan_mask].copy()

    # 3. Calculate Business Measures
    valid['gross_amount'] = valid['quantity'] * valid['unit_price']
    valid['discount_amount'] = valid['gross_amount'] * valid['discount_pct']
    valid['net_amount'] = valid['gross_amount'] - valid['discount_amount']

    # 4. Map Columns to match PostgreSQL Schema exactly
    valid = valid.rename(columns={
        'city': 'customer_city',
        'tier': 'customer_tier',
        'name': 'product_name',
        'category_name': 'category'
    })

    # Ensure missing target columns are safely handled if missing from source data
    for col in ['customer_tier', 'brand']:
        if col not in valid.columns:
            valid[col] = None

    # 5. Audit Columns & Deterministic Hash
    valid['source_updated_at'] = valid[['updated_at', 'updated_at_cust', 'updated_at_prod']].max(axis=1)
    valid['pipeline_run_id'] = run_id
    valid['processed_at_utc'] = utc_now_iso()

    hash_cols = ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'discount_pct', 'status']
    
    def generate_hash(row):
        row_str = "|".join([str(row[c]) for c in hash_cols])
        return hashlib.md5(row_str.encode('utf-8')).hexdigest()

    valid['record_hash'] = valid.apply(generate_hash, axis=1)

    # Clean up column names and order to match destination table (email removed)
    final_cols = [
        'order_id', 'customer_id', 'product_id', 'order_timestamp', 'status',
        'quantity', 'unit_price', 'discount_pct', 'gross_amount', 'discount_amount', 'net_amount',
        'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
        'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash'
    ]
    
    curated_df = valid[[c for c in final_cols if c in valid.columns]]

    # 6. Save Outputs
    curated_dir = path_for('curated_dir') / f'run_id={run_id}'
    curated_dir.mkdir(parents=True, exist_ok=True)
    curated_df.to_parquet(curated_dir / 'sales_order_lines.parquet', index=False)

    if quarantine_rows:
        q_df = pd.DataFrame(quarantine_rows)
        quarantine_dir = path_for('quarantine_dir') / f'run_id={run_id}'
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        q_df.to_parquet(quarantine_dir / 'curated_quarantine.parquet', index=False)

    return len(curated_df), len(quarantine_rows)