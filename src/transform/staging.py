import json
from pathlib import Path

import pandas as pd

from src.config import path_for, SETTINGS
from src.common.audit import utc_now_iso


def _read_customers(raw_dir: Path) -> pd.DataFrame:
    return pd.read_csv(raw_dir / 'customers.csv', dtype=str)


def _read_products(raw_dir: Path) -> pd.DataFrame:
    with (raw_dir / 'products.json').open('r', encoding='utf-8') as f:
        records = json.load(f) # Corrected back to standard JSON
    df = pd.json_normalize(records, sep='_')
    return df


def _read_orders(raw_dir: Path) -> pd.DataFrame:
    return pd.read_csv(raw_dir / 'orders.csv', dtype=str)


def _dedupe_keep_latest(df: pd.DataFrame, key: str, updated_col: str) -> pd.DataFrame:
    df = df.sort_values(updated_col)
    return df.drop_duplicates(subset=[key], keep='last').reset_index(drop=True)


def _to_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors='coerce')


def _quarantine_row(source_table, business_key, reason, run_id, record):
    return {
        'source_table': source_table,
        'business_key': business_key,
        'reason': reason,
        'pipeline_run_id': run_id,
        'staged_at_utc': utc_now_iso(),
        'raw_record': json.dumps(record, default=str),
    }


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets."""
    raw_dir = Path(raw_dir)
    now = utc_now_iso()
    quarantine_rows = []
    q = SETTINGS['quality']
    allowed_statuses = set(q['allowed_order_statuses'])
    min_qty, max_qty = q['min_quantity'], q['max_quantity']

    # ---------------- customers ----------------
    customers = _read_customers(raw_dir)
    customers.loc[:, 'email'] = customers['email'].astype(str).str.strip().str.lower()
    customers.loc[:, 'city'] = customers['city'].astype(str).str.strip().str.title()
    customers['created_at'] = _to_utc(customers['created_at'])
    customers['updated_at'] = _to_utc(customers['updated_at'])

    bad_customers = customers[customers['updated_at'].isna() | customers['customer_id'].isna()]
    for _, row in bad_customers.iterrows():
        quarantine_rows.append(_quarantine_row(
            'customers', row.get('customer_id'), 'unparseable updated_at or missing customer_id',
            run_id, row.to_dict(),
        ))
    customers = customers.drop(bad_customers.index)
    customers = _dedupe_keep_latest(customers, 'customer_id', 'updated_at')
    customers['pipeline_run_id'] = run_id
    customers['staged_at_utc'] = now

    # ---------------- products ----------------
    products = _read_products(raw_dir)
    products['unit_price'] = pd.to_numeric(products['unit_price'], errors='coerce')
    products['updated_at'] = _to_utc(products['updated_at'])

    bad_price = products['unit_price'].isna() | (products['unit_price'] < 0)
    bad_updated = products['updated_at'].isna()
    bad_products = products[bad_price | bad_updated]
    for _, row in bad_products.iterrows():
        reason = 'negative or unparseable unit_price' if bad_price.get(row.name, False) else 'unparseable updated_at'
        quarantine_rows.append(_quarantine_row('products', row.get('product_id'), reason, run_id, row.to_dict()))
    products = products.drop(bad_products.index)
    products = _dedupe_keep_latest(products, 'product_id', 'updated_at')
    products['pipeline_run_id'] = run_id
    products['staged_at_utc'] = now

    # ---------------- orders ----------------
    orders = _read_orders(raw_dir)
    orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')
    orders['unit_price'] = pd.to_numeric(orders['unit_price'], errors='coerce')
    orders['discount_pct'] = pd.to_numeric(orders['discount_pct'], errors='coerce')
    orders['order_timestamp'] = _to_utc(orders['order_timestamp'])
    orders['updated_at'] = _to_utc(orders['updated_at'])

    invalid_mask = (
        orders['updated_at'].isna()
        | orders['quantity'].isna()
        | (orders['quantity'] < min_qty)
        | (orders['quantity'] > max_qty)
        | ~orders['status'].isin(allowed_statuses)
    )
    bad_orders = orders[invalid_mask]
    for _, row in bad_orders.iterrows():
        if row.get('status') not in allowed_statuses:
            reason = f"status '{row.get('status')}' not in allowed set"
        elif pd.isna(row.get('quantity')) or not (min_qty <= row.get('quantity', -1) <= max_qty):
            reason = f'quantity out of range [{min_qty}, {max_qty}]'
        else:
            reason = 'unparseable updated_at'
        quarantine_rows.append(_quarantine_row('orders', row.get('order_id'), reason, run_id, row.to_dict()))
    orders = orders.drop(bad_orders.index)
    orders = _dedupe_keep_latest(orders, 'order_id', 'updated_at')
    orders['pipeline_run_id'] = run_id
    orders['staged_at_utc'] = now

    # ---------------- write staging Parquet ----------------
    staging_dir = path_for('staging_dir') / f'run_id={run_id}'
    staging_dir.mkdir(parents=True, exist_ok=True)
    customers.to_parquet(staging_dir / 'customers.parquet', index=False)
    products.to_parquet(staging_dir / 'products.parquet', index=False)
    orders.to_parquet(staging_dir / 'orders.parquet', index=False)

    # ---------------- write quarantine ----------------
    quarantine_df = pd.DataFrame(quarantine_rows)
    quarantine_dir = path_for('quarantine_dir') / f'run_id={run_id}'
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    if not quarantine_df.empty:
        quarantine_df.to_parquet(quarantine_dir / 'staging_quarantine.parquet', index=False)

    return {
        'staging': {'customers': customers, 'products': products, 'orders': orders},
        'quarantine': quarantine_df,
    }