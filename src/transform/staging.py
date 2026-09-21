import pandas as pd
import json
import time
from datetime import datetime, timezone
from pathlib import Path

def build_staging(raw_dir: str, run_id: str):
    """Create cleaned, typed staging datasets with error quarantining."""
    raw_path = Path(raw_dir)
    staged_at = datetime.now(timezone.utc).isoformat()
    quarantined = []
    
    print(" -> [Debug] Pausing for 1 second to release Windows file locks...")
    time.sleep(1)

    print(" -> [Debug] Starting Customers...")
    # Using engine='python' to avoid C-level memory crashes on Windows
    customers = pd.read_csv(raw_path / 'customers.csv', engine='python')
    # Using format='mixed' and errors='coerce' for safe parsing
    customers['updated_at'] = pd.to_datetime(customers['updated_at'], errors='coerce', format='mixed', utc=True)
    
    customers = customers.sort_values('updated_at').drop_duplicates(subset=['customer_id'], keep='last')
    customers['email'] = customers['email'].astype(str).str.lower().str.strip()
    customers['city'] = customers['city'].astype(str).str.title().str.strip()
    customers['pipeline_run_id'] = run_id
    customers['staged_at_utc'] = staged_at
    print(" -> [Debug] Customers complete.")

    print(" -> [Debug] Starting Products...")
    try:
        with open(raw_path / 'products.json', 'r', encoding='utf-8') as f:
            products_raw = [json.loads(line) for line in f]
    except json.JSONDecodeError:
        with open(raw_path / 'products.json', 'r', encoding='utf-8') as f:
            products_raw = json.load(f)
            
    products = pd.json_normalize(products_raw) 
    products['updated_at'] = pd.to_datetime(products['updated_at'], errors='coerce', format='mixed', utc=True)
    products['price'] = pd.to_numeric(products['price'], errors='coerce')
    products = products.sort_values('updated_at').drop_duplicates(subset=['product_id'], keep='last')
    
    invalid_prices = products[products['price'].isna() | (products['price'] <= 0)].copy()
    if not invalid_prices.empty:
        invalid_prices['quarantine_reason'] = 'Invalid or negative price'
        invalid_prices['source_entity'] = 'products'
        quarantined.append(invalid_prices)
        
    valid_products = products[products['price'] > 0].copy()
    valid_products['pipeline_run_id'] = run_id
    valid_products['staged_at_utc'] = staged_at
    print(" -> [Debug] Products complete.")

    print(" -> [Debug] Starting Orders...")
    orders = pd.read_csv(raw_path / 'orders.csv', engine='python')
    orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'], errors='coerce', format='mixed', utc=True)
    orders['updated_at'] = pd.to_datetime(orders['updated_at'], errors='coerce', format='mixed', utc=True)
    orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')
    
    orders = orders.sort_values('updated_at').drop_duplicates(subset=['order_id'], keep='last')
    
    valid_statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED']
    invalid_orders_mask = orders['quantity'].isna() | ~(orders['quantity'].between(1, 20)) | ~(orders['status'].astype(str).str.upper().isin(valid_statuses))
    
    invalid_orders = orders[invalid_orders_mask].copy()
    if not invalid_orders.empty:
        invalid_orders['quarantine_reason'] = 'Invalid quantity (not 1-20) or unapproved status'
        invalid_orders['source_entity'] = 'orders'
        quarantined.append(invalid_orders)
        
    valid_orders = orders[~invalid_orders_mask].copy()
    valid_orders['pipeline_run_id'] = run_id
    valid_orders['staged_at_utc'] = staged_at
    print(" -> [Debug] Orders complete.")
    
    quarantine_df = pd.concat(quarantined, ignore_index=True) if quarantined else pd.DataFrame()

    print(" -> [Debug] Staging transformations finished successfully.")
    return {
        'customers': customers,
        'products': valid_products,
        'orders': valid_orders
    }, quarantine_df