from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments specifying retries, timeouts, and error handling
default_args = {
    'owner': 'dss150p',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

def failure_callback(context):
    """Callback function executed on task failure."""
    print(f"\n[AIRFLOW TASK FAILURE] Task failed: {context.get('task_instance').task_id}")
    print(f"Run ID: {context.get('run_id')}")
    print(f"Execution Time: {context.get('execution_date')}\n")

with DAG(
    dag_id='dss150p_sales_pipeline',
    default_args=default_args,
    description='Production modular data pipeline for DSS150P Lab 3',
    schedule_interval='0 2 * * *',  # Daily at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        'run_mode': 'full',  # Options: 'full' or 'partition'
        'year': 2026,
        'month': 1
    },
    tags=['dss150p', 'production', 'modular'],
) as dag:

    # Task 1: Extract Source Files
    extract_task = BashOperator(
        task_id='extract_sources',
        bash_command='python -m src.cli run-all',  # Delegates cleanly to CLI
        on_failure_callback=failure_callback,
    )

    # Task 2: Optional Conditional Partition Load (if run_mode == 'partition')
    # Note: Our run-all CLI command already executes extract, transform, and full load. 
    # For a partition-specific run, we can expose a dedicated command step.
    partition_load_task = BashOperator(
        task_id='load_selected_partition',
        bash_command='python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}',
        on_failure_callback=failure_callback,
    )

    # Define task dependencies (Extract/Run-All -> Partition Load check)
    extract_task >> partition_load_task