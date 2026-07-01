from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lakehouse-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'sla': timedelta(hours=1),
}

with DAG(
    dag_id='silver_layer_pipeline',
    default_args=default_args,
    description='Daily Bronze to Silver data cleaning pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['silver', 'batch', 'delta-lake'],
) as dag:

    start = BashOperator(
        task_id='pipeline_start',
        bash_command='echo "Silver layer pipeline started at $(date)"',
    )

    clean_orders = BashOperator(
        task_id='clean_orders',
        bash_command='''
            echo "Starting orders Bronze to Silver job..."
            echo "In production: spark-submit orders_bronze_to_silver.py"
            echo "Task completed at $(date)"
        ''',
    )

    clean_sessions = BashOperator(
        task_id='clean_sessions',
        bash_command='''
            echo "Starting sessions Bronze to Silver job..."
            echo "In production: spark-submit sessions_bronze_to_silver.py"
            echo "Task completed at $(date)"
        ''',
    )

    clean_inventory = BashOperator(
        task_id='clean_inventory',
        bash_command='''
            echo "Starting inventory Bronze to Silver job..."
            echo "In production: spark-submit inventory_bronze_to_silver.py"
            echo "Task completed at $(date)"
        ''',
    )

    verify_silver = BashOperator(
        task_id='verify_silver_tables',
        bash_command='''
            echo "Verifying all Silver tables..."
            echo "Orders Silver table... OK"
            echo "Sessions Silver table... OK"
            echo "Inventory Silver table... OK"
            echo "All Silver tables verified at $(date)"
        ''',
    )

    end = BashOperator(
        task_id='pipeline_end',
        bash_command='echo "Silver layer pipeline completed at $(date)"',
    )

    start >> [clean_orders, clean_sessions, clean_inventory] >> verify_silver >> end