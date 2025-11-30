"""
DAG: Transform and Export (Conservative)
Triggers less frequently to avoid DB contention
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import csv
import os
import logging
import shutil

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 30),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transform_and_export',
    default_args=default_args,
    description='dbt transform + export (runs every hour)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['transformation', 'export'],
)

# Check if there's new data to transform
def check_new_data(**context):
    """Check if we have untransformed data"""
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    
    # Count bronze vs silver matches
    cursor.execute("SELECT COUNT(*) FROM bronze.matches")
    bronze_count = cursor.fetchone()[0]
    
    # Check if silver.silver_matches table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'silver' 
            AND table_name = 'silver_matches'
        )
    """)
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:
        # First time run - silver tables don't exist yet
        logging.info(f"Silver tables don't exist yet. Bronze has {bronze_count} matches. Running dbt for first time...")
        cursor.close()
        conn.close()
        return bronze_count
    
    # Table exists, count records
    cursor.execute("SELECT COUNT(*) FROM silver.silver_matches")
    silver_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    new_data_count = bronze_count - silver_count
    
    logging.info(f"Bronze: {bronze_count}, Silver: {silver_count}, New: {new_data_count}")
    
    if new_data_count <= 0:
        logging.info("No new data to transform, skipping")
        raise ValueError("No new data")
    
    logging.info(f"Found {new_data_count} new matches to transform")
    return new_data_count

check_data = PythonOperator(
    task_id='check_new_data',
    python_callable=check_new_data,
    dag=dag,
)

# dbt run
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='docker exec dota2_dbt dbt run --project-dir /dbt/hybrid_engineer --profiles-dir /root/.dbt',
    dag=dag,
)

# dbt test
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='docker exec dota2_dbt dbt test --project-dir /dbt/hybrid_engineer --profiles-dir /root/.dbt',
    dag=dag,
)

# Export to CSV
def export_to_csv(**context):
    """Export gold tables to CSV"""
    
    export_dir = '/opt/airflow/export'
    onedrive_dir = '/opt/airflow/onedrive_sync'
    
    os.makedirs(export_dir, exist_ok=True)
    os.makedirs(onedrive_dir, exist_ok=True)
    
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    
    tables = [
        ('gold', 'gold_match_analytics'),
        ('gold', 'gold_player_stats'),
    ]
    
    for schema, table in tables:
        try:
            logging.info(f"Exporting {schema}.{table}...")
            
            cursor.execute(f'SELECT * FROM {schema}.{table}')
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            csv_filename = f'{table}.csv'
            csv_path = os.path.join(export_dir, csv_filename)
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columns)
                writer.writerows(rows)
            
            # Copy to OneDrive
            onedrive_path = os.path.join(onedrive_dir, csv_filename)
            shutil.copy2(csv_path, onedrive_path)
            
            logging.info(f"✓ Exported {len(rows)} rows to {csv_filename}")
            
        except Exception as e:
            logging.error(f"Error exporting {schema}.{table}: {e}")
            raise
    
    cursor.close()
    conn.close()
    
    logging.info("✅ Export complete!")

export_task = PythonOperator(
    task_id='export_to_csv',
    python_callable=export_to_csv,
    dag=dag,
)

# Task dependencies
check_data >> dbt_run >> dbt_test >> export_task
