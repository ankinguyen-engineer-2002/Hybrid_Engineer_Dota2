"""
DAG: Controller DAG to orchestrate the Dota2 workflow
Order: Refresh Metadata -> Ingest Match Details -> Transform and Export
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 30),
    'email_on_failure': False,
    'retries': 0,
}

dag = DAG(
    'dota2_workflow_controller',
    default_args=default_args,
    description='Orchestrate Dota2 workflow: Metadata -> Ingest -> Transform',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['controller', 'dota2'],
)

# 1. Trigger Refresh Metadata
trigger_refresh_metadata = TriggerDagRunOperator(
    task_id='trigger_refresh_metadata',
    trigger_dag_id='refresh_metadata',
    wait_for_completion=True,  # Wait for it to finish before moving on
    poke_interval=30,
    dag=dag,
)

# 2. Trigger Ingest Match Details
trigger_ingest = TriggerDagRunOperator(
    task_id='trigger_ingest',
    trigger_dag_id='ingest_match_details',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# 3. Trigger Transform and Export
trigger_transform = TriggerDagRunOperator(
    task_id='trigger_transform',
    trigger_dag_id='transform_and_export',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# Define execution order
trigger_refresh_metadata >> trigger_ingest >> trigger_transform
