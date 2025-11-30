"""
DAG: Refresh Dota2 Metadata Daily
Completely REPLACE all dimension tables every day
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 30),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'refresh_metadata',
    default_args=default_args,
    description='Daily REPLACE of all dimension tables',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['metadata', 'dota2'],
)

def fetch_with_retry(url, max_retries=3):
    """Fetch URL with retry logic for rate limiting"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            
            # Handle rate limiting
            if response.status_code == 429:
                wait_time = 60 * (attempt + 1)  # 60s, 120s, 180s
                logging.warning(f"⚠️ Rate limited! Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                logging.warning(f"Timeout, retrying in 10s...")
                time.sleep(10)
            else:
                raise
    
    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")

def refresh_all_metadata(**context):
    """Truncate and reload ALL dimension tables"""
    
    base_url = 'https://api.opendota.com/api'
    
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    
    try:
        # ====================
        # 1. HEROES
        # ====================
        logging.info("Refreshing dim_heroes...")
        cursor.execute("TRUNCATE TABLE dota.dim_heroes CASCADE")
        
        response = fetch_with_retry(f'{base_url}/heroes')
        heroes = response.json()
        
        # Validate response
        if not isinstance(heroes, list):
            raise ValueError(f"Expected list, got {type(heroes)}: {heroes}")
        
        if not heroes:
            raise ValueError("Heroes list is empty")
        
        for hero in heroes:
            if not isinstance(hero, dict):
                logging.warning(f"Skipping invalid hero: {hero}")
                continue
                
            cursor.execute("""
                INSERT INTO dota.dim_heroes (id, name, localized_name, primary_attr, attack_type, roles)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                hero.get('id'),
                hero.get('name', ''),
                hero.get('localized_name', ''),
                hero.get('primary_attr'),
                hero.get('attack_type'),
                hero.get('roles', [])
            ))
        
        conn.commit()
        logging.info(f"✓ Loaded {len(heroes)} heroes")
        
        # ====================
        # 2. GAME MODES
        # ====================
        logging.info("Refreshing dim_game_modes...")
        cursor.execute("TRUNCATE TABLE dota.dim_game_modes CASCADE")
        
        response = fetch_with_retry(f'{base_url}/constants/game_modes')
        game_modes = response.json()
        
        for mode_id, mode_data in game_modes.items():
            cursor.execute("""
                INSERT INTO dota.dim_game_modes (id, name, balanced)
                VALUES (%s, %s, %s)
            """, (
                int(mode_id),
                mode_data.get('name', f'Mode {mode_id}'),
                mode_data.get('balanced', False)
            ))
        
        conn.commit()
        logging.info(f"✓ Loaded {len(game_modes)} game modes")
        
        # ====================
        # 3. LOBBY TYPES
        # ====================
        logging.info("Refreshing dim_lobby_types...")
        cursor.execute("TRUNCATE TABLE dota.dim_lobby_types CASCADE")
        
        response = fetch_with_retry(f'{base_url}/constants/lobby_type')
        lobby_types = response.json()
        
        for lobby_id, lobby_data in lobby_types.items():
            cursor.execute("""
                INSERT INTO dota.dim_lobby_types (id, name)
                VALUES (%s, %s)
            """, (
                int(lobby_id),
                lobby_data.get('name', f'Lobby {lobby_id}')
            ))
        
        conn.commit()
        logging.info(f"✓ Loaded {len(lobby_types)} lobby types")
        
        logging.info("✅ All metadata refreshed successfully!")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error refreshing metadata: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

refresh_task = PythonOperator(
    task_id='refresh_all_metadata',
    python_callable=refresh_all_metadata,
    dag=dag,
)
