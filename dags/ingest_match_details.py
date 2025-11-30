"""
DAG: Ingest Dota2 Match Details (Conservative - Free Tier Safe)
Workflow:
1. Get list of public match IDs
2. For each match ID: Check if exists in DB
3. If NOT exists: Call API /matches/{match_id} for FULL DETAILS
4. Insert into bronze.matches

Rate limiting strategy:
- Run every 15 minutes (not 5)
- Batch size: 10 matches (not 20)
- Sleep 3s between requests (not 1.5s)
- Total: ~40 req/hour (well under 60/min limit)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import psycopg2
import json
import logging
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 30),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_match_details',
    default_args=default_args,
    description='Fetch DETAILED match data from OpenDota API (Conservative)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['ingestion', 'dota2'],
)

def fetch_with_retry(url, params=None, max_retries=3):
    """Fetch URL with retry logic for rate limiting"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            
            # Handle rate limiting
            if response.status_code == 429:
                wait_time = 120 * (attempt + 1)  # 2min, 4min, 6min (aggressive backoff)
                logging.warning(f"⚠️ Rate limited! Waiting {wait_time}s before retry...")
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
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logging.warning(f"Request error: {e}, retrying in 10s...")
                time.sleep(10)
            else:
                raise
    
    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")

def get_existing_match_ids(cursor):
    """Get set of match IDs already in database"""
    cursor.execute("SELECT match_id FROM bronze.matches")
    return set(row[0] for row in cursor.fetchall())

def fetch_match_details(match_id):
    """Fetch detailed match data from OpenDota API"""
    url = f'https://api.opendota.com/api/matches/{match_id}'
    
    try:
        response = fetch_with_retry(url)
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch match {match_id}: {e}")
        return None

def ingest_match_details(**context):
    """Main ingestion logic"""
    
    # Get last processed match_id
    try:
        last_match_id = int(Variable.get('last_match_id', default_var='0'))
    except:
        last_match_id = 0
    
    logging.info(f"Starting from match_id: {last_match_id}")
    
    # Connect to Postgres
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    
    try:
        # Get existing match IDs to avoid duplicates
        existing_ids = get_existing_match_ids(cursor)
        logging.info(f"Found {len(existing_ids)} existing matches in DB")
        
        # Step 1: Get list of public match IDs (with retry)
        logging.info("Fetching public match list...")
        public_matches_url = 'https://api.opendota.com/api/publicMatches'
        params = {'min_match_id': last_match_id}
        
        response = fetch_with_retry(public_matches_url, params=params)
        public_matches = response.json()
        
        if not public_matches:
            logging.info("No new matches found")
            return
        
        logging.info(f"Found {len(public_matches)} potential new matches")
        
        # Step 2: Filter out matches we already have
        new_match_ids = [m['match_id'] for m in public_matches if m['match_id'] not in existing_ids]
        
        if not new_match_ids:
            logging.info("All matches already in database")
            # Still update last_match_id
            max_id = max(m['match_id'] for m in public_matches)
            Variable.set('last_match_id', str(max_id))
            return
        
        logging.info(f"Fetching details for {len(new_match_ids)} NEW matches")
        
        # Step 3: Fetch DETAILED data for each new match
        inserted_count = 0
        skipped_count = 0
        max_match_id = last_match_id
        
        # CONSERVATIVE: Only 10 matches per run (was 20)
        batch_size = 10
        new_match_ids = new_match_ids[:batch_size]
        
        logging.info(f"Processing batch of {len(new_match_ids)} matches (conservative mode)")
        
        for match_id in new_match_ids:
            try:
                logging.info(f"Fetching details for match {match_id}...")
                
                # Fetch FULL match details
                match_details = fetch_match_details(match_id)
                
                if not match_details:
                    logging.warning(f"No details returned for match {match_id}")
                    skipped_count += 1
                    continue
                
                # Check if match has required fields
                if 'players' not in match_details or not match_details.get('players'):
                    logging.warning(f"Match {match_id} has no players data, skipping")
                    skipped_count += 1
                    continue
                
                # Insert into bronze.matches
                cursor.execute("""
                    INSERT INTO bronze.matches (match_id, raw_data)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (match_id) DO NOTHING
                """, (match_id, json.dumps(match_details)))
                
                if cursor.rowcount > 0:
                    inserted_count += 1
                    logging.info(f"✓ Inserted match {match_id}")
                
                # Track max match_id
                if match_id > max_match_id:
                    max_match_id = match_id
                
                # CONSERVATIVE: 3 seconds between requests (was 1.5s)
                # 10 matches × 3s = 30s total
                # = 20 requests/min (well under 60/min limit)
                time.sleep(3)
                
            except Exception as e:
                logging.error(f"Error processing match {match_id}: {e}")
                continue
        
        # Commit all inserts
        conn.commit()
        
        # Update last_match_id
        Variable.set('last_match_id', str(max_match_id))
        
        logging.info(f"""
        Ingestion Summary (Conservative Mode):
        - Processed: {len(new_match_ids)} matches
        - Inserted: {inserted_count} matches
        - Skipped: {skipped_count} matches
        - Updated last_match_id to: {max_match_id}
        - Next run: 15 minutes
        """)
        
        # Push metrics to XCom for downstream tasks
        context['ti'].xcom_push(key='inserted_count', value=inserted_count)
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

ingest_task = PythonOperator(
    task_id='fetch_match_details',
    python_callable=ingest_match_details,
    dag=dag,
)
