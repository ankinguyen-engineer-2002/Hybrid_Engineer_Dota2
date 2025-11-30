import requests
import pandas as pd
from sqlalchemy import create_engine
import logging

# Cấu hình Postgres
# Lưu ý: Chạy trong Docker network nên host là 'postgres'
DB_CONN = 'postgresql://airflow:airflow@postgres:5432/airflow'

# Các Endpoint Metadata
ENDPOINTS = {
    'heroes': 'https://api.opendota.com/api/constants/heroes',
    'items': 'https://api.opendota.com/api/constants/items',
    'abilities': 'https://api.opendota.com/api/constants/abilities',
    'game_modes': 'https://api.opendota.com/api/constants/game_mode',
    'region': 'https://api.opendota.com/api/constants/region'
}

logging.basicConfig(level=logging.INFO)

from sqlalchemy import create_engine, text

def load_metadata():
    engine = create_engine(DB_CONN)
    
    # 1. Tạo schema 'dota' nếu chưa có
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dota;"))
        logging.info("Schema 'dota' checked/created.")
    
    for name, url in ENDPOINTS.items():
        logging.info(f"Downloading {name} from {url}...")
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()
            
            # Xử lý dữ liệu tùy theo cấu trúc trả về
            df = None
            
            if name == 'heroes':
                # Heroes trả về dict {id: {...}} -> chuyển thành list
                df = pd.DataFrame.from_dict(data, orient='index')
                
            elif name == 'items':
                # Items trả về dict {item_name: {...}}
                df = pd.DataFrame.from_dict(data, orient='index')
                
            elif name == 'abilities':
                # Abilities trả về dict {ability_name: {...}}
                df = pd.DataFrame.from_dict(data, orient='index')
                
            elif name == 'game_modes':
                # Game modes trả về dict {id: {...}}
                df = pd.DataFrame.from_dict(data, orient='index')
                
            elif name == 'region':
                # Region trả về dict {id: "Name"} -> hơi khác chút
                # Chuyển thành DataFrame có cột id và name
                df = pd.DataFrame(list(data.items()), columns=['region_id', 'region_name'])
            
            if df is not None:
                # Lưu vào Postgres (Schema: dota)
                table_name = f"dim_{name}"
                logging.info(f"Saving {len(df)} rows to table dota.{table_name}...")
                
                # Chiến lược: REPLACE (Ghi đè hoàn toàn)
                # Lý do: Dữ liệu này nhỏ và ít thay đổi. Replace đảm bảo luôn khớp với API mới nhất.
                df.to_sql(table_name, engine, schema='dota', if_exists='replace', index=False)
                logging.info(f"Done dota.{table_name}.")
                
        except Exception as e:
            logging.error(f"Failed to load {name}: {e}")

if __name__ == "__main__":
    load_metadata()
