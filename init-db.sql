-- Initialize database schemas

-- Create schemas for Data Lakehouse layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Create schema for static metadata
CREATE SCHEMA IF NOT EXISTS dota;

-- Bronze: Raw data from API
CREATE TABLE IF NOT EXISTS bronze.matches (
    match_id BIGINT PRIMARY KEY,
    raw_data JSONB NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_bronze_matches_ingested ON bronze.matches(ingested_at);

-- Dota metadata tables
CREATE TABLE IF NOT EXISTS dota.dim_heroes (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    localized_name VARCHAR(255),
    primary_attr VARCHAR(50),
    attack_type VARCHAR(50),
    roles TEXT[]
);

CREATE TABLE IF NOT EXISTS dota.dim_items (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    cost INTEGER,
    secret_shop BOOLEAN,
    side_shop BOOLEAN
);

CREATE TABLE IF NOT EXISTS dota.dim_game_modes (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    balanced BOOLEAN
);

CREATE TABLE IF NOT EXISTS dota.dim_lobby_types (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255)
);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA silver TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA gold TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA dota TO airflow;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dota TO airflow;
