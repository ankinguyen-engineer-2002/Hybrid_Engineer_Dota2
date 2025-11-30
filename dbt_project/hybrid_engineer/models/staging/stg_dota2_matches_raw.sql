-- models/staging/stg_dota2_matches_raw.sql
-- Read raw JSON from bronze.matches table

{{
  config(
    materialized='view'
  )
}}

SELECT 
  match_id,
  raw_data,
  ingested_at
FROM {{ source('bronze', 'matches') }}
