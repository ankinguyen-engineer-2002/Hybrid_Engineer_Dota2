-- models/staging/stg_matches_raw.sql
-- Stage raw matches from bronze

{{
  config(
    materialized='view',
    schema='silver'
  )
}}

SELECT
    match_id,
    raw_data,
    ingested_at
FROM {{ source('bronze', 'matches') }}
WHERE match_id IS NOT NULL
