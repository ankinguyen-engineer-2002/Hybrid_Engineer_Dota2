-- models/silver/silver_matches.sql
-- Clean and structure match data

{{
  config(
    materialized='table',
    schema='silver'
  )
}}

SELECT
    match_id,
    TO_TIMESTAMP((raw_data->>'start_time')::BIGINT) as match_datetime,
    (raw_data->>'duration')::INTEGER as duration_seconds,
    (raw_data->>'duration')::INTEGER / 60.0 as duration_minutes,
    (raw_data->>'radiant_win')::BOOLEAN as radiant_win,
    (raw_data->>'game_mode')::INTEGER as game_mode,
    (raw_data->>'lobby_type')::INTEGER as lobby_type,
    raw_data->'players' as players_json,
    ingested_at
FROM {{ ref('stg_dota2_matches_raw') }}
