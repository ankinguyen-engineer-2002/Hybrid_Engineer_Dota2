-- models/silver/silver_players.sql
-- Unnest players array into individual rows

{{
  config(
    materialized='table',
    schema='silver'
  )
}}

WITH unnested_players AS (
    SELECT
        match_id,
        match_datetime,
        duration_seconds,
        radiant_win,
        game_mode,
        lobby_type,
        jsonb_array_elements(players_json) as player_data
    FROM {{ ref('silver_matches') }}
)

SELECT
    match_id,
    match_datetime,
    duration_seconds,
    radiant_win,
    game_mode,
    lobby_type,
    (player_data->>'account_id')::BIGINT as account_id,
    (player_data->>'hero_id')::INTEGER as hero_id,
    (player_data->>'player_slot')::INTEGER as player_slot,
    (player_data->>'kills')::INTEGER as kills,
    (player_data->>'deaths')::INTEGER as deaths,
    (player_data->>'assists')::INTEGER as assists,
    (player_data->>'gold_per_min')::INTEGER as gold_per_min,
    (player_data->>'xp_per_min')::INTEGER as xp_per_min,
    (player_data->>'level')::INTEGER as level,
    (player_data->>'hero_damage')::INTEGER as hero_damage,
    (player_data->>'tower_damage')::INTEGER as tower_damage,
    (player_data->>'hero_healing')::INTEGER as hero_healing,
    (player_data->>'last_hits')::INTEGER as last_hits,
    (player_data->>'denies')::INTEGER as denies,
    -- Determine if player won
    CASE
        WHEN (player_data->>'player_slot')::INTEGER < 128 THEN radiant_win
        ELSE NOT radiant_win
    END as player_won
FROM unnested_players
WHERE (player_data->>'account_id') IS NOT NULL
