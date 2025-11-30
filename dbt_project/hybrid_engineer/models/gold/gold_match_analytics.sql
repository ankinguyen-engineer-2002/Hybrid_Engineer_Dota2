-- models/gold/gold_match_analytics.sql
-- Enrich match data with metadata

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    m.match_id,
    m.match_datetime,
    m.duration_minutes,
    m.radiant_win,
    CASE
        WHEN m.radiant_win THEN 'Radiant'
        ELSE 'Dire'
    END as winning_team,
    m.game_mode,
    gm.name as game_mode_name,
    gm.balanced as is_balanced_mode,
    m.lobby_type,
    lt.name as lobby_type_name
FROM {{ ref('silver_matches') }} m
LEFT JOIN {{ source('dota', 'dim_game_modes') }} gm
    ON m.game_mode = gm.id
LEFT JOIN {{ source('dota', 'dim_lobby_types') }} lt
    ON m.lobby_type = lt.id
