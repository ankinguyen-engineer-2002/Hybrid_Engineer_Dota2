-- models/gold/gold_player_stats.sql
-- Aggregate player statistics

{{
  config(
    materialized='table',
    schema='gold'
  )
}}

SELECT
    p.account_id,
    p.hero_id,
    h.localized_name as hero_name,
    COUNT(*) as total_matches,
    SUM(CASE WHEN p.player_won THEN 1 ELSE 0 END) as wins,
    ROUND(100.0 * SUM(CASE WHEN p.player_won THEN 1 ELSE 0 END) / COUNT(*), 2) as win_rate_pct,
    ROUND(AVG(p.kills), 2) as avg_kills,
    ROUND(AVG(p.deaths), 2) as avg_deaths,
    ROUND(AVG(p.assists), 2) as avg_assists,
    ROUND(AVG(p.kills + p.assists) / NULLIF(AVG(p.deaths), 0), 2) as kda_ratio,
    ROUND(AVG(p.gold_per_min), 0) as avg_gpm,
    ROUND(AVG(p.xp_per_min), 0) as avg_xpm,
    ROUND(AVG(p.hero_damage), 0) as avg_hero_damage,
    ROUND(AVG(p.last_hits), 0) as avg_last_hits
FROM {{ ref('silver_players') }} p
LEFT JOIN {{ source('dota', 'dim_heroes') }} h
    ON p.hero_id = h.id
WHERE p.account_id IS NOT NULL
GROUP BY p.account_id, p.hero_id, h.localized_name
HAVING COUNT(*) >= 1  -- At least 1 match
ORDER BY total_matches DESC, win_rate_pct DESC
