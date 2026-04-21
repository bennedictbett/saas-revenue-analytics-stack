
-- wau.sql — Weekly Active Users (WAU)
--
-- WAU = distinct users who performed at least one event in a
-- given calendar week.
--
-- Layers:
--   1. weekly_active   → raw WAU count per week
--   2. with_growth     → adds WoW absolute + % change
--   3. final           → adds 4-week rolling average (smooths noise)
--
-- Run locally:
--   python sql/run_query.py wau

WITH

-- Layer 1: count distinct users per calendar week
weekly_active AS (
    SELECT
        DATE_TRUNC('week', event_date)      AS week_start,
        COUNT(DISTINCT user_id)             AS wau,
        COUNT(*)                            AS total_events,
        COUNT(DISTINCT session_id)          AS total_sessions,
        ROUND(
            COUNT(*) * 1.0 /
            NULLIF(COUNT(DISTINCT user_id), 0)
        , 1)                                AS events_per_user
    FROM read_csv_auto('data/product_events.csv')
    GROUP BY 1
),

-- Layer 2: week-over-week change
with_growth AS (
    SELECT
        week_start,
        wau,
        total_events,
        total_sessions,
        events_per_user,
        LAG(wau) OVER (ORDER BY week_start) AS prev_week_wau,
        wau - LAG(wau) OVER (ORDER BY week_start)
                                            AS wau_change,
        ROUND(
            (wau - LAG(wau) OVER (ORDER BY week_start)) * 100.0 /
            NULLIF(LAG(wau) OVER (ORDER BY week_start), 0)
        , 1)                                AS wau_growth_pct
    FROM weekly_active
),

-- Layer 3: 4-week rolling average
final AS (
    SELECT
        week_start,
        wau,
        wau_change,
        wau_growth_pct,
        total_sessions,
        events_per_user,
        ROUND(
            AVG(wau) OVER (
                ORDER BY week_start
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            )
        , 1)                                AS wau_4wk_avg
    FROM with_growth
)

SELECT * FROM final
ORDER BY week_start;