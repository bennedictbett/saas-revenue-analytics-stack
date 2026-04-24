-- ============================================================
-- funnel.sql — Lead-to-Customer Conversion Funnel
-- ============================================================
-- Answers screening question 5 (attribution gap analysis).
--
-- 4 sections:
--   1. funnel_overview      → volume + drop-off at each stage
--   2. conversion_by_source → which channels convert best
--   3. loss_reasons         → where and why leads die
--   4. time_to_convert      → days from lead to signup by source/plan
--
-- Run: python sql/run_query.py funnel
-- ============================================================

WITH leads AS (
    SELECT
        lead_id,
        user_id,
        source,
        stage,
        industry,
        company_size,
        loss_reason,
        CAST(created_date   AS DATE)            AS created_date,
        CAST(converted_date AS DATE)            AS converted_date
    FROM read_csv_auto('data/leads.csv')
),

leads_with_mrr AS (
    SELECT
        l.*,
        s.mrr,
        s.plan
    FROM leads l
    LEFT JOIN read_csv_auto('data/subscriptions.csv') s
        USING (user_id)
),

-- ── 1. FUNNEL OVERVIEW ────────────────────────────────────────
stage_order AS (
    SELECT 1 AS ord, 'new'         AS stage
    UNION ALL SELECT 2, 'qualified'
    UNION ALL SELECT 3, 'demo_booked'
    UNION ALL SELECT 4, 'converted'
),

stage_counts AS (
    SELECT stage, COUNT(*) AS leads_at_stage
    FROM leads
    GROUP BY stage
),

funnel_overview AS (
    SELECT
        so.ord,
        so.stage,
        COALESCE(sc.leads_at_stage, 0)              AS total_leads,
        COALESCE(sc.leads_at_stage, 0) -
            LEAD(COALESCE(sc.leads_at_stage, 0))
                OVER (ORDER BY so.ord)              AS drop_off,
        ROUND(
            COALESCE(sc.leads_at_stage, 0) * 100.0 /
            NULLIF(
                FIRST_VALUE(COALESCE(sc.leads_at_stage, 0))
                    OVER (ORDER BY so.ord)
            , 0)
        , 1)                                        AS pct_of_top_of_funnel
    FROM stage_order so
    LEFT JOIN stage_counts sc USING (stage)
),

-- ── 2. CONVERSION BY SOURCE ───────────────────────────────────
conversion_by_source AS (
    SELECT
        source,
        COUNT(*)                                    AS total_leads,
        SUM(CASE WHEN stage = 'converted' THEN 1 ELSE 0 END)
                                                    AS converted,
        ROUND(
            SUM(CASE WHEN stage = 'converted' THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*)
        , 1)                                        AS conversion_rate_pct,
        SUM(CASE WHEN stage = 'converted' THEN mrr ELSE 0 END)
                                                    AS mrr_generated,
        ROUND(AVG(CASE WHEN stage = 'converted'
            THEN DATEDIFF('day', created_date, converted_date)
        END), 0)                                    AS avg_days_to_convert
    FROM leads_with_mrr
    GROUP BY 1
    ORDER BY conversion_rate_pct DESC
),

-- ── 3. LOSS REASONS ───────────────────────────────────────────
loss_reasons AS (
    SELECT
        stage,
        loss_reason,
        COUNT(*)                                    AS lost_leads,
        ROUND(
            COUNT(*) * 100.0 /
            SUM(COUNT(*)) OVER (PARTITION BY stage)
        , 1)                                        AS pct_within_stage
    FROM leads
    WHERE stage      != 'converted'
      AND loss_reason IS NOT NULL
    GROUP BY 1, 2
    ORDER BY stage, lost_leads DESC
),

-- ── 4. TIME TO CONVERT ────────────────────────────────────────
time_to_convert AS (
    SELECT
        source,
        plan,
        COUNT(*)                                    AS customers,
        ROUND(AVG(
            DATEDIFF('day', created_date, converted_date)
        ), 0)                                       AS avg_days,
        MIN(DATEDIFF('day', created_date, converted_date))
                                                    AS fastest_days,
        MAX(DATEDIFF('day', created_date, converted_date))
                                                    AS slowest_days,
        SUM(mrr)                                    AS total_mrr
    FROM leads_with_mrr
    WHERE stage          = 'converted'
      AND converted_date IS NOT NULL
      AND created_date   IS NOT NULL
    GROUP BY 1, 2
    ORDER BY avg_days
),

-- ── COMBINE INTO ONE OUTPUT ───────────────────────────────────
all_sections AS (
    SELECT
        '1_funnel'                                  AS section,
        LPAD(CAST(ord AS VARCHAR), 2, '0')
            || '_' || stage                         AS sort_key,
        stage                                       AS dim,
        CAST(total_leads        AS VARCHAR)         AS col1,
        CAST(drop_off           AS VARCHAR)         AS col2,
        CAST(pct_of_top_of_funnel AS VARCHAR)       AS col3,
        NULL                                        AS col4
    FROM funnel_overview

    UNION ALL
    SELECT '2_by_source', source, source,
        CAST(total_leads         AS VARCHAR),
        CAST(converted           AS VARCHAR),
        CAST(conversion_rate_pct AS VARCHAR),
        CAST(mrr_generated       AS VARCHAR)
    FROM conversion_by_source

    UNION ALL
    SELECT '3_loss_reasons',
        CONCAT(stage, loss_reason),
        CONCAT(stage, ' → ', loss_reason),
        CAST(lost_leads        AS VARCHAR),
        CAST(pct_within_stage  AS VARCHAR),
        NULL, NULL
    FROM loss_reasons

    UNION ALL
    SELECT '4_time_to_convert',
        CONCAT(source, plan),
        CONCAT(source, ' / ', plan),
        CAST(customers         AS VARCHAR),
        CAST(avg_days          AS VARCHAR),
        CAST(fastest_days      AS VARCHAR),
        CAST(slowest_days      AS VARCHAR)
    FROM time_to_convert
)

SELECT section, dim, col1, col2, col3, col4
FROM all_sections
ORDER BY section, sort_key;
