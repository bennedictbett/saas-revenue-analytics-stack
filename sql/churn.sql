
-- Churn Analysis (4 sections)

-- 1. monthly_churn  → churn % and lost MRR each month
-- 2. by_plan        → which plans churn most
-- 3. by_reason      → why users are leaving
-- 4. tenure         → how long churned users lasted by plan


WITH subs AS (
    SELECT
        user_id,
        plan,
        mrr,
        status,
        CAST(signup_date AS DATE)                               AS signup_date,
        CAST(churn_date  AS DATE)                               AS churn_date,
        churn_reason,
        DATEDIFF('day',
            CAST(signup_date AS DATE),
            COALESCE(CAST(churn_date AS DATE), CURRENT_DATE)
        )                                                       AS tenure_days
    FROM read_csv_auto('data/subscriptions.csv')
    WHERE signup_date IS NOT NULL
),

-- MONTHLY CHURN RATE
months AS (
    SELECT *
    FROM generate_series(
        DATE_TRUNC('month', (SELECT MIN(signup_date) FROM subs)),
        DATE_TRUNC('month', CURRENT_DATE),
        INTERVAL '1 month'
    ) AS month
),

active_at_start AS (
    SELECT
        m.month,
        COUNT(s.user_id)                                        AS active_count
    FROM months m
    LEFT JOIN subs s
        ON  s.signup_date <  m.month
        AND (s.churn_date IS NULL OR s.churn_date > m.month)
    GROUP BY 1
),

churned_during AS (
    SELECT
        DATE_TRUNC('month', churn_date)                         AS month,
        COUNT(*)                                                AS churned_count,
        SUM(mrr)                                                AS churned_mrr
    FROM subs
    WHERE status = 'churned'
      AND churn_date IS NOT NULL
    GROUP BY 1
),

monthly_churn AS (
    SELECT
        STRFTIME(a.month, '%Y-%m')                              AS month,
        a.active_count,
        COALESCE(c.churned_count, 0)                            AS churned,
        COALESCE(c.churned_mrr,   0)                            AS lost_mrr,
        ROUND(
            COALESCE(c.churned_count, 0) * 100.0 /
            NULLIF(a.active_count, 0)
        , 2)                                                    AS churn_rate_pct
    FROM active_at_start a
    LEFT JOIN churned_during c 
        ON a.month = c.month
    WHERE a.month < DATE_TRUNC('month', CURRENT_DATE)
),

-- 2. CHURN BY PLAN

churn_by_plan AS (
    SELECT
        plan,
        COUNT(*)                                                AS total_users,
        SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END)    AS churned_users,
        ROUND(
            SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*)
        , 1)                                                    AS churn_rate_pct,
        ROUND(AVG(
            CASE WHEN status = 'churned' THEN tenure_days END
        ), 0)                                                   AS avg_days_to_churn
    FROM subs
    GROUP BY 1
    ORDER BY churn_rate_pct DESC
),

--CHURN BY REASON
churn_by_reason AS (
    SELECT
        churn_reason,
        COUNT(*)                                                AS churned_users,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)     AS pct_of_churned,
        SUM(mrr)                                                AS lost_mrr,
        ROUND(AVG(tenure_days), 0)                              AS avg_tenure_days
    FROM subs
    WHERE status       = 'churned'
      AND churn_reason IS NOT NULL
    GROUP BY 1
    ORDER BY churned_users DESC
),

--AVERAGE TENURE BY PLAN + STATUS 
avg_tenure AS (
    SELECT
        plan,
        status,
        COUNT(*)                                                AS users,
        ROUND(AVG(tenure_days), 0)                              AS avg_days,
        ROUND(MIN(tenure_days), 0)                              AS min_days,
        ROUND(MAX(tenure_days), 0)                              AS max_days
    FROM subs
    GROUP BY 1, 2
    ORDER BY plan, status
)

-- OUTPUT: all 4 sections in one result 
SELECT '1_monthly_churn'  AS section, month           AS dim,
    CAST(active_count     AS VARCHAR)                  AS col1,
    CAST(churned          AS VARCHAR)                  AS col2,
    CAST(lost_mrr         AS VARCHAR)                  AS col3,
    CAST(churn_rate_pct   AS VARCHAR)                  AS col4
FROM monthly_churn

UNION ALL
SELECT '2_by_plan', plan,
    CAST(total_users      AS VARCHAR),
    CAST(churned_users    AS VARCHAR),
    CAST(churn_rate_pct   AS VARCHAR),
    CAST(avg_days_to_churn AS VARCHAR)
FROM churn_by_plan

UNION ALL
SELECT '3_by_reason', churn_reason,
    CAST(churned_users    AS VARCHAR),
    CAST(pct_of_churned   AS VARCHAR),
    CAST(lost_mrr         AS VARCHAR),
    CAST(avg_tenure_days  AS VARCHAR)
FROM churn_by_reason

UNION ALL
SELECT '4_tenure', CONCAT(plan, ' / ', status),
    CAST(users            AS VARCHAR),
    CAST(avg_days         AS VARCHAR),
    CAST(min_days         AS VARCHAR),
    CAST(max_days         AS VARCHAR)
FROM avg_tenure

ORDER BY 
    CASE section
        WHEN '1_monthly_churn' THEN 1
        WHEN '2_by_plan'      THEN 2
        WHEN '3_by_reason'    THEN 3
        WHEN '4_tenure'       THEN 4
    END,
    dim;