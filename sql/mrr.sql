
-- mrr.sql — Monthly Recurring Revenue (MRR)

-- Tracks revenue health month by month.
--
-- Key metrics:
--   new_mrr           → revenue added from new customers
--   churned_mrr       → revenue lost from cancellations
--   net_mrr           → new_mrr - churned_mrr
--   cumulative_mrr    → running total active MRR
--   mrr_growth_pct    → MoM % change on cumulative MRR
--   revenue_churn_pct → churned MRR / prior month MRR

WITH

--  new MRR per month 
monthly_new AS (
    SELECT
        DATE_TRUNC('month', signup_date)    AS month,
        COUNT(*)                            AS new_customers,
        SUM(mrr)                            AS new_mrr
    FROM read_csv_auto('data/subscriptions.csv')
    WHERE signup_date IS NOT NULL
    GROUP BY 1
),

-- churned MRR per month
monthly_churned AS (
    SELECT
        DATE_TRUNC('month', churn_date)     AS month,
        COUNT(*)                            AS churned_customers,
        SUM(mrr)                            AS churned_mrr
    FROM read_csv_auto('data/subscriptions.csv')
    WHERE status     = 'churned'
      AND churn_date IS NOT NULL
    GROUP BY 1
),

-- net MRR per month
all_months AS (
    SELECT DISTINCT DATE_TRUNC('month', signup_date) AS month
    FROM read_csv_auto('data/subscriptions.csv')

    UNION

    SELECT DATE_TRUNC('month', churn_date) AS month
    FROM read_csv_auto('data/subscriptions.csv')
    WHERE churn_date IS NOT NULL
),

combined AS (
    SELECT
        m.month,

        COALESCE(n.new_customers, 0)      AS new_customers,
        COALESCE(n.new_mrr, 0)            AS new_mrr,

        COALESCE(c.churned_customers, 0)  AS churned_customers,
        COALESCE(c.churned_mrr, 0)        AS churned_mrr,

        COALESCE(n.new_mrr, 0)
        - COALESCE(c.churned_mrr, 0)      AS net_mrr

    FROM all_months m
    LEFT JOIN monthly_new n     ON m.month = n.month
    LEFT JOIN monthly_churned c ON m.month = c.month
),


-- cumulative MRR (running total)
with_cumulative AS (
    SELECT
        *,
        SUM(net_mrr) OVER (
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                       AS cumulative_mrr
    FROM combined
),

-- MoM growth + revenue churn %
-- 
final AS (
    SELECT
        month,
        new_customers,
        new_mrr,
        churned_customers,
        churned_mrr,
        net_mrr,
        cumulative_mrr,

        ROUND(
            (cumulative_mrr - LAG(cumulative_mrr) OVER (ORDER BY month))
            * 100.0 /
            NULLIF(LAG(cumulative_mrr) OVER (ORDER BY month), 0)
        , 1)                                    AS mrr_growth_pct,

        ROUND(
            churned_mrr * 100.0 /
            NULLIF(LAG(cumulative_mrr) OVER (ORDER BY month), 0)
        , 1)                                    AS revenue_churn_pct

    FROM with_cumulative
)

SELECT
    STRFTIME(month, '%Y-%m')                    AS month,
    new_customers,
    new_mrr,
    churned_customers,
    churned_mrr,
    net_mrr,
    cumulative_mrr,
    mrr_growth_pct,
    revenue_churn_pct
FROM final
WHERE month < DATE_TRUNC('month', CURRENT_DATE)
ORDER BY month;