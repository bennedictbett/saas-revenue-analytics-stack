# saas-revenue-analytics-stack

An end-to-end data + ML project simulating the analytics stack of a

B2B SaaS company — from raw data generation through SQL analysis,

machine learning, and AI-powered business reporting.

Built as a portfolio project targeting data analytics / ML engineering

roles at early-stage SaaS companies.

---

## What This Project Covers

| Layer | What was built |

|---|---|

| Data generation | 5 synthetic datasets simulating Stripe, Firebase, Brevo, Smartlead |

| SQL analysis | KPI queries for WAU, MRR, churn rate, and lead funnel |

| ML — Churn | XGBoost churn prediction with SHAP explainability |

| ML — Anomaly | Z-score + rolling band + Prophet anomaly detection |

| AI brief | LLM-generated weekly business summary from live KPIs |

---

## Project Structure
saas-revenue-analytics-stack/
│
├── data/                        # synthetic raw data (gitignored)
│   ├── subscriptions.csv        # 500 users — source of truth
│   ├── product_events.csv       # 200k+ usage events
│   ├── leads.csv                # 1,700 outbound leads
│   ├── email_campaigns.csv      # 34 campaigns
│   └── email_sends.csv          # 3,000 individual sends
│
├── ingestion/                   # data generators
│   ├── generate_data.py         # master runner
│   ├── generate_subscriptions.py
│   ├── generate_events.py
│   ├── generate_leads.py
│   ├── generate_email_campaigns.py
│   └── upload_to_bq.py          # BigQuery upload (activate when ready)
│
├── sql/                         # standalone KPI queries (DuckDB)
│   ├── run_query.py             # query runner
│   ├── wau.sql                  # weekly active users
│   ├── mrr.sql                  # monthly recurring revenue
│   ├── churn.sql                # churn rate by plan + reason
│   └── funnel.sql               # lead-to-customer conversion
│
├── notebooks/
│   ├── 01_data_generation.ipynb # data layer walkthrough
│   ├── 02_eda.ipynb             # exploratory analysis — all 5 tables
│   ├── 03_churn_model.ipynb     # churn prediction model
│   └── 04_anomaly_detection.ipynb # anomaly detection on KPIs
│
├── ml/                          # production-style ML scripts
│   ├── churn_model.py
│   └── anomaly_detection.py
│
├── ai_brief/                    # LLM-powered weekly business summary
│   ├── weekly_ai_brief.py
│   └── prompt_template.txt
│
├── dbt/                         # transformation models (BigQuery)
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       └── marts/
│
├── tests/
│   └── data_tests.sql           # data quality assertions
│
├── requirements.txt
└── README.md

---

## Datasets

All data is synthetically generated with realistic business logic.

No real user data is used.

### subscriptions.csv

Source of truth for revenue and churn.

| Column | Description |

|---|---|

| user_id | Primary key — shared across all tables |

| plan | starter / growth / pro / enterprise |

| mrr | Monthly recurring revenue in USD |

| status | active / churned |

| signup_date | Subscription start date |

| churn_date | Cancellation date (null if active) |

| churn_reason | Why the user left (null if active) |

| payment_failures | Count of failed billing attempts |

| country | ISO 2-letter country code |

| industry | Company vertical |

Churn rates by plan: starter ~37%, growth ~33%, pro ~18%, enterprise ~3%.

Enterprise users are on 180-day minimum contracts — they can't churn early.

### product_events.csv

Firebase-style usage events. 200k+ rows.

Churned users show a 3-phase decay pattern before leaving:

- Phase 1 (0–60% of tenure): normal activity

- Phase 2 (60–85%): 60% drop in sessions

- Phase 3 (85–100%): near-silent before churn

### leads.csv

Full outbound funnel: new → qualified → demo_booked → converted.

Converted leads link to real `user_id` values in subscriptions.

### email_campaigns.csv + email_sends.csv

34 campaigns across 7 types. Metrics vary by type:

onboarding has highest open + reply rates, winback has lowest.

---

## Setup

### 1. Clone and install

```bash

git clone https://github.com/bennedictbett/saas-revenue-analytics-stack.git

cd saas-revenue-analytics-stack

python -m venv venv

venv\Scripts\activate        # Windows

# source venv/bin/activate   # Mac/Linux

pip install -r requirements.txt

```

### 2. Generate data

```bash

python ingestion/generate_subscriptions.py

python ingestion/generate_events.py

python ingestion/generate_leads.py

python ingestion/generate_email_campaigns.py

```

Or run all at once:

```bash

python ingestion/generate_data.py

```

### 3. Run SQL queries

```bash

python sql/run_query.py wau

python sql/run_query.py mrr

python sql/run_query.py churn

python sql/run_query.py funnel

```

### 4. Run notebooks

```bash

jupyter notebook notebooks/

```

Open in order: `01` → `02` → `03` → `04`

### 5. Run AI weekly brief

```bash

python ai_brief/weekly_ai_brief.py

```

Requires an Anthropic API key in `.env`:

ANTHROPIC_API_KEY=your_key_here

---

## SQL Layer

All queries run locally against CSV files using DuckDB.

The same SQL works in BigQuery when you're ready to move to the cloud.

### Screening question answers

**Q2 — Weekly Active Users:**

```sql

SELECT

    DATE_TRUNC('week', event_date) AS week,

    COUNT(DISTINCT user_id)        AS wau

FROM product_events

GROUP BY 1

ORDER BY 1;

```

**Q3 — MRR Forecasting:**

See `sql/mrr.sql` for full trend analysis.

See `notebooks/04_anomaly_detection.ipynb` for Prophet forecasting.

**Q4 — Anomaly Detection:**

```python

# Z-score on weekly reply rate

z = (reply_rate - rolling_mean) / rolling_std

if abs(z) > 2.0:

    flag_anomaly()

```

See `notebooks/04_anomaly_detection.ipynb` for full implementation.

**Q5 — Attribution Gap:**

See `sql/funnel.sql` — joins leads to subscriptions via user_id,

exposing exactly which sources convert and which leak.

---

## ML Models

### Churn Prediction (`notebooks/03_churn_model.ipynb`)

**Features (13 total):**

| Feature | Source | Signal |

|---|---|---|

| plan_encoded | subscriptions | Value tier |

| mrr | subscriptions | Revenue at risk |

| payment_failures | subscriptions | Financial stress |

| tenure_days | subscriptions | Longevity |

| total_events | product_events | Overall engagement |

| active_days | product_events | Breadth of usage |

| total_sessions | product_events | Session frequency |

| days_since_last_event | product_events | Recency |

| events_per_day | product_events | Intensity |

| login_count | product_events | Basic engagement |

| advanced_feature_uses | product_events | Power user signal |

| events_last_30d | product_events | Recent engagement |

| unique_events | product_events | Feature breadth |

**Anti-leakage:** all event features computed relative to

`churn_date` for churned users, `today` for active users.

**Results:**

| Model | ROC-AUC |

|---|---|

| Logistic Regression (baseline) | ~0.997 |

| XGBoost | ~1.000 |

> Note: near-perfect scores are expected on synthetic data where

> behavioral differences between active and churned users are

> deliberately strong. The methodology — feature engineering,

> leakage prevention, SHAP explainability — transfers directly

> to real data where AUC of 0.75–0.88 is typical.

### Anomaly Detection (`notebooks/04_anomaly_detection.ipynb`)

Three methods implemented:

| Method | Best for |

|---|---|

| Z-score | Fast real-time alerting |

| Rolling bands | Trending metrics like MRR |

| Prophet | Metrics with trend + seasonality |

---

## Stack

| Layer | Tool |

|---|---|

| Language | Python 3.11+ |

| Data generation | Faker, pandas, numpy |

| Local warehouse | DuckDB |

| ML | scikit-learn, XGBoost, SHAP |

| Forecasting | Prophet |

| AI brief | Anthropic Claude API |

| Notebooks | Jupyter |

| Transformations | dbt Core (BigQuery-ready) |

---

## Roadmap

- [ ] Connect BigQuery — upload CSVs, run dbt models

- [ ] Build Looker Studio dashboard on BigQuery marts

- [ ] Deploy AI brief as scheduled Cloud Function

- [ ] Add MLflow for model versioning

- [ ] Retrain churn model on real data

---

## Author

**Benedict Bett**

Data Analytics / ML Engineering

[GitHub](https://github.com/bennedictbett) · [LinkedIn](https://www.linkedin.com/in/benedict-bett-a9899038a/)