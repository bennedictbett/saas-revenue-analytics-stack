import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s')

project_name = "."

list_of_files = [
    # Data
    f"{project_name}/data/subscriptions.csv",
    f"{project_name}/data/product_events.csv",
    f"{project_name}/data/email_campaigns.csv",
    f"{project_name}/data/email_sends.csv",
    f"{project_name}/data/leads.csv",

    # Ingestion
    f"{project_name}/ingestion/__init__.py",
    f"{project_name}/ingestion/generate_data.py",
    f"{project_name}/ingestion/upload_to_bq.py",

    # dbt
    f"{project_name}/dbt/dbt_project.yml",
    f"{project_name}/dbt/models/staging/stg_subscriptions.sql",
    f"{project_name}/dbt/models/staging/stg_events.sql",
    f"{project_name}/dbt/models/staging/stg_leads.sql",
    f"{project_name}/dbt/models/marts/mart_revenue.sql",
    f"{project_name}/dbt/models/marts/mart_growth.sql",
    f"{project_name}/dbt/models/marts/mart_churn.sql",
    f"{project_name}/dbt/models/marts/mart_campaigns.sql",
    f"{project_name}/dbt/models/schema.yml",

    # SQL
    f"{project_name}/sql/wau.sql",
    f"{project_name}/sql/mrr.sql",
    f"{project_name}/sql/churn.sql",
    f"{project_name}/sql/funnel.sql",

    # Notebooks
    f"{project_name}/notebooks/01_data_generation.ipynb",
    f"{project_name}/notebooks/02_eda.ipynb",
    f"{project_name}/notebooks/03_churn_model.ipynb",
    f"{project_name}/notebooks/04_anomaly_detection.ipynb",

    # ML
    f"{project_name}/ml/__init__.py",
    f"{project_name}/ml/churn_model.py",
    f"{project_name}/ml/forecast_mrr.sql",
    f"{project_name}/ml/anomaly_detection.py",

    # Dashboards
    f"{project_name}/dashboards/looker_links.md",
    f"{project_name}/dashboards/screenshots/.gitkeep",

    # AI Brief
    f"{project_name}/ai_brief/__init__.py",
    f"{project_name}/ai_brief/weekly_ai_brief.py",
    f"{project_name}/ai_brief/prompt_template.txt",

    # Utils
    f"{project_name}/utils/__init__.py",
    f"{project_name}/utils/config.py",

    # Tests
    f"{project_name}/tests/data_tests.sql",

    # Root
    f"{project_name}/requirements.txt",
    f"{project_name}/README.md",
    f"{project_name}/.gitignore",
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)


    if filedir != "":
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Creating directory: {filedir} for file: {filename}")

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath, "w") as f:
            pass
        logging.info(f"Creating file: {filepath}")

    else:
        logging.info(f"File already exists: {filepath}, skipping creation.")