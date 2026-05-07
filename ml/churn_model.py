import os
import json
import logging
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    confusion_matrix,
)
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
import shap
import warnings
warnings.filterwarnings('ignore')

# Logging
logging.basicConfig(
    level  = logging.INFO,
    format = '[%(asctime)s] %(message)s',
    datefmt= '%H:%M:%S'
)
log = logging.getLogger(__name__)

# Paths 
ROOT       = Path(__file__).resolve().parent.parent
DATA_DIR   = ROOT / 'data'
MODELS_DIR = ROOT / 'ml' / 'models'
MODELS_DIR.mkdir(parents=True, exist_ok=True)

TODAY = pd.Timestamp.today().normalize()

# Feature columns 
FEATURE_COLS = [
    'plan_encoded', 'mrr', 'payment_failures', 'tenure_days',
    'total_events', 'active_days', 'total_sessions', 'unique_events',
    'days_since_last_event', 'events_per_day', 'login_count',
    'advanced_feature_uses', 'events_last_30d'
]

ADVANCED_EVENTS = [
    'integration_connected', 'automation_created', 'api_key_generated',
    'webhook_created', 'bulk_export', 'scheduled_report_set',
    'custom_dashboard_built', 'api_called'
]


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info("Loading data...")
    subs   = pd.read_csv(
        DATA_DIR / 'subscriptions.csv',
        parse_dates=['signup_date', 'churn_date']
    )
    events = pd.read_csv(
        DATA_DIR / 'product_events.csv',
        parse_dates=['event_date']
    )
    log.info(f"  Subscriptions : {len(subs):,} rows")
    log.info(f"  Events        : {len(events):,} rows")
    log.info(f"  Churn rate    : {subs['status'].eq('churned').mean()*100:.1f}%")
    return subs, events



# FEATURE ENGINEERING

def build_features(subs: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    log.info("Engineering features...")

    # Reference date — prevents leakage
    subs = subs.copy()
    subs['ref_date']     = subs['churn_date'].fillna(TODAY)
    subs['tenure_days']  = (subs['ref_date'] - subs['signup_date']).dt.days.clip(lower=1)
    subs['plan_encoded'] = subs['plan'].map(
        {'starter': 0, 'growth': 1, 'pro': 2, 'enterprise': 3}
    )

    # Filter events to before ref_date
    ec = events.merge(
        subs[['user_id', 'signup_date', 'ref_date']], on='user_id', how='left'
    )
    ec = ec[
        (ec['event_date'] >= ec['signup_date']) &
        (ec['event_date'] <= ec['ref_date'])
    ]
    log.info(f"  Events after leakage filter: {len(ec):,}")

    # Core engagement
    agg = ec.groupby('user_id').agg(
        total_events    = ('event_id',   'count'),
        active_days     = ('event_date', 'nunique'),
        total_sessions  = ('session_id', 'nunique'),
        unique_events   = ('event_name', 'nunique'),
        last_event_date = ('event_date', 'max'),
    ).reset_index()

    ref_map = subs.set_index('user_id')['ref_date']
    agg['days_since_last_event'] = (
        agg['user_id'].map(ref_map) - agg['last_event_date']
    ).dt.days
    agg['events_per_day'] = (
        agg['total_events'] / agg['active_days'].clip(lower=1)
    ).round(2)

    # Login count
    login_counts = (
        ec[ec['event_name'].isin(['login', 'sso_login'])]
        .groupby('user_id').size()
        .reset_index(name='login_count')
    )

    # Advanced feature usage
    advanced_counts = (
        ec[ec['event_name'].isin(ADVANCED_EVENTS)]
        .groupby('user_id').size()
        .reset_index(name='advanced_feature_uses')
    )

    # Last 30 days — relative to ref_date
    recent = (
        ec[ec['event_date'] >= (ec['ref_date'] - pd.Timedelta(days=30))]
        .groupby('user_id').size()
        .reset_index(name='events_last_30d')
    )

    # Merge
    features = (
        subs[['user_id', 'plan', 'plan_encoded', 'mrr',
              'payment_failures', 'tenure_days', 'status']]
        .merge(agg[['user_id', 'total_events', 'active_days', 'total_sessions',
                    'unique_events', 'days_since_last_event', 'events_per_day']],
               on='user_id', how='left')
        .merge(login_counts,    on='user_id', how='left')
        .merge(advanced_counts, on='user_id', how='left')
        .merge(recent,          on='user_id', how='left')
    )

    # Fill nulls
    fill_zero = ['total_events', 'active_days', 'total_sessions', 'unique_events',
                 'login_count', 'advanced_feature_uses', 'events_last_30d']
    features[fill_zero]               = features[fill_zero].fillna(0)
    features['days_since_last_event'] = features['days_since_last_event'].fillna(999)
    features['events_per_day']        = features['events_per_day'].fillna(0)
    features['churned']               = (features['status'] == 'churned').astype(int)

    log.info(f"  Feature matrix : {features[FEATURE_COLS].shape}")
    log.info(f"  Nulls          : {features[FEATURE_COLS].isna().sum().sum()}")

    return features


# 3. TRAIN

def train(features: pd.DataFrame) -> tuple:
    log.info("Training models...")

    X = features[FEATURE_COLS]
    y = features['churned']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    log.info(f"  Train: {len(X_train)}  Test: {len(X_test)}")

    # Baseline
    scaler         = StandardScaler()
    X_train_sc     = scaler.fit_transform(X_train)
    X_test_sc      = scaler.transform(X_test)
    lr             = LogisticRegression(max_iter=1000, random_state=42,
                                        class_weight='balanced')
    lr.fit(X_train_sc, y_train)
    lr_auc = roc_auc_score(y_test, lr.predict_proba(X_test_sc)[:, 1])
    log.info(f"  Logistic Regression ROC-AUC : {lr_auc:.4f}")

    # XGBoost
    scale_pos = (y_train == 0).sum() / (y_train == 1).sum()
    xgb = XGBClassifier(
        n_estimators     = 300,
        max_depth        = 4,
        learning_rate    = 0.05,
        subsample        = 0.8,
        colsample_bytree = 0.8,
        scale_pos_weight = scale_pos,
        eval_metric      = 'logloss',
        random_state     = 42,
        verbosity        = 0,
    )
    xgb.fit(X_train, y_train)

    xgb_preds = xgb.predict(X_test)
    xgb_proba = xgb.predict_proba(X_test)[:, 1]
    xgb_auc   = roc_auc_score(y_test, xgb_proba)

    # Cross-validation
    cv        = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(xgb, X, y, cv=cv, scoring='roc_auc')

    log.info(f"  XGBoost ROC-AUC     : {xgb_auc:.4f}")
    log.info(f"  5-Fold CV ROC-AUC   : {cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")

    return xgb, X_test, y_test, xgb_preds, xgb_proba, xgb_auc, cv_scores


#  EVALUATE

def evaluate(xgb, X_test, y_test, xgb_preds, xgb_proba, xgb_auc, cv_scores):
    log.info("Evaluating...")

    print("\n Classification Report ")
    print(classification_report(y_test, xgb_preds, target_names=['Active', 'Churned']))

    print(" Confusion Matrix ")
    cm = confusion_matrix(y_test, xgb_preds)
    print(f"  TN={cm[0,0]}  FP={cm[0,1]}")
    print(f"  FN={cm[1,0]}  TP={cm[1,1]}")

    print("\n Model Scores")
    print(f"  ROC-AUC       : {xgb_auc:.4f}")
    print(f"  5-Fold CV     : {cv_scores.mean():.4f} +/- {cv_scores.std():.4f}")



# SHAP EXPLANATIONS

def explain(xgb, X_test: pd.DataFrame) -> pd.DataFrame:
    log.info("Computing SHAP values...")

    explainer   = shap.TreeExplainer(xgb)
    shap_values = explainer.shap_values(X_test)

    importance = pd.DataFrame({
        'feature':   FEATURE_COLS,
        'mean_shap': np.abs(shap_values).mean(axis=0)
    }).sort_values('mean_shap', ascending=False).reset_index(drop=True)

    print("\n SHAP Feature Importance ")
    print(importance.to_string(index=False))

    # Save plot
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(importance['feature'], importance['mean_shap'],
            color='#3b82f6', edgecolor='white')
    ax.set_xlabel('Mean |SHAP value|')
    ax.set_title('Churn Model — Feature Importance (SHAP)')
    ax.invert_yaxis()
    plt.tight_layout()
    plot_path = MODELS_DIR / 'shap_importance.png'
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close()
    log.info(f"  SHAP plot saved → {plot_path}")

    return importance



#  SCORE + SAVE


def score_and_save(xgb, features: pd.DataFrame) -> pd.DataFrame:
    log.info("Scoring all users...")

    X = features[FEATURE_COLS]
    features = features.copy()
    features['churn_probability'] = xgb.predict_proba(X)[:, 1].round(4)
    features['risk_tier']         = pd.cut(
        features['churn_probability'],
        bins   = [0, 0.30, 0.60, 1.0],
        labels = ['low', 'medium', 'high']
    )

    active = features[features['status'] == 'active']
    log.info("  Risk distribution (active users):")
    for tier in ['low', 'medium', 'high']:
        subset = active[active['risk_tier'] == tier]
        log.info(f"    {tier:<8} : {len(subset):>4} users   ${subset['mrr'].sum():>8,.0f} MRR")

    # Save scores
    output_cols = ['user_id', 'plan', 'mrr', 'status', 'tenure_days',
                   'total_events', 'active_days', 'payment_failures',
                   'days_since_last_event', 'events_last_30d',
                   'churn_probability', 'risk_tier']
    output = features[output_cols]
    scores_path = DATA_DIR / 'churn_scores.csv'
    output.to_csv(scores_path, index=False)
    log.info(f"  Scores saved → {scores_path}")

    # Save model
    model_path = MODELS_DIR / 'xgb_churn.json'
    xgb.save_model(str(model_path))
    log.info(f"  Model saved  → {model_path}")

    # Save metadata
    meta = {
        'trained_at':     datetime.now().isoformat(),
        'n_users':        len(features),
        'n_features':     len(FEATURE_COLS),
        'feature_cols':   FEATURE_COLS,
        'churn_rate':     round(features['churned'].mean(), 4),
        'high_risk_users':int((active['risk_tier'] == 'high').sum()),
        'high_risk_mrr':  int(active[active['risk_tier'] == 'high']['mrr'].sum()),
    }
    meta_path = MODELS_DIR / 'model_metadata.json'
    meta_path.write_text(json.dumps(meta, indent=2))
    log.info(f"  Metadata saved → {meta_path}")

    return output


def main():
    log.info(" Churn Model Pipeline")

    subs, events = load_data()
    features     = build_features(subs, events)
    xgb, X_test, y_test, preds, proba, auc, cv = train(features)

    evaluate(xgb, X_test, y_test, preds, proba, auc, cv)
    explain(xgb, X_test)
    score_and_save(xgb, features)

    log.info(" Pipeline complete ")


if __name__ == '__main__':
    main()