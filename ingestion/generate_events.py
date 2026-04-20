"""
product_events.csv

Behavioral rules:
  - Plan tier drives baseline session frequency and event depth
  - Churned users decay through 3 phases before leaving:
      Phase 1: normal activity
      Phase 2: 60% drop (disengagement)
      Phase 3: near-silent (last 2 weeks before churn)
  - B2B pattern: Mon-Fri weighted, weekends sparse
  - Sessions produce clusters of related events (not random noise)
  - Enterprise uses advanced features, starter mostly login/dashboard
"""

import logging

import pandas as pd
import numpy as np
from faker import Faker
from datetime import date, timedelta
import random, os, uuid

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s]: %(message)s'
)

fake = Faker()
Faker.seed(99)
random.seed(99)
np.random.seed(99)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)
TODAY = date.today()

# Event catalogue by plan tier
EVENTS_BY_TIER = {
    "starter": [
        ("login",              0.30),
        ("dashboard_view",     0.28),
        ("report_viewed",      0.15),
        ("settings_viewed",    0.10),
        ("help_viewed",        0.09),
        ("logout",             0.08),
    ],
    "growth": [
        ("login",              0.18),
        ("dashboard_view",     0.18),
        ("report_viewed",      0.12),
        ("report_created",     0.10),
        ("export_csv",         0.09),
        ("filter_applied",     0.09),
        ("chart_customised",   0.07),
        ("comment_added",      0.07),
        ("share_link_created", 0.05),
        ("settings_viewed",    0.05),
    ],
    "pro": [
        ("login",                 0.10),
        ("dashboard_view",        0.10),
        ("report_created",        0.10),
        ("export_csv",            0.08),
        ("integration_connected", 0.08),
        ("automation_created",    0.08),
        ("team_member_invited",   0.08),
        ("api_key_generated",     0.07),
        ("webhook_created",       0.07),
        ("scheduled_report_set",  0.07),
        ("filter_applied",        0.07),
        ("bulk_export",           0.05),
        ("comment_added",         0.05),
    ],
    "enterprise": [
        ("sso_login",              0.10),
        ("custom_dashboard_built", 0.09),
        ("api_called",             0.09),
        ("audit_log_viewed",       0.08),
        ("role_permission_set",    0.08),
        ("data_governance_view",   0.07),
        ("report_created",         0.07),
        ("automation_created",     0.07),
        ("integration_connected",  0.07),
        ("bulk_export",            0.07),
        ("team_member_invited",    0.06),
        ("scheduled_report_set",   0.06),
        ("webhook_created",        0.05),
        ("export_csv",             0.04),
    ],
}

# Sessions per active month (min, max) and events per session (min, max)
PLAN_ACTIVITY = {
    "starter":    (6,  12,  2,  4),
    "growth":     (10, 20,  3,  6),
    "pro":        (18, 30,  5,  9),
    "enterprise": (28, 45,  8, 14),
}

DOW_WEIGHTS = [1.0, 1.0, 1.0, 1.0, 0.8, 0.2, 0.15]

_event_counter = 0

def next_event_id():
    global _event_counter
    _event_counter += 1
    return f"evt_{_event_counter:07d}"

def pick_date_weighted(start: date, end: date) -> date:
    delta = (end - start).days
    if delta <= 0:
        return start
    candidates = [start + timedelta(days=i) for i in range(delta + 1)]
    weights    = [DOW_WEIGHTS[d.weekday()] for d in candidates]
    return random.choices(candidates, weights=weights, k=1)[0]

def generate_events_for_user(user: pd.Series) -> list:
    plan        = user["plan"]
    status      = user["status"]
    signup_date = pd.to_datetime(user["signup_date"]).date()
    churn_date  = (
        pd.to_datetime(user["churn_date"]).date()
        if pd.notna(user["churn_date"]) else None
    )
    user_id = user["user_id"]

    active_end  = churn_date if churn_date else TODAY
    tenure_days = max((active_end - signup_date).days, 1)

    sess_min, sess_max, evt_min, evt_max = PLAN_ACTIVITY[plan]
    pool_events, pool_weights = zip(*EVENTS_BY_TIER[plan])

    # Activity phases
    if status == "active":
        phases = [(signup_date, active_end, 1.0)]
    else:
        p1_end = signup_date + timedelta(days=int(tenure_days * 0.60))
        p2_end = signup_date + timedelta(days=int(tenure_days * 0.85))
        phases = [
            (signup_date, p1_end,   1.0),
            (p1_end,      p2_end,   0.4),
            (p2_end,      active_end, 0.08),
        ]

    records = []
    session_counter = 0

    for phase_start, phase_end, multiplier in phases:
        if phase_start >= phase_end:
            continue

        phase_months = max((phase_end - phase_start).days / 30, 0.1)
        n_sessions   = max(int(
            random.randint(sess_min, sess_max) * phase_months * multiplier
        ), 0)

        for _ in range(n_sessions):
            session_counter += 1
            session_id   = f"ses_{user_id}_{session_counter:04d}"
            session_date = pick_date_weighted(phase_start, phase_end)
            n_events     = random.randint(evt_min, evt_max)

            first_event  = "sso_login" if plan == "enterprise" else "login"
            session_evts = [first_event] + list(
                random.choices(pool_events, weights=pool_weights, k=n_events - 1)
            )

            for evt in session_evts:
                records.append({
                    "event_id":   next_event_id(),
                    "user_id":    user_id,
                    "event_name": evt,
                    "event_date": session_date,
                    "platform":   random.choices(["web","mobile"], weights=[0.78, 0.22])[0],
                    "session_id": session_id,
                })

    return records


def validate(df, subs):
    errors = []

    dupes = df["event_id"].duplicated().sum()
    if dupes: errors.append(f"  FAIL: {dupes} duplicate event_ids")
    else: logging.info("PASS: all event_ids unique")

    orphans = ~df["user_id"].isin(subs["user_id"])
    if orphans.sum(): errors.append(f"  FAIL: {orphans.sum()} events for unknown users")
    else: logging.info("PASS: all user_ids in subscriptions")

    merged = df.merge(subs[["user_id","churn_date"]].dropna(), on="user_id", how="inner")
    merged["event_date"] = pd.to_datetime(merged["event_date"])
    merged["churn_date"] = pd.to_datetime(merged["churn_date"])
    bad = (merged["event_date"] > merged["churn_date"]).sum()
    if bad: errors.append(f"  FAIL: {bad} events after churn_date")
    else: logging.info("PASS: no events after churn_date")

    merged2 = df.merge(subs[["user_id","signup_date"]], on="user_id", how="inner")
    merged2["event_date"]  = pd.to_datetime(merged2["event_date"])
    merged2["signup_date"] = pd.to_datetime(merged2["signup_date"])
    early = (merged2["event_date"] < merged2["signup_date"]).sum()
    if early: errors.append(f"  FAIL: {early} events before signup_date")
    else: logging.info("PASS: no events before signup_date")

    if errors:
        for e in errors: print(e)
        raise ValueError("Fix issues before saving.")
    print("  All checks passed.")


if __name__ == "__main__":
    subs = pd.read_csv(os.path.join(OUTPUT_DIR, "subscriptions.csv"))
    print(f"Generating events for {len(subs):,} users...\n")

    all_records = []
    for _, user in subs.iterrows():
        all_records.extend(generate_events_for_user(user))

    df = (
        pd.DataFrame(all_records)
        .sort_values(["user_id", "event_date"])
        .reset_index(drop=True)
    )

    logging.info("Validating data...")
    validate(df, subs)

    # Summary
    plan_map   = subs.set_index("user_id")["plan"]
    status_map = subs.set_index("user_id")["status"]
    df["plan"]   = df["user_id"].map(plan_map)
    df["status"] = df["user_id"].map(status_map)

    logging.info("Generating summary statistics...")
    plan_stats = df.groupby("plan").agg(
        total_events=("event_id", "count"),
        unique_users=("user_id",  "nunique"),
    )
    plan_stats["events_per_user"] = (
        plan_stats["total_events"] / plan_stats["unique_users"]
    ).round(1)
    logging.info("\nPlan statistics:\n%s", plan_stats.to_string())

    logging.info("Avg events per user (active vs churned):")
    avg = (
        df.groupby(["status","user_id"])
        .size()
        .reset_index(name="n")
        .groupby("status")["n"]
        .mean()
        .round(1)
    )
    logging.info("\nAvg events per user (active vs churned):\n%s", avg.to_string())

    logging.info("\nTop 8 events:\n%s", df["event_name"].value_counts().head(8).to_string())

    logging.info("\nDecay check (churned users, events by phase):")
    churned_ids = subs[subs.status == "churned"][["user_id","signup_date","churn_date"]].copy()
    churned_ids["signup_date"] = pd.to_datetime(churned_ids["signup_date"])
    churned_ids["churn_date"]  = pd.to_datetime(churned_ids["churn_date"])
    churned_ids["tenure"]      = (churned_ids["churn_date"] - churned_ids["signup_date"]).dt.days
    churned_ids["p1_end"]      = churned_ids["signup_date"] + pd.to_timedelta((churned_ids["tenure"] * 0.60).astype(int), unit="d")
    churned_ids["p2_end"]      = churned_ids["signup_date"] + pd.to_timedelta((churned_ids["tenure"] * 0.85).astype(int), unit="d")

    evts_c = df[df.status == "churned"].merge(churned_ids, on="user_id")
    evts_c["event_date"] = pd.to_datetime(evts_c["event_date"])

    phase1 = (evts_c["event_date"] <= evts_c["p1_end"]).sum()
    phase2 = ((evts_c["event_date"] > evts_c["p1_end"]) & (evts_c["event_date"] <= evts_c["p2_end"])).sum()
    phase3 = (evts_c["event_date"] > evts_c["p2_end"]).sum()
    total  = phase1 + phase2 + phase3
    logging.info(f"  Phase 1 (normal):   {phase1:,}  ({phase1/total*100:.0f}%)")
    logging.info(f"  Phase 2 (decline):  {phase2:,}  ({phase2/total*100:.0f}%)")
    logging.info(f"  Phase 3 (silent):   {phase3:,}  ({phase3/total*100:.0f}%)")

    df.drop(columns=["plan","status"], inplace=True)

    out = os.path.join(OUTPUT_DIR, "product_events.csv")
    df.to_csv(out, index=False)
    logging.info(f"Total events: {len(df):,}")
    logging.info(f"Saved → {out}")  