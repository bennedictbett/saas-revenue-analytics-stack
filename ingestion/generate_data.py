"""
Simulates a realistic B2B SaaS subscription base.

Design principles:
  - Plan tier drives churn probability (enterprise churns least)
  - Churn reasons are plan-aware (starter = price, enterprise = contract_ended)
  - Signup dates weighted toward recent months (growth trend)
  - Churn date always after signup, within a realistic tenure window
  - Payment failures correlate with churn (not random)
  - Company names sound like real B2B buyers
  - MRR is fixed per plan (reflects real SaaS pricing tiers)
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import date, timedelta
import random
import os


Faker.seed(42)
fake = Faker()
random.seed(42)
np.random.seed(42)

N_USERS    = 500
TODAY      = date.today()
START_WINDOW = TODAY - timedelta(days=365)   # last 12 months

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# PLAN DEFINITIONS

PLANS = {
    
    "starter":    {"mrr": 49,  "churn_rate": 0.13, "min_tenure": 14,  "weight": 0.40},
    "growth":     {"mrr": 149, "churn_rate": 0.07, "min_tenure": 30,  "weight": 0.30},
    "pro":        {"mrr": 399, "churn_rate": 0.04, "min_tenure": 60,  "weight": 0.20},
    "enterprise": {"mrr": 999, "churn_rate": 0.02, "min_tenure": 180, "weight": 0.10},
}

# Churn reasons by plan
CHURN_REASONS = {
    "starter": [
        ("too_expensive",        0.40),
        ("missing_features",     0.25),
        ("switched_competitor",  0.20),
        ("no_longer_needed",     0.10),
        ("poor_onboarding",      0.05),
    ],
    "growth": [
        ("missing_features",     0.30),
        ("switched_competitor",  0.25),
        ("too_expensive",        0.25),
        ("poor_support",         0.10),
        ("no_longer_needed",     0.10),
    ],
    "pro": [
        ("switched_competitor",  0.30),
        ("missing_features",     0.30),
        ("poor_support",         0.20),
        ("too_expensive",        0.10),
        ("contract_ended",       0.10),
    ],
    "enterprise": [
        ("contract_ended",       0.40),
        ("switched_competitor",  0.25),
        ("missing_features",     0.20),
        ("poor_support",         0.10),
        ("acquisition",          0.05),
    ],
}

# B2B-realistic countries (weighted toward English-speaking SaaS markets)
COUNTRIES = [
    ("US", 0.45), ("GB", 0.12), ("CA", 0.08), ("AU", 0.06),
    ("DE", 0.06), ("FR", 0.05), ("IN", 0.05), ("NL", 0.03),
    ("SG", 0.03), ("KE", 0.02), ("NG", 0.02), ("ZA", 0.02),
    ("BR", 0.01),
]

# SaaS-relevant industries
INDUSTRIES = [
    "SaaS / Software",
    "Fintech",
    "E-commerce",
    "Healthcare Tech",
    "Logistics",
    "Marketing / AdTech",
    "Education Tech",
    "Cybersecurity",
    "HR Tech",
    "Real Estate Tech",
]

ACQUISITION_CHANNELS = [
    ("organic", 0.35),
    ("paid_ads", 0.25),
    ("referral", 0.15),
    ("sales", 0.15),
    ("partner", 0.10),
]


def weighted_choice(options):
    """
    Pick from a list of (value, weight) tuples.
    Weights don't need to sum to 1 — they're normalised internally.
    """
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


def pick_signup_date():
    """
    Signup dates over the last 12 months, weighted toward recent months.
    This simulates a company that's growing — more new signups lately.
    """
    days_ago = random.choices(
        range(1, 366),
        weights=[1 + (2 * (365 - d) / 365) for d in range(1, 366)],
        k=1
    )[0]
    return TODAY - timedelta(days=days_ago)


def pick_churn_date(signup_date, plan):
    """
    Churn date logic:
      - Must be AFTER signup_date
      - Must respect the plan's minimum tenure (enterprise = 180 days contract)
      - Can't be in the future
      - Weighted toward earlier in tenure (most churn happens early)
    """
    min_tenure = PLANS[plan]["min_tenure"]
    days_since_signup = (TODAY - signup_date).days

    earliest_churn_day = min_tenure
    latest_churn_day   = days_since_signup

    if earliest_churn_day >= latest_churn_day:
        # User hasn't been active long enough to have churned yet
        return None

    available_days = latest_churn_day - earliest_churn_day
    weights = [
        np.exp(-0.015 * d) for d in range(available_days)
    ]
    days_until_churn = random.choices(range(available_days), weights=weights, k=1)[0]

    churn_date = signup_date + timedelta(days=earliest_churn_day + days_until_churn)

    # Sanity check 
    return min(churn_date, TODAY)


def pick_payment_failures(status, plan):
    """
    Payment failures correlate with churn.
    Active users: mostly 0, occasionally 1.
    Churned users: higher failure counts, especially on cheaper plans.
    """
    if status == "active":
        return random.choices([0, 1, 2], weights=[0.85, 0.12, 0.03], k=1)[0]

    # Churned — weighted by plan 
    if plan == "starter":
        return random.choices([0, 1, 2, 3, 4], weights=[0.20, 0.30, 0.25, 0.15, 0.10], k=1)[0]
    elif plan == "growth":
        return random.choices([0, 1, 2, 3],    weights=[0.30, 0.35, 0.25, 0.10], k=1)[0]
    elif plan == "pro":
        return random.choices([0, 1, 2],        weights=[0.45, 0.35, 0.20], k=1)[0]
    else:  # enterprise
        return random.choices([0, 1],           weights=[0.70, 0.30], k=1)[0]


def make_company_name():
    """
    Generate B2B-sounding company names.
    Mix of Faker's company names with occasional tech-flavoured suffixes.
    """
    style = random.random()

    if style < 0.5:
        # Standard Faker company name
        return fake.company()
    elif style < 0.75:
        # Word + Tech suffix
        word   = fake.last_name()
        suffix = random.choice(["Tech", "Labs", "IO", "HQ", "AI", "Systems", "Works"])
        return f"{word} {suffix}"
    else:
        # Two words + Inc/Ltd
        word1  = fake.last_name()
        word2  = random.choice(["Solutions", "Analytics", "Ventures", "Digital", "Cloud"])
        ending = random.choice(["Inc", "Ltd", "Group"])
        return f"{word1} {word2} {ending}"



def generate_subscriptions(n=N_USERS):
    records = []

    plan_names   = list(PLANS.keys())
    plan_weights = [PLANS[p]["weight"] for p in plan_names]

    for i in range(n):
        # Identity
        user_id      = f"usr_{i+1:04d}"
        company_name = make_company_name()
        email        = fake.company_email()
        industry     = random.choice(INDUSTRIES)
        country      = weighted_choice(COUNTRIES)

        # Plan
        plan         = random.choices(plan_names, weights=plan_weights, k=1)[0]
        mrr          = PLANS[plan]["mrr"]
        churn_rate   = PLANS[plan]["churn_rate"]

        # Dates 
        signup_date  = pick_signup_date()

        # Churn decision
        months_active = max((TODAY - signup_date).days / 30, 1)
        prob_churned  = 1 - (1 - churn_rate) ** months_active
        did_churn     = random.random() < prob_churned

        if did_churn:
            churn_date = pick_churn_date(signup_date, plan)
            if churn_date is None:
                # Not enough tenure to have churned — keep active
                did_churn  = False
                churn_date = None
                status     = "active"
            else:
                status     = "churned"
        else:
            churn_date = None
            status     = "active"

        churn_reason = (
            weighted_choice(CHURN_REASONS[plan]) if status == "churned" else None
        )

        # Payment failures
        payment_failures = pick_payment_failures(status, plan)

        records.append({
            "user_id":             user_id,
            "company_name":        company_name,
            "email":               email,
            "plan":                plan,
            "mrr":                 mrr,
            "status":              status,
            "signup_date":         signup_date,
            "churn_date":          churn_date,
            "churn_reason":        churn_reason,
            "payment_failures":    payment_failures,
            "country":             country,
            "industry":            industry,
            "created_at":          signup_date,
            "account_age_days":    (TODAY - signup_date).days,
            "acquisition_channel": weighted_choice(ACQUISITION_CHANNELS),
        })

    return pd.DataFrame(records)



#  VALIDATION 


def validate(df):
    errors = []

    # No duplicate user_ids
    dupes = df["user_id"].duplicated().sum()
    if dupes:
        errors.append(f"  FAIL: {dupes} duplicate user_ids")
    else:
        print("  PASS: no duplicate user_ids")

    
    churned = df[df["status"] == "churned"]
    bad_dates = (churned["churn_date"] <= churned["signup_date"]).sum()
    if bad_dates:
        errors.append(f"  FAIL: {bad_dates} churn_dates before signup_date")
    else:
        print("  PASS: all churn_dates after signup_date")

    # No future churn dates
    future = (pd.to_datetime(df["churn_date"], errors="coerce") > pd.Timestamp.today()).sum()
    if future:
        errors.append(f"  FAIL: {future} churn_dates in the future")
    else:
        print("  PASS: no future churn_dates")

    # Churned rows: churn_reason
    missing_reason = churned["churn_reason"].isna().sum()
    if missing_reason:
        errors.append(f"  FAIL: {missing_reason} churned rows missing churn_reason")
    else:
        print("  PASS: all churned rows have churn_reason")

    
    active = df[df["status"] == "active"]
    bad_active = active["churn_date"].notna().sum()
    if bad_active:
        errors.append(f"  FAIL: {bad_active} active rows have a churn_date")
    else:
        print("  PASS: no active rows have churn_date")
    
    bad_reason_active = active["churn_reason"].notna().sum()
    if bad_reason_active:
        errors.append(f"  FAIL: {bad_reason_active} active rows have a churn_reason")
    else:
        print("  PASS: no active rows have churn_reason")

    # MRR values match plan definitions
    for plan, cfg in PLANS.items():
        wrong_mrr = df[(df["plan"] == plan) & (df["mrr"] != cfg["mrr"])]
        if len(wrong_mrr):
            errors.append(f"  FAIL: {len(wrong_mrr)} {plan} rows have wrong MRR")
    print("  PASS: all MRR values match plan definitions")

    if errors:
        print("\nValidation FAILED:")
        for e in errors:
            print(e)
        raise ValueError("Fix data issues before saving.")
    else:
        print("\n  All validation checks passed.")



# distribution sanity check 


def print_summary(df):
    print("\n Plan distribution ")
    plan_summary = df.groupby("plan").agg(
        count    = ("user_id", "count"),
        churned  = ("status",  lambda x: (x == "churned").sum()),
        total_mrr= ("mrr",     "sum"),
    )
    plan_summary["churn_rate"] = (
        plan_summary["churned"] / plan_summary["count"] * 100
    ).round(1).astype(str) + "%"
    plan_summary["total_mrr"] = plan_summary["total_mrr"].apply(lambda x: f"${x:,.0f}")
    print(plan_summary[["count", "churned", "churn_rate", "total_mrr"]].to_string())

    print("\n Overall stats ")
    active_mrr = df[df["status"] == "active"]["mrr"].sum()
    print(f"  Total users:   {len(df):,}")
    print(f"  Active:        {(df.status == 'active').sum():,}")
    print(f"  Churned:       {(df.status == 'churned').sum():,}")
    print(f"  Active MRR:    ${active_mrr:,.0f}")

    print("\n Top churn reasons")
    reasons = df["churn_reason"].value_counts().head(6)
    for reason, count in reasons.items():
        print(f"  {reason:<25} {count}")

    print("\nSignup date spread ")
    df["signup_month"] = pd.to_datetime(df["signup_date"]).dt.to_period("M")
    monthly = df.groupby("signup_month").size()
    print(f"  Oldest signup: {df['signup_date'].min()}")
    print(f"  Newest signup: {df['signup_date'].max()}")
    print(f"  Monthly range: {monthly.min()}–{monthly.max()} signups/month")



if __name__ == "__main__":
    print(f"\nGenerating {N_USERS} subscription records.\n")

    df = generate_subscriptions()

    print("Validating ")
    validate(df)

    print_summary(df)

    out_path = os.path.join(OUTPUT_DIR, "subscriptions.csv")
    df.to_csv(out_path, index=False)
    print(f"\n  Saved → {out_path}  ({len(df):,} rows)\n")