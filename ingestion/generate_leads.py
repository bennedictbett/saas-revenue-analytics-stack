"""
leads.csv
Represents the full marketing + sales funnel:
    Lead → Qualified → Demo → Converted (→ subscription)

Key design rules:
  - Converted leads MUST link to a real user_id in subscriptions.csv
  - Lead source influences conversion rate (referral converts best)
  - Funnel stages are sequential — you can't skip from new → converted
  - Lead created_date is always BEFORE the linked subscription signup_date
  - Lost leads have a loss_reason that makes business sense per stage
  - Non-converted leads have no user_id (null) — they never became customers
"""

import logging

import pandas as pd
import numpy as np
from faker import Faker
from datetime import date, timedelta
import random
import os

VERBOSE = False

fake = Faker()
Faker.seed(77)
random.seed(77)
np.random.seed(77)

OUTPUT_DIR = "data"
TODAY      = date.today()

# Funnel configuration 

SOURCES = {
    #                 conv_rate  speed_days(min,max)  volume_weight
    "referral":      (0.28,      (7,  30),            0.12),
    "inbound":       (0.22,      (3,  21),            0.18),
    "linkedin":      (0.12,      (14, 45),            0.22),
    "cold_email":    (0.07,      (21, 60),            0.30),
    "cold_call":     (0.09,      (10, 35),            0.10),
    "paid_ad":       (0.05,      (3,  14),            0.08),
}

# Funnel stages in order
STAGES = ["new", "qualified", "demo_booked", "converted", "lost"]

DEAD_END_STAGE = {
    "new":         0.20,
    "qualified":   0.40,
    "demo_booked": 0.40,
}

# Loss reasons per stage — makes business sense
LOSS_REASONS = {
    "new":         ["no_response",       "wrong_icp",        "duplicate"],
    "qualified":   ["budget_constraint", "no_response",      "wrong_icp",      "chose_competitor"],
    "demo_booked": ["no_show",           "budget_constraint","chose_competitor","not_ready"],
}

INDUSTRIES = [
    "SaaS / Software", "Fintech", "E-commerce", "Healthcare Tech",
    "Logistics", "Marketing / AdTech", "Education Tech",
    "Cybersecurity", "HR Tech", "Real Estate Tech",
]

COMPANY_SIZES = [
    ("1-10",    0.20),
    ("11-50",   0.35),
    ("51-200",  0.25),
    ("201-500", 0.12),
    ("500+",    0.08),
]

def weighted_choice(options):
    values, weights = zip(*options)
    return random.choices(values, weights=weights, k=1)[0]


#Core builder

def build_leads(subs: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy:
      1. Take every converted subscriber and backfill a lead record for them.
         This guarantees the funnel connects to real subscriptions.
      2. Add extra non-converted leads to simulate realistic funnel volume
         (most leads never convert).
    """

    records = []

    # converted leads (linked to subscriptions) 
    for _, sub in subs.iterrows():
        signup_date = pd.to_datetime(sub["signup_date"]).date()

        source           = weighted_choice([(s, v[2]) for s, v in SOURCES.items()])
        _, speed, _      = SOURCES[source]
        speed_min, speed_max = speed


        days_before = random.randint(speed_min, speed_max)
        created_date = signup_date - timedelta(days=days_before)

    
        window_start = TODAY - timedelta(days=400)
        if created_date < window_start:
            created_date = window_start

        records.append({
            "lead_id":       f"led_{len(records)+1:05d}",
            "user_id":       sub["user_id"],      # ← linked to subscription
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "company":       sub["company_name"],
            "email":         sub["email"],
            "industry":      sub["industry"],
            "company_size":  weighted_choice(COMPANY_SIZES),
            "source":        source,
            "stage":         "converted",
            "created_date":  created_date,
            "converted_date":signup_date,
            "loss_reason":   None,
        })

    

    avg_conv_rate = np.mean([v[0] for v in SOURCES.values()])
    n_unconverted = int(len(subs) * (1 - avg_conv_rate) / avg_conv_rate)
    n_unconverted = min(n_unconverted, 1200)   # cap for manageability

    window_start = TODAY - timedelta(days=400)

    for _ in range(n_unconverted):
        source           = weighted_choice([(s, v[2]) for s, v in SOURCES.items()])
        created_days_ago = random.randint(1, 400)
        created_date     = max(TODAY - timedelta(days=created_days_ago), window_start)

        # Where did they die in the funnel?
        dead_stage   = weighted_choice(list(DEAD_END_STAGE.items()))
        loss_reason  = random.choice(LOSS_REASONS[dead_stage])

        records.append({
            "lead_id":       f"led_{len(records)+1:05d}",
            "user_id":       None,              # ← never converted
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "company":       fake.company(),
            "email":         fake.company_email(),
            "industry":      random.choice(INDUSTRIES),
            "company_size":  weighted_choice(COMPANY_SIZES),
            "source":        source,
            "stage":         dead_stage,
            "created_date":  created_date,
            "converted_date":None,
            "loss_reason":   loss_reason,
        })

    df = pd.DataFrame(records).sample(frac=1, random_state=77).reset_index(drop=True)

    # Re-assign lead_ids cleanly after shuffle
    df["lead_id"] = [f"led_{i+1:05d}" for i in range(len(df))]

    return df


# Validation 

def validate(df: pd.DataFrame, subs: pd.DataFrame):
    errors = []

    # Unique lead_ids
    dupes = df["lead_id"].duplicated().sum()
    if dupes: errors.append(f"  FAIL: {dupes} duplicate lead_ids")
    else: print("  PASS: all lead_ids unique")

    conv = df[df["stage"] == "converted"]
    missing_uid = conv["user_id"].isna().sum()
    if missing_uid: errors.append(f"  FAIL: {missing_uid} converted leads missing user_id")
    else: logging.info("  PASS: all converted leads have user_id")

    lead_uids = set(df["user_id"].dropna())
    sub_uids  = set(subs["user_id"])
    orphans   = lead_uids - sub_uids
    if orphans: errors.append(f"  FAIL: {len(orphans)} user_ids not in subscriptions")
    else: logging.info("  PASS: all user_ids exist in subscriptions")

    # Every subscriber has at least one lead
    subs_with_lead = set(df["user_id"].dropna())
    missing_subs   = sub_uids - subs_with_lead
    if missing_subs: errors.append(f"  FAIL: {len(missing_subs)} subscribers have no lead record")
    else: print("  PASS: every subscriber has a lead record")

    # created_date must be before converted_date (signup_date)
    conv_dated = conv.dropna(subset=["converted_date"]).copy()
    conv_dated["created_date"]   = pd.to_datetime(conv_dated["created_date"])
    conv_dated["converted_date"] = pd.to_datetime(conv_dated["converted_date"])
    bad_dates = (conv_dated["created_date"] >= conv_dated["converted_date"]).sum()
    if bad_dates: errors.append(f"  FAIL: {bad_dates} leads created on/after conversion")
    else: print("  PASS: all created_dates before converted_date")

    # Non-converted leads have no user_id
    non_conv     = df[df["stage"] != "converted"]
    bad_non_conv = non_conv["user_id"].notna().sum()
    if bad_non_conv: errors.append(f"  FAIL: {bad_non_conv} non-converted leads have user_id")
    else: print("  PASS: no user_id on non-converted leads")

    # Loss reason present on all non-converted leads
    missing_reason = non_conv["loss_reason"].isna().sum()
    if missing_reason: errors.append(f"  FAIL: {missing_reason} lost leads missing loss_reason")
    else: print("  PASS: all lost leads have loss_reason")

    if errors:
        for e in errors: print(e)
        raise ValueError("Fix data issues before saving.")
    print("  All checks passed.")


# Summary

def print_summary(df: pd.DataFrame):
    total     = len(df)
    converted = (df["stage"] == "converted").sum()

    logging.info("\nFunnel overview ")
    logging.info(f"  Total leads:      {total:,}")
    logging.info(f"  Converted:        {converted:,}  ({converted/total*100:.1f}%)")
    logging.info(f"  Non-converted:    {total - converted:,}")

    logging.info("\n Stage breakdown ")
    logging.info(df["stage"].value_counts().to_string())

    logging.info("\n Conversion rate by source ")
    src = df.groupby("source").apply(
        lambda x: pd.Series({
            "total":     len(x),
            "converted": (x["stage"] == "converted").sum(),
            "conv_rate": f"{(x['stage'] == 'converted').mean()*100:.1f}%",
        })
    )
    logging.info(src.to_string())

    logging.info("\n Loss reason breakdown ")
    logging.info(df["loss_reason"].value_counts().to_string())

    logging.info("\n Company size mix ")
    logging.info(df["company_size"].value_counts().to_string())



if __name__ == "__main__":
    subs = pd.read_csv(os.path.join(OUTPUT_DIR, "subscriptions.csv"))
    logging.info(f"Loaded {len(subs):,} subscriptions.\n")
    logging.info("Building leads funnel...\n")

    df = build_leads(subs)

    logging.info(" Validating ")
    validate(df, subs)
    
    if VERBOSE: 
        logging.info(" Printing summary ")
        print_summary(df)

    out = os.path.join(OUTPUT_DIR, "leads.csv")
    df.to_csv(out, index=False)
    logging.info(f"  Saved → {out}  ({len(df):,} rows)")