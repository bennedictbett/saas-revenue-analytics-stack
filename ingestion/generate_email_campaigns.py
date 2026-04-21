"""
email_campaigns.csv + email_sends.csv
Two related tables:

  campaigns.csv  — one row per campaign (the "what")
  email_sends.csv — one row per user-campaign send (the "who received it")

Campaign types and their logic:
  onboarding    → sent to users in first 7 days, high open rate
  activation    → sent to low-activity users at day 14, push them to engage
  newsletter    → monthly, all active users
  promo         → quarterly discount offers, all users
  upsell        → targets starter/growth users, push to next plan
  re_engagement → targets users silent for 30+ days (churn risk signal)
  winback       → targets churned users (trying to recover them)

Realistic metrics per type:
  - Onboarding has highest open + reply (user is fresh and curious)
  - Re-engagement and winback have lowest (user already checked out)
  - Upsell has moderate open but low reply (people read, rarely respond)
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import date, timedelta
import random
import os

import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s')

VERBOSE = False  

fake = Faker()
Faker.seed(55)
random.seed(55)
np.random.seed(55)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data")
TODAY      = date.today()

os.makedirs(OUTPUT_DIR, exist_ok=True)


 

CAMPAIGN_TYPES = {
    #                  open_rate       click_rate      reply_rate      unsubscribe_rate
    "onboarding":   ((0.55, 0.70),   (0.20, 0.35),   (0.08, 0.18),   (0.001, 0.005)),
    "activation":   ((0.35, 0.50),   (0.12, 0.22),   (0.04, 0.10),   (0.002, 0.008)),
    "newsletter":   ((0.28, 0.42),   (0.08, 0.16),   (0.01, 0.04),   (0.003, 0.010)),
    "promo":        ((0.30, 0.45),   (0.10, 0.20),   (0.02, 0.06),   (0.005, 0.015)),
    "upsell":       ((0.32, 0.48),   (0.10, 0.18),   (0.02, 0.05),   (0.004, 0.012)),
    "re_engagement":((0.15, 0.28),   (0.05, 0.12),   (0.01, 0.03),   (0.008, 0.020)),
    "winback":      ((0.10, 0.20),   (0.03, 0.08),   (0.005, 0.02),  (0.010, 0.025)),
}


CAMPAIGN_SCHEDULE = {
    "onboarding":    0,   # triggered per-user — handled in sends, not as bulk campaigns
    "activation":    0,   # same — per-user trigger
    "newsletter":   12,   # monthly
    "promo":         4,   # quarterly
    "upsell":        6,   # every ~2 months
    "re_engagement": 8,   # roughly every 6 weeks
    "winback":       4,   # quarterly
}


SEGMENTS = {
    "newsletter":    "all_active",
    "promo":         "all_active",
    "upsell":        "starter_growth",     
    "re_engagement": "low_activity",       
    "winback":       "churned",            
}


def log(msg):
    if VERBOSE:
        logging.info(msg)


def spread_dates(n: int, days_back: int = 365, min_gap: int = 5) -> list[date]:
    """
    Distribute n campaign dates across the last `days_back` days,
    ensuring at least `min_gap` days between consecutive sends.
    """
    window_start = TODAY - timedelta(days=days_back)
    available    = list(range(days_back))
    dates        = []
    used         = set()

    attempts = 0
    while len(dates) < n and attempts < 10000:
        attempts += 1
        d = random.choice(available)
        if all(abs(d - u) >= min_gap for u in used):
            used.add(d)
            dates.append(window_start + timedelta(days=d))

    return sorted(dates)


def build_campaign_calendar() -> list[dict]:
    """
    Build ordered list of campaigns with names, types, and send dates.
    """
    campaigns = []
    cid       = 1

    for ctype, count in CAMPAIGN_SCHEDULE.items():
        if count == 0:
            continue

        dates = spread_dates(count, days_back=365, min_gap=6)

        for i, send_date in enumerate(dates):
            month_label = send_date.strftime("%b %Y")

            if ctype == "newsletter":
                name = f"Monthly Newsletter — {month_label}"
            elif ctype == "promo":
                quarter = (send_date.month - 1) // 3 + 1
                name    = f"Q{quarter} Promotion — {send_date.year}"
            elif ctype == "upsell":
                name = f"Upgrade Offer #{i+1} — {month_label}"
            elif ctype == "re_engagement":
                name = f"Re-engagement #{i+1} — {month_label}"
            elif ctype == "winback":
                name = f"Winback Campaign #{i+1} — {month_label}"
            else:
                name = f"{ctype.title()} #{i+1} — {month_label}"

            campaigns.append({
                "campaign_id":   f"cmp_{cid:03d}",
                "campaign_name": name,
                "campaign_type": ctype,
                "segment":       SEGMENTS.get(ctype, "all_active"),
                "send_date":     send_date,
                "subject_line":  _subject_line(ctype, i),
            })
            cid += 1

    return sorted(campaigns, key=lambda x: x["send_date"])


def _subject_line(ctype: str, variant: int) -> str:
    lines = {
        "newsletter":    [
            "Your monthly product update is here",
            "What's new this month at SaaS Co",
            "This month in analytics — your digest",
            "Monthly round-up: features, tips & more",
        ],
        "promo":         [
            "Limited time: 20% off all annual plans",
            "Upgrade today — exclusive Q-end offer",
            "Special offer inside — ends this Friday",
            "Your discount expires in 48 hours",
        ],
        "upsell":        [
            "You're outgrowing your current plan",
            "Unlock these features on the next tier",
            "Teams like yours upgraded last month",
            "See what Pro users are doing differently",
            "Ready to scale? Here's what's waiting",
            "Your usage suggests you need more power",
        ],
        "re_engagement": [
            "We miss you — here's what you've missed",
            "It's been a while. Come back?",
            "Your dashboard has new insights waiting",
            "Don't let your data go stale",
            "Still there? Your account is active",
            "Quick check-in from the team",
            "New features since you last logged in",
            "Your team is waiting for you",
        ],
        "winback":       [
            "We'd love to have you back",
            "A lot has changed since you left",
            "Special offer — come back at 30% off",
            "We fixed the things that bothered you",
        ],
    }
    pool = lines.get(ctype, [f"{ctype.title()} email"])
    return pool[variant % len(pool)]


# Segment resolution

def resolve_segment(segment: str, send_date: date, subs: pd.DataFrame,
                    events: pd.DataFrame) -> list[str]:
    """
    Return list of user_ids that qualify for this segment on send_date.
    """
    active_on_date = subs[
        (pd.to_datetime(subs["signup_date"]).dt.date <= send_date) &
        (
            subs["churn_date"].isna() |
            (pd.to_datetime(subs["churn_date"]).dt.date > send_date)
        )
    ]["user_id"].tolist()

    if segment == "all_active":
        return active_on_date

    elif segment == "starter_growth":
        return subs[
            (subs["user_id"].isin(active_on_date)) &
            (subs["plan"].isin(["starter", "growth"]))
        ]["user_id"].tolist()

    elif segment == "low_activity":
        # Users active but no event in last 30 days before send_date
        cutoff = send_date - timedelta(days=30)
        recent_users = set(
            events[
                (pd.to_datetime(events["event_date"]).dt.date > cutoff) &
                (pd.to_datetime(events["event_date"]).dt.date <= send_date)
            ]["user_id"]
        )
        return [u for u in active_on_date if u not in recent_users]

    elif segment == "churned":
        return subs[
            (subs["status"] == "churned") &
            (pd.to_datetime(subs["churn_date"]).dt.date <= send_date)
        ]["user_id"].tolist()

    return active_on_date


# Send record builder

def build_sends(campaigns: list[dict], subs: pd.DataFrame,
                events: pd.DataFrame) -> pd.DataFrame:
    all_sends = []
    send_id   = 1

    for camp in campaigns:
        ctype     = camp["campaign_type"]
        send_date = camp["send_date"]
        rates     = CAMPAIGN_TYPES[ctype]

        targets = resolve_segment(camp["segment"], send_date, subs, events)

        # Cap large segments — not every user gets every bulk email
        if len(targets) > 300:
            targets = random.sample(targets, random.randint(200, 300))

        if not targets:
            log(f"  [{camp['campaign_id']}] {camp['campaign_name']} — 0 recipients, skipping")
            continue

        log(f"  [{camp['campaign_id']}] {camp['campaign_name']} → {len(targets)} recipients")

        # Draw per-campaign rates (vary slightly each campaign)
        open_rate        = random.uniform(*rates[0])
        click_rate       = random.uniform(*rates[1])
        reply_rate       = random.uniform(*rates[2])
        unsubscribe_rate = random.uniform(*rates[3])

        for uid in targets:
            opened      = random.random() < open_rate
            clicked     = opened and (random.random() < click_rate)
            replied     = opened and (random.random() < reply_rate)
            unsubscribed= random.random() < unsubscribe_rate

            all_sends.append({
                "send_id":        f"snd_{send_id:06d}",
                "campaign_id":    camp["campaign_id"],
                "user_id":        uid,
                "send_date":      send_date,
                "opened":         opened,
                "clicked":        clicked,
                "replied":        replied,
                "unsubscribed":   unsubscribed,
            })
            send_id += 1

    return pd.DataFrame(all_sends)


# Validation

def validate(campaigns_df: pd.DataFrame, sends_df: pd.DataFrame,
             subs: pd.DataFrame):
    errors = []

    # Unique campaign_ids
    dupes = campaigns_df["campaign_id"].duplicated().sum()
    if dupes: errors.append(f"  FAIL: {dupes} duplicate campaign_ids")
    else: logging.info("  PASS: all campaign_ids unique")

    # Unique send_ids
    dupes = sends_df["send_id"].duplicated().sum()
    if dupes: errors.append(f"  FAIL: {dupes} duplicate send_ids")
    else: logging.info("  PASS: all send_ids unique")

    # All campaign_ids in sends exist in campaigns
    orphan_camps = ~sends_df["campaign_id"].isin(campaigns_df["campaign_id"])
    if orphan_camps.sum(): errors.append(f"  FAIL: {orphan_camps.sum()} sends reference unknown campaigns")
    else: logging.info("  PASS: all campaign_ids in sends are valid")

    # All user_ids in sends exist in subscriptions
    orphan_users = ~sends_df["user_id"].isin(subs["user_id"])
    if orphan_users.sum(): errors.append(f"  FAIL: {orphan_users.sum()} sends reference unknown users")
    else: logging.info("  PASS: all user_ids in sends exist in subscriptions")

    # clicked implies opened
    bad_clicks = sends_df[sends_df["clicked"] & ~sends_df["opened"]]
    if len(bad_clicks): errors.append(f"  FAIL: {len(bad_clicks)} clicks without opens")
    else: logging.info("  PASS: no clicks without opens")

    # replied implies opened
    bad_replies = sends_df[sends_df["replied"] & ~sends_df["opened"]]
    if len(bad_replies): errors.append(f"  FAIL: {len(bad_replies)} replies without opens")
    else: logging.info("  PASS: no replies without opens")

    # send_date never in future
    future = (pd.to_datetime(sends_df["send_date"]).dt.date > TODAY).sum()
    if future: errors.append(f"  FAIL: {future} sends in the future")
    else: logging.info("  PASS: no future send dates")

    if errors:
        for e in errors: logging.error(e)
        raise ValueError("Fix data issues before saving.")
    logging.info("  All checks passed.")


# Summary

def print_summary(campaigns_df: pd.DataFrame, sends_df: pd.DataFrame):
    logging.info(f"\nCampaign calendar ({len(campaigns_df)} campaigns)")
    logging.info(f"  Date range: {campaigns_df['send_date'].min()} → {campaigns_df['send_date'].max()}")
    logging.info(f"\n  By type:")
    logging.info(campaigns_df["campaign_type"].value_counts().to_string())

    logging.info(f"\nSend performance by campaign type")
    merged = sends_df.merge(
        campaigns_df[["campaign_id", "campaign_type"]], on="campaign_id"
    )
    perf = merged.groupby("campaign_type").agg(
        sends       =("send_id",      "count"),
        open_rate   =("opened",       "mean"),
        click_rate  =("clicked",      "mean"),
        reply_rate  =("replied",      "mean"),
        unsub_rate  =("unsubscribed", "mean"),
    ).round(3)
    logging.info(perf.to_string())

    logging.info(f"\nTotal sends: {len(sends_df):,}")
    logging.info(f"   Overall open rate:  {sends_df['opened'].mean()*100:.1f}%")
    logging.info(f"   Overall reply rate: {sends_df['replied'].mean()*100:.1f}%")




if __name__ == "__main__":
    subs   = pd.read_csv(os.path.join(OUTPUT_DIR, "subscriptions.csv"))
    events = pd.read_csv(os.path.join(OUTPUT_DIR, "product_events.csv"))
    logging.info(f"Loaded {len(subs):,} subscriptions, {len(events):,} events.\n")

    logging.info(" Building campaign calendar")
    campaigns = build_campaign_calendar()
    logging.info(f"  Scheduled {len(campaigns)} campaigns.\n")

    logging.info(" Generating sends")
    sends_df     = build_sends(campaigns, subs, events)
    campaigns_df = pd.DataFrame(campaigns)

    logging.info(" Validating")
    validate(campaigns_df, sends_df, subs)
     
    if VERBOSE:
        logging.info(" Printing summary")
        print_summary(campaigns_df, sends_df)

    
    camp_path  = os.path.join(OUTPUT_DIR, "email_campaigns.csv")
    sends_path = os.path.join(OUTPUT_DIR, "email_sends.csv")
    campaigns_df.to_csv(camp_path,  index=False)
    sends_df.to_csv(sends_path, index=False)
    logging.info(f"\n  Saved → {camp_path}  ({len(campaigns_df)} rows)")
    logging.info(f"  Saved → {sends_path}  ({len(sends_df):,} rows)\n")