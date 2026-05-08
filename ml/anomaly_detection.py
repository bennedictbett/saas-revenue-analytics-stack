import json
import logging
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from datetime import datetime
from scipy import stats
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
ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR  = ROOT / 'data'
PLOTS_DIR = ROOT / 'ml' / 'models' / 'anomaly_plots'
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

TODAY     = pd.Timestamp.today().normalize()
THIS_WEEK = TODAY - pd.Timedelta(days=TODAY.weekday())

# Config 
CONFIG = {
    'zscore_window':    4,      # rolling window (weeks/months)
    'zscore_threshold': 2.0,    # flag if |z| > this
    'min_periods':      2,      # minimum periods for rolling stats
}

COLORS = {
    'normal':   '#3b82f6',
    'anomaly':  '#ef4444',
    'band':     '#93c5fd',
    'mean':     '#6b7280',
}


# 1. LOAD

def load_data() -> dict:
    log.info("Loading data...")
    data = {
        'subs':   pd.read_csv(DATA_DIR / 'subscriptions.csv',
                              parse_dates=['signup_date', 'churn_date']),
        'events': pd.read_csv(DATA_DIR / 'product_events.csv',
                              parse_dates=['event_date']),
        'sends':  pd.read_csv(DATA_DIR / 'email_sends.csv',
                              parse_dates=['send_date']),
    }
    log.info(f"  Loaded {len(data)} tables")
    return data



# 2. BUILD TIME SERIES


def build_time_series(data: dict) -> dict:
    log.info("Building time series...")

    subs   = data['subs']
    events = data['events']
    sends  = data['sends']

    # Weekly reply rate
    weekly_reply = (
        sends
        .assign(week=sends['send_date'].dt.to_period('W').dt.start_time)
        .groupby('week')
        .agg(
            total_sent  = ('send_id',  'count'),
            total_reply = ('replied',  'sum'),
        )
        .reset_index()
    )
    weekly_reply['reply_rate'] = (
        weekly_reply['total_reply'] / weekly_reply['total_sent']
    ).round(4)
    weekly_reply = weekly_reply[weekly_reply['week'] < THIS_WEEK]

    # Monthly net MRR 
    monthly_new = (
        subs
        .assign(month=subs['signup_date'].dt.to_period('M').dt.start_time)
        .groupby('month')['mrr'].sum()
        .reset_index(name='new_mrr')
    )
    churned = subs[subs['status'] == 'churned'].copy()
    monthly_churned = (
        churned
        .assign(month=churned['churn_date'].dt.to_period('M').dt.start_time)
        .groupby('month')['mrr'].sum()
        .reset_index(name='churned_mrr')
    )
    monthly_mrr = (
        monthly_new
        .merge(monthly_churned, on='month', how='left')
        .fillna(0)
    )
    monthly_mrr['net_mrr'] = monthly_mrr['new_mrr'] - monthly_mrr['churned_mrr']
    monthly_mrr = monthly_mrr[
        monthly_mrr['month'] < TODAY.to_period('M').to_timestamp()
    ]

    #  Weekly WAU 
    weekly_wau = (
        events
        .assign(week=events['event_date'].dt.to_period('W').dt.start_time)
        .groupby('week')['user_id'].nunique()
        .reset_index(name='wau')
    )
    weekly_wau = weekly_wau[weekly_wau['week'] < THIS_WEEK]

    log.info(f"  Reply rate series : {len(weekly_reply)} weeks")
    log.info(f"  Net MRR series    : {len(monthly_mrr)} months")
    log.info(f"  WAU series        : {len(weekly_wau)} weeks")

    return {
        'reply_rate': weekly_reply,
        'net_mrr':    monthly_mrr,
        'wau':        weekly_wau,
    }

# 3. DETECT ANOMALIES

def detect_anomalies(series: pd.DataFrame, date_col: str,
                     value_col: str, label: str) -> pd.DataFrame:
    """
    Applies z-score anomaly detection with past-only rolling window.

    Args:
        series    : DataFrame with time series data
        date_col  : name of the date column
        value_col : name of the metric column
        label     : human-readable metric name

    Returns:
        DataFrame with anomaly flags, z-scores, p-values
    """
    df = series.copy().sort_values(date_col).reset_index(drop=True)
    w  = CONFIG['zscore_window']
    t  = CONFIG['zscore_threshold']
    mp = CONFIG['min_periods']

    # Past-only window — shift(1) prevents current period
    # from influencing its own baseline
    df['rolling_mean'] = df[value_col].rolling(w, min_periods=mp).mean().shift(1)
    df['rolling_std']  = df[value_col].rolling(w, min_periods=mp).std().shift(1)

    df['z_score'] = (
        (df[value_col] - df['rolling_mean']) /
        df['rolling_std'].replace(0, np.nan)
    ).round(3)

    df['p_value']    = df['z_score'].apply(
        lambda z: round(2 * (1 - stats.norm.cdf(abs(z))), 4)
        if not np.isnan(z) else np.nan
    )
    df['is_anomaly'] = df['z_score'].abs() > t
    df['direction']  = np.where(
        df['z_score'] < -t, 'drop',
        np.where(df['z_score'] > t, 'spike', 'normal')
    )
    df['significant'] = df['p_value'] < 0.05

    anomalies = df[df['is_anomaly']]
    log.info(f"  {label:<20} : {len(anomalies)} anomalies detected")

    if len(anomalies):
        for _, row in anomalies.iterrows():
            sig = "significant" if row['significant'] else "not significant"
            log.info(
                f"    {str(row[date_col])[:10]}  "
                f"{value_col}={row[value_col]:.4f}  "
                f"z={row['z_score']:.2f}  "
                f"p={row['p_value']:.4f}  "
                f"({sig})"
            )

    return df


# 4. PLOT

def plot_metric(df: pd.DataFrame, date_col: str, value_col: str,
                label: str, y_format: str = 'number') -> Path:
    """
    Saves a 2-panel anomaly chart for a single metric.
    Top panel: metric + rolling band.
    Bottom panel: z-score bar chart.
    """
    fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
    fig.suptitle(f'Anomaly Detection — {label}', fontsize=14, fontweight='bold')

    t = CONFIG['zscore_threshold']

    # Top: metric + bands 
    ax = axes[0]
    ax.plot(df[date_col], df[value_col],
            color=COLORS['normal'], lw=1.8, label=label, zorder=3)
    ax.plot(df[date_col], df['rolling_mean'],
            color=COLORS['mean'], lw=1.5, linestyle='--',
            label=f'{CONFIG["zscore_window"]}w rolling mean')
    ax.fill_between(
        df[date_col],
        df['rolling_mean'] - t * df['rolling_std'],
        df['rolling_mean'] + t * df['rolling_std'],
        alpha=0.15, color=COLORS['band'],
        label=f'±{t}σ band'
    )

    drops  = df[df['direction'] == 'drop']
    spikes = df[df['direction'] == 'spike']
    if len(drops):
        ax.scatter(drops[date_col], drops[value_col],
                   color=COLORS['anomaly'], s=100, zorder=5,
                   label='Drop', marker='v')
    if len(spikes):
        ax.scatter(spikes[date_col], spikes[value_col],
                   color='#f97316', s=100, zorder=5,
                   label='Spike', marker='^')

    if y_format == 'percent':
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f'{x:.1%}')
        )
    elif y_format == 'currency':
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f'${x:,.0f}')
        )
    ax.set_ylabel(label)
    ax.legend(fontsize=9)

    # Bottom: z-score bars 
    ax = axes[1]
    bar_colors = [
        COLORS['anomaly'] if a else COLORS['normal']
        for a in df['is_anomaly']
    ]
    ax.bar(df[date_col], df['z_score'],
           color=bar_colors, alpha=0.8, width=5)
    ax.axhline( t, color=COLORS['mean'], linestyle='--',
                alpha=0.6, label=f'+{t}σ')
    ax.axhline(-t, color=COLORS['mean'], linestyle='--',
                alpha=0.6, label=f'-{t}σ')
    ax.axhline(0, color='black', lw=0.5)
    ax.set_ylabel('Z-Score')
    ax.set_xlabel('Date')
    ax.legend(fontsize=9)

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha='right')

    plt.tight_layout()
    plot_path = PLOTS_DIR / f'anomaly_{value_col}.png'
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close()
    log.info(f"  Plot saved → {plot_path}")
    return plot_path


# 5. BUILD REPORT

def build_report(results: dict) -> dict:
    """
    Builds a structured JSON report summarising all anomalies.
    """
    report = {
        'generated_at': datetime.now().isoformat(),
        'week_of':      THIS_WEEK.strftime('%Y-%m-%d'),
        'config':       CONFIG,
        'metrics':      {},
        'alerts':       [],
    }

    for metric_key, df_result in results.items():
        info        = df_result['info']
        df          = df_result['df']
        anomalies   = df[df['is_anomaly']]

        report['metrics'][metric_key] = {
            'label':           info['label'],
            'date_col':        info['date_col'],
            'value_col':       info['value_col'],
            'total_periods':   len(df),
            'anomalies_found': len(anomalies),
            'latest_value':    round(df[info['value_col']].iloc[-1], 4),
            'latest_z_score':  round(df['z_score'].iloc[-1], 3)
                               if not np.isnan(df['z_score'].iloc[-1]) else None,
            'latest_direction':df['direction'].iloc[-1],
            'latest_p_value':  round(df['p_value'].iloc[-1], 4)
                               if not np.isnan(df['p_value'].iloc[-1]) else None,
        }

        # Build alerts for significant anomalies
        sig_anomalies = anomalies[anomalies['significant'] == True]
        for _, row in sig_anomalies.iterrows():
            report['alerts'].append({
                'metric':    info['label'],
                'date':      str(row[info['date_col']])[:10],
                'value':     round(row[info['value_col']], 4),
                'z_score':   round(row['z_score'], 3),
                'p_value':   round(row['p_value'], 4),
                'direction': row['direction'],
                'severity':  'high' if abs(row['z_score']) > 3 else 'medium',
            })

    report['total_alerts'] = len(report['alerts'])
    return report


def main():
    log.info(" Anomaly Detection Pipeline ")

    # Load
    data       = load_data()
    series     = build_time_series(data)

    # Define metrics to monitor
    metrics = [
        {
            'key':       'reply_rate',
            'df':        series['reply_rate'],
            'date_col':  'week',
            'value_col': 'reply_rate',
            'label':     'Email Reply Rate',
            'y_format':  'percent',
        },
        {
            'key':       'net_mrr',
            'df':        series['net_mrr'],
            'date_col':  'month',
            'value_col': 'net_mrr',
            'label':     'Net MRR',
            'y_format':  'currency',
        },
        {
            'key':       'wau',
            'df':        series['wau'],
            'date_col':  'week',
            'value_col': 'wau',
            'label':     'Weekly Active Users',
            'y_format':  'number',
        },
    ]

    # Detect + plot
    log.info("Running detection...")
    results = {}
    for m in metrics:
        df_result = detect_anomalies(
            m['df'], m['date_col'], m['value_col'], m['label']
        )
        plot_metric(
            df_result, m['date_col'], m['value_col'],
            m['label'], m['y_format']
        )
        results[m['key']] = {
            'df':   df_result,
            'info': m,
        }

    # Report
    log.info("Building report...")
    report     = build_report(results)
    report_path = DATA_DIR / 'anomaly_report.json'
    report_path.write_text(json.dumps(report, indent=2, default=str))
    log.info(f"  Report saved → {report_path}")

    # Summary
    print("\nAnomaly Summary ")
    print(f"  Week of       : {THIS_WEEK.strftime('%B %d, %Y')}")
    print(f"  Metrics checked: {len(metrics)}")
    print(f"  Total alerts  : {report['total_alerts']}")

    if report['alerts']:
        print("\nActive Alerts ")
        for alert in report['alerts']:
            print(
                f"  [{alert['severity'].upper():<6}] "
                f"{alert['metric']:<22} "
                f"{alert['direction'].upper():<6} "
                f"z={alert['z_score']:>6.2f}  "
                f"p={alert['p_value']:.4f}"
            )
    else:
        print("\n  No significant anomalies detected this period.")

    print("\nMetric Status ")
    for key, result in results.items():
        m   = result['info']
        df  = result['df']
        row = df.iloc[-1]
        z   = row['z_score']
        z_str = f"{z:+.2f}" if not np.isnan(z) else "  N/A"
        flag  = "XX " if row['is_anomaly'] else "  "
        print(
            f"  {flag}{m['label']:<22} "
            f"latest={row[m['value_col']]:.4f}  "
            f"z={z_str}  "
            f"{row['direction']}"
        )

    log.info("Pipeline complete ")


if __name__ == '__main__':
    main()