# =====================================
# CENTRALIQ - MethoBot Module
# Purpose: Automated Data Quality System
# Checks all data in CoreVault for issues
# Maps to DSIM: Data Quality Framework
# =====================================

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime

# ── DATABASE CONNECTION ───────────────
def get_engine():
    return create_engine(
        "postgresql://postgres:Karan@localhost/centraliq"
    )

# ── LOAD ALL INDICATORS ───────────────
def load_all_indicators():
    """Get list of all indicators in CoreVault"""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT DISTINCT indicator_name, source
        FROM macro_indicators
        ORDER BY source, indicator_name
    """, engine)
    return df

def load_indicator_data(indicator_name):
    """Load data for one specific indicator"""
    engine = get_engine()
    df = pd.read_sql(f"""
        SELECT date, value
        FROM macro_indicators
        WHERE indicator_name = '{indicator_name}'
        ORDER BY date ASC
    """, engine)
    df['date'] = pd.to_datetime(df['date'])
    return df

# ── CHECK 1: MISSING VALUES ───────────
def check_missing_values(df, indicator_name):
    """
    Check for missing/null values in data
    
    Missing values = gaps in time series
    Very bad for forecasting models
    """
    issues = []
    
    missing_count = df['value'].isna().sum()
    missing_pct = (missing_count / len(df)) * 100
    
    if missing_count > 0:
        issues.append({
            'issue_type': 'MISSING_VALUES',
            'details': f"{missing_count} missing values ({missing_pct:.1f}% of data)",
            'severity': 'HIGH' if missing_pct > 10 else 'MEDIUM'
        })
    
    return issues, missing_count

# ── CHECK 2: OUTLIERS ─────────────────
def check_outliers(df, indicator_name):
    """
    Check for statistical outliers
    
    Method: Z-Score
    If a value is more than 3 standard deviations
    from the mean, it's flagged as outlier
    
    Z-Score = (value - mean) / std_deviation
    """
    issues = []
    outlier_dates = []
    
    if len(df) < 10:
        return issues, outlier_dates
    
    mean = df['value'].mean()
    std = df['value'].std()
    
    if std == 0:
        return issues, outlier_dates
    
    # Calculate Z-scores
    df['z_score'] = np.abs((df['value'] - mean) / std)
    outliers = df[df['z_score'] > 3]
    
    if len(outliers) > 0:
        outlier_dates = outliers['date'].tolist()
        issues.append({
            'issue_type': 'OUTLIERS',
            'details': f"{len(outliers)} outliers detected (Z-score > 3): {[str(d.date()) for d in outlier_dates[:3]]}",
            'severity': 'MEDIUM'
        })
    
    return issues, outlier_dates

# ── CHECK 3: DUPLICATES ───────────────
def check_duplicates(df, indicator_name):
    """
    Check for duplicate dates
    Same date appearing twice = data error
    """
    issues = []
    
    duplicate_dates = df[df.duplicated(subset=['date'], keep=False)]['date']
    dup_count = len(duplicate_dates.unique())
    
    if dup_count > 0:
        issues.append({
            'issue_type': 'DUPLICATES',
            'details': f"{dup_count} duplicate dates found",
            'severity': 'HIGH'
        })
    
    return issues, dup_count

# ── CHECK 4: GAPS IN TIME SERIES ─────
def check_time_gaps(df, indicator_name, source):
    """
    Check for unexpected gaps in time series
    
    Monthly data should have no month gaps
    Annual data should have no year gaps
    """
    issues = []
    
    if len(df) < 3:
        return issues
    
    df_sorted = df.sort_values('date')
    
    # Calculate gaps between consecutive dates
    gaps = df_sorted['date'].diff().dropna()
    
    # For monthly data (RBI, forecasts)
    if source in ['RBI', 'ForecastIQ']:
        # Expected gap = ~30 days
        large_gaps = gaps[gaps.dt.days > 45]
        if len(large_gaps) > 0:
            gap_dates = df_sorted['date'].iloc[large_gaps.index].tolist()
            issues.append({
                'issue_type': 'TIME_GAPS',
                'details': f"{len(large_gaps)} monthly gaps found",
                'severity': 'MEDIUM'
            })
    
    return issues

# ── CHECK 5: DATA FRESHNESS ───────────
def check_freshness(df, indicator_name, source):
    """
    Check if data is up to date
    
    RBI data should be updated monthly
    WorldBank data is annual - ok to be 1 year old
    """
    issues = []
    
    if df.empty:
        return issues
    
    latest_date = df['date'].max()
    today = pd.Timestamp.now()
    days_old = (today - latest_date).days
    
    # Set thresholds by source
    thresholds = {
        'RBI': 60,           # Should update every month
        'WorldBank': 400,    # Annual data
        'GoogleTrends': 14,  # Should be very recent
        'ForecastIQ': 60     # Forecast results
    }
    
    threshold = thresholds.get(source, 365)
    
    if days_old > threshold:
        issues.append({
            'issue_type': 'STALE_DATA',
            'details': f"Last update was {days_old} days ago (threshold: {threshold} days)",
            'severity': 'LOW'
        })
    
    return issues

# ── CALCULATE QUALITY SCORE ───────────
def calculate_quality_score(all_issues, total_records):
    """
    Calculate overall quality score 0-100
    
    Start at 100, deduct for each issue:
    HIGH severity   → -20 points
    MEDIUM severity → -10 points
    LOW severity    → -5 points
    """
    score = 100
    
    for issue in all_issues:
        severity = issue.get('severity', 'LOW')
        if severity == 'HIGH':
            score -= 20
        elif severity == 'MEDIUM':
            score -= 10
        elif severity == 'LOW':
            score -= 5
    
    return max(0, score)  # Never go below 0

# ── LOG ISSUES TO DATABASE ────────────
def log_issues(indicator_name, issues):
    """Save quality issues to data_quality_log table"""
    if not issues:
        return
    
    engine = get_engine()
    
    rows = []
    for issue in issues:
        rows.append({
            'table_name': 'macro_indicators',
            'issue_type': f"{indicator_name}_{issue['issue_type']}",
            'details': issue['details']
        })
    
    df = pd.DataFrame(rows)
    df.to_sql('data_quality_log', engine, if_exists='append', index=False)

# ── RUN FULL QUALITY CHECK ────────────
def run_quality_check(indicator_name, source):
    """Run all quality checks on one indicator"""
    df = load_indicator_data(indicator_name)
    
    if df.empty:
        return None
    
    all_issues = []
    
    # Run all 5 checks
    missing_issues, _ = check_missing_values(df, indicator_name)
    outlier_issues, _ = check_outliers(df, indicator_name)
    dup_issues, _ = check_duplicates(df, indicator_name)
    gap_issues = check_time_gaps(df, indicator_name, source)
    fresh_issues = check_freshness(df, indicator_name, source)
    
    all_issues = missing_issues + outlier_issues + dup_issues + gap_issues + fresh_issues
    
    # Calculate score
    score = calculate_quality_score(all_issues, len(df))
    
    # Log issues to database
    log_issues(indicator_name, all_issues)
    
    return {
        'indicator': indicator_name,
        'source': source,
        'records': len(df),
        'issues_found': len(all_issues),
        'quality_score': score,
        'issues': all_issues
    }

# ── GENERATE QUALITY REPORT ───────────
def generate_report(results):
    """Generate final quality report"""
    print("\n" + "="*65)
    print("📋 CENTRALIQ DATA QUALITY REPORT")
    print("="*65)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*65)
    
    report_rows = []
    
    for r in results:
        if r is None:
            continue
        
        # Quality emoji
        if r['quality_score'] >= 90:
            grade = "🟢 EXCELLENT"
        elif r['quality_score'] >= 70:
            grade = "🟡 GOOD"
        elif r['quality_score'] >= 50:
            grade = "🟠 FAIR"
        else:
            grade = "🔴 POOR"
        
        report_rows.append({
            'Indicator': r['indicator'],
            'Records': r['records'],
            'Issues': r['issues_found'],
            'Score': r['quality_score'],
            'Grade': grade
        })
        
        # Print individual issues
        if r['issues']:
            print(f"\n⚠️  {r['indicator']} ({r['source']})")
            for issue in r['issues']:
                severity_icon = "🔴" if issue['severity'] == 'HIGH' else "🟡" if issue['severity'] == 'MEDIUM' else "🔵"
                print(f"   {severity_icon} {issue['issue_type']}: {issue['details']}")
    
    # Summary table
    print("\n" + "="*65)
    print("📊 SUMMARY TABLE")
    print("="*65)
    report_df = pd.DataFrame(report_rows)
    print(report_df.to_string(index=False))
    
    # Overall stats
    avg_score = report_df['Score'].mean()
    total_issues = report_df['Issues'].sum()
    
    print(f"\n{'='*65}")
    print(f"Overall Average Quality Score: {avg_score:.1f}/100")
    print(f"Total Issues Found: {total_issues}")
    
    if avg_score >= 90:
        print("✅ CoreVault data quality is EXCELLENT!")
    elif avg_score >= 70:
        print("⚠️  CoreVault data quality is GOOD with minor issues")
    else:
        print("❌ CoreVault data quality needs attention!")
    
    return report_df

# ── RUN METHOBOT ──────────────────────
def run_methobot():
    print("="*65)
    print("🤖 CENTRALIQ MethoBot — Data Quality System Starting...")
    print("="*65)
    
    # Load all indicators
    indicators = load_all_indicators()
    print(f"\n📦 Found {len(indicators)} indicators to check\n")
    
    results = []
    
    for _, row in indicators.iterrows():
        indicator = row['indicator_name']
        source = row['source']
        
        print(f"🔍 Checking: {indicator} ({source})...")
        result = run_quality_check(indicator, source)
        
        if result:
            score = result['quality_score']
            icon = "🟢" if score >= 90 else "🟡" if score >= 70 else "🔴"
            print(f"   {icon} Score: {score}/100 | Issues: {result['issues_found']}")
            results.append(result)
    
    # Generate report
    report = generate_report(results)
    
    print(f"\n✅ MethoBot Complete!")
    print(f"📝 All issues logged to data_quality_log table")

# ── START ─────────────────────────────
run_methobot()