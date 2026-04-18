# =====================================
# CENTRALIQ - API Module
# Purpose: Serve CoreVault data to
# all other modules via web requests
# =====================================

from fastapi import FastAPI
from sqlalchemy import create_engine
import pandas as pd

# ── SETUP ─────────────────────────────
app = FastAPI(
    title="CENTRALIQ API",
    description="Macroeconomic Intelligence System for Central Banks",
    version="1.0"
)

def get_engine():
    return create_engine(
        "postgresql://postgres:Karan@localhost/centraliq"
    )

# ── ROUTES ────────────────────────────

@app.get("/")
def home():
    """Welcome message"""
    return {
        "system": "CENTRALIQ",
        "status": "🟢 Running",
        "message": "AI-Powered Macroeconomic Intelligence System",
        "modules": ["CoreVault", "DataHarvest", "ForecastIQ", "MethoBot", "StatAssist"]
    }

@app.get("/indicators")
def list_indicators():
    """Show all indicators available in CoreVault"""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT 
            indicator_name,
            COUNT(*) as total_records,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            ROUND(AVG(value)::numeric, 2) as avg_value
        FROM macro_indicators
        GROUP BY indicator_name
        ORDER BY indicator_name
    """, engine)
    return df.to_dict(orient='records')

@app.get("/data/{indicator}")
def get_indicator_data(indicator: str, limit: int = 24):
    """
    Get data for any indicator
    Example: /data/CPI?limit=12
    Returns last N months of data
    """
    engine = get_engine()
    df = pd.read_sql(f"""
        SELECT date, value, source, sector
        FROM macro_indicators
        WHERE UPPER(indicator_name) = UPPER('{indicator}')
        ORDER BY date DESC
        LIMIT {limit}
    """, engine)

    if df.empty:
        return {"error": f"No data found for {indicator}"}

    # Convert date to string for JSON
    df['date'] = df['date'].astype(str)

    return {
        "indicator": indicator.upper(),
        "total_returned": len(df),
        "data": df.to_dict(orient='records')
    }

@app.get("/data/{indicator}/latest")
def get_latest(indicator: str):
    """Get the most recent value for an indicator"""
    engine = get_engine()
    df = pd.read_sql(f"""
        SELECT date, value, source
        FROM macro_indicators
        WHERE UPPER(indicator_name) = UPPER('{indicator}')
        ORDER BY date DESC
        LIMIT 1
    """, engine)

    if df.empty:
        return {"error": f"No data found for {indicator}"}

    df['date'] = df['date'].astype(str)
    row = df.iloc[0]

    return {
        "indicator": indicator.upper(),
        "latest_date": row['date'],
        "latest_value": row['value'],
        "source": row['source']
    }

@app.get("/summary")
def database_summary():
    """Full summary of what's in CoreVault"""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT COUNT(*) as total_records,
               COUNT(DISTINCT indicator_name) as indicators,
               MIN(date) as oldest_date,
               MAX(date) as newest_date
        FROM macro_indicators
    """, engine)
    df['oldest_date'] = df['oldest_date'].astype(str)
    df['newest_date'] = df['newest_date'].astype(str)
    return df.to_dict(orient='records')[0]