# =====================================
# CENTRALIQ - DataHarvest Engine
# Purpose: Automatically collect
# economic data from multiple sources
# =====================================

import requests
import pandas as pd
from pytrends.request import TrendReq
from sqlalchemy import create_engine
from datetime import datetime
import time

# ── DATABASE CONNECTION ───────────────
def get_engine():
    return create_engine(
        "postgresql://postgres:Karan@localhost/centraliq"
    )

# ── SOURCE 1: WORLD BANK API ──────────
def fetch_worldbank_data(indicator_code, indicator_name, country='IN'):
    """
    Fetch data from World Bank's free API
    
    indicator_code : World Bank code
                     NY.GDP.MKTP.CD = GDP
                     FP.CPI.TOTL.ZG = Inflation rate
    indicator_name : What we call it (GDP, INFLATION)
    country        : IN = India
    """
    print(f"\n🌍 Fetching {indicator_name} from World Bank...")
    
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}"
    
    params = {
        'format': 'json',
        'per_page': 30,        # last 30 years
        'mrv': 30              # most recent values
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        # World Bank returns [metadata, actual_data]
        if len(data) < 2:
            print(f"   ❌ No data returned")
            return None
            
        records = data[1]
        
        if not records:
            print(f"   ❌ Empty dataset")
            return None
        
        # Build clean rows
        rows = []
        for record in records:
            if record['value'] is None:
                continue
                
            rows.append({
                'indicator_name': indicator_name,
                'value': float(record['value']),
                'date': f"{record['date']}-01-01",
                'source': 'WorldBank',
                'sector': 'Macro'
            })
        
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        print(f"   ✅ Got {len(df)} years of {indicator_name} data")
        print(f"   Preview: {df.tail(3)[['date','value']].to_string()}")
        
        return df
        
    except Exception as e:
        print(f"   ❌ Error fetching {indicator_name}: {e}")
        return None

# ── SOURCE 2: GOOGLE TRENDS ───────────
def fetch_google_trends(keywords, timeframe='today 5-y'):
    """
    Fetch Google Trends data for economic keywords
    
    keywords  : list of search terms
    timeframe : how far back to look
    """
    print(f"\n📈 Fetching Google Trends for: {keywords}")
    
    try:
        # Initialize pytrends
        pytrends = TrendReq(hl='en-IN', tz=330)
        
        # Build payload — tell Google what we want
        pytrends.build_payload(
            keywords,
            timeframe=timeframe,
            geo='IN'           # India only
        )
        
        # Get interest over time
        df = pytrends.interest_over_time()
        
        if df.empty:
            print(f"   ❌ No trends data returned")
            return None
        
        # Remove the isPartial column
        if 'isPartial' in df.columns:
            df = df.drop(columns=['isPartial'])
        
        # Reset index so date becomes a column
        df = df.reset_index()
        
        print(f"   ✅ Got {len(df)} weeks of trends data")
        print(f"   Columns: {df.columns.tolist()}")
        print(f"   Preview:\n{df.tail(3).to_string()}")
        
        return df
        
    except Exception as e:
        print(f"   ❌ Error fetching trends: {e}")
        return None

# ── SAVE TO DATABASE ──────────────────
def save_to_corevault(df, indicator_name):
    """
    Save a dataframe to CoreVault
    Checks for duplicates before saving
    """
    if df is None or df.empty:
        print(f"   ⚠️ Nothing to save for {indicator_name}")
        return
    
    engine = get_engine()
    
    # Check how many records already exist
    existing = pd.read_sql(f"""
        SELECT COUNT(*) as count 
        FROM macro_indicators 
        WHERE indicator_name = '{indicator_name}'
    """, engine)
    
    before = existing['count'].iloc[0]
    
    # Save to database
    df.to_sql(
        'macro_indicators',
        engine,
        if_exists='append',
        index=False
    )
    
    # Check how many now
    after_count = pd.read_sql(f"""
        SELECT COUNT(*) as count 
        FROM macro_indicators 
        WHERE indicator_name = '{indicator_name}'
    """, engine)
    
    after = after_count['count'].iloc[0]
    new_records = after - before
    
    print(f"   ✅ Saved {new_records} new records for {indicator_name}")

# ── SAVE TRENDS TO DATABASE ───────────
def save_trends_to_corevault(df, keyword):
    """Save Google Trends data to database"""
    if df is None or df.empty:
        return
    
    engine = get_engine()
    rows = []
    
    for _, row in df.iterrows():
        rows.append({
            'indicator_name': f'TREND_{keyword.upper().replace(" ", "_")}',
            'value': float(row[keyword]),
            'date': pd.to_datetime(row['date']),
            'source': 'GoogleTrends',
            'sector': 'Sentiment'
        })
    
    trends_df = pd.DataFrame(rows)
    trends_df.to_sql(
        'macro_indicators',
        engine,
        if_exists='append',
        index=False
    )
    print(f"   ✅ Saved {len(trends_df)} trend records for '{keyword}'")

# ── SHOW DATABASE SUMMARY ─────────────
def show_summary():
    """Show everything now in CoreVault"""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT 
            indicator_name,
            COUNT(*) as records,
            MIN(date) as earliest,
            MAX(date) as latest,
            source
        FROM macro_indicators
        GROUP BY indicator_name, source
        ORDER BY source, indicator_name
    """, engine)
    
    print(f"\n📊 CoreVault Complete Summary:")
    print(f"{'='*65}")
    print(df.to_string())
    print(f"{'='*65}")
    print(f"Total indicators: {len(df)}")

# ── RUN DATAHARVEST ───────────────────
def run_harvest():
    print("="*50)
    print("🌾 CENTRALIQ DataHarvest Engine Starting...")
    print("="*50)
    
    # ── WORLD BANK DATA ───────────────
    print("\n📦 MODULE 1: World Bank Data")
    
    # GDP for India
    gdp_df = fetch_worldbank_data(
        'NY.GDP.MKTP.CD',
        'GDP'
    )
    save_to_corevault(gdp_df, 'GDP')
    
    # Small wait to avoid rate limiting
    time.sleep(2)
    
    # Inflation rate
    inflation_df = fetch_worldbank_data(
        'FP.CPI.TOTL.ZG',
        'INFLATION_RATE'
    )
    save_to_corevault(inflation_df, 'INFLATION_RATE')
    
    time.sleep(2)
    
    # GDP Growth Rate
    gdp_growth_df = fetch_worldbank_data(
        'NY.GDP.MKTP.KD.ZG',
        'GDP_GROWTH'
    )
    save_to_corevault(gdp_growth_df, 'GDP_GROWTH')
    
    time.sleep(2)
    
    # Unemployment rate
    unemployment_df = fetch_worldbank_data(
        'SL.UEM.TOTL.ZS',
        'UNEMPLOYMENT'
    )
    save_to_corevault(unemployment_df, 'UNEMPLOYMENT')

    # ── GOOGLE TRENDS DATA ────────────
    print("\n📦 MODULE 2: Google Trends Data")
    
    # Economic keywords Indians search
    economic_keywords = [
        'petrol price',
        'home loan',
        'inflation',
        'job vacancy'
    ]
    
    # Fetch one keyword at a time
    # (Google limits to 5 at once)
    for keyword in economic_keywords:
        trends_df = fetch_google_trends([keyword])
        if trends_df is not None:
            save_trends_to_corevault(trends_df, keyword)
        time.sleep(3)   # Wait between requests
    
    # ── FINAL SUMMARY ─────────────────
    show_summary()
    
    print("\n✅ DataHarvest Complete!")
    print("All data saved to CoreVault 🎉")

# ── START ─────────────────────────────
run_harvest()