# =====================================
# CENTRALIQ - CoreVault Module
# Purpose: Connect Python to database
# and load real RBI CPI data
# =====================================

import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# ── DATABASE DETAILS ─────────────────
DB_CONFIG = {
    "host": "localhost",
    "database": "centraliq",
    "user": "postgres",
    "password": "Karan"
}

# ── ENGINE FOR PANDAS ─────────────────
def get_engine():
    url = "postgresql://postgres:Karan@localhost/centraliq"
    return create_engine(url)

# ── TEST CONNECTION ───────────────────
def test_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print("✅ Connected to database!")
        print(f"   Version: {version[0]}")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        print("\n✅ Tables in database:")
        for table in tables:
            print(f"   → {table[0]}")
        conn.close()
        print("\n✅ Connection closed cleanly\n")
    except Exception as e:
        print(f"❌ Error: {e}")

# ── LOAD CPI DATA ─────────────────────
def load_cpi_data(filepath):
    print(f"📄 Loading CPI data from: {filepath}")

    # Read file, skip the title rows at top
    df = pd.read_excel(
        filepath,
        sheet_name='CPI- Current Series',
        skiprows=8
    )

    # Drop the row with numbers 1,2,3...
    df = df.drop(index=0).reset_index(drop=True)

    # Drop unnamed first column if exists
    df = df.drop(columns=['Unnamed: 0'], errors='ignore')

    # Drop rows where Year/Month is empty
    df = df.dropna(subset=['Year/Month'])

    # Month name to month number mapping
    month_map = {
        'Apr.': 4, 'May.': 5, 'Jun.': 6,
        'Jul.': 7, 'Aug.': 8, 'Sep.': 9,
        'Oct.': 10, 'Nov.': 11, 'Dec.': 12,
        'Jan.': 1, 'Feb.': 2, 'Mar.': 3
    }

    # Build clean rows one by one
    rows = []

    for _, row in df.iterrows():
        year_str = str(row['Year/Month']).strip()

        # ✅ SKIP any row that is not a real year like "2013-14"
        # Real years are short (7 chars) and have exactly one dash
        # Headers like "CPI: Urban..." are long text — we skip them
        if len(year_str) > 10:
            continue
        if '-' not in year_str:
            continue

        # Try to read the year number safely
        try:
            start_year = int(year_str.split('-')[0])
        except:
            continue  # skip if it's not a real year number

        # Loop through each month column
        for month_col, month_num in month_map.items():
            if month_col not in df.columns:
                continue

            value = row.get(month_col)

            # Skip empty cells
            if pd.isna(value):
                continue

            # Skip non-numeric values
            try:
                value = float(value)
            except:
                continue

            # April to December = start year
            # January to March = start year + 1
            if month_num >= 4:
                actual_year = start_year
            else:
                actual_year = start_year + 1

            rows.append({
                'indicator_name': 'CPI',
                'value': value,
                'date': f"{actual_year}-{month_num:02d}-01",
                'source': 'RBI',
                'sector': 'Prices'
            })

    # Create clean dataframe
    clean_df = pd.DataFrame(rows)
    clean_df['date'] = pd.to_datetime(clean_df['date'])

    # Sort by date
    clean_df = clean_df.sort_values('date').reset_index(drop=True)

    print(f"   ✅ Converted to {len(clean_df)} monthly rows")
    print(f"\n📋 Sample data:")
    print(clean_df.head(6).to_string())

    # Save to database
    engine = get_engine()
    clean_df.to_sql(
        'macro_indicators',
        engine,
        if_exists='append',
        index=False
    )

    print(f"\n✅ Loaded {len(clean_df)} CPI records into CoreVault!")

# ── VERIFY DATA IN DATABASE ───────────
def verify_data():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT 
            indicator_name,
            COUNT(*) as total_records,
            MIN(date) as earliest,
            MAX(date) as latest,
            ROUND(AVG(value)::numeric, 2) as avg_value
        FROM macro_indicators
        GROUP BY indicator_name
    """, engine)
    print(f"\n📊 CoreVault Summary:")
    print(df.to_string())

# ── RUN ALL ───────────────────────────
test_connection()
verify_data()