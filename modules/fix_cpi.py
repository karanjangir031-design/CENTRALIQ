import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:Karan@localhost/centraliq')

# Step 1 - Delete ALL existing CPI records
with engine.connect() as conn:
    conn.execute(text("DELETE FROM macro_indicators WHERE indicator_name = 'CPI'"))
    conn.commit()
print("✅ Deleted old CPI data")

# Step 2 - Reload Excel
df = pd.read_excel(
    r'C:\Users\jangi\Desktop\CENTRALIQ\data\raw\cpi.xlsx',
    sheet_name='CPI- Current Series',
    skiprows=8
)

df = df.drop(index=0).reset_index(drop=True)
df = df.drop(columns=['Unnamed: 0'], errors='ignore')
df = df.dropna(subset=['Year/Month'])

# Month mapping
month_map = {
    'Apr.': 4, 'May.': 5, 'Jun.': 6,
    'Jul.': 7, 'Aug.': 8, 'Sep.': 9,
    'Oct.': 10, 'Nov.': 11, 'Dec.': 12,
    'Jan.': 1, 'Feb.': 2, 'Mar.': 3
}

rows = []
current_section = None
combined_count = 0  # Track how many Combined sections seen

for _, row in df.iterrows():
    year_str = str(row['Year/Month']).strip()

    # Detect section headers
    if 'CPI: Combined: All India General Index' in year_str:
        combined_count += 1
        if combined_count == 1:
            # First occurrence = the one we want
            current_section = 'LOAD'
            print(f"✅ Loading section: {year_str}")
        else:
            # Second occurrence = stop loading
            current_section = 'STOP'
            print(f"⏹️ Stopping at: {year_str}")
        continue
    
    # Any other header = stop or skip
    elif any(keyword in year_str for keyword in ['CPI:', 'Rural', 'Urban', 'Notes', 'Sources', 'Food']):
        if current_section == 'LOAD':
            current_section = 'STOP'
            print(f"⏹️ Stopping at: {year_str[:50]}")
        continue

    # Only load if we're in the right section
    if current_section != 'LOAD':
        continue

    # Skip non-year rows
    if len(year_str) > 10 or '-' not in year_str:
        continue

    try:
        start_year = int(year_str.split('-')[0])
    except:
        continue

    for month_col, month_num in month_map.items():
        if month_col not in df.columns:
            continue
        value = row.get(month_col)
        if pd.isna(value):
            continue
        try:
            value = float(value)
        except:
            continue

        # Only keep values in realistic CPI range
        if value < 90 or value > 250:
            continue

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

clean_df = pd.DataFrame(rows)
clean_df['date'] = pd.to_datetime(clean_df['date'])
clean_df = clean_df.drop_duplicates(subset=['date'])
clean_df = clean_df.sort_values('date').reset_index(drop=True)

print(f"\n✅ Clean CPI rows: {len(clean_df)}")
print(f"   Value range: {clean_df['value'].min():.1f} → {clean_df['value'].max():.1f}")
print(f"\n📋 Sample:")
print(clean_df.head(8).to_string())
print(f"\n📋 Recent values:")
print(clean_df.tail(6).to_string())

# Step 3 - Save
clean_df.to_sql('macro_indicators', engine, if_exists='append', index=False)
print(f"\n✅ Saved {len(clean_df)} clean CPI records!")