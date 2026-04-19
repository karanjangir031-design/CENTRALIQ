import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:Karan@localhost/centraliq')

df = pd.read_sql("""
    SELECT date, value
    FROM macro_indicators
    WHERE indicator_name = 'CPI'
    AND source = 'RBI'
    ORDER BY date ASC
""", engine)

df['date'] = pd.to_datetime(df['date'])
df['date'] = df['date'].dt.strftime('%Y-%m')

print(f"Total CPI records: {len(df)}")
print(f"\nFull CPI series:")
print(df.to_string())