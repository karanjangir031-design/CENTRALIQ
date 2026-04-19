import requests
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:Karan@localhost/centraliq')
url = 'https://api.worldbank.org/v2/country/IN/indicator/NY.GDP.MKTP.CD'
params = {'format': 'json', 'per_page': 30}

try:
    response = requests.get(url, params=params, timeout=30)
    data = response.json()[1]
    rows = []
    for r in data:
        if r['value']:
            rows.append({
                'indicator_name': 'GDP',
                'value': float(r['value']),
                'date': f"{r['date']}-01-01",
                'source': 'WorldBank',
                'sector': 'Macro'
            })
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    df.to_sql('macro_indicators', engine, if_exists='append', index=False)
    print(f'✅ GDP loaded: {len(df)} records')
except Exception as e:
    print(f'❌ Error: {e}')