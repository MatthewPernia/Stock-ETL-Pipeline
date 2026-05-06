import pandas as pd
from sqlalchemy import create_engine, text
import os

# Load transformed stock data into PostgreSQL database
def load_stock_data(df, db_config):
    print('Loading data into PostgreSQL database...')
    engine = create_engine(
        f"postgresql://{db_config['db_user']}:{db_config['db_password']}@{db_config['db_host']}:{db_config['db_port']}/{db_config['db_name']}"
    )

    # Schema validation
    required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"DataFrame missing required columns: {required_columns}")

    # Data type validation
    df['date'] = pd.to_datetime(df['date']).dt.date
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Create schema if not exists
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'sql', 'schema.sql')
    with open(schema_path, 'r') as f:
        schema_sql = f.read()

    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        conn.commit()

    # Load data with deduplication (handled by UNIQUE constraint)
    df.to_sql('raw_daily_prices', engine, if_exists='append', index=False, method='multi')

    print(f"Loaded {len(df)} records into database.")
