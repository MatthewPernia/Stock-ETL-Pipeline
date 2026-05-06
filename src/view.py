import psycopg2
import pandas as pd
from pathlib import Path
import os

# Load config
config_path = Path(__file__).resolve().parents[1] / 'config' / 'config.json'
with open(config_path, 'r') as f:
    config = pd.read_json(f)

conn = psycopg2.connect(
    host=config['db_host'],
    port=config['db_port'],
    dbname=config['db_name'],
    user=config['db_user'],
    password=config['db_password']
)

# Query for data integrity check
def check_data_integrity():
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT symbol, COUNT(*) as total_records,
                   COUNT(CASE WHEN open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL THEN 1 END) as null_records,
                   COUNT(DISTINCT date) as unique_dates
            FROM raw_daily_prices
            GROUP BY symbol
            ORDER BY symbol
        """)
        return pd.DataFrame(cursor.fetchall(), columns=['symbol', 'total_records', 'null_records', 'unique_dates'])

# Query for moving averages and trends
def get_moving_averages():
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT symbol, date, close,
                   AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS 4 PRECEDING) as ma_5,
                   AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS 19 PRECEDING) as ma_20,
                   AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS 49 PRECEDING) as ma_50,
                   daily_return
            FROM raw_daily_prices
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY symbol, date
        """)
        return pd.DataFrame(cursor.fetchall(), columns=['symbol', 'date', 'close', 'ma_5', 'ma_20', 'ma_50', 'daily_return'])

# Query for daily returns analysis
def get_daily_returns_analysis():
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT symbol,
                   AVG(daily_return) as avg_daily_return,
                   STDDEV(daily_return) as volatility,
                   MIN(daily_return) as min_return,
                   MAX(daily_return) as max_return
            FROM raw_daily_prices
            WHERE daily_return IS NOT NULL
            GROUP BY symbol
            ORDER BY symbol
        """)
        return pd.DataFrame(cursor.fetchall(), columns=['symbol', 'avg_daily_return', 'volatility', 'min_return', 'max_return'])

# Sample data view
def get_sample_data():
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT symbol, date, open, high, low, close, volume,
                   ROUND(daily_return * 100, 2) as daily_return_pct
            FROM raw_daily_prices
            ORDER BY symbol, date DESC
            LIMIT 50
        """)
        return pd.DataFrame(cursor.fetchall(), columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'daily_return_pct'])

if __name__ == '__main__':
    print('Data Integrity Check:')
    print(check_data_integrity().to_string(index=False))
    print('\nMoving Averages (last 30 days):')
    print(get_moving_averages().head(20).to_string(index=False))
    print('\nDaily Returns Analysis:')
    print(get_daily_returns_analysis().to_string(index=False))
    print('\nSample Data:')
    print(get_sample_data().to_string(index=False))

conn.close()
