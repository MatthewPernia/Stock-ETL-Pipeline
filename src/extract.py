import yfinance as yf
import pandas as pd

# Extract stock data from Yahoo Finance
def extract_stock_data(symbol, start_date, end_date):
    print(f'Extracting data for {symbol} from {start_date} to {end_date}...')
    df = yf.download(symbol, start = start_date, end = end_date, progress=False)
    if df is None or df.empty:
        raise ValueError(f'No data found for symbol: {symbol} in the given date range.')
    return df.reset_index()
