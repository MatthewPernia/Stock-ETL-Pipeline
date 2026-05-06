import json
import os
import pandas as pd
from extract import extract_stock_data
from transform import transform_stock_data
from load import load_stock_data

# Run the ETL pipeline for stock data from JSON file
def run_pipeline(symbols, start_date, end_date, db_config):
    transformed_data = []

    for symbol in symbols:
        df = extract_stock_data(symbol, start_date, end_date)
        df = transform_stock_data(df, symbol)
        transformed_data.append(df)

    final_df = pd.concat(transformed_data, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=['symbol', 'date'], keep='last')
    load_stock_data(final_df, db_config)


# Load configuration from JSON file
if __name__ == '__main__':
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    print('ETL pipeline starting!')

    run_pipeline(
        symbols=config['symbols'],
        start_date=config['start_date'],
        end_date=config['end_date'],
        db_config=config
    )

    print('ETL pipeline completed successfully!')
