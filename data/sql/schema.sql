-- Schema for stock data ETL pipeline

CREATE TABLE IF NOT EXISTS raw_daily_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10, 4),
    high DECIMAL(10, 4),
    low DECIMAL(10, 4),
    close DECIMAL(10, 4),
    adj_close DECIMAL(10, 4),
    volume BIGINT,
    daily_return DECIMAL(10, 6),
    weekly_return DECIMAL(10, 6),
    monthly_return DECIMAL(10, 6),
    UNIQUE(symbol, date)
);

-- Index for performance
CREATE INDEX IF NOT EXISTS idx_symbol_date ON raw_daily_prices (symbol, date);