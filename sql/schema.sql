-- SQL schema for raw_daily_prices table
CREATE TABLE IF NOT EXISTS raw_daily_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    date_added TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (symbol, date)
);
