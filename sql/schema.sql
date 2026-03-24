-- SQLite schema for the stock ETL pipeline (run once per database)

CREATE TABLE IF NOT EXISTS raw_daily_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume INTEGER,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (symbol, date)
);
