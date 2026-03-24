"""
Minimal stock ETL: extract from Yahoo Finance -> transform -> load into SQLite.

Run from project root:
    python src/run_pipeline.py
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "stocks.db"
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

# Start with one symbol; change this when you are comfortable.
DEFAULT_SYMBOL = "AAPL"
DEFAULT_PERIOD = "3mo"


def ensure_db() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    return conn


def extract(symbol: str, period: str) -> pd.DataFrame:
    df = yf.download(symbol, period=period, progress=False, auto_adjust=False)
    if df.empty:
        raise RuntimeError(f"No rows returned for {symbol}. Check the symbol or try again later.")
    df = df.reset_index()
    # Flatten possible MultiIndex columns from yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    date_col = "date" if "date" in df.columns else "index"
    if date_col not in df.columns:
        raise RuntimeError(f"Unexpected columns: {list(df.columns)}")
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["symbol"] = symbol.upper()
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing expected columns {missing}; got {list(df.columns)}")
    out = df[cols].copy()
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")
    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    out["ingested_at"] = ingested
    return out


def _numpy_to_python(val: object) -> object:
    """SQLite stores numpy scalars as BLOB; convert to native Python types."""
    if isinstance(val, np.generic):
        return val.item()
    return val


def load(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Upsert by (symbol, date). Returns number of rows written."""
    columns = list(df.columns)
    placeholders = ",".join(["?"] * len(columns))
    col_list = ",".join(columns)
    update_cols = [c for c in columns if c not in ("symbol", "date")]
    set_clause = ",".join(f"{c}=excluded.{c}" for c in update_cols)
    sql = f"""
        INSERT INTO raw_daily_prices ({col_list})
        VALUES ({placeholders})
        ON CONFLICT(symbol, date) DO UPDATE SET {set_clause}
    """
    rows = [
        tuple(_numpy_to_python(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    cur = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return cur.rowcount if cur.rowcount >= 0 else len(df)


def main() -> None:
    conn = ensure_db()
    try:
        raw = extract(DEFAULT_SYMBOL, DEFAULT_PERIOD)
        clean = transform(raw)
        n = load(conn, clean)
        print(f"Loaded {len(clean)} rows for {DEFAULT_SYMBOL} into {DB_PATH}")
        print(f"Upsert affected rows (driver-dependent): {n}")
        sample = pd.read_sql(
            "SELECT symbol, date, close, volume FROM raw_daily_prices ORDER BY date DESC LIMIT 5",
            conn,
        )
        print("\nLatest 5 rows:")
        print(sample.to_string(index=False))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
