#!/usr/bin/env python3
import math
import os
from datetime import date, datetime, timedelta

import mysql.connector
import pandas as pd

_DB_OPTS = {
    'option_files': os.path.expanduser('~/.my.cnf'),
    'database': 'stocks20260416',
}

def _conn():
    return mysql.connector.connect(**_DB_OPTS)

def period_start(period: str) -> date:
    today = date.today()
    if period == 'ytd':
        return date(today.year, 1, 1)
    days = {'1mo': 35, '3mo': 95, '6mo': 185, '1y': 370}
    return today - timedelta(days=days.get(period, 35))

# ── Companies ─────────────────────────────────────────────────────────────────

def companies_count() -> int:
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute("SELECT COUNT(*) FROM companies")
        return cur.fetchone()[0]
    finally:
        cn.close()

def companies_load() -> dict:
    cn = _conn()
    try:
        cur = cn.cursor(dictionary=True)
        cur.execute("SELECT cik, ticker, title, sic FROM companies")
        return {r['cik']: {'ticker': r['ticker'], 'title': r['title'], 'sic': int(r['sic'])}
                for r in cur.fetchall()}
    finally:
        cn.close()

def companies_save(rows: list) -> None:
    """rows: [(cik, ticker, title, sic, division_code, major_group), ...]"""
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.executemany("""
            INSERT INTO companies (cik, ticker, title, sic, division_code, major_group)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              ticker=VALUES(ticker), title=VALUES(title),
              sic=VALUES(sic), division_code=VALUES(division_code),
              major_group=VALUES(major_group)
        """, rows)
        cn.commit()
    finally:
        cn.close()

# ── Prices ────────────────────────────────────────────────────────────────────

def prices_fresh(tickers: list, period: str) -> bool:
    """True if price_history has sufficient recent data for all tickers and period."""
    if not tickers:
        return True
    since = period_start(period)
    cutoff = date.today() - timedelta(days=4)   # allow gap for weekends/holidays
    placeholders = ','.join(['%s'] * len(tickers))
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute(f"""
            SELECT COUNT(DISTINCT ticker), MIN(min_d), MAX(max_d)
            FROM (
                SELECT ticker,
                       MIN(price_date) AS min_d,
                       MAX(price_date) AS max_d
                FROM price_history
                WHERE ticker IN ({placeholders})
                GROUP BY ticker
            ) t
        """, tickers)
        row = cur.fetchone()
    finally:
        cn.close()
    if not row or not row[0]:
        return False
    count, oldest, newest = row
    return (count >= len(tickers) * 0.5
            and newest is not None and newest >= cutoff
            and oldest is not None and oldest <= since)

def prices_get(tickers: list, since: date) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()
    placeholders = ','.join(['%s'] * len(tickers))
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute(f"""
            SELECT ticker, price_date, close_price
            FROM price_history
            WHERE ticker IN ({placeholders}) AND price_date >= %s
            ORDER BY price_date
        """, tickers + [since])
        rows = cur.fetchall()
    finally:
        cn.close()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=['ticker', 'date', 'price'])
    df['price'] = df['price'].astype(float)
    pivot = df.pivot(index='date', columns='ticker', values='price')
    pivot.index = pd.to_datetime(pivot.index)
    pivot.columns.name = None
    return pivot

def prices_upsert(df: pd.DataFrame) -> None:
    if df.empty:
        return
    rows = []
    for dt, row in df.iterrows():
        d = dt.date() if hasattr(dt, 'date') else dt
        for ticker, price in row.items():
            try:
                p = float(price)
            except (TypeError, ValueError):
                continue
            if math.isfinite(p) and p > 0:         # reject NaN, inf, negatives
                rows.append((str(ticker), d, round(p, 4)))
    if not rows:
        return
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.executemany("""
            INSERT INTO price_history (ticker, price_date, close_price)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE close_price = VALUES(close_price)
        """, rows)
        cn.commit()
    finally:
        cn.close()

# ── Market caps ───────────────────────────────────────────────────────────────

def caps_get(tickers: list) -> dict:
    """Return cached market caps fresh within 24 h."""
    if not tickers:
        return {}
    placeholders = ','.join(['%s'] * len(tickers))
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute(f"""
            SELECT ticker, market_cap FROM market_caps
            WHERE ticker IN ({placeholders})
              AND fetched_at >= NOW() - INTERVAL 24 HOUR
        """, tickers)
        return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        cn.close()

def caps_save(caps: dict) -> None:
    if not caps:
        return
    rows = [(t, v, datetime.now()) for t, v in caps.items()]
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.executemany("""
            INSERT INTO market_caps (ticker, market_cap, fetched_at)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
              market_cap = VALUES(market_cap),
              fetched_at = VALUES(fetched_at)
        """, rows)
        cn.commit()
    finally:
        cn.close()

# ── Outliers ──────────────────────────────────────────────────────────────────

# ── Sync log ──────────────────────────────────────────────────────────────────

def sync_start(sync_type: str) -> int:
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute("""
            INSERT INTO sync_log (sync_type, started_at, status)
            VALUES (%s, %s, 'running')
        """, (sync_type, datetime.now()))
        cn.commit()
        return cur.lastrowid
    finally:
        cn.close()

def sync_complete(sync_id: int, status: str,
                  tickers: int = 0, rows: int = 0, error: str = None) -> None:
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute("""
            UPDATE sync_log
               SET completed_at=%s, status=%s,
                   tickers_processed=%s, rows_upserted=%s, error_message=%s
             WHERE id=%s
        """, (datetime.now(), status, tickers, rows, error, sync_id))
        cn.commit()
    finally:
        cn.close()

def clear_market_caps() -> None:
    """Clear market_caps so they are re-fetched fresh during daytime queries."""
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute('TRUNCATE TABLE market_caps')
        cn.commit()
    finally:
        cn.close()

def prune_old_prices(keep_years: int = 2) -> int:
    """Delete price_history rows older than keep_years. Returns rows deleted."""
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute("""
            DELETE FROM price_history
            WHERE price_date < DATE_SUB(CURDATE(), INTERVAL %s YEAR)
        """, (keep_years,))
        deleted = cur.rowcount
        cn.commit()
        return deleted
    finally:
        cn.close()

# ── Table init ────────────────────────────────────────────────────────────────

def init_tables() -> None:
    """Create tables that don't exist yet. Safe to call on every startup."""
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS outliers (
              id            INT AUTO_INCREMENT PRIMARY KEY,
              ticker        VARCHAR(20)      NOT NULL,
              title         VARCHAR(500),
              cik           VARCHAR(20),
              division_code CHAR(1)          NOT NULL,
              major_group   TINYINT UNSIGNED NOT NULL,
              period        VARCHAR(10)      NOT NULL,
              reason        ENUM('sparse_data','iqr_return') NOT NULL,
              return_pct    DECIMAL(10,4),
              coverage_pct  DECIMAL(5,2),
              iqr_lower     DECIMAL(10,4),
              iqr_upper     DECIMAL(10,4),
              market_cap    BIGINT,
              detected_at   DATETIME         NOT NULL,
              detected_date DATE             NOT NULL,
              UNIQUE KEY uq_outlier (ticker, division_code, major_group,
                                     period, reason, detected_date),
              KEY idx_ticker        (ticker),
              KEY idx_div_maj_period (division_code, major_group, period),
              KEY idx_reason        (reason),
              KEY idx_detected_date (detected_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        cn.commit()
    finally:
        cn.close()

def outliers_save(rows: list) -> None:
    """
    rows: [(ticker, title, cik, division_code, major_group, period, reason,
            return_pct, coverage_pct, iqr_lower, iqr_upper, market_cap,
            detected_at, detected_date), ...]
    Duplicate (ticker, div, major, period, reason, date) are silently ignored.
    """
    if not rows:
        return
    cn = _conn()
    try:
        cur = cn.cursor()
        cur.executemany("""
            INSERT IGNORE INTO outliers
              (ticker, title, cik, division_code, major_group, period, reason,
               return_pct, coverage_pct, iqr_lower, iqr_upper, market_cap,
               detected_at, detected_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)
        cn.commit()
    finally:
        cn.close()
