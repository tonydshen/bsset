#!/usr/bin/env python3
"""
BSSETDB Nightly Sync
====================
Fetches 2 years of price history for every ticker from yfinance, then
truncates and reloads price_history and market_caps in MySQL.

Run nightly (cron or systemd timer). Typical duration: 15-40 min.

After loading, signals the Flask app to clear its in-process cache so
the first daytime request reads fresh data rather than stale averages.

Usage:
  venv/bin/python3 sync.py              # full sync
  venv/bin/python3 sync.py --dry-run    # count tickers, no DB writes
"""

import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

# Ensure imports resolve from this directory
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import yfinance as yf

import db
from bssetdb import load_or_build_cache, build_tree

# ── Config ────────────────────────────────────────────────────────────────────
BATCH_SIZE   = 100          # tickers per yfinance call
SYNC_PERIOD  = '2y'         # covers all query periods: 1mo 3mo 6mo 1y ytd
BATCH_PAUSE  = 5.0          # seconds between batches (polite to yfinance)
RATE_RETRIES = 3            # retries on rate-limit errors
RATE_WAIT    = 120          # seconds to wait per retry attempt
BENCHMARKS   = ['^DJI', '^IXIC', '^GSPC']
APP_URL      = 'http://127.0.0.1:5002/api/admin/reload-cache'


def log(msg: str) -> None:
    print(f'[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}', flush=True)


def fetch_batch(tickers: list) -> pd.DataFrame:
    """Download SYNC_PERIOD closes for a batch of tickers, with rate-limit retry."""
    for attempt in range(1, RATE_RETRIES + 1):
        try:
            df = yf.download(tickers, period=SYNC_PERIOD, auto_adjust=True,
                             progress=False, threads=True)
            if df.empty:
                return pd.DataFrame()
            closes = df['Close']
            if isinstance(closes, pd.Series):      # single-ticker edge case
                closes = closes.to_frame(name=tickers[0])
            return closes.dropna(axis=1, how='all').ffill()
        except Exception as e:
            if 'RateLimit' in type(e).__name__ or 'Too Many Requests' in str(e):
                wait = RATE_WAIT * attempt
                log(f'  rate limited — waiting {wait}s (attempt {attempt}/{RATE_RETRIES})')
                time.sleep(wait)
            else:
                log(f'  batch error: {e}')
                return pd.DataFrame()
    log(f'  batch failed after {RATE_RETRIES} retries')
    return pd.DataFrame()


def main(dry_run: bool = False) -> None:
    log('=== BSSETDB Nightly Sync started ===')
    if dry_run:
        log('DRY RUN — no DB writes')

    sync_id = db.sync_start('full_sync') if not dry_run else None

    try:
        # 1. Collect all tickers ───────────────────────────────────────────────
        log('Loading company list from MySQL / sec_cache...')
        cache    = load_or_build_cache()
        tree     = build_tree(cache)
        tickers  = sorted({
            co['ticker'].upper()
            for sector in tree.values()
            for ind in sector['industries'].values()
            for co in ind['companies']
        })
        tickers += BENCHMARKS
        log(f'  {len(tickers):,} tickers to fetch.')

        if dry_run:
            log('Dry run complete.')
            return

        # 2. Clear market_caps so they refresh from yfinance during daytime ─────
        log('Clearing market_caps...')
        db.clear_market_caps()

        # 3. Fetch in batches and load ─────────────────────────────────────────
        batches        = [tickers[i:i+BATCH_SIZE]
                          for i in range(0, len(tickers), BATCH_SIZE)]
        total_tickers  = 0
        total_rows     = 0

        log(f'Fetching {len(batches)} batches × {BATCH_SIZE} tickers '
            f'({SYNC_PERIOD} history each)...')

        for i, batch in enumerate(batches, 1):
            t0     = time.time()
            closes = fetch_batch(batch)
            elapsed = time.time() - t0

            if not closes.empty:
                try:
                    db.prices_upsert(closes)
                    total_tickers += closes.shape[1]
                    total_rows    += closes.shape[0] * closes.shape[1]
                    log(f'  [{i:>3}/{len(batches)}] '
                        f'{closes.shape[1]:>3}/{len(batch)} tickers loaded '
                        f'({closes.shape[0]} rows each) — {elapsed:.1f}s')
                except Exception as e:
                    log(f'  [{i:>3}/{len(batches)}] upsert error (batch skipped): {e}')
            else:
                log(f'  [{i:>3}/{len(batches)}] no data returned')

            time.sleep(BATCH_PAUSE)

        log(f'Load complete: {total_tickers:,} tickers, '
            f'~{total_rows:,} price rows inserted.')

        # 4. Prune rows older than 2 years (keeps DB lean after repeated syncs) ─
        log('Pruning price_history rows older than 2 years...')
        pruned = db.prune_old_prices()
        log(f'  {pruned:,} old rows deleted.')

        # 4. Signal app to clear in-process cache ─────────────────────────────
        try:
            req = urllib.request.Request(APP_URL, data=b'', method='POST')
            urllib.request.urlopen(req, timeout=5)
            log('App cache and chart files cleared.')
        except Exception as e:
            log(f'Cache clear skipped (app may be stopped): {e}')

        pct = total_tickers / len(tickers) * 100
        status = 'success' if pct >= 80 else 'partial'
        db.sync_complete(sync_id, status, total_tickers, total_rows)
        log(f'=== Sync finished: {status.upper()} '
            f'({total_tickers:,}/{len(tickers):,} tickers = {pct:.0f}%) ===')

    except Exception as e:
        log(f'FATAL: {e}')
        if sync_id:
            db.sync_complete(sync_id, 'error', error=str(e))
        sys.exit(1)


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    main(dry_run=dry_run)
