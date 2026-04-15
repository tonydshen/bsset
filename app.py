#!/usr/bin/env python3
"""
BSSET Web Application — Flask backend
Performance charts using yfinance data and SEC EDGAR taxonomy.

Architecture:
  GET /api/divisions                          → list of 11 SIC divisions
  GET /api/division/<code>?period=1mo         → division chart + industry list
  GET /api/industry/<div>/<major>?period=1mo  → industry chart + best/worst performers
  GET /api/stock/<ticker>?period=1mo&...      → stock chart vs all comparisons
  GET /charts/<filename>                      → serve generated PNG files

Caching:
  data_cache.json   — computed performance data (24 h TTL per key)
  charts/           — generated PNG files (24 h TTL by mtime)
"""

import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import matplotlib
matplotlib.use('Agg')   # headless — must come before pyplot import
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, send_file, send_from_directory, request, abort

from bsset import MAJOR_GROUPS, load_or_build_cache, build_tree

# ── Configuration ──────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).parent
CHARTS_DIR      = BASE_DIR / 'charts'
DATA_CACHE_PATH = BASE_DIR / 'data_cache.json'

CACHE_TTL      = 86_400       # 24 hours
BENCHMARKS     = ['^DJI', '^IXIC', '^GSPC']
BENCH_LABELS   = {'^DJI': 'Dow Jones', '^IXIC': 'NASDAQ', '^GSPC': 'S&P 500'}
VALID_PERIODS  = {'1mo', '3mo', '6mo', '1y', 'ytd'}
DEFAULT_PERIOD = '1mo'
PERIOD_LABELS  = {'1mo': '1 Month', '3mo': '3 Months', '6mo': '6 Months',
                  '1y': '1 Year', 'ytd': 'YTD'}

# Chart colour palette (dark theme)
C_BG    = '#0f172a'
C_PANEL = '#1e293b'
C_GRID  = '#334155'
C_TEXT  = '#cbd5e1'
C_ZERO  = '#475569'
C_DIV   = '#4ade80'     # green  — division avg
C_IND   = '#60a5fa'     # blue   — industry avg
C_STK   = '#c084fc'     # violet — individual stock
BENCH_CLR = {'^DJI': '#fbbf24', '^IXIC': '#22d3ee', '^GSPC': '#f87171'}

# ── Flask app + module-level globals ───────────────────────────────────────────
app = Flask(__name__)

_sec_cache:     dict = {}   # CIK str → {ticker, title, sic}
_tree:          dict = {}   # built by build_tree()
_ticker_to_cik: dict = {}   # TICKER_UPPER → cik str (reverse map)
_data_cache:    dict = {}   # computed performance data (persisted to JSON)
_lock           = threading.Lock()


def _startup() -> None:
    global _sec_cache, _tree, _ticker_to_cik, _data_cache
    CHARTS_DIR.mkdir(exist_ok=True)
    print('Loading SEC cache...')
    _sec_cache = load_or_build_cache()
    _tree = build_tree(_sec_cache)
    _ticker_to_cik = {v['ticker'].upper(): k for k, v in _sec_cache.items()}
    if DATA_CACHE_PATH.exists():
        try:
            _data_cache = json.loads(DATA_CACHE_PATH.read_text())
        except Exception:
            pass
    total = sum(sum(len(i['companies']) for i in s['industries'].values())
                for s in _tree.values())
    print(f'Ready — {total:,} companies in {len(_tree)} divisions.')


_startup()

# ── Cache utilities ────────────────────────────────────────────────────────────
def _fresh(e: dict | None) -> bool:
    return bool(e) and (time.time() - e.get('ts', 0)) < CACHE_TTL


def _cget(key: str) -> dict | None:
    with _lock:
        e = _data_cache.get(key)
    return e if _fresh(e) else None


def _cset(key: str, val: dict) -> None:
    val['ts'] = time.time()
    with _lock:
        _data_cache[key] = val
        DATA_CACHE_PATH.write_text(json.dumps(_data_cache))


def _chart_fresh(name: str) -> bool:
    p = CHARTS_DIR / name
    return p.exists() and (time.time() - p.stat().st_mtime) < CACHE_TTL

# ── Price download ─────────────────────────────────────────────────────────────
def _yf_kwargs(period: str) -> dict:
    """Return yfinance date kwargs for the given period string."""
    if period == 'ytd':
        return {'start': date(date.today().year, 1, 1).isoformat(),
                'end':   date.today().isoformat()}
    return {'period': period}


def download_closes(tickers: list[str], period: str) -> pd.DataFrame:
    """
    Download auto-adjusted close prices for a list of tickers.
    yfinance 1.x always returns a MultiIndex DataFrame; df['Close'] is a DataFrame.
    Returns a clean DataFrame (tickers as columns), dropping all-NaN columns.
    """
    if not tickers:
        return pd.DataFrame()
    try:
        df = yf.download(tickers, auto_adjust=True, progress=False,
                         threads=True, **_yf_kwargs(period))
    except Exception as exc:
        print(f'  yfinance error: {exc}')
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    closes = df['Close']                          # DataFrame in yfinance 1.x
    closes = closes.dropna(axis=1, how='all')    # drop tickers with no data
    closes = closes.ffill().dropna(how='all')    # forward-fill gaps; drop all-NaN rows
    return closes


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize each column to 100 at its first valid price."""
    return df.div(df.bfill().iloc[0]).mul(100.0)


def _group_avg(normed: pd.DataFrame) -> pd.Series:
    """Equal-weighted mean across all ticker columns."""
    return normed.mean(axis=1)


def get_benchmarks(period: str) -> pd.DataFrame:
    """Download and normalize DJIA/NASDAQ/S&P500. Cached 24 h."""
    key = f'bench_{period}'
    e = _cget(key)
    if e:
        idx = pd.to_datetime(e['dates'])
        return pd.DataFrame({t: e[t] for t in BENCHMARKS if t in e}, index=idx)

    df = download_closes(BENCHMARKS, period)
    if df.empty:
        return pd.DataFrame()
    normed = _normalize(df)
    rec = {'dates': normed.index.strftime('%Y-%m-%d').tolist()}
    for t in BENCHMARKS:
        if t in normed.columns:
            rec[t] = [round(v, 4) for v in normed[t].tolist()]
    _cset(key, rec)
    return normed

# ── Market cap ─────────────────────────────────────────────────────────────────
def _one_cap(ticker: str) -> int | None:
    try:
        v = yf.Ticker(ticker).fast_info.market_cap
        return int(v) if v else None
    except Exception:
        return None


def _many_caps(tickers: list[str]) -> dict[str, int | None]:
    """Parallel market-cap fetch (up to 8 concurrent threads)."""
    out: dict = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_one_cap, t): t for t in tickers}
        for f in as_completed(futs):
            out[futs[f]] = f.result()
    return out

# ── Division + industry computation ───────────────────────────────────────────
def get_division(code: str, period: str) -> dict:
    """
    Download ALL tickers in a division in one batch.
    Computes division avg and, per industry: avg + per-ticker total returns.
    All data cached for 24 h under key 'div_{code}_{period}'.

    Returned entry schema:
      {ts, code, dates, avg, industries: {
          '<major_str>': {name, dates, avg, total_returns, ticker_meta}
      }}
    """
    key = f'div_{code}_{period}'
    e = _cget(key)
    if e:
        return e

    sector = _tree.get(code)
    if not sector:
        return {}

    # Build ticker list with per-ticker metadata
    all_t: list[str] = []
    meta: dict[str, dict] = {}
    for major, ind in sector['industries'].items():
        for co in ind['companies']:
            t = co['ticker'].upper()
            all_t.append(t)
            meta[t] = {'title': co['title'],
                        'cik':   _ticker_to_cik.get(t, ''),
                        'major': major}

    closes = download_closes(all_t, period)
    if closes.empty:
        return {}

    normed  = _normalize(closes)
    div_avg = _group_avg(normed)

    # Per-industry averages and total returns (all computed from the same batch)
    industries: dict[str, dict] = {}
    for major, ind in sector['industries'].items():
        ind_t = [t for t in normed.columns if meta.get(t, {}).get('major') == major]
        if not ind_t:
            continue
        sub = normed[ind_t]
        ia  = _group_avg(sub)
        ret = (sub.iloc[-1] - 100.0).to_dict()
        industries[str(major)] = {
            'name':          ind['name'],
            'dates':         ia.index.strftime('%Y-%m-%d').tolist(),
            'avg':           [round(v, 4) for v in ia.tolist()],
            'total_returns': {t: round(float(r), 4) for t, r in ret.items()},
            'ticker_meta':   {t: meta[t] for t in ind_t},
        }

    entry = {
        'code':       code,
        'dates':      div_avg.index.strftime('%Y-%m-%d').tolist(),
        'avg':        [round(v, 4) for v in div_avg.tolist()],
        'industries': industries,
    }
    _cset(key, entry)
    return entry


def get_industry(div_code: str, major: int, period: str) -> dict:
    """
    Uses the division cache (calls get_division if needed).
    Fetches market caps for top/bottom 10th-percentile performers.
    Cached under 'ind_{div_code}_{major}_{period}'.

    Returned entry schema:
      {ts, name, dates, avg,
       best:  [{ticker, title, cik, return_pct, market_cap}],
       worst: [{...}]}
    """
    key = f'ind_{div_code}_{major}_{period}'
    e = _cget(key)
    if e:
        return e

    div = get_division(div_code, period)
    if not div:
        return {}

    ind_data = div['industries'].get(str(major))
    if not ind_data:
        return {}

    total_ret = pd.Series(ind_data['total_returns'])
    tmeta     = ind_data['ticker_meta']
    n         = len(total_ret)
    n_cut     = max(1, n // 10)      # 10th percentile, minimum 1

    sorted_r  = total_ret.sort_values()
    worst_t   = sorted_r.iloc[:n_cut].index.tolist()
    best_t    = sorted_r.iloc[-n_cut:].index.tolist()[::-1]  # highest return first

    cap_map   = _many_caps(list(set(best_t + worst_t)))

    def _build(tlist: list[str]) -> list[dict]:
        return [{
            'ticker':     t,
            'title':      tmeta.get(t, {}).get('title', t),
            'cik':        tmeta.get(t, {}).get('cik', ''),
            'return_pct': round(float(total_ret[t]), 2),
            'market_cap': cap_map.get(t),
        } for t in tlist]

    entry = {
        'name':  ind_data['name'],
        'dates': ind_data['dates'],
        'avg':   ind_data['avg'],
        'best':  _build(best_t),
        'worst': _build(worst_t),
    }
    _cset(key, entry)
    return entry

# ── Chart generation ───────────────────────────────────────────────────────────
def _base_fig() -> tuple[plt.Figure, plt.Axes]:
    fig, ax = plt.subplots(figsize=(11, 5.5))
    fig.patch.set_facecolor(C_BG)
    ax.set_facecolor(C_PANEL)
    ax.tick_params(colors=C_TEXT, labelsize=8)
    for sp in ax.spines.values():
        sp.set_edgecolor(C_GRID)
    ax.grid(color=C_GRID, linewidth=0.4, alpha=0.6)
    ax.axhline(100, color=C_ZERO, linewidth=0.7, linestyle='--', zorder=0)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    fig.autofmt_xdate(rotation=25)
    return fig, ax


def _finalize(fig: plt.Figure, ax: plt.Axes, title: str,
              note: str, path: Path) -> str:
    ax.set_title(title, color=C_TEXT, fontsize=12, fontweight='bold', pad=10)
    ax.set_ylabel('Normalized (100 = start of period)', color=C_TEXT, fontsize=8)
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, loc='upper left', fontsize=7.5,
                  framealpha=0.4, facecolor=C_PANEL,
                  edgecolor=C_GRID, labelcolor=C_TEXT)
    if note:
        ax.text(0.99, 0.01, note, transform=ax.transAxes,
                fontsize=7, color='#64748b', ha='right', va='bottom')
    fig.tight_layout()
    fig.savefig(str(path), dpi=150, bbox_inches='tight',
                facecolor=fig.get_facecolor())
    plt.close(fig)
    return str(path)


def _add_benchmarks(ax: plt.Axes, bench: pd.DataFrame) -> None:
    for t in BENCHMARKS:
        if t in bench.columns:
            ax.plot(bench.index, bench[t], color=BENCH_CLR[t],
                    linewidth=1.2, linestyle='--', alpha=0.9,
                    label=BENCH_LABELS[t])


def make_division_chart(code: str, period: str,
                         div_s: pd.Series, bench: pd.DataFrame) -> str:
    """Division avg vs. three benchmark indices."""
    fname = f'div_{code}_{period}.png'
    if _chart_fresh(fname):
        return str(CHARTS_DIR / fname)
    fig, ax = _base_fig()
    _add_benchmarks(ax, bench)
    div_name = _tree[code]['name']
    ax.plot(div_s.index, div_s, color=C_DIV, linewidth=2.5,
            label=f'Division {code}: {div_name}')
    title = (f'Division {code}: {div_name} — '
             f'{PERIOD_LABELS.get(period, period)} Performance')
    return _finalize(fig, ax, title, '', CHARTS_DIR / fname)


def make_industry_chart(div_code: str, major: int, period: str,
                         ind_s: pd.Series, div_s: pd.Series,
                         bench: pd.DataFrame) -> str:
    """Industry avg vs. division avg and three benchmarks."""
    fname = f'ind_{div_code}_{major}_{period}.png'
    if _chart_fresh(fname):
        return str(CHARTS_DIR / fname)
    fig, ax = _base_fig()
    _add_benchmarks(ax, bench)
    div_name = _tree[div_code]['name']
    ax.plot(div_s.index, div_s, color=C_DIV, linewidth=1.6,
            linestyle='-.', alpha=0.85,
            label=f'Division {div_code}: {div_name}')
    ind_name = MAJOR_GROUPS.get(major, f'Group {major}')
    ax.plot(ind_s.index, ind_s, color=C_IND, linewidth=2.5,
            label=f'{ind_name} Avg')
    title = (f'Industry {major:02d}xx: {ind_name} — '
             f'{PERIOD_LABELS.get(period, period)} Performance')
    return _finalize(fig, ax, title, '', CHARTS_DIR / fname)


def make_stock_chart(ticker: str, div_code: str, major: int, period: str,
                      stk_s: pd.Series, div_s: pd.Series,
                      ind_s: pd.Series, bench: pd.DataFrame) -> str:
    """Stock vs. industry avg, division avg, and three benchmarks."""
    fname = f'stk_{ticker}_{div_code}_{major}_{period}.png'
    # Stock charts always regenerate (each ticker is unique per request)
    fig, ax = _base_fig()
    _add_benchmarks(ax, bench)
    div_name = _tree.get(div_code, {}).get('name', div_code)
    ind_name = MAJOR_GROUPS.get(major, f'Group {major}')
    ax.plot(div_s.index, div_s, color=C_DIV, linewidth=1.2,
            linestyle='-.', alpha=0.75,
            label=f'Division {div_code}: {div_name}')
    ax.plot(ind_s.index, ind_s, color=C_IND, linewidth=1.6,
            linestyle='-.', alpha=0.85,
            label=f'{ind_name} Avg')
    cik      = _ticker_to_cik.get(ticker, '')
    co_title = _sec_cache.get(cik, {}).get('title', ticker) if cik else ticker
    ax.plot(stk_s.index, stk_s, color=C_STK, linewidth=2.8,
            label=f'{ticker}: {co_title[:36]}')
    title = (f'{ticker} ({co_title[:44]}) — '
             f'{PERIOD_LABELS.get(period, period)} Performance')
    return _finalize(fig, ax, title, '', CHARTS_DIR / fname)

# ── Flask routes ───────────────────────────────────────────────────────────────
@app.route('/')
def root():
    return send_file(BASE_DIR / 'index.html')


@app.route('/charts/<path:fname>')
def serve_chart(fname: str):
    p = CHARTS_DIR / fname
    if not p.exists():
        abort(404)
    return send_from_directory(str(CHARTS_DIR), fname)


@app.route('/api/divisions')
def api_divisions():
    out = []
    for code in sorted(_tree):
        sector = _tree[code]
        count  = sum(len(i['companies']) for i in sector['industries'].values())
        out.append({'code': code, 'name': sector['name'], 'count': count})
    return jsonify(out)


@app.route('/api/division/<code>')
def api_division(code: str):
    code   = code.upper()
    period = request.args.get('period', DEFAULT_PERIOD)
    if period not in VALID_PERIODS:
        return jsonify({'error': 'Invalid period'}), 400
    if code not in _tree:
        return jsonify({'error': f'Unknown division: {code}'}), 404

    div_entry = get_division(code, period)
    if not div_entry:
        return jsonify({'error': 'No market data available for this division'}), 503

    bench = get_benchmarks(period)
    div_s = pd.Series(div_entry['avg'],
                      index=pd.to_datetime(div_entry['dates']))
    chart_path = make_division_chart(code, period, div_s, bench)
    chart_url  = '/' + Path(chart_path).relative_to(BASE_DIR).as_posix()

    industries = [
        {'major': int(k), 'name': v['name'], 'count': len(v['total_returns'])}
        for k, v in sorted(div_entry['industries'].items(), key=lambda x: int(x[0]))
    ]

    return jsonify({
        'code':       code,
        'name':       _tree[code]['name'],
        'period':     period,
        'chart_url':  chart_url,
        'industries': industries,
    })


@app.route('/api/industry/<div_code>/<int:major>')
def api_industry(div_code: str, major: int):
    div_code = div_code.upper()
    period   = request.args.get('period', DEFAULT_PERIOD)
    if period not in VALID_PERIODS:
        return jsonify({'error': 'Invalid period'}), 400
    if div_code not in _tree or major not in _tree[div_code]['industries']:
        return jsonify({'error': f'Unknown industry {major} in division {div_code}'}), 404

    ind_entry = get_industry(div_code, major, period)
    if not ind_entry:
        return jsonify({'error': 'No market data available for this industry'}), 503

    div_entry = _cget(f'div_{div_code}_{period}') or get_division(div_code, period)
    bench     = get_benchmarks(period)
    ind_s     = pd.Series(ind_entry['avg'],
                          index=pd.to_datetime(ind_entry['dates']))
    div_s     = pd.Series(div_entry['avg'],
                          index=pd.to_datetime(div_entry['dates']))
    chart_path = make_industry_chart(div_code, major, period, ind_s, div_s, bench)
    chart_url  = '/' + Path(chart_path).relative_to(BASE_DIR).as_posix()

    return jsonify({
        'division':  div_code,
        'major':     major,
        'name':      ind_entry['name'],
        'period':    period,
        'chart_url': chart_url,
        'best':      ind_entry['best'],
        'worst':     ind_entry['worst'],
    })


@app.route('/api/stock/<ticker>')
def api_stock(ticker: str):
    ticker    = ticker.upper()
    period    = request.args.get('period', DEFAULT_PERIOD)
    div_code  = request.args.get('division', '').upper()
    major_str = request.args.get('industry', '')

    if period not in VALID_PERIODS:
        return jsonify({'error': 'Invalid period'}), 400
    if not div_code or not major_str:
        return jsonify({'error': 'division and industry params are required'}), 400
    try:
        major = int(major_str)
    except ValueError:
        return jsonify({'error': 'industry must be an integer'}), 400

    # Ensure division + industry are cached (navigation normally does this; fall back if not)
    div_entry = _cget(f'div_{div_code}_{period}') or get_division(div_code, period)
    ind_entry = (_cget(f'ind_{div_code}_{major}_{period}')
                 or get_industry(div_code, major, period))
    if not div_entry or not ind_entry:
        return jsonify({'error': 'Division or industry data unavailable'}), 503

    closes = download_closes([ticker], period)
    if closes.empty or ticker not in closes.columns:
        return jsonify({'error': f'No price data for {ticker}'}), 404

    stk_s = _normalize(closes)[ticker]
    div_s = pd.Series(div_entry['avg'], index=pd.to_datetime(div_entry['dates']))
    ind_s = pd.Series(ind_entry['avg'], index=pd.to_datetime(ind_entry['dates']))
    bench = get_benchmarks(period)

    try:
        chart_path = make_stock_chart(ticker, div_code, major, period,
                                       stk_s, div_s, ind_s, bench)
    except Exception as exc:
        return jsonify({'error': f'Chart generation failed: {exc}'}), 500

    chart_url = '/' + Path(chart_path).relative_to(BASE_DIR).as_posix()
    cik       = _ticker_to_cik.get(ticker, '')
    co_title  = _sec_cache.get(cik, {}).get('title', ticker) if cik else ticker
    ret_pct   = round(float(stk_s.iloc[-1]) - 100.0, 2)
    cap       = _one_cap(ticker)

    return jsonify({
        'ticker':     ticker,
        'title':      co_title,
        'cik':        cik,
        'period':     period,
        'chart_url':  chart_url,
        'return_pct': ret_pct,
        'market_cap': cap,
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
