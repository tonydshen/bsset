#!/usr/bin/env python3
"""
BSSETDB Web Application — Flask backend (MySQL edition)
Performance charts using yfinance data and SEC EDGAR taxonomy.

Architecture:
  GET /api/divisions                          → list of 11 SIC divisions
  GET /api/division/<code>?period=1mo         → division chart + industry list
  GET /api/industry/<div>/<major>?period=1mo  → industry chart + best/worst performers
  GET /api/stock/<ticker>?period=1mo&...      → stock chart vs all comparisons
  GET /charts/<filename>                      → serve generated PNG files

Caching:
  MySQL price_history  — raw close prices (persistent across restarts)
  MySQL market_caps    — market caps with 24 h TTL
  In-process dict      — computed division/industry averages (24 h TTL, cleared on restart)
  charts/              — generated PNG files (24 h TTL by mtime)
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import matplotlib
matplotlib.use('Agg')   # headless — must come before pyplot import
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, send_file, send_from_directory, request, abort

import db
from bssetdb import MAJOR_GROUPS, load_or_build_cache, build_tree

# ── Configuration ──────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
CHARTS_DIR = BASE_DIR / 'charts'

CACHE_TTL      = 86_400       # 24 hours (charts + in-process memory)
BENCHMARKS     = ['^DJI', '^IXIC', '^GSPC']
BENCH_LABELS   = {'^DJI': 'Dow Jones', '^IXIC': 'NASDAQ', '^GSPC': 'S&P 500'}
VALID_PERIODS  = {'1mo', '3mo', '6mo', '1y', 'ytd'}
VALID_PCTS     = {10, 20, 30, 40, 50}
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

# In-process memory cache (computed averages; cleared on restart)
_mem_cache: dict = {}
_mem_lock        = threading.Lock()


def _startup() -> None:
    global _sec_cache, _tree, _ticker_to_cik
    CHARTS_DIR.mkdir(exist_ok=True)
    db.init_tables()
    print('Loading SEC cache...')
    _sec_cache = load_or_build_cache()
    _tree = build_tree(_sec_cache)
    _ticker_to_cik = {v['ticker'].upper(): k for k, v in _sec_cache.items()}
    total = sum(sum(len(i['companies']) for i in s['industries'].values())
                for s in _tree.values())
    print(f'Ready — {total:,} companies in {len(_tree)} divisions.')


_startup()


# ── In-process memory cache ────────────────────────────────────────────────────
def _mget(key: str):
    with _mem_lock:
        e = _mem_cache.get(key)
    if e and (time.time() - e['ts']) < CACHE_TTL:
        return e['v']
    return None


def _mset(key: str, val) -> None:
    with _mem_lock:
        _mem_cache[key] = {'v': val, 'ts': time.time()}


def _chart_fresh(name: str) -> bool:
    p = CHARTS_DIR / name
    return p.exists() and (time.time() - p.stat().st_mtime) < CACHE_TTL

# ── Price download ─────────────────────────────────────────────────────────────
def _yf_kwargs(period: str) -> dict:
    if period == 'ytd':
        return {'start': date(date.today().year, 1, 1).isoformat(),
                'end':   date.today().isoformat()}
    return {'period': period}


def download_closes(tickers: list[str], period: str) -> pd.DataFrame:
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
    closes = df['Close']
    closes = closes.dropna(axis=1, how='all')
    closes = closes.ffill().dropna(how='all')
    return closes


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    return df.div(df.bfill().iloc[0]).mul(100.0)


def _group_avg(normed: pd.DataFrame) -> pd.Series:
    return normed.mean(axis=1)

# ── Market cap ─────────────────────────────────────────────────────────────────
def _one_cap(ticker: str) -> int | None:
    try:
        v = yf.Ticker(ticker).fast_info.market_cap
        return int(v) if v else None
    except Exception:
        return None


def _many_caps(tickers: list[str]) -> dict[str, int | None]:
    cached  = db.caps_get(tickers)
    missing = [t for t in tickers if t not in cached]
    if missing:
        fetched: dict = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(_one_cap, t): t for t in missing}
            for f in as_completed(futs):
                fetched[futs[f]] = f.result()
        db.caps_save(fetched)
        cached.update(fetched)
    return cached

# ── Division + industry computation ───────────────────────────────────────────
def get_division(code: str, period: str) -> dict:
    key = f'div_{code}_{period}'
    cached = _mget(key)
    if cached:
        return cached

    sector = _tree.get(code)
    if not sector:
        return {}

    all_t: list[str] = []
    meta: dict[str, dict] = {}
    for major, ind in sector['industries'].items():
        for co in ind['companies']:
            t = co['ticker'].upper()
            all_t.append(t)
            meta[t] = {'title': co['title'],
                       'cik':   _ticker_to_cik.get(t, ''),
                       'major': major}

    # DB-only — nightly sync populates price_history; no yfinance here
    closes_raw = db.prices_get(all_t, db.period_start(period))
    if closes_raw.empty:
        return {}

    closes_raw = closes_raw.dropna(axis=1, how='all')

    # ── Layer 1: data quality filter — require ≥75% trading day coverage ──────
    max_days = closes_raw.shape[0]
    coverage = closes_raw.notna().sum()
    min_days = max(1, int(max_days * 0.75))

    sparse_t = coverage[coverage < min_days].index.tolist()
    good_t   = coverage[coverage >= min_days].index.tolist()

    if sparse_t:
        now, today = datetime.now(), date.today()
        db.outliers_save([
            (t, meta.get(t, {}).get('title', t), meta.get(t, {}).get('cik', ''),
             code, meta.get(t, {}).get('major', 99), period,
             'sparse_data', None,
             round(float(coverage[t]) / max_days * 100, 2),
             None, None, None, now, today)
            for t in sparse_t
        ])

    if not good_t:
        return {}

    closes = closes_raw[good_t].ffill().dropna(how='all')
    if closes.empty:
        return {}

    normed = _normalize(closes).dropna(axis=1, how='all')
    if normed.empty:
        return {}

    # ── Layer 2: IQR return filter — exclude extreme outliers from averages ────
    per_ret = normed.iloc[-1] - 100.0
    q1, q3  = per_ret.quantile(0.25), per_ret.quantile(0.75)
    iqr     = q3 - q1
    lower   = q1 - 3.0 * iqr
    upper   = q3 + 3.0 * iqr

    iqr_mask = (per_ret < lower) | (per_ret > upper)
    iqr_t    = per_ret[iqr_mask].index.tolist()
    clean_t  = per_ret[~iqr_mask].index.tolist()

    if iqr_t:
        now, today = datetime.now(), date.today()
        db.outliers_save([
            (t, meta.get(t, {}).get('title', t), meta.get(t, {}).get('cik', ''),
             code, meta.get(t, {}).get('major', 99), period,
             'iqr_return',
             round(float(per_ret[t]), 4), None,
             round(float(lower), 4), round(float(upper), 4),
             None, now, today)
            for t in iqr_t
        ])

    # Averages use clean tickers only; returns/meta include all good tickers
    # so performers display can still show the big winners/losers.
    normed_clean = normed[clean_t] if clean_t else normed
    div_avg      = _group_avg(normed_clean)

    industries: dict[str, dict] = {}
    for major, ind in sector['industries'].items():
        ind_all   = [t for t in normed.columns       if meta.get(t, {}).get('major') == major]
        ind_clean = [t for t in normed_clean.columns if meta.get(t, {}).get('major') == major]
        if not ind_all:
            continue

        avg_src = normed_clean[ind_clean] if ind_clean else normed[ind_all]
        ia      = _group_avg(avg_src)
        ret     = (normed[ind_all].iloc[-1] - 100.0).to_dict()

        industries[str(major)] = {
            'name':          ind['name'],
            'dates':         ia.index.strftime('%Y-%m-%d').tolist(),
            'avg':           [round(v, 4) for v in ia.tolist()],
            'total_returns': {t: round(float(r), 4)
                              for t, r in ret.items() if pd.notna(r)},
            'ticker_meta':   {t: meta[t] for t in ind_all},
        }

    entry = {
        'code':       code,
        'dates':      div_avg.index.strftime('%Y-%m-%d').tolist(),
        'avg':        [round(v, 4) for v in div_avg.tolist()],
        'industries': industries,
    }
    _mset(key, entry)
    return entry


def get_industry(div_code: str, major: int, period: str) -> dict:
    key = f'ind_{div_code}_{major}_{period}'
    cached = _mget(key)
    if cached:
        return cached

    div = get_division(div_code, period)
    if not div:
        return {}

    ind_data = div['industries'].get(str(major))
    if not ind_data:
        return {}

    total_ret = pd.Series(ind_data['total_returns'])
    tmeta     = ind_data['ticker_meta']
    n         = len(total_ret)
    n_cut     = max(1, n // 10)

    sorted_r  = total_ret.sort_values()
    worst_t   = sorted_r.iloc[:n_cut].index.tolist()
    best_t    = sorted_r.iloc[-n_cut:].index.tolist()[::-1]

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
        'name':    ind_data['name'],
        'dates':   ind_data['dates'],
        'avg':     ind_data['avg'],
        'best':    _build(best_t),
        'worst':   _build(worst_t),
        'cap_map': cap_map,
    }
    _mset(key, entry)
    return entry


def get_performers(div_code: str, major: int, period: str,
                   best_pct: float, worst_pct: float) -> dict:
    div = get_division(div_code, period)
    if not div:
        return {}

    ind_data = div['industries'].get(str(major))
    if not ind_data:
        return {}

    total_ret = pd.Series(ind_data['total_returns'])
    tmeta     = ind_data['ticker_meta']
    n         = len(total_ret)

    n_best  = max(1, int(n * best_pct))
    n_worst = max(1, int(n * worst_pct))

    sorted_r = total_ret.sort_values()
    worst_t  = sorted_r.iloc[:n_worst].index.tolist()
    best_t   = sorted_r.iloc[-n_best:].index.tolist()[::-1]

    cap_map = _many_caps(list(set(best_t + worst_t)))

    def _build(tlist: list[str]) -> list[dict]:
        return [{
            'ticker':     t,
            'title':      tmeta.get(t, {}).get('title', t),
            'cik':        tmeta.get(t, {}).get('cik', ''),
            'return_pct': round(float(total_ret[t]), 2),
            'market_cap': cap_map.get(t),
        } for t in tlist]

    return {'best': _build(best_t), 'worst': _build(worst_t)}


def get_benchmarks(period: str) -> pd.DataFrame:
    key = f'bench_{period}'
    cached = _mget(key)
    if cached is not None:
        idx = pd.to_datetime(cached['dates'])
        return pd.DataFrame({t: cached[t] for t in BENCHMARKS if t in cached}, index=idx)

    # DB-only — nightly sync populates benchmarks; no yfinance here
    df = db.prices_get(BENCHMARKS, db.period_start(period))
    if df.empty:
        return pd.DataFrame()

    df     = df.dropna(axis=1, how='all').ffill().dropna(how='all')
    normed = _normalize(df)

    rec = {'dates': normed.index.strftime('%Y-%m-%d').tolist()}
    for t in BENCHMARKS:
        if t in normed.columns:
            rec[t] = [round(v, 4) for v in normed[t].tolist()]
    _mset(key, rec)
    return normed

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
    fname = f'stk_{ticker}_{div_code}_{major}_{period}.png'
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

    div_entry = get_division(div_code, period)
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


@app.route('/api/lookup/<ticker>')
def api_lookup(ticker: str):
    ticker = ticker.strip().upper()
    cik    = _ticker_to_cik.get(ticker)
    if not cik:
        return jsonify({'error': f'Ticker "{ticker}" not found in SEC data'}), 404

    co    = _sec_cache.get(cik, {})
    sic   = co.get('sic', 0)
    major = sic // 100

    for code, sector in _tree.items():
        if major in sector['industries']:
            ind = sector['industries'][major]
            return jsonify({
                'ticker':   ticker,
                'title':    co.get('title', ticker),
                'cik':      cik,
                'division': code,
                'div_name': sector['name'],
                'major':    major,
                'ind_name': ind['name'],
            })

    return jsonify({'error': f'Industry not found for SIC {sic}'}), 404


@app.route('/api/performers/<div_code>/<int:major>')
def api_performers(div_code: str, major: int):
    div_code = div_code.upper()
    period   = request.args.get('period', DEFAULT_PERIOD)

    try:
        best_pct  = int(request.args.get('best_pct',  10))
        worst_pct = int(request.args.get('worst_pct', 10))
    except ValueError:
        return jsonify({'error': 'best_pct and worst_pct must be integers'}), 400

    if period not in VALID_PERIODS:
        return jsonify({'error': 'Invalid period'}), 400
    if best_pct not in VALID_PCTS or worst_pct not in VALID_PCTS:
        return jsonify({'error': 'Percentile must be 10, 20, 30, 40, or 50'}), 400

    result = get_performers(div_code, major, period,
                            best_pct / 100.0, worst_pct / 100.0)
    if not result:
        return jsonify({'error': 'Performer data unavailable — load the industry first'}), 503

    return jsonify(result)


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

    div_entry = get_division(div_code, period)
    ind_entry = get_industry(div_code, major, period)
    if not div_entry or not ind_entry:
        return jsonify({'error': 'Division or industry data unavailable'}), 503

    # Try DB first; fall back to yfinance for individual tickers not yet in DB
    closes = db.prices_get([ticker], db.period_start(period))
    if closes.empty or ticker not in closes.columns:
        closes = download_closes([ticker], period)
        if not closes.empty:
            db.prices_upsert(closes)
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


def _prewarm() -> None:
    """Pre-compute division caches in background so first user request is instant.
    Largest divisions (most tickers) are warmed first."""
    by_size = sorted(
        _tree.keys(),
        key=lambda c: sum(len(i['companies']) for i in _tree[c]['industries'].values()),
        reverse=True
    )
    for code in by_size:
        try:
            get_division(code, DEFAULT_PERIOD)
            print(f'Pre-warmed div {code}/{DEFAULT_PERIOD}', flush=True)
        except Exception as e:
            print(f'Pre-warm failed div {code}: {e}', flush=True)


threading.Thread(target=_prewarm, daemon=True).start()


@app.route('/api/admin/reload-cache', methods=['POST'])
def api_reload_cache():
    """Clear in-process memory cache. Called by sync.py after nightly reload."""
    if request.remote_addr != '127.0.0.1':
        abort(403)
    with _mem_lock:
        _mem_cache.clear()
    for f in CHARTS_DIR.glob('*.png'):
        f.unlink(missing_ok=True)
    return jsonify({'status': 'cache cleared'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=False, threaded=True)
