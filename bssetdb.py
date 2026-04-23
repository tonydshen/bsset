#!/usr/bin/env python3
"""
BSSETDB Backend – SEC EDGAR Sector / Industry / Ticker Browser (MySQL edition)

Data sources:
  https://www.sec.gov/files/company_tickers.json  – all tickers + CIKs
  https://data.sec.gov/submissions/CIK{:010d}.json – per-company SIC code

First run builds the companies table in MySQL (~20 min, rate-limited per SEC policy).
Subsequent runs load instantly from MySQL.
"""

import json
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────
HEADERS         = {'User-Agent': 'BSSET tshen@datacommlab.com'}
CACHE_PATH      = Path(__file__).parent / 'sec_cache.json'   # fallback only
TICKERS_URL     = 'https://www.sec.gov/files/company_tickers.json'
SUBMISSIONS_URL = 'https://data.sec.gov/submissions/CIK{:010d}.json'
RATE_LIMIT      = 0.12          # seconds between requests (≤10 req/s per SEC policy)

# ── SIC Divisions → Sectors ───────────────────────────────────────────────────
SECTORS = [
    ('A', 'Agriculture, Forestry & Fishing',           (100,   999)),
    ('B', 'Mining',                                     (1000, 1499)),
    ('C', 'Construction',                               (1500, 1799)),
    ('D', 'Manufacturing',                              (2000, 3999)),
    ('E', 'Transportation, Communications & Utilities', (4000, 4999)),
    ('F', 'Wholesale Trade',                            (5000, 5199)),
    ('G', 'Retail Trade',                               (5200, 5999)),
    ('H', 'Finance, Insurance & Real Estate',           (6000, 6799)),
    ('I', 'Services',                                   (7000, 8999)),
    ('J', 'Public Administration',                      (9100, 9729)),
    ('K', 'Nonclassifiable',                            (9900, 9999)),
]

# ── SIC Major Groups (2-digit) → Industry Names ───────────────────────────────
MAJOR_GROUPS = {
     1: 'Crop Production',
     2: 'Agricultural Production – Livestock',
     7: 'Agricultural Services',
     8: 'Forestry',
     9: 'Fishing, Hunting & Trapping',
    10: 'Metal Mining',
    11: 'Anthracite Mining',
    12: 'Bituminous Coal & Lignite Mining',
    13: 'Oil & Gas Extraction',
    14: 'Mining & Quarrying of Nonmetallic Minerals',
    15: 'Building Construction',
    16: 'Heavy Construction',
    17: 'Special Trade Contractors',
    20: 'Food & Kindred Products',
    21: 'Tobacco Products',
    22: 'Textile Mill Products',
    23: 'Apparel & Other Textile Products',
    24: 'Lumber & Wood Products',
    25: 'Furniture & Fixtures',
    26: 'Paper & Allied Products',
    27: 'Printing & Publishing',
    28: 'Chemical & Allied Products',
    29: 'Petroleum & Coal Products',
    30: 'Rubber & Misc. Plastics',
    31: 'Leather & Leather Products',
    32: 'Stone, Clay & Glass Products',
    33: 'Primary Metal Industries',
    34: 'Fabricated Metal Products',
    35: 'Industrial Machinery & Equipment',
    36: 'Electronic & Other Electric Equipment',
    37: 'Transportation Equipment',
    38: 'Instruments & Related Products',
    39: 'Misc. Manufacturing',
    40: 'Railroad Transportation',
    41: 'Local & Suburban Transit',
    42: 'Trucking & Warehousing',
    44: 'Water Transportation',
    45: 'Air Transportation',
    46: 'Pipelines (Except Natural Gas)',
    47: 'Transportation Services',
    48: 'Communications',
    49: 'Electric, Gas & Sanitary Services',
    50: 'Wholesale – Durable Goods',
    51: 'Wholesale – Nondurable Goods',
    52: 'Building Materials & Garden Supplies',
    53: 'General Merchandise Stores',
    54: 'Food Stores',
    55: 'Auto Dealers & Service Stations',
    56: 'Apparel & Accessory Stores',
    57: 'Furniture & Home Furnishings Stores',
    58: 'Eating & Drinking Places',
    59: 'Misc. Retail',
    60: 'Depository Institutions',
    61: 'Nondepository Credit Institutions',
    62: 'Security & Commodity Brokers',
    63: 'Insurance Carriers',
    64: 'Insurance Agents, Brokers & Service',
    65: 'Real Estate',
    67: 'Holding & Other Investment Offices',
    70: 'Hotels & Other Lodging Places',
    72: 'Personal Services',
    73: 'Business Services',
    75: 'Auto Repair, Services & Parking',
    76: 'Misc. Repair Services',
    78: 'Motion Pictures',
    79: 'Amusement & Recreation Services',
    80: 'Health Services',
    81: 'Legal Services',
    82: 'Educational Services',
    83: 'Social Services',
    84: 'Museums, Art Galleries & Botanical',
    86: 'Membership Organizations',
    87: 'Engineering & Management Services',
    89: 'Services, NEC',
    91: 'Executive, Legislative & General Government',
    92: 'Justice, Public Order & Safety',
    93: 'Finance, Taxation & Monetary Policy',
    94: 'Administration of Human Resource Programs',
    95: 'Environmental Quality & Housing Programs',
    96: 'Administration of Economic Programs',
    97: 'National Security & International Affairs',
    99: 'Nonclassifiable Establishments',
}


# ── Network ───────────────────────────────────────────────────────────────────
def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


# ── SIC Lookups ───────────────────────────────────────────────────────────────
def sic_to_sector(sic: int) -> tuple:
    for code, name, (lo, hi) in SECTORS:
        if lo <= sic <= hi:
            return code, name
    return 'K', 'Nonclassifiable'


def sic_to_industry(sic: int) -> str:
    major = sic // 100
    return MAJOR_GROUPS.get(major, f'SIC Group {major:02d}xx')


# ── Cache / DB ────────────────────────────────────────────────────────────────
def load_or_build_cache() -> dict:
    try:
        import db
        if db.companies_count() > 0:
            print('Loading companies from MySQL ...', end=' ', flush=True)
            cache = db.companies_load()
            print(f'{len(cache)} companies.')
            return cache
    except Exception as e:
        print(f'MySQL unavailable ({e}), falling back to file cache.')
    if CACHE_PATH.exists():
        print(f'Loading cache from {CACHE_PATH.name} ...', end=' ', flush=True)
        cache = json.loads(CACHE_PATH.read_text())
        print(f'{len(cache)} companies.')
        try:
            import db
            print('Seeding MySQL companies table ...', end=' ', flush=True)
            rows = []
            for cik, info in cache.items():
                sic = info['sic']
                div_code, _ = sic_to_sector(sic)
                rows.append((str(cik), info['ticker'], info['title'], sic, div_code, sic // 100))
            db.companies_save(rows)
            print(f'{len(rows)} rows saved.')
        except Exception as e:
            print(f'Could not seed MySQL: {e}')
        return cache
    return _build_cache()


def _build_cache() -> dict:
    print('Fetching company list from SEC ...')
    raw       = fetch_json(TICKERS_URL)
    companies = list(raw.values())
    total     = len(companies)
    print(f'  {total} tickers found.')
    print('  Fetching SIC codes from data.sec.gov (runs once; ~20 min).')
    print('  Progress is saved every 100 companies — safe to interrupt.\n')

    cache: dict = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text())

    fetched = 0
    for i, co in enumerate(companies):
        cik = int(co['cik_str'])
        key = str(cik)
        if key in cache:
            continue

        try:
            sub = fetch_json(SUBMISSIONS_URL.format(cik))
            sic = int(sub.get('sic') or 9999)
        except Exception:
            sic = 9999

        cache[key] = {
            'ticker': co['ticker'],
            'title':  co['title'],
            'sic':    sic,
        }
        fetched += 1

        if fetched % 100 == 0:
            CACHE_PATH.write_text(json.dumps(cache))
            pct = (i + 1) / total * 100
            print(f'  {i+1:>5}/{total}  ({pct:4.1f}%)  — checkpoint saved', end='\r')

        time.sleep(RATE_LIMIT)

    CACHE_PATH.write_text(json.dumps(cache))
    print(f'\nDone. {len(cache)} companies cached to {CACHE_PATH.name}.')

    try:
        import db
        print('Saving companies to MySQL ...', end=' ', flush=True)
        rows = []
        for cik, info in cache.items():
            sic = info['sic']
            div_code, _ = sic_to_sector(sic)
            rows.append((str(cik), info['ticker'], info['title'], sic, div_code, sic // 100))
        db.companies_save(rows)
        print(f'{len(rows)} rows saved.')
    except Exception as e:
        print(f'Could not save to MySQL: {e}')

    return cache


# ── Tree ──────────────────────────────────────────────────────────────────────
def build_tree(cache: dict) -> dict:
    """
    tree[sector_code] = {
        'name': str,
        'industries': {
            major_int: {'name': str, 'companies': [{ticker, title, sic}]}
        }
    }
    """
    tree: dict = {}
    for _cik, info in cache.items():
        sic       = info['sic']
        sc, sname = sic_to_sector(sic)
        major     = sic // 100
        iname     = sic_to_industry(sic)

        tree.setdefault(sc, {'name': sname, 'industries': {}})
        tree[sc]['industries'].setdefault(major, {'name': iname, 'companies': []})
        tree[sc]['industries'][major]['companies'].append({
            'ticker': info['ticker'],
            'title':  info['title'],
            'sic':    sic,
        })
    return tree


# ── CLI UI (unchanged from bsset.py) ─────────────────────────────────────────
def pick(items: list, prompt: str):
    print()
    for i, item in enumerate(items, 1):
        print(f'  {i:>3}. {item}')
    print('    0. Back / Quit')
    while True:
        raw = input(f'\n{prompt}: ').strip()
        if raw == '0':
            return None
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(items):
                return items[idx]
        print('  Enter a number from the list.')


def main() -> None:
    print('═' * 52)
    print('  BSSETDB — SEC Sector / Industry / Ticker Browser')
    print('═' * 52)
    print()

    cache = load_or_build_cache()
    tree  = build_tree(cache)

    while True:
        codes  = sorted(tree.keys())
        labels = [
            f'{sc}  {tree[sc]["name"]}  '
            f'({sum(len(v["companies"]) for v in tree[sc]["industries"].values())} cos)'
            for sc in codes
        ]
        print('\n── SECTORS ' + '─' * 40)
        sel1 = pick(labels, 'Select sector')
        if sel1 is None:
            print('Goodbye.')
            sys.exit(0)

        sc     = codes[labels.index(sel1)]
        sector = tree[sc]

        while True:
            ikeys   = sorted(sector['industries'].keys())
            ilabels = [
                f'{sector["industries"][k]["name"]}  '
                f'({len(sector["industries"][k]["companies"])} cos)'
                for k in ikeys
            ]
            print(f'\n── INDUSTRIES — {sector["name"]} ' + '─' * 20)
            sel2 = pick(ilabels, 'Select industry')
            if sel2 is None:
                break

            ik       = ikeys[ilabels.index(sel2)]
            industry = sector['industries'][ik]

            cos = sorted(industry['companies'], key=lambda c: c['ticker'])
            print(f'\n── TICKERS — {industry["name"]} ' + '─' * 20)
            print(f'  {"TICKER":<8}  {"SIC":<6}  COMPANY')
            print(f'  {"──────":<8}  {"───":<6}  ───────')
            for co in cos:
                print(f'  {co["ticker"]:<8}  {co["sic"]:<6}  {co["title"]}')
            print(f'\n  {len(cos)} companies listed.')
            input('\nPress Enter to continue ...')


USAGE = """\
Usage: bssetdb.py [OPTION]

Options:
  (none)           Launch interactive sector/industry/ticker browser
  --build-cache    Download SIC data for all companies and save to MySQL
  --cache-status   Show how many companies are in MySQL vs total from SEC
  --help           Show this message and exit
"""

if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) > 1 else ''

    if arg in ('-h', '--help'):
        print(USAGE)
        sys.exit(0)

    if arg == '--cache-status':
        total_raw = fetch_json(TICKERS_URL)
        total = len(total_raw)
        try:
            import db
            cached = db.companies_count()
        except Exception:
            cached = len(json.loads(CACHE_PATH.read_text())) if CACHE_PATH.exists() else 0
        pct = cached / total * 100 if total else 0
        print(f'Cached : {cached:,}')
        print(f'Total  : {total:,}')
        print(f'Coverage: {pct:.1f}%')
        sys.exit(0)

    if arg == '--build-cache':
        _build_cache()
        sys.exit(0)

    if arg:
        print(f'Unknown option: {arg}', file=sys.stderr)
        print(USAGE)
        sys.exit(1)

    main()
