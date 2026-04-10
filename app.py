#!/usr/bin/env python3
"""BSSET Flask web app — serves SEC sector/industry/ticker data via REST API."""

from flask import Flask, jsonify, render_template
import bsset

app = Flask(__name__)

# Load cache and build tree once at startup
print('Loading SEC cache...')
_cache = bsset.load_or_build_cache()
_tree  = bsset.build_tree(_cache)
print(f'Ready — {len(_cache)} companies across {len(_tree)} sectors.')


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/sectors')
def api_sectors():
    result = []
    for sc in sorted(_tree.keys()):
        sector = _tree[sc]
        total  = sum(len(v['companies']) for v in sector['industries'].values())
        result.append({'code': sc, 'name': sector['name'], 'count': total})
    return jsonify(result)


@app.route('/api/industries/<sector_code>')
def api_industries(sector_code):
    sector = _tree.get(sector_code.upper())
    if not sector:
        return jsonify([])
    result = []
    for major in sorted(sector['industries'].keys()):
        ind = sector['industries'][major]
        result.append({'major': major, 'name': ind['name'], 'count': len(ind['companies'])})
    return jsonify(result)


@app.route('/api/companies/<int:major>')
def api_companies(major):
    for sector in _tree.values():
        if major in sector['industries']:
            ind = sector['industries'][major]
            cos = sorted(ind['companies'], key=lambda c: c['ticker'])
            return jsonify({'industry': ind['name'], 'companies': cos})
    return jsonify({'industry': '', 'companies': []})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
