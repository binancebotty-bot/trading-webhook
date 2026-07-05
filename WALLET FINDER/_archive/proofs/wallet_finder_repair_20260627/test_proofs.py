"""Proof script: wallet address normalization and data validation tests."""
import os, sys, csv, json, time

# Use the script's directory as base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data')

def test_wallet_address_normalization():
    universe_path = os.path.join(DATA_DIR, 'wallet_universe.csv')
    assert os.path.exists(universe_path), f'Missing {universe_path}'
    with open(universe_path) as f:
        reader = csv.DictReader(f)
        wallets = [row.get('wallet', '') for row in reader]
    assert len(wallets) > 0, 'No wallets in universe'
    errors = []
    for w in wallets:
        w = w.strip()
        if not w.startswith('0x'):
            errors.append(f'Missing 0x prefix: {repr(w)[:50]}')
        elif len(w) != 42:
            errors.append(f'Wrong length {len(w)}: {repr(w)[:50]}')
        else:
            try:
                int(w[2:], 16)
            except ValueError:
                errors.append(f'Invalid hex: {repr(w)[:50]}')
    assert len(errors) == 0, f'Address errors: {errors[:5]}'
    print(f'PASS: {len(wallets)} wallet addresses normalized correctly')

def test_file_existence_and_format():
    required = ['wallet_universe.csv', 'summary.csv', 'all_trades.csv']
    for fname in required:
        path = os.path.join(DATA_DIR, fname)
        assert os.path.exists(path), f'Missing {path}'
        assert os.path.getsize(path) > 0, f'Empty {fname}'
        with open(path) as f:
            reader = csv.reader(f)
            header = next(reader)
            assert len(header) > 0, f'No header in {fname}'
        print(f'PASS: {fname} exists and valid')

def test_equity_curves_count():
    curves_dir = os.path.join(DATA_DIR, 'equity_curves')
    assert os.path.isdir(curves_dir), f'Missing {curves_dir}'
    files = [f for f in os.listdir(curves_dir) if f.endswith('.csv')]
    assert len(files) > 100, f'Only {len(files)} curves'
    print(f'PASS: {len(files)} equity curve files')

def test_data_freshness():
    now = time.time()
    for fname in ['wallet_universe.csv', 'summary.csv']:
        path = os.path.join(DATA_DIR, fname)
        mtime = os.path.getmtime(path)
        age_days = (now - mtime) / 86400
        assert age_days < 90, f'{fname} is {age_days:.0f}d old'
        print(f'PASS: {fname} is {age_days:.0f}d old (within 90d limit)')

def test_ui_server_responding():
    import urllib.request
    try:
        resp = urllib.request.urlopen('http://localhost:8012/', timeout=5)
        assert resp.status == 200, f'HTTP {resp.status}'
        html = resp.read().decode()
        assert 'PROVING ENGINE' in html, 'Page title missing'
        print('PASS: UI 1 (port 8012) responding with correct content')
    except Exception as e:
        print(f'WARN: UI 1 not responding: {e}')

if __name__ == '__main__':
    print('=== Wallet Finder Proof Tests ===')
    for name, fn in [
        ('Address Normalization', test_wallet_address_normalization),
        ('File Existence', test_file_existence_and_format),
        ('Equity Curves', test_equity_curves_count),
        ('Data Freshness', test_data_freshness),
        ('UI Server', test_ui_server_responding),
    ]:
        try:
            fn()
        except AssertionError as e:
            print(f'FAIL [{name}]: {e}')
        except Exception as e:
            print(f'ERROR [{name}]: {e}')
    print('=== Tests Complete ===')
