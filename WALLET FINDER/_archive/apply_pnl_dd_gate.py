import csv
import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

def main():
    # Define paths
    base_path = Path(__file__).parent
    data_path = base_path / 'data'
    summary_path = data_path / 'summary.csv'
    wallet_universe_path = data_path / 'wallet_universe.csv'
    copyable_wallets_path = data_path / 'copyable_wallets.csv'
    wallet_portfolios_path = data_path / 'wallet_portfolios'
    pre_dd_gate_backup_path = data_path / 'pre_dd_gate_backup'
    backup_path = data_path / 'pre_pnl_dd_gate_backup'

    # Step 1: Backup current CSVs
    print("Backing up current CSVs...")
    backup_path.mkdir(parents=True, exist_ok=True)
    for file_name in ['summary.csv', 'wallet_universe.csv', 'copyable_wallets.csv']:
        src = data_path / file_name
        dst = backup_path / file_name
        if src.exists():
            shutil.copy2(src, dst)
            print(f"  Backed up {file_name}")
        else:
            print(f"  Warning: {src} not found for backup")

    # Step 2: Restore summary.csv from pre_dd_gate_backup
    print("Restoring summary.csv from pre_dd_gate_backup...")
    src_summary = pre_dd_gate_backup_path / 'summary.csv'
    dst_summary = data_path / 'summary.csv'
    if src_summary.exists():
        shutil.copy2(src_summary, dst_summary)
        print(f"  Restored {src_summary} to {dst_summary}")
    else:
        print(f"Error: Backup summary.csv not found at {src_summary}")
        return

    # Step 3: Read summary.csv and build wallet data dictionary
    print("Reading summary.csv...")
    wallet_data = {}
    with open(summary_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = row['wallet'].strip().lower()
            wallet_data[wallet] = row

    # Initialize counters and lists
    total_wallets = len(wallet_data)
    have_mtm_data = 0
    have_portfolio_fallback = 0
    no_dd_data = 0
    pass_wallets = []
    fail_details = []  # list of dicts for failed wallets

    # Define hard gate thresholds
    MIN_TRADES = 50
    MIN_TIMESPAN_HOURS = 24
    MAX_LARGEST_WIN_RATIO = 0.8
    MIN_PROFIT_FACTOR = 1.05
    MIN_TRADES_7D = 1
    MIN_PNL_DD_RATIO = 1.5

    # For ratio distribution
    ratio_ranges = {
        '1.5-2.0': 0,
        '2.0-3.0': 0,
        '3.0-5.0': 0,
        '5.0+': 0
    }

    # Process each wallet
    print("Processing wallets...")
    for wallet, row in wallet_data.items():
        # Check total_pnl
        total_pnl_str = row.get('total_pnl', '').strip()
        if not total_pnl_str:
            fail_details.append({
                'wallet': wallet,
                'reason': 'total_pnl missing',
                'pnl_dd_ratio': None,
                'dd_usd': None,
                'total_pnl': None
            })
            continue
        try:
            total_pnl = float(total_pnl_str)
        except ValueError:
            fail_details.append({
                'wallet': wallet,
                'reason': 'total_pnl invalid',
                'pnl_dd_ratio': None,
                'dd_usd': None,
                'total_pnl': total_pnl_str
            })
            continue

        if total_pnl <= 0:
            fail_details.append({
                'wallet': wallet,
                'reason': 'total_pnl <= 0',
                'pnl_dd_ratio': None,
                'dd_usd': None,
                'total_pnl': total_pnl
            })
            continue

        # Check DD source
        mtm_source = row.get('mtm_source', '').strip()
        dd_usd = None
        dd_source_used = None
        peak_val = None  # to store peak account value for DD% calculation

        if mtm_source == 'hl_portfolio_api':
            # Use allTime_max_drawdown_mtm and allTime_acctV_peak from summary
            dd_str = row.get('allTime_max_drawdown_mtm', '').strip()
            peak_str = row.get('allTime_acctV_peak', '').strip()
            if dd_str and peak_str:
                try:
                    dd_raw = float(dd_str)  # This is negative (drawdown)
                    peak_val = float(peak_str)
                    if peak_val > 0:
                        dd_usd = abs(dd_raw)  # Make positive
                        dd_source_used = 'hl_portfolio_api'
                        have_mtm_data += 1
                    else:
                        dd_usd = None
                except ValueError:
                    dd_usd = None
            else:
                dd_usd = None
        elif mtm_source == 'http_429':
            # Fallback to portfolio JSON
            json_path = wallet_portfolios_path / f"{wallet}.json"
            if json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    # Expected format: [["allTime", {"accountValueHistory": [[timestamp_ms, "value"], ...]}]]
                    if isinstance(data, list) and len(data) > 0:
                        # Find the allTime entry
                        for item in data:
                            if isinstance(item, list) and len(item) == 2 and item[0] == 'allTime':
                                account_value_history = item[1].get('accountValueHistory')
                                if isinstance(account_value_history, list) and len(account_value_history) > 0:
                                    # Calculate drawdown: track running peak, find max drawdown (negative)
                                    peak = None
                                    max_drawdown = 0  # will store the most negative value (so actually the largest drop)
                                    for point in account_value_history:
                                        if isinstance(point, list) and len(point) == 2:
                                            try:
                                                value = float(point[1])
                                            except (ValueError, TypeError):
                                                continue
                                            if peak is None or value > peak:
                                                peak = value
                                            drawdown = value - peak  # negative or zero
                                            if drawdown < max_drawdown:
                                                max_drawdown = drawdown
                                    if peak is not None and peak > 0:
                                        dd_usd = abs(max_drawdown)  # positive drawdown amount
                                        dd_source_used = 'http_429'
                                        have_portfolio_fallback += 1
                                        peak_val = peak   # store the peak
                                    else:
                                        dd_usd = None
                                break
                except (json.JSONDecodeError, KeyError, IndexError, ValueError) as e:
                    dd_usd = None
            else:
                dd_usd = None
        else:
            dd_usd = None

        if dd_usd is None:
            no_dd_data += 1
            fail_details.append({
                'wallet': wallet,
                'reason': 'No DD data',
                'pnl_dd_ratio': None,
                'dd_usd': None,
                'total_pnl': total_pnl
            })
            continue

        # Calculate ratio
        if dd_usd <= 0:
            fail_details.append({
                'wallet': wallet,
                'reason': 'dd_usd <= 0',
                'pnl_dd_ratio': None,
                'dd_usd': dd_usd,
                'total_pnl': total_pnl
            })
            continue

        ratio = total_pnl / dd_usd

        # Check hard gates (all must pass)
        fail_reasons = []

        # trades >= 50
        trades_str = row.get('trades', '').strip()
        try:
            trades = int(float(trades_str)) if trades_str else 0
        except ValueError:
            trades = 0
        if trades < MIN_TRADES:
            fail_reasons.append(f'trades < {MIN_TRADES}')

        # timespan_hours >= 24
        timespan_str = row.get('timespan_hours', '').strip()
        try:
            timespan_hours = float(timespan_str) if timespan_str else 0
        except ValueError:
            timespan_hours = 0
        if timespan_hours < MIN_TIMESPAN_HOURS:
            fail_reasons.append(f'timespan < {MIN_TIMESPAN_HOURS}h')

        # largest_win_ratio < 0.8
        largest_win_ratio_str = row.get('largest_win_ratio', '').strip()
        try:
            largest_win_ratio = float(largest_win_ratio_str) if largest_win_ratio_str else 1.0
        except ValueError:
            largest_win_ratio = 1.0
        if largest_win_ratio >= MAX_LARGEST_WIN_RATIO:
            fail_reasons.append(f'largest_win_ratio >= {MAX_LARGEST_WIN_RATIO}')

        # profit_factor >= 1.05
        profit_factor_str = row.get('profit_factor', '').strip()
        try:
            profit_factor = float(profit_factor_str) if profit_factor_str else 0
        except ValueError:
            profit_factor = 0
        if profit_factor < MIN_PROFIT_FACTOR:
            fail_reasons.append(f'profit_factor < {MIN_PROFIT_FACTOR}')

        # equity_collapse_flag == 0
        equity_collapse_flag_str = row.get('equity_collapse_flag', '').strip()
        try:
            equity_collapse_flag = int(equity_collapse_flag_str) if equity_collapse_flag_str else 1
        except ValueError:
            equity_collapse_flag = 1
        if equity_collapse_flag != 0:
            fail_reasons.append('equity_collapse_flag = 1')

        # trades_7d >= 1
        trades_7d_str = row.get('trades_7d', '').strip()
        try:
            trades_7d = int(float(trades_7d_str)) if trades_7d_str else 0
        except ValueError:
            trades_7d = 0
        if trades_7d < MIN_TRADES_7D:
            fail_reasons.append(f'trades_7d < {MIN_TRADES_7D}')

        # total_pnl > 0 (already checked) and dd_usd > 0 (already checked)
        # ratio >= 1.5
        if ratio < MIN_PNL_DD_RATIO:
            fail_reasons.append(f'PnL/DD ratio < {MIN_PNL_DD_RATIO}')

        if fail_reasons:
            fail_details.append({
                'wallet': wallet,
                'reason': '; '.join(fail_reasons),
                'pnl_dd_ratio': ratio,
                'dd_usd': dd_usd,
                'total_pnl': total_pnl
            })
        else:
            # Pass
            pass_wallets.append({
                'wallet': wallet,
                'total_pnl': total_pnl,
                'dd_usd': dd_usd,
                'ratio': ratio,
                'peak': peak_val,   # store peak for DD% calculation
                'row': row  # keep original row for output
            })
            # Categorize ratio for distribution
            if ratio < 2.0:
                ratio_ranges['1.5-2.0'] += 1
            elif ratio < 3.0:
                ratio_ranges['2.0-3.0'] += 1
            elif ratio < 5.0:
                ratio_ranges['3.0-5.0'] += 1
            else:
                ratio_ranges['5.0+'] += 1

    # Prepare data for output files
    pass_wallet_set = {item['wallet'] for item in pass_wallets}

    # Update summary.csv with passing wallets and add new columns
    print("Updating summary.csv...")
    updated_summary_rows = []
    with open(summary_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames) if reader.fieldnames is not None else []
        if fieldnames is None:
            fieldnames = []
        # Add new columns if not present
        if '_pnl_dd_ratio' not in fieldnames:
            fieldnames.append('_pnl_dd_ratio')
        if '_dd_usd' not in fieldnames:
            fieldnames.append('_dd_usd')
        if '_dd_pct' not in fieldnames:
            fieldnames.append('_dd_pct')
        for row in reader:
            wallet = row['wallet'].strip().lower()
            if wallet in pass_wallet_set:
                # Find the corresponding pass wallet data to get ratio and dd_usd
                for item in pass_wallets:
                    if item['wallet'] == wallet:
                        row['_pnl_dd_ratio'] = f"{item['ratio']:.4f}"
                        row['_dd_usd'] = f"{item['dd_usd']:.2f}"
                        peak = item.get('peak')
                        if peak and peak > 0:
                            row['_dd_pct'] = f"{item['dd_usd'] / peak * 100:.1f}"
                        else:
                            row['_dd_pct'] = ""
                        break
                updated_summary_rows.append(row)

    # Write updated summary.csv
    with open(summary_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_summary_rows)

    # Filter wallet_universe.csv
    print("Filtering wallet_universe.csv...")
    universe_rows = []
    with open(wallet_universe_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        universe_fieldnames = list(reader.fieldnames) if reader.fieldnames is not None else []
        for row in reader:
            wallet = row['wallet'].strip().lower()
            if wallet in pass_wallet_set:
                universe_rows.append(row)
    with open(wallet_universe_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=universe_fieldnames)
        writer.writeheader()
        writer.writerows(universe_rows)

    # Filter copyable_wallets.csv
    print("Filtering copyable_wallets.csv...")
    copyable_rows = []
    with open(copyable_wallets_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        copyable_fieldnames = list(reader.fieldnames) if reader.fieldnames is not None else []
        for row in reader:
            wallet = row['wallet'].strip().lower()
            if wallet in pass_wallet_set:
                copyable_rows.append(row)
    with open(copyable_wallets_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=copyable_fieldnames)
        writer.writeheader()
        writer.writerows(copyable_rows)

    # Generate sit_out_list.csv
    print("Generating sit_out_list.csv...")
    sit_out_rows = []
    today = datetime.today().date()
    sit_out_until = today + timedelta(days=90)  # approximately 3 months
    sit_out_until_str = sit_out_until.isoformat()
    added_date_str = today.isoformat()
    for fail in fail_details:
        sit_out_rows.append({
            'wallet': fail['wallet'],
            'reason': fail['reason'],
            'pnl_dd_ratio': f"{fail['pnl_dd_ratio']:.4f}" if fail['pnl_dd_ratio'] is not None else '',
            'dd_usd': f"{fail['dd_usd']:.2f}" if fail['dd_usd'] is not None else '',
            'total_pnl': f"{fail['total_pnl']:.2f}" if fail['total_pnl'] is not None else '',
            'sit_out_until': sit_out_until_str,
            'added_date': added_date_str
        })
    sit_out_fieldnames = ['wallet', 'reason', 'pnl_dd_ratio', 'dd_usd', 'total_pnl', 'sit_out_until', 'added_date']
    sit_out_path = data_path / 'sit_out_list.csv'
    with open(sit_out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=sit_out_fieldnames)
        writer.writeheader()
        writer.writerows(sit_out_rows)

    # Calculate counts for report
    pass_count = len(pass_wallets)
    fail_count = len(fail_details)
    no_dd_count = no_dd_data  # already counted in fail_details with reason 'No DD data'
    # We need to break down fail reasons
    fail_reasons_counts = {
        'PnL/DD ratio < 1.5': 0,
        'No DD data': 0,
        'trades < 50': 0,
        'timespan < 24h': 0,
        'largest_win_ratio > 0.8': 0,
        'profit_factor < 1.05': 0,
        'equity_collapse_flag = 1': 0,
        'trades_7d < 1': 0,
        'total_pnl <= 0': 0
    }
    for fail in fail_details:
        reason = fail['reason']
        if 'PnL/DD ratio < 1.5' in reason:
            fail_reasons_counts['PnL/DD ratio < 1.5'] += 1
        if 'No DD data' in reason:
            fail_reasons_counts['No DD data'] += 1
        if 'trades < 50' in reason:
            fail_reasons_counts['trades < 50'] += 1
        if 'timespan < 24h' in reason:
            fail_reasons_counts['timespan < 24h'] += 1
        if 'largest_win_ratio > 0.8' in reason:
            fail_reasons_counts['largest_win_ratio > 0.8'] += 1
        if 'profit_factor < 1.05' in reason:
            fail_reasons_counts['profit_factor < 1.05'] += 1
        if 'equity_collapse_flag = 1' in reason:
            fail_reasons_counts['equity_collapse_flag = 1'] += 1
        if 'trades_7d < 1' in reason:
            fail_reasons_counts['trades_7d < 1'] += 1
        if 'total_pnl <= 0' in reason or 'total_pnl missing' in reason or 'total_pnl invalid' in reason:
            fail_reasons_counts['total_pnl <= 0'] += 1

    # Print acceptance proof
    print("\n=== PnL/DD GATE RESULTS ===")
    print(f"Starting universe: {total_wallets} wallets")
    print(f"  - Have MTM data in summary: {have_mtm_data}")
    print(f"  - Have portfolio JSON fallback: {have_portfolio_fallback}")
    print(f"  - No DD data available: {no_dd_count} (REJECTED)")
    print()
    print("Hard gate results:")
    print(f"  PASS: {pass_count} wallets")
    print(f"  FAIL: {fail_count} wallets")
    print(f"    - PnL/DD ratio < 1.5: {fail_reasons_counts['PnL/DD ratio < 1.5']}")
    print(f"    - No DD data: {fail_reasons_counts['No DD data']}")
    print(f"    - trades < 50: {fail_reasons_counts['trades < 50']}")
    print(f"    - timespan < 24h: {fail_reasons_counts['timespan < 24h']}")
    print(f"    - largest_win_ratio > 0.8: {fail_reasons_counts['largest_win_ratio > 0.8']}")
    print(f"    - profit_factor < 1.05: {fail_reasons_counts['profit_factor < 1.05']}")
    print(f"    - equity_collapse_flag = 1: {fail_reasons_counts['equity_collapse_flag = 1']}")
    print(f"    - trades_7d < 1: {fail_reasons_counts['trades_7d < 1']}")
    print(f"    - total_pnl <= 0: {fail_reasons_counts['total_pnl <= 0']}")
    print()
    print("PnL/DD ratio distribution of survivors:")
    print(f"  1.5-2.0: {ratio_ranges['1.5-2.0']} wallets")
    print(f"  2.0-3.0: {ratio_ranges['2.0-3.0']} wallets")
    print(f"  3.0-5.0: {ratio_ranges['3.0-5.0']} wallets")
    print(f"  5.0+: {ratio_ranges['5.0+']} wallets")
    print()
    sit_out_count = len(sit_out_rows)
    print(f"Sit-out list: {sit_out_count} wallets (3-month exclusion until {sit_out_until_str})")
    print()
    print("Pipeline files updated:")
    print(f"  summary.csv: {total_wallets} → {pass_count}")
    print(f"  wallet_universe.csv: {len(universe_rows)} → {len([r for r in universe_rows if r['wallet'].lower() in pass_wallet_set])}")  # Actually we already filtered
    print(f"  copyable_wallets.csv: {len(copyable_rows)} → {len([r for r in copyable_rows if r['wallet'].lower() in pass_wallet_set])}")
    print()
    # Top 20 survivors by PnL/DD ratio
    sorted_passes = sorted(pass_wallets, key=lambda x: x['ratio'], reverse=True)
    top_20 = sorted_passes[:20]
    print("Top 20 survivors by PnL/DD ratio:")
    print("{:<10} {:>12} {:>12} {:>10} {:>10}".format('Wallet', 'Total PnL', 'DD ($)', 'DD %', 'Ratio'))
    for item in top_20:
        wallet = item['wallet']
        total_pnl = item['total_pnl']
        dd_usd = item['dd_usd']
        peak = item['peak']
        ratio = item['ratio']
        # Calculate DD%: (dd_usd / peak) * 100
        if peak is not None and peak > 0:
            dd_pct = (dd_usd / peak) * 100
            dd_pct_str = f"{dd_pct:.2f}"
        else:
            dd_pct_str = "N/A"
        print(f"{wallet:<10} {total_pnl:>12.2f} {dd_usd:>12.2f} {dd_pct_str:>10} {ratio:>10.2f}")

if __name__ == '__main__':
    main()