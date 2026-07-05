import csv
with open(r'data\pre_dd_gate_backup\summary.csv', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    print('Columns:', reader.fieldnames)
    row = next(reader)
    keys = ['wallet','total_pnl','realised_pnl','allTime_max_drawdown_mtm','allTime_acctV_peak',
            'max_drawdown','max_drawdown_mtm','mtm_calmar','trades','timespan_hrs',
            'martingale_flag','equity_collapse_flag','one_big_trade_flag','largest_win_ratio',
            'profit_factor','mtm_source','total_volume']
    for k in keys:
        v = row.get(k, 'MISSING')
        print(f'  {k}: {v}')
