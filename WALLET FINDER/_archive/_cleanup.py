import os
for f in ['_add_last_trade_col.py', '_read_row.py', '_row_builder.txt']:
    if os.path.exists(f): os.remove(f); print(f'Deleted {f}')
print('Done')
