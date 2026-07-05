import re

with open('hl_live_copy_audit/app_cache/model_dashboard_last_good.html', 'r', encoding='utf-8') as f:
    html = f.read()

user_wallets = ['0xd405f0', '0x9db82c', '0x82d7eb', '0xf83858', '0x811e8f']

for w in user_wallets:
    pattern = 'data-sort="(' + w + r'[a-f0-9]*)"'
    matches = list(re.finditer(pattern, html))
    if not matches:
        print(f'{w}: NOT FOUND in dashboard')
        continue

    for m in matches:
        full = m.group(1)
        tr_start = html.rfind('<tr', 0, m.start())
        tr_end = html.find('</tr>', m.start())
        if tr_start < 0 or tr_end < 0:
            continue
        row_html = html[tr_start:tr_end+5]
        if 'sticky-wallet' not in row_html:
            continue

        # Extract all td contents with data-sort
        tds = re.findall(r'<td[^>]*data-sort="([^"]+)"[^>]*>(.*?)</td>', row_html)
        # Filter out the wallet address td
        tds = [(v, re.sub(r'<[^>]+>', '', txt).strip()) for v, txt in tds if not v.startswith('0x')]

        print(f'\n{"="*70}')
        print(f'  {w} ({full[:10]}...)')
        print(f'  Mode: FIXED $12 | norm_base: 1000 | fee_bps: 0 | friction_bps: 5')
        print(f'{"="*70}')

        cols = ['LEAD_EQ', 'COPY_EQ', 'LEAD_REAL', 'COPY_REAL', 'LEAD_UNREAL', 'COPY_UNREAL',
                'LEAD_DD', 'COPY_DD', 'LEAD_MAXDD', 'COPY_MAXDD', 'DELTA', 'PNL/HR',
                'AVG_TRADE%', 'WIN%', 'AVG_POS$', 'MAX_POS$', 'AVG_NOTIONAL', 'PCT_GE10',
                'REQ_LEV', 'FILLS', 'EXITS', 'POS']

        for i, (val, txt) in enumerate(tds):
            if i < len(cols):
                try:
                    v = float(val)
                    if cols[i] in ('LEAD_EQ','COPY_EQ'):
                        print(f'  {cols[i]:>12}: ${v:>10,.2f}  (display: {txt})')
                    elif cols[i] in ('LEAD_REAL','COPY_REAL','LEAD_UNREAL','COPY_UNREAL',
                                     'AVG_POS$','MAX_POS$','AVG_NOTIONAL'):
                        print(f'  {cols[i]:>12}: ${v:>10,.2f}  (display: {txt})')
                    elif cols[i] in ('LEAD_DD','COPY_DD','LEAD_MAXDD','COPY_MAXDD','DELTA'):
                        print(f'  {cols[i]:>12}: ${v:>10,.2f}  (display: {txt})')
                    elif cols[i] in ('PNL/HR',):
                        print(f'  {cols[i]:>12}: ${v:>10,.2f}/hr')
                    elif cols[i] in ('WIN%','AVG_TRADE%','PCT_GE10'):
                        print(f'  {cols[i]:>12}: {v:>10.1f}%')
                    elif cols[i] in ('REQ_LEV',):
                        print(f'  {cols[i]:>12}: {v:>10.0f}x')
                    elif cols[i] in ('FILLS','EXITS','POS'):
                        print(f'  {cols[i]:>12}: L/C = {txt}')
                    else:
                        print(f'  {cols[i]:>12}: {val} ({txt})')
                except:
                    print(f'  {cols[i]:>12}: {val} ({txt})')
        break
