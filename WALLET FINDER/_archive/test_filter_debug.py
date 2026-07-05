"""Final verification: check all displayed PnLs are >= filter threshold"""
import requests
import re

r = requests.post('http://localhost:8012/filter', data={'min_pnl': '50000', 'sort': 'compound_score', 'dir': 'desc'})
html = r.text

# Extract wallet rows - for non-selected wallets, all PnL should be >= 50000
rows = re.findall(r'<tr class="wallet-row([^"]*)">(.*?)</tr>', html, re.DOTALL)
below_threshold = 0
above_threshold = 0
for cls, row_html in rows:
    is_selected = 'selected-row' in cls
    # Find the PnL cell (4th td)
    cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
    if len(cells) >= 5:
        pnl_cell = cells[4]  # PnL is 5th column (0-indexed: 4)
        pnl_match = re.search(r'>(\$[\d,.]+)<', pnl_cell)
        if pnl_match:
            pnl_str = pnl_match.group(1).replace('$', '').replace(',', '')
            try:
                pnl_val = float(pnl_str)
                if pnl_val < 50000:
                    below_threshold += 1
                    label = "SELECTED" if is_selected else "BUG"
                    print(f"  PnL {pnl_match.group(1)} < 50000 ({label})")
                else:
                    above_threshold += 1
            except:
                pass

print(f"\nAbove $50k: {above_threshold}, Below $50k: {below_threshold}")
print(f"All below-threshold rows are selected (bypass filter): {below_threshold > 0 and below_threshold == sum(1 for cls, _ in rows if 'selected-row' in cls and True)}")
