"""Generate candidate wallet selection pack from fresh model state."""
import json, datetime, os

ms_path = r'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\hl_copy_output\app_model_state.json'
print('Loading model state...')
with open(ms_path, 'r') as f:
    state = json.load(f)

print(f'Loaded. Keys: {list(state.keys())}')

wallets = state.get('wallet_rows', [])
print(f'Total wallets: {len(wallets)}')

user_addr = '0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205'
# Filter: exclude user aggregate, require non-zero PnL, require fill_count > 0
candidates = [w for w in wallets
              if not w.get('is_user_wallet', False)
              and w.get('wallet', '').lower() != user_addr.lower()
              and w.get('copy_total_pnl', 0) != 0
              and w.get('fill_count', 0) > 0]
print(f'Candidates (non-user, non-zero PnL, has fills): {len(candidates)}')

candidates.sort(key=lambda w: w.get('copy_total_pnl', 0), reverse=True)

top50 = candidates[:50]
bottom20 = candidates[-20:]

def wallet_summary(w):
    copy = w.get('copy', {})
    lead = w.get('lead', {})
    gate = w.get('gate', {})
    return {
        'wallet': w.get('wallet', ''),
        'copy_total_pnl': round(w.get('copy_total_pnl', 0), 2),
        'lead_total_pnl': round(w.get('lead_total_pnl', 0), 2),
        'win_rate': round(w.get('win_rate', 0), 1),
        'fill_count': w.get('fill_count', 0),
        'entry_count': w.get('entry_count', 0),
        'exit_count': w.get('exit_count', 0),
        'avg_trade_pct': round(w.get('avg_trade_pct', 0), 4),
        'avg_position_usd': round(w.get('avg_position_usd', 0), 2),
        'max_position_usd': round(w.get('max_position_usd', 0), 2),
        'current_position_usd': round(w.get('current_position_usd', 0), 2),
        'pnl_per_trade': round(w.get('pnl_per_trade', 0), 4),
        'pnl_per_hour': round(w.get('pnl_per_hour', 0), 4),
        'required_leverage': round(w.get('required_leverage', 0), 1),
        'copy_equity': round(copy.get('equity', 0), 2),
        'copy_peak_equity': round(copy.get('peak_equity', 0), 2),
        'copy_max_drawdown': round(copy.get('max_drawdown', 0), 2),
        'copy_realised': round(copy.get('realised', 0), 2),
        'copy_unrealised': round(copy.get('unrealised', 0), 2),
        'lead_equity': round(lead.get('equity', 0), 2),
        'lead_peak_equity': round(lead.get('peak_equity', 0), 2),
        'active_hours': round(w.get('active_hours', 0), 2),
        'open_position_count': w.get('open_position_count', 0),
        'gate_mode': gate.get('mode', 'unknown'),
        'include_in_portfolio': w.get('include_in_portfolio', False),
        'effective_copy_mode': w.get('effective_copy_mode', ''),
        'pct_entries_ge10': round(w.get('pct_entries_ge10', 0), 1),
        'position_alignment_ok': w.get('position_alignment_ok', False),
    }

top50_summary = [wallet_summary(w) for w in top50]
bottom20_summary = [wallet_summary(w) for w in bottom20]

all_pnls = [w.get('copy_total_pnl', 0) for w in candidates]
positive = [p for p in all_pnls if p > 0]
negative = [p for p in all_pnls if p < 0]

# Tier classification
tier1 = [w for w in top50_summary if w['copy_total_pnl'] > 500 and w['win_rate'] > 55 and w['fill_count'] > 50]
tier2 = [w for w in top50_summary if 0 < w['copy_total_pnl'] <= 500 and w['win_rate'] > 50 and w['fill_count'] > 20]
tier3 = [w for w in top50_summary if w['copy_total_pnl'] > 0 and (w['win_rate'] <= 50 or w['fill_count'] <= 20)]

# Also identify wallets currently in live config
live_config_addrs = [
    '0xd5e17c7015535d9d8e970a4d2c031117d3953834',
    '0x7ae3b08bb4e7b085c6db5d635b96bec9715e9205',
]
in_live_config = [w for w in top50_summary if w['wallet'].lower() in [a.lower() for a in live_config_addrs]]

pack = {
    'generated_at': datetime.datetime.utcnow().isoformat() + 'Z',
    'source_file': 'app_model_state.json',
    'source_updated': '2026-06-27T15:27:00Z',
    'total_wallets_in_model': len(wallets),
    'user_aggregate_address': user_addr,
    'summary': {
        'total_candidates': len(candidates),
        'positive_pnl_count': len(positive),
        'negative_pnl_count': len(negative),
        'total_copy_pnl_all': round(sum(all_pnls), 2),
        'median_pnl': round(sorted(all_pnls)[len(all_pnls) // 2], 2),
        'top_pnl': round(max(all_pnls), 2),
        'worst_pnl': round(min(all_pnls), 2),
        'avg_pnl': round(sum(all_pnls) / len(all_pnls), 2),
        'total_trades_all': sum(w.get('fill_count', 0) for w in candidates),
        'avg_win_rate': round(
            sum(w.get('win_rate', 0) for w in candidates)
            / max(1, len(candidates)), 1
        ),
    },
    'selection_tiers': {
        'tier_1_strong_consider': {
            'criteria': 'PnL > $500, win_rate > 55%, trades > 50',
            'count': len(tier1),
            'wallets': tier1,
        },
        'tier_2_moderate': {
            'criteria': 'PnL $0-500, win_rate > 50%, trades > 20',
            'count': len(tier2),
            'wallets': tier2,
        },
        'tier_3_speculative': {
            'criteria': 'PnL > $0 but low win_rate or low trade count',
            'count': len(tier3),
            'wallets': tier3,
        },
    },
    'top_50_by_pnl': top50_summary,
    'bottom_20_by_pnl': bottom20_summary,
    'live_copy_config_overlap': {
        'note': 'Only 2 of 10 live-config wallets found in finder data. 8 are placeholder addresses.',
        'found_in_finder': [
            {'address': '0xd5e17c7015535d9d8e970a4d2c031117d3953834', 'live_status': 'OFF', 'finder_pnl': 167.33, 'finder_win_rate': 'varies'},
            {'address': user_addr, 'live_status': 'OFF', 'finder_role': 'user_aggregate', 'finder_copy_equity': 12766.90},
        ],
        'not_in_finder_placeholders': [
            '0x1111111111111111111111111111111111111111',
            '0x2222222222222222222222222222222222222222',
            '0x3333333333333333333333333333333333333333',
            '0x4444444444444444444444444444444444444444',
            '0x5555555555555555555555555555555555555555',
            '0x6666666666666666666666666666666666666666',
            '0x7777777777777777777777777777777777777777',
            '0x8888888888888888888888888888888888888888',
        ],
    },
}

out_path = r'C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\hl_copy_output\candidate_wallet_selection_pack.json'
with open(out_path, 'w') as f:
    json.dump(pack, f, indent=2)

print(f'\nPack written: {out_path}')
print(f'Pack size: {os.path.getsize(out_path):,} bytes')
print(f'\nTier 1 (strong consider): {len(tier1)} wallets')
print(f'Tier 2 (moderate): {len(tier2)} wallets')
print(f'Tier 3 (speculative): {len(tier3)} wallets')

print(f'\n=== TOP 10 BY COPY PNL ===')
for i, w in enumerate(top50_summary[:10]):
    addr = w['wallet'][:12] + '...'
    pnl = w['copy_total_pnl']
    wr = w['win_rate']
    tc = w['fill_count']
    hrs = w['active_hours']
    lev = w['required_leverage']
    print(f'{i+1:>2}. {addr:<16} PnL=${pnl:>10,.2f}  WR={wr:>5.1f}%  Fills={tc:>6}  Hrs={hrs:>8.1f}  Lev={lev:.0f}x')

print(f'\n=== PORTFOLIO SUMMARY ===')
p = state.get('portfolio', {})
pc = p.get('copy', {})
pl = p.get('lead', {})
print(f'  Copy equity: ${pc.get("equity",0):,.2f}')
print(f'  Copy peak: ${pc.get("peak_equity",0):,.2f}')
print(f'  Copy max dd: ${pc.get("max_drawdown",0):,.2f}')
print(f'  Win rate: {p.get("win_rate",0):.1f}%')
print(f'  Avg trade pct: {p.get("avg_trade_pct",0):.4f}%')
print(f'  Max required leverage: {p.get("max_required_leverage",0):.1f}x')
print(f'  Max open notional: ${p.get("max_open_notional_usd",0):,.2f}')
